import os

# import deal
from h5py import File, Dataset
from datetime import datetime, timedelta
import numpy as np
import pytz

from config import (
    PATH,
    SAVE_PATH,
    TIME_DIFF_THRESHOLD,
    CHUNK_SIZE,
    UNIT_SIZE,
    SPACE_SAMPLES,
    SPS,
)
from concat.hdf import H5_FILE
from log.logger import set_file_logger, compose_log_message, set_logger
from concat.status import (
    get_dirs,
    get_queue,
    save_status,
    reset_chunks,
)


def require_h5(chunk_time: float) -> Dataset | None:
    """Creates h5 file (if necessary)

    Args:
        chunk_time (float): Time of the chunk

    Returns:
        h5py.Dataset: Returns dataset of the created h5 file
    """

    save_date_dt = datetime.fromtimestamp(chunk_time, tz=pytz.UTC)
    save_date = datetime.strftime(save_date_dt, "%Y%m%d")
    filename = save_date + "_" + str(chunk_time) + ".h5"
    file = File(os.path.join(SAVE_PATH, filename), "a")
    dset = file.require_dataset(
        "data_down",
        (0, SPACE_SAMPLES),
        maxshape=(None, SPACE_SAMPLES),
        chunks=True,
        dtype=np.float32,
    )
    log.debug(f"Provided chunk time: {chunk_time}. File: {filename} provided")
    return dset


# @deal.post(lambda x: x >= 0)
# @deal.pre(lambda x: x >= 0)
def calculate_chunk_offset(processed_units: int):
    return (processed_units // CHUNK_SIZE) * CHUNK_SIZE


def concat_to_chunk_by_time(
    h5_file: H5_FILE,
    processed_time: int,
    start_chunk_time: float,
    saving_dir: str,
):
    # @deal.post(lambda x: type(x) is Dataset)
    def concat_h5(dset_concat_from: Dataset, dset_concat_to: Dataset):
        try:
            dset_concat_to.resize(
                dset_concat_to.shape[0] + dset_concat_from.shape[0], axis=0
            )
            dset_concat_to[-dset_concat_from.shape[0] :] = dset_concat_from
            return dset_concat_to
        except Exception as err:
            # If we have critical error with saving chunk,
            # We may want to preserve last chunk data to investigate
            log.critical(
                compose_log_message(
                    working_dir=saving_dir,
                    file=h5_file.file_name,
                    message="Critical error while saving last chunk,\
preserving last chunk. Error"
                    + str(err),
                )
            )

    chunk_time = start_chunk_time

    dset_concat = require_h5(chunk_time)

    log.debug(
        compose_log_message(
            working_dir=saving_dir,
            file=h5_file.file_name,
            message=f"Concatenating {h5_file.file_name}",
        )
    )

    if h5_file.dset_split is not None:
        dset_concat = concat_h5(
            dset_concat_from=h5_file.dset_split, dset_concat_to=dset_concat
        )
    else:
        dset_concat = concat_h5(
            dset_concat_from=h5_file.dset, dset_concat_to=dset_concat
        )

    processed_time += int(h5_file.split_time)

    log.debug(
        compose_log_message(
            working_dir=saving_dir,
            file=h5_file.file_name,
            message=f"Concat has shape:  {dset_concat.shape}",
        )
    )
    # Flip next day
    if h5_file.is_day_end:
        start_chunk_time = h5_file.packet_time + h5_file.split_time
        dset_concat = require_h5(start_chunk_time)
        if h5_file.dset_carry is not None:
            log.info(
                compose_log_message(
                    working_dir=saving_dir,
                    file=h5_file.file_name,
                    message="Carry has been created and used in the next day chunk",
                )
            )
        dset_concat = concat_h5(
            dset_concat_from=h5_file.dset_carry, dset_concat_to=dset_concat
        )
        log.debug(
            compose_log_message(
                working_dir=saving_dir,
                file=h5_file.file_name,
                message=f"Next day Concat has shape:  {dset_concat.shape}",
            )
        )
        processed_time = UNIT_SIZE - h5_file.split_time

        save_status(
            filedir_r=(h5_file.packet_datetime + timedelta(days=1)).strftime("%Y%m%d"),
            last_filename=h5_file.file_name,
            last_filedir_r=(h5_file.packet_datetime + timedelta(days=1)).strftime(
                "%Y%m%d"
            ),
            start_chunk_time=start_chunk_time,
            processed_time=processed_time,
        )
        return chunk_time, processed_time
    # Flip to next chunk
    if processed_time % CHUNK_SIZE == 0:
        # Recalculate new chunk time
        chunk_time = h5_file.packet_time + h5_file.split_time
        # Create new h5 chunk file
        dset_concat = require_h5(chunk_time)
        if h5_file.dset_carry is not None:
            log.info(
                compose_log_message(
                    working_dir=saving_dir,
                    file=h5_file.file_name,
                    message="Carry has been created and used in the next chunk",
                )
            )
            dset_concat = concat_h5(
                dset_concat_from=h5_file.dset_carry, dset_concat_to=dset_concat
            )
            processed_time = UNIT_SIZE - h5_file.split_time
        return chunk_time, processed_time
    return chunk_time, processed_time


def get_next_h5(
    h5_major_list: list, h5_minor_list: list, working_dir_r: str, last_timestamp: float
):
    if len(h5_major_list) > 0:
        major_filename = h5_major_list[-1]
        h5_file = H5_FILE(file_dir=working_dir_r, file_name=major_filename)
        is_major, h5_unpack_error = h5_file.check_h5(last_timestamp=last_timestamp)
    else:
        is_major = False

    if is_major is False:
        minor_filename = h5_minor_list[-1]

        h5_file = H5_FILE(file_dir=working_dir_r, file_name=minor_filename)
        is_minor, h5_unpack_error = h5_file.check_h5(last_timestamp=last_timestamp)

        # We tested both major and minor files. Both corrupted in some way
        if is_minor is False:
            log.critical(
                compose_log_message(
                    working_dir=working_dir_r,
                    message="Data has a gap. Closing concat chunk",
                )
            )
            return (None, False, "gap")

    log.info(
        compose_log_message(
            working_dir=working_dir_r,
            file=h5_file.file_name,
            message=f"Using {'major' if is_major else 'minor'}",
        )
    )
    return (h5_file, is_major, h5_unpack_error)


def concat_files(
    curr_dir: str,
) -> tuple[bool, Exception | None]:
    # TODO: add annotation for function

    def files_split(files: list):
        files_major: list[str] = files[::2][::-1]
        files_minor: list[str] = files[1::2][::-1]
        return files_major, files_minor

    working_dir_r = curr_dir
    # Staring from the last saved
    (
        h5_files_list,
        start_chunk_time,
        processed_time,
        last_timestamp,
    ) = get_queue(filepath_r=working_dir_r)

    last_is_major = None

    h5_major_list, h5_minor_list = files_split(files=h5_files_list)

    while len(h5_major_list) > 0 or len(h5_minor_list) > 0:
        h5_file, is_major, h5_unpack_error = get_next_h5(
            h5_major_list=h5_major_list,
            h5_minor_list=h5_minor_list,
            working_dir_r=working_dir_r,
            last_timestamp=last_timestamp,
        )
        if h5_file is None:
            return False
        next_date = h5_file.packet_datetime.replace(
            hour=0, minute=0, second=0
        ) + timedelta(days=1)
        till_midnight = (next_date - h5_file.packet_datetime).seconds

        till_next_chunk = CHUNK_SIZE - ((CHUNK_SIZE + processed_time) % CHUNK_SIZE)
        if till_midnight < UNIT_SIZE:
            split_offset = int(SPS * till_midnight)

            h5_file.dset_split = h5_file.dset[:split_offset, :]
            # Creating carry to use in the next chunk
            h5_file.dset_carry = h5_file.dset[split_offset:, :]

            h5_file.is_day_end = True
            h5_file.split_time = till_midnight

        # if end of the chunk is half of the packet
        # and is major or minor after major was skipped:
        # Split and take first half of the packet
        elif (
            is_major or h5_unpack_error == "missing"
        ) and till_next_chunk < TIME_DIFF_THRESHOLD:
            split_offset = int(SPS * till_next_chunk)

            h5_file.dset_split = h5_file.dset[:split_offset, :]
            # Creating carry to use in the next chunk
            h5_file.dset_carry = h5_file.dset[split_offset:, :]

            h5_file.is_chunk_end = True
            h5_file.split_time = till_next_chunk
        else:
            packet_diff = int(h5_file.packet_time - last_timestamp)
            if packet_diff < TIME_DIFF_THRESHOLD:
                h5_file.dset_split = h5_file.dset[SPS * packet_diff :, :]
                h5_file.split_time = packet_diff
            else:
                h5_file.split_time = UNIT_SIZE

        # Concatenation if OK
        start_chunk_time, processed_time = concat_to_chunk_by_time(
            h5_file=h5_file,
            processed_time=processed_time,
            start_chunk_time=start_chunk_time,
            saving_dir=working_dir_r,
        )

        # Cleaning the queue
        if is_major:
            if last_is_major is True:
                h5_minor_list.pop()
            h5_major_list.pop()
        elif is_major is False:
            if last_is_major is False:
                h5_major_list.pop()
            h5_minor_list.pop()

        last_timestamp = h5_file.packet_time
        last_is_major = is_major

        # Save last processed
        save_status(
            filedir_r=working_dir_r,
            last_filename=h5_file.file_name,
            last_filedir_r=h5_file.file_dir,
            start_chunk_time=start_chunk_time,
            processed_time=processed_time,
        )

        if h5_file.is_day_end:
            return True

    return True


def main():
    global log
    log = set_logger("CONCAT", global_concat_log=True, global_log_level="DEBUG")
    start_time = datetime.now()

    # delete_dirs()
    dirs = get_dirs(filedir_r=PATH)
    for working_dir in dirs:
        # Local logger
        set_file_logger(
            log=log, log_level="DEBUG", log_file=os.path.join(PATH, working_dir, "log")
        )
        proc_status = False
        while proc_status is not True:
            proc_status = concat_files(curr_dir=working_dir)
            if proc_status:
                log.info(
                    compose_log_message(
                        working_dir=working_dir,
                        message="Saving finished with success",
                    )
                )
            else:
                log.critical(
                    compose_log_message(
                        working_dir=working_dir,
                        message="Concatenation was finished prematurely",
                    )
                )
                # Remove start_chunk_time and total_unit_size
                # to continue processing from new chunk upon error
                reset_chunks(os.path.join(PATH, working_dir))

    end_time = datetime.now()
    print("Code finished in:", end_time - start_time)


if __name__ == "__main__":
    # deal.disable()
    # set_console_logger(log=log, log_level="DEBUG")
    main()
