import os
from h5py import File, Dataset
import datetime
import numpy as np

from config import PATH, SAVE_PATH, TIME_DIFF_THRESHOLD, CONCAT_TIME, UNIT_SIZE
from hdf import H5_FILE
from logger import set_console_logger, set_file_logger, compose_log_message, log
from files import (
    get_dirs,
    get_h5_files,
    get_queue,
    preserve_last_processed,
    reset_chunks,
    track_to_be_deleted,
    save_last,
    delete_processed_files,
    delete_dirs,
)

# from delete import delete_files

# Already correctly downsampled file for reference
referenceFile = File("downsampled_reference.h5")
TimeSamples, SpaceSamples = referenceFile["data_down"].shape

CHUNK_SIZE = int(CONCAT_TIME / (UNIT_SIZE / 2))


def require_h5(working_dir: str, chunk_time: float) -> Dataset:
    """Creates h5 file (if necessary)

    Args:
        working_dir (str): Name of the directory files are located at (for name of the new file)
        chunk_time (float): time of the necessary chunk

    Returns:
        h5py.Dataset: Returns dataset of the created h5 file
    """

    file = File(
        os.path.join(SAVE_PATH, working_dir + "_" + str(chunk_time) + ".h5"), "a"
    )
    dset = file.require_dataset(
        "data_down",
        (0, SpaceSamples),
        maxshape=(None, SpaceSamples),
        chunks=True,
        dtype=np.float32,
    )

    return dset


def calculate_chunk_offset(total_unit_size: int):
    return (total_unit_size // CONCAT_TIME) * CONCAT_TIME


def concat_to_chunk_by_time(
    file: H5_FILE,
    total_unit_size: int,
    start_chunk_time: float,
    saving_dir: str,
    concat_unit_size: int,
    last: bool,
):
    def concat_h5(dset_concat_from: Dataset, dset_concat_to: Dataset):
        try:
            dset_concat_to.resize(
                dset_concat_to.shape[0] + dset_concat_from.shape[0], axis=0
            )
            dset_concat_to[-dset_concat_from.shape[0] :] = dset_concat_from[()]
            return dset_concat_to
        except Exception as err:
            # If we have critical error with saving chunk, we may want to preserve last chunk data to investigate
            log.critical(
                compose_log_message(
                    working_dir=saving_dir,
                    file=file.file_name,
                    message="Critical error while saving last chunk, preserving last chunk. Error"
                    + str(err),
                )
            )
            preserve_last_processed()

    chunk_time = start_chunk_time + calculate_chunk_offset(total_unit_size)

    require_h5(saving_dir, chunk_time)

    file_concat = File(
        os.path.join(SAVE_PATH, saving_dir + "_" + str(chunk_time) + ".h5"), "a"
    )
    dset_concat = file_concat["data_down"]

    log.debug(
        compose_log_message(
            working_dir=saving_dir,
            file=file.file_name,
            message=f"Concatenating {file.file_name}",
        )
    )

    if file.dset_split is not None:
        dset_concat = concat_h5(
            dset_concat_from=file.dset_split, dset_concat_to=dset_concat
        )
    else:
        dset_concat = concat_h5(dset_concat_from=file.dset, dset_concat_to=dset_concat)

    total_unit_size += int(concat_unit_size)

    log.debug(
        compose_log_message(
            working_dir=saving_dir,
            file=file.file_name,
            message=f"Concat has shape:  {dset_concat.shape}",
        )
    )

    # Flip to next chunk
    if total_unit_size % CONCAT_TIME == 0:
        log.info(
            compose_log_message(
                working_dir=saving_dir,
                file=file.file_name,
                message=f"Final shape: {File(os.path.join(SAVE_PATH, saving_dir + '_' + str(chunk_time) + '.h5'))['data_down'].shape}",
            )
        )
        if last is False:
            # Recalculate new chunk time
            chunk_time = start_chunk_time + calculate_chunk_offset(total_unit_size)
            # Create new h5 chunk file
            dset_concat = require_h5(saving_dir, chunk_time)
            if file.dset_carry is not None:
                log.info(
                    compose_log_message(
                        working_dir=saving_dir,
                        file=file.file_name,
                        message="Carry has been created and used in the next chunk",
                    )
                )
                dset_concat = concat_h5(
                    dset_concat_from=file.dset_carry, dset_concat_to=dset_concat
                )
                total_unit_size += int(UNIT_SIZE / 2)
        else:
            return -1
    return total_unit_size


def concat_files(
    curr_dir: str,
) -> tuple[bool, Exception | None]:
    # TODO: add annotation for function

    def files_split(files: list):
        files_major: list[str] = files[::2][::-1]
        files_minor: list[str] = files[1::2][::-1]
        return files_major, files_minor

    path_dir: str = os.path.join(PATH, curr_dir)
    saving_dir = curr_dir
    # Staring from the last saved
    file_names_tbd, start_chunk_time, total_unit_size, last_timestamp = get_queue(
        path_dir=path_dir
    )

    last_major_status = None

    major_file_names_tbd, minor_file_names_tbd = files_split(files=file_names_tbd)
    last = False

    while len(major_file_names_tbd) > 0 or len(minor_file_names_tbd) > 0:
        if len(major_file_names_tbd) > 0:
            major_file_name = major_file_names_tbd[-1]
            file = H5_FILE(file_dir=curr_dir, file_name=major_file_name)
            major, reason = file.check_h5(last_timestamp=last_timestamp)
        else:
            major = False

        if major is False:
            minor_file_name = minor_file_names_tbd[-1]

            file = H5_FILE(file_dir=curr_dir, file_name=minor_file_name)
            minor, reason = file.check_h5(last_timestamp=last_timestamp)

            # We tested both major and minor files. Both corrupted in some way
            if minor is False:
                log.critical(
                    compose_log_message(
                        working_dir=curr_dir,
                        file=None,
                        message="Data has a gap. Closing concat chunk",
                    )
                )
                return False

        log.info(
            compose_log_message(
                working_dir=curr_dir,
                file=file.file_name,
                message=f"Using {'major' if major else 'minor'}",
            )
        )
        # if end of the chunk is half of the packet and is major or minor after major was skipped:
        # Split and take first half of the packet
        if (major or reason == "missing") and CONCAT_TIME - (
            (CONCAT_TIME + total_unit_size) % CONCAT_TIME
        ) < TIME_DIFF_THRESHOLD:
            split_offset = int(file.dset.shape[0] / 2)

            file.dset_split = file.dset[:split_offset, :]
            # Creating carry to use in the next chunk
            file.dset_carry = file.dset[split_offset:, :]

            concat_unit_size = UNIT_SIZE / 2  # 2
        else:
            if file.packet_time - last_timestamp < TIME_DIFF_THRESHOLD:
                file.dset_split = file.dset[int(file.dset.shape[0] / 2) :, :]
                concat_unit_size = UNIT_SIZE / 2  # 2
            else:
                concat_unit_size = UNIT_SIZE  # 4

        # Concatenation if OK
        total_unit_size = concat_to_chunk_by_time(
            file=file,
            total_unit_size=total_unit_size,
            start_chunk_time=start_chunk_time,
            saving_dir=saving_dir,
            concat_unit_size=concat_unit_size,
            last=last,
        )
        if total_unit_size == -1:
            return True
        # Cleaning the queue
        if major:
            track_to_be_deleted(curr_dir, major_file_names_tbd[-1])
            major_file_names_tbd.pop()
            if last_major_status is True:
                track_to_be_deleted(curr_dir, minor_file_names_tbd[-1])
                minor_file_names_tbd.pop()
        elif major is False:
            track_to_be_deleted(curr_dir, minor_file_names_tbd[-1])
            minor_file_names_tbd.pop()
            if last_major_status is False:
                track_to_be_deleted(curr_dir, major_file_names_tbd[-1])
                major_file_names_tbd.pop()

        last_timestamp = file.packet_time
        last_major_status = major

        # Save last processed
        save_last(
            path_dir=path_dir,
            file_name=file.file_name,
            start_chunk_time=start_chunk_time,
            total_unit_size=total_unit_size,
        )

        if (
            len(major_file_names_tbd) == 0
            and len(minor_file_names_tbd) == 0
            and last is False
            and (CONCAT_TIME + total_unit_size) % CONCAT_TIME != 0
        ):
            log.debug("LAST")
            next_dir_ = datetime.datetime.strptime(
                curr_dir, "%Y%m%d"
            ) + datetime.timedelta(days=1)
            curr_dir = datetime.datetime.strftime(next_dir_, "%Y%m%d")
            file_names_tbd = get_h5_files(
                path=os.path.join(PATH, curr_dir),
                limit=int(4 * CHUNK_SIZE / UNIT_SIZE + 1),
            )
            major_file_names_tbd, minor_file_names_tbd = files_split(file_names_tbd)
            last = True

    if (CONCAT_TIME + total_unit_size) % CONCAT_TIME != 0:
        return False
    return True


def main():
    # Global logger
    set_file_logger(
        log=log, log_level="WARNING", log_file=os.path.join(SAVE_PATH, "log")
    )

    set_console_logger(log=log, log_level="DEBUG")
    dirs = get_dirs(path=PATH)
    for working_dir in dirs:
        # Local logger
        set_file_logger(
            log=log, log_level="DEBUG", log_file=os.path.join(PATH, working_dir, "log")
        )
        status = False
        while status is not True:
            status = concat_files(curr_dir=working_dir)
            if status:
                log.info(
                    compose_log_message(
                        working_dir=working_dir,
                        file=None,
                        message="Saving finished with success",
                    )
                )
                delete_processed_files()
            else:
                log.critical(
                    compose_log_message(
                        working_dir=working_dir,
                        file=None,
                        message="Concatenation was not finished due to error",
                    )
                )
                # Remove start_chunk_time and total_unit_size
                # to continue processing from new chunk upon error
                reset_chunks(os.path.join(PATH + working_dir))

    delete_dirs()


if __name__ == "__main__":
    start_time = datetime.datetime.now()
    main()
    end_time = datetime.datetime.now()
    print("Code finished in:", end_time - start_time)
