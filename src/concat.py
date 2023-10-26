import h5py
import os

from hdf2mseed import HDF2MSEED
from hdf import H5_FILE
from config import PATH, SAVE_PATH, TIME_DIFF_THRESHOLD

from logger import log, set_file_logger, set_console_logger


def get_dirs(path: str) -> list:
    dirs = [dir for dir in os.listdir(PATH) if os.path.isdir(path + dir)]
    return dirs


def get_h5_files(path: str) -> list:
    """Returns h5 files for processing

    Args:
        path (str): Path to directory with h5 files

    Returns:
        list: List of h5 files in directory
    """
    file_names = sorted([name for name in os.listdir(path) if name.endswith(".h5")])
    return file_names


def concat_files(curr_dir: str) -> tuple[bool, Exception | None]:
    """Concatenates h5 files to one file

    Args:
        file_names (list): h5 files for concatenation

    Returns:
        bool: Returns Status
    """
    m2mseed = HDF2MSEED(stations=1667, SPS=100, outpath=SAVE_PATH)
    path_dir = PATH + curr_dir + "/"
    file_names = get_h5_files(path_dir)

    # Staring from the last saved if file .last exists in the {date} directory
    if os.path.isfile(path_dir + ".last"):
        last_file = open(path_dir + ".last", "r").read()
        file_names_tbd = file_names[file_names.index(last_file) + 1 :].copy()
        last_timestamp = h5py.File(path_dir + last_file).attrs["packet_time"]
    else:
        file_names_tbd = file_names.copy()
        last_timestamp = 0

    last_major_status = None

    # Creating major and minor lists in reverse order (FIFO)
    # Major and Minor lists has step of full {unit_size} seconds, so if it is possible
    # script will use only major list to reduce computations
    major_file_names_tbd = file_names_tbd[::2][::-1]
    minor_file_names_tbd = file_names_tbd[1::2][::-1]

    while len(major_file_names_tbd) > 0 or len(minor_file_names_tbd) > 0:
        if len(major_file_names_tbd) > 0:
            # Trying to use file from major list
            major_file_name = major_file_names_tbd[-1]

            msg = f"{curr_dir} | Trying to use major file: {major_file_name}"
            log.info(msg)

            file = H5_FILE(file_dir=curr_dir, file_name=major_file_name)
            major_status = file.check_h5(last_timestamp=last_timestamp)
        else:
            # Major list is over, so we have to use minor
            major_status = False

        if major_status is False:
            # Trying to use file from minor list
            minor_file_name = minor_file_names_tbd[-1]

            msg = f"{curr_dir} | Trying to use minor file: {minor_file_name}"
            log.info(msg)

            file = H5_FILE(file_dir=curr_dir, file_name=minor_file_name)
            minor_status = file.check_h5(last_timestamp=last_timestamp)

        if major_status is False and minor_status is False:
            # If both minor and major files failed checks,
            # we cannot proceed with concat, so raise exception
            raise Exception("DATA IS CORRUPTED IN THE UNRECOVERABLE WAY")

        # Can be used to tweak time if mseed goes crazy
        # epsilon = 0.1
        # if abs(file.attrs.packet_time - last_timestamp - 2) < epsilon:
        #     print('used')
        #     file.attrs.packet_time = last_timestamp + 2

        # If we getting packets at rate less than TIME_THRESHOLD, take whole packet
        # else take only second half

        # Concatenation if OK
        if file.attrs.packet_time - last_timestamp > TIME_DIFF_THRESHOLD:
            # In mseed we used matrix in shape ({unit_size}/2, {channels}),
            # so we have to feed only half of the data at the time to avoid
            # creation additional traces in mseed
            m = file.dset[: int(file.dset.shape[0] / 2), :]
            m2mseed.save_matrix(m, file.attrs.packet_time, curr_dir)
            m = file.dset[int(file.dset.shape[0] / 2) :, :]
            m2mseed.save_matrix(m, file.attrs.packet_time + 2, curr_dir)

        else:
            # If we using minor after major or vise versa, we have to
            # avoid overlaps, so if time diff smaller than {TIME_DIFF_THRESHOLD},
            # we use only second half of the data
            m = file.dset[int(file.dset.shape[0] / 2) :, :]
            m2mseed.save_matrix(m, file.attrs.packet_time + 2, curr_dir)

        # Cleaning the queue
        if major_status:
            major_file_names_tbd.pop()
            if last_major_status is True:
                # If we had two major elements in the row,
                # previous minor element has to be deleted
                minor_file_names_tbd.pop()
        if major_status is False:
            minor_file_names_tbd.pop()
            if last_major_status is False:
                # If we had two minor elements in the row,
                # previous major element has to be deleted
                major_file_names_tbd.pop()

        last_timestamp = file.attrs.packet_time
        last_major_status = major_status

        with open(path_dir + ".last", "w") as status_file:
            status_file.write(file.file_name)
    return True


def main():
    # Global logger
    set_file_logger(log=log, log_level="WARNING", log_file=SAVE_PATH + "log")

    set_console_logger(log=log, log_level="INFO")
    dirs = get_dirs(path=PATH)
    for dir in dirs:
        # Local logger
        set_file_logger(log=log, log_level="INFO", log_file=PATH + dir + "/log")
        status = concat_files(curr_dir=dir)
        if status:
            log.info(f"{dir} | Saving finished with success")

        else:
            log.critical(f"{dir} | Concatenation was not finished due to error")


if __name__ == "__main__":
    main()
