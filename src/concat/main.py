import os

# import deal
from h5py import Dataset
from datetime import datetime, timedelta
import pytz

from config import (
    TIME_DIFF_THRESHOLD,
    CHUNK_SIZE,
    UNIT_SIZE,
    SPS,
)
from concat.hdf import H5File

from log.main_logger import logger as log
from concat.status import FileManager
from typing import Union, Tuple


class Concatenator:
    """Class responsible for concatenating H5 files into chunks.

    Attributes:
        file_manager (FileManager): Object responsible for managing H5 files.
    """

    def __init__(self):
        self.file_manager = FileManager()

    def concat_to_chunk_by_time(
        self,
        h5_file: H5File,
        processed_time: int,
        start_chunk_time: float,
    ) -> Tuple[float, int]:
        """Appends chunk with new data.
        If necessary, creates new chunk next to previous or on the next day

        Args:
            h5_file (H5File): Object of the file to be appended
            processed_time (int): Processed time since start of the chunk
            start_chunk_time (float): Timestamp of the chunk to append to

        Returns:
            tuple[chunk_time, processed_time]: updated starting
                chunk time and processed time
        """

        def concat_h5(dset_concat_from: Dataset, dset_concat_to: Dataset) -> Dataset:
            """Resizes and resizes h5py Dataset according to another h5py Dataset

            Args:
                dset_concat_from (Dataset): Donor Dataset
                dset_concat_to (Dataset): Dataset to be resized and appended to

            Returns:
                Dataset: Resized and appended dataset
            """
            try:
                dset_concat_to.resize(
                    dset_concat_to.shape[0] + dset_concat_from.shape[0], axis=0
                )
                dset_concat_to[-dset_concat_from.shape[0] :] = dset_concat_from
                return dset_concat_to
            except Exception as err:
                log.exception(f"Critical error while saving last chunk: {err}")

        chunk_time = start_chunk_time
        # Get chunk's Dataset according to provided timestamp
        dset_concat = self.file_manager.require_h5(chunk_time)

        log.debug(f"Concatenating {h5_file.file_name}")

        # If Dataset was splitted, we would take only splitted part of the packet
        if h5_file.dset_split is not None:
            dset_concat = concat_h5(
                dset_concat_from=h5_file.dset_split, dset_concat_to=dset_concat
            )
        else:
            dset_concat = concat_h5(
                dset_concat_from=h5_file.dset, dset_concat_to=dset_concat
            )
        log.debug(f"Concat has shape: {dset_concat.shape}")
        # Update processed time
        processed_time += int(h5_file.split_time)

        # Flip next day/chunk
        if h5_file.is_day_end or processed_time % CHUNK_SIZE == 0:
            # Get timestamp for next chunk
            chunk_time = h5_file.packet_time + h5_file.split_time
            # Get newly generated chunk's Dataset
            dset_concat = self.file_manager.require_h5(chunk_time)
            # If packet has carry to be appended to new chunk
            if h5_file.dset_carry is not None:
                log.info(f"Carry with shape {h5_file.dset_carry.shape} has been used")
                dset_concat = concat_h5(
                    dset_concat_from=h5_file.dset_carry, dset_concat_to=dset_concat
                )
                log.debug(f"Next chunk concat shape: {dset_concat.shape}.")
                processed_time = UNIT_SIZE - h5_file.split_time
            if h5_file.is_day_end:
                # Save status data to next day for processing
                self.file_manager.save_status(
                    filedir_r=(h5_file.packet_datetime + timedelta(days=1)).strftime(
                        "%Y%m%d"
                    ),
                    last_filename=h5_file.file_name,
                    last_filedir_r=(
                        h5_file.packet_datetime + timedelta(days=1)
                    ).strftime("%Y%m%d"),
                    start_chunk_time=chunk_time,
                    processed_time=processed_time,
                )
            return chunk_time, processed_time

        return chunk_time, processed_time

    def get_next_h5(
        self,
        h5_major_list: list,
        h5_minor_list: list,
        working_dir_r: str,
        last_timestamp: float,
    ) -> Tuple[Union[H5File, None], Union[bool, None], Union[str, None]]:
        """Returns next packet for processing

        Args:
            h5_major_list (list): List of the odd packets in directory
            h5_minor_list (list): List of the even packets in directory
            working_dir_r (str): Currently processing directory
            last_timestamp (float): Timestamp of the last processed packet

        Returns:
            tuple[H5_File| None, bool | None, None | Error]: returns H5_File
            is_major bool if all checks passed successfully
            otherwise returns Error and Nones for ofter values
        """
        if len(h5_major_list) > 0:
            # Checking next major list file
            major_filename = h5_major_list[-1]
            h5_file = H5File(file_dir=working_dir_r, file_name=major_filename)
            is_major, h5_unpack_error = h5_file.check_h5(last_timestamp=last_timestamp)
        else:
            # If no major files left
            is_major = False

        if is_major is False:
            # Checking next minor list file
            minor_filename = h5_minor_list[-1]
            h5_file = H5File(file_dir=working_dir_r, file_name=minor_filename)
            is_minor, h5_unpack_error = h5_file.check_h5(last_timestamp=last_timestamp)

            # We tested both major and minor files. Both corrupted in some way
            if is_minor is False:
                log.critical(
                    f"Data has gap in {working_dir_r}. Last timestamp: {last_timestamp}"
                )
                return (None, False, "gap")

        log.debug(f"Using {'major' if is_major else 'minor'}")
        return (h5_file, is_major, h5_unpack_error)

    def concat_files(self, curr_dir: str) -> Tuple[bool, Union[Exception, None]]:
        """Main entry point to packet concatenation

        Args:
            curr_dir (str): Currently processed directory

        Returns:
            tuple[bool, Exception | None]: Returns bool status
                If error, also returns error attrs.
        """

        def files_split(files: list) -> Tuple[list, list]:
            """Splits list in major/minor (odd/even) lists of filenames

            Args:
                files (list): Input list

            Returns:
                tuple[list, list]: odd list, even list of filenames
            """
            files_major: list[str] = files[::2][::-1]
            files_minor: list[str] = files[1::2][::-1]
            return files_major, files_minor

        working_dir_r = curr_dir
        # Getting all necessary environmental vars from status file (if exists)
        #  otherwise return default values
        (
            h5_files_list,
            start_chunk_time,
            processed_time,
            last_timestamp,
        ) = self.file_manager.get_queue(filepath_r=working_dir_r)

        last_is_major = None

        h5_major_list, h5_minor_list = files_split(files=h5_files_list)
        # Main concatenation loop
        # Processing continues until no filenames left in both lists
        while len(h5_major_list) > 0 or len(h5_minor_list) > 0:
            h5_file, is_major, h5_unpack_error = self.get_next_h5(
                h5_major_list=h5_major_list,
                h5_minor_list=h5_minor_list,
                working_dir_r=working_dir_r,
                last_timestamp=last_timestamp,
            )
            # Exit if error with file (reading error, gap in data, etc)
            if h5_file is None:
                return False

            # Calculates time till next day (for day splitting)
            next_date = h5_file.packet_datetime.replace(
                hour=0, minute=0, second=0
            ) + timedelta(days=1)
            till_midnight = (next_date - h5_file.packet_datetime).seconds

            till_next_chunk = CHUNK_SIZE - ((CHUNK_SIZE + processed_time) % CHUNK_SIZE)

            # If time to split chunk to next day
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
            # Regular splitting within chunk otherwise
            else:
                packet_diff = int(h5_file.packet_time - last_timestamp)
                if packet_diff < TIME_DIFF_THRESHOLD:
                    h5_file.dset_split = h5_file.dset[SPS * packet_diff :, :]
                    h5_file.split_time = packet_diff
                else:
                    h5_file.split_time = UNIT_SIZE

            # Main concatenation entry point
            start_chunk_time, processed_time = self.concat_to_chunk_by_time(
                h5_file=h5_file,
                processed_time=processed_time,
                start_chunk_time=start_chunk_time,
            )

            last_timestamp = h5_file.packet_time
            # Save updated status data
            self.file_manager.save_status(
                filedir_r=working_dir_r,
                last_filename=h5_file.file_name,
                last_filedir_r=h5_file.file_dir,
                start_chunk_time=start_chunk_time,
                processed_time=processed_time,
            )

            # Cleaning the queue
            # If now and previous time was major:
            # We can delete minor as irrelevant and vise versa
            if is_major:
                if last_is_major is True:
                    h5_minor_list.pop()
                h5_major_list.pop()
            elif is_major is False:
                if last_is_major is False:
                    h5_major_list.pop()
                h5_minor_list.pop()

            last_is_major = is_major

            # Prematurely exit main concatenation loop
            # Even if filenames left in major/minor list
            if h5_file.is_day_end:
                return True

        return True

    def run(self):
        start_time = datetime.now(tz=pytz.UTC)
        dirs = self.file_manager.get_sorted_dirs()
        for working_dir in dirs:
            proc_status = False
            while proc_status is not True:
                proc_status = self.concat_files(curr_dir=working_dir)
                if proc_status:
                    log.info(
                        f"Concat of chunks {working_dir} was finished with success"
                    )
                else:
                    log.critical(
                        f"Concat of chunks {working_dir} was finished prematurely"
                    )
                    # Remove start_chunk_time and total_unit_size
                    # to continue processing from new chunk upon error
                    self.file_manager.reset_chunks(os.path.join(self.path, working_dir))
        end_time = datetime.now(tz=pytz.UTC)
        print("Code finished in:", end_time - start_time)
