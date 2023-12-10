from h5py import Dataset
from datetime import datetime, timedelta
import numpy as np
import pytz

from config import (
    TIME_DIFF_THRESHOLD,
    CHUNK_SIZE,
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
        space_samples (int): Number of space samples.
        time_samples (int): Number of time samples.
        unit_size (int): Size of each unit.
    """

    def __init__(self):
        self.space_samples = 0
        self.time_samples = 0
        self.unit_size = 0

        self.file_manager = FileManager()

    def concat_to_chunk_by_time(
        self,
        h5_file: H5File,
        start_chunk_time: float,
    ) -> Tuple[float, int]:
        """Appends chunk with new data.
        If necessary, creates new chunk next to previous or on the next day

        Args:
            h5_file (H5File): Object of the file to be appended
            start_chunk_time (float): Timestamp of the chunk to append to

        Returns:
            Tuple[float, int]: Updated starting chunk time and processed time
        """

        def concat_h5(dset_concat_from: Dataset) -> Dataset:
            """Resizes and resizes h5py Dataset according to another h5py Dataset

            Args:
                dset_concat_from (Dataset): Donor Dataset
                dset_concat_to (Dataset): Dataset to be resized and appended to

            Returns:
                Dataset: Resized and appended dataset
            """
            if dset_concat_from.shape[0] == 0:
                return None
            # print(dset_concat_from.shape, dset_concat_to.shape)
            try:
                # # Appending transposed data to the end of the Dataset
                # # (Mekorot and Natgas datasets are transposed)
                self.file_manager.h5_dset[
                    :,
                    self.file_manager.start_offset : self.file_manager.start_offset
                    + dset_concat_from.shape[0],
                ] = np.transpose(dset_concat_from[()])

                self.file_manager.start_offset += dset_concat_from.shape[0]

                if self.file_manager.start_offset >= self.file_manager.h5_dset.shape[1]:
                    raise IndexError("Matrix offset out of bounds")
                log.debug(f"Offset: {self.file_manager.start_offset}")
                log.info("Concat successful")
                log.debug(f"Writing to file {self.file_manager.h5_file.filename}")
            except Exception as err:
                log.exception(f"Critical error while saving last chunk: {err}")

        chunk_time = start_chunk_time
        # Get chunk's Dataset according to provided timestamp
        if self.file_manager.h5_file is None:
            self.file_manager.require_h5(chunk_time, self.space_samples)

        log.debug(f"Concatenating {h5_file.file_name}")
        # If Dataset was splitted, we would take only splitted part of the packet
        if h5_file.dset_split is not None:
            concat_h5(dset_concat_from=h5_file.dset_split)
        else:
            concat_h5(dset_concat_from=h5_file.dset)

        # Flip next day/chunk
        if h5_file.is_day_end or h5_file.is_chunk_end:
            log.debug("Flipping to next day/chunk")
            self.file_manager.close_h5()
            self.file_manager.reset_h5_offset()
            # Get timestamp for next chunk
            chunk_time = h5_file.packet_time + h5_file.split_time
            log.debug(f"Next chunk time: {chunk_time}")
            # Get newly generated chunk's Dataset
            self.file_manager.require_h5(chunk_time, space_samples=self.space_samples)
            # If packet has carry to be appended to new chunk
            if h5_file.dset_carry is not None:
                log.info(f"Carry with shape {h5_file.dset_carry.shape} has been used")
                concat_h5(dset_concat_from=h5_file.dset_carry)
            if h5_file.is_day_end:
                next_day_datetime = h5_file.packet_datetime + timedelta(days=1)
                next_day_str = next_day_datetime.strftime("%Y%m%d")
                # Save status data to next day for processing
                self.file_manager.save_status(
                    filedir_r=next_day_str,
                    last_filename=h5_file.file_name,
                    last_filedir_r=next_day_str,
                    start_chunk_time=chunk_time,
                )
                log.debug(f"Next day status saved to {next_day_str}")
            return chunk_time

        return chunk_time

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
            h5_file = H5File(
                file_dir=working_dir_r,
                file_name=major_filename,
                space_samples=self.space_samples,
                time_samples=self.time_samples,
            )
            is_major, h5_unpack_error = h5_file.check_h5(last_timestamp=last_timestamp)
        else:
            # If no major files left
            is_major = False

        if is_major is False:
            # Checking next minor list file
            if len(h5_minor_list) > 0:
                minor_filename = h5_minor_list[-1]
                h5_file = H5File(
                    file_dir=working_dir_r,
                    file_name=minor_filename,
                    space_samples=self.space_samples,
                    time_samples=self.time_samples,
                )
                is_minor, h5_unpack_error = h5_file.check_h5(
                    last_timestamp=last_timestamp
                )

                # We tested both major and minor files. Both corrupted in some way
                if is_minor is False:
                    log.critical(
                        f"Data has gap in {working_dir_r}."
                        f"Last timestamp: {last_timestamp}"
                    )
                    return (None, False, "gap")

        log.debug(f"Using {'major' if is_major else 'minor'}")
        log.debug(f"Using {h5_file.file_name}")
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
        # read attrs.json from current directory
        attrs = self.file_manager.read_attrs(working_dir_r)

        self.space_samples = int(
            np.ceil((attrs["index"][1] + 1) / attrs["down_factor_space"])
        )
        self.time_samples = int(
            np.ceil((attrs["index"][3] + 1) / attrs["down_factor_time"])
        )
        self.unit_size = int(attrs["unit_size"])

        # Getting all necessary environmental vars from status file (if exists)
        #  otherwise return default values
        (
            h5_files_list,
            start_chunk_time,
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
            start_day_datetime = h5_file.packet_datetime.replace(
                hour=0, minute=0, second=0
            )
            next_day_datetime = start_day_datetime + timedelta(days=1)

            till_midnight = int(
                np.round(
                    next_day_datetime.timestamp() - h5_file.packet_datetime.timestamp(),
                    0,
                )
            )
            till_next_chunk = int(
                np.round(
                    start_chunk_time + CHUNK_SIZE - h5_file.packet_datetime.timestamp(),
                    0,
                )
            )

            # If time to split chunk to next day
            if till_midnight < self.unit_size:
                log.info(f"Splitting to next day: time till midnight {till_midnight}")
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
                log.info(
                    f"Splitting to next chunk: time till next chunk {till_next_chunk}"
                )
                split_offset = int(SPS * till_next_chunk)

                h5_file.dset_split = h5_file.dset[:split_offset, :]
                # Creating carry to use in the next chunk
                h5_file.dset_carry = h5_file.dset[split_offset:, :]

                h5_file.is_chunk_end = True
                h5_file.split_time = till_next_chunk
            # Regular splitting within chunk otherwise
            else:
                packet_diff = int(np.round(h5_file.packet_time - last_timestamp, 0))
                if packet_diff < TIME_DIFF_THRESHOLD:
                    log.info(f"Splitting to next packet: packet diff {packet_diff}")
                    h5_file.dset_split = h5_file.dset[SPS * packet_diff :, :]
                    h5_file.split_time = packet_diff
                else:
                    log.debug("No splitting")
                    h5_file.split_time = self.unit_size

            # Main concatenation entry point
            start_chunk_time = self.concat_to_chunk_by_time(
                h5_file=h5_file,
                start_chunk_time=start_chunk_time,
            )

            last_timestamp = h5_file.packet_time
            # Save updated status data
            self.file_manager.save_status(
                filedir_r=working_dir_r,
                last_filename=h5_file.file_name,
                last_filedir_r=h5_file.file_dir,
                start_chunk_time=start_chunk_time,
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

            del h5_file  # Delete H5File object to free memory
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
                    self.file_manager.reset_chunks(working_dir)
            self.file_manager.close_h5()
            self.file_manager.reset_h5_offset()
            self.file_manager.set_completed(working_dir)
        end_time = datetime.now(tz=pytz.UTC)
        print("Code finished in:", end_time - start_time)
