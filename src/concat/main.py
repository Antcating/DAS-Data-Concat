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

        def concat_h5(dset_concat_from: Dataset, dset_concat_to: Dataset) -> Dataset:
            """Resizes and resizes h5py Dataset according to another h5py Dataset

            Args:
                dset_concat_from (Dataset): Donor Dataset
                dset_concat_to (Dataset): Dataset to be resized and appended to

            Returns:
                Dataset: Resized and appended dataset
            """
            if dset_concat_from.shape[0] == 0:
                return dset_concat_to
            # print(dset_concat_from.shape, dset_concat_to.shape)
            try:
                dset_concat_to.resize(
                    dset_concat_to.shape[1] + dset_concat_from.shape[0], axis=1
                )
                # Appending transposed data to the end of the Dataset
                # (Mekorot and Natgas datasets are transposed)
                dset_concat_to[:, -dset_concat_from.shape[0] :] = np.transpose(
                    dset_concat_from[()]
                )
                log.info(
                    f"Concat successful, resulting dataset: {dset_concat_to.shape}"
                )
                return dset_concat_to
            except Exception as err:
                log.exception(f"Critical error while saving last chunk: {err}")

        chunk_time = start_chunk_time
        # Get chunk's Dataset according to provided timestamp
        dset_concat: Dataset = self.file_manager.require_h5(
            chunk_time, space_samples=self.space_samples
        )

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
        if dset_concat.shape[1] == SPS * CHUNK_SIZE:
            log.debug(f"Chunk is full: {dset_concat.shape}")
            h5_file.is_chunk_end = True
        # Flip next day/chunk
        if h5_file.is_day_end or h5_file.is_chunk_end:
            # Get timestamp for next chunk
            chunk_time = h5_file.packet_time + h5_file.split_time
            log.debug(f"Next chunk time: {chunk_time}")
            # Get newly generated chunk's Dataset
            dset_concat = self.file_manager.require_h5(
                chunk_time, space_samples=self.space_samples
            )
            # If packet has carry to be appended to new chunk
            if h5_file.dset_carry is not None:
                log.info(f"Carry with shape {h5_file.dset_carry.shape} has been used")
                dset_concat = concat_h5(
                    dset_concat_from=h5_file.dset_carry, dset_concat_to=dset_concat
                )
                log.debug(f"New chunk concat shape: {dset_concat.shape}.")
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

    def concat_files(self, curr_dir: str) -> Tuple[bool, Union[Exception, None]]:
        """Main entry point to packet concatenation

        Args:
            curr_dir (str): Currently processed directory

        Returns:
            tuple[bool, Exception | None]: Returns bool status
                If error, also returns error attrs.
        """

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

        # Main concatenation loop
        h5_files_list = h5_files_list[::-1]

        if last_timestamp == 0:
            last_file = H5File(
                file_dir=working_dir_r,
                file_name=h5_files_list[-1],
                space_samples=self.space_samples,
                time_samples=self.time_samples,
            )

            self.concat_to_chunk_by_time(
                h5_file=last_file,
                start_chunk_time=start_chunk_time,
            )
            h5_files_list.pop(-1)
            last_timestamp = last_file.packet_datetime.timestamp()
        log.debug(f"Last timestamp: {last_timestamp}")
        while len(h5_files_list) > 0:
            if len(h5_files_list) > 1:
                next_file_name = h5_files_list[-2]
                next_file_timestamp = float(
                    next_file_name.split("_")[-1].rsplit(".", maxsplit=1)[0]
                )
                if np.round(next_file_timestamp - last_timestamp) > self.unit_size:
                    log.warning(f"Data has an uncritical gap in {working_dir_r}.")
                    next_file_name = h5_files_list[-1]
                    next_file_timestamp = float(
                        next_file_name.split("_")[-1].rsplit(".", maxsplit=1)[0]
                    )
                    if np.round(next_file_timestamp - last_timestamp) > self.unit_size:
                        log.critical(
                            f"Data has gap in {working_dir_r}."
                            f"Last timestamp: {last_timestamp}"
                        )

                        return False
                    else:
                        log.debug("Using next file.")
                        h5_files_list.pop(-1)
                else:
                    log.debug("Using file after next.")
                    h5_files_list.pop(-1)
                    h5_files_list.pop(-1)
            else:
                next_file_name = h5_files_list[-1]
                if np.round(next_file_timestamp - last_timestamp) > self.unit_size:
                    log.critical(
                        f"Data has gap in {working_dir_r}."
                        f"Last timestamp: {last_timestamp}"
                    )

                    return False
                else:
                    h5_files_list.pop(-1)

            next_file = H5File(
                file_dir=working_dir_r,
                file_name=next_file_name,
                space_samples=self.space_samples,
                time_samples=self.time_samples,
            )

            # Calculates time till next day (for day splitting)
            start_day_datetime = next_file.packet_datetime.replace(
                hour=0, minute=0, second=0
            )
            next_day_datetime = start_day_datetime + timedelta(days=1)

            till_midnight = int(
                np.round(
                    next_day_datetime.timestamp()
                    - next_file.packet_datetime.timestamp(),
                    0,
                )
            )
            till_next_chunk = int(
                np.round(
                    start_chunk_time
                    + CHUNK_SIZE
                    - next_file.packet_datetime.timestamp(),
                    0,
                )
            )

            # if end of the chunk is half of the packet
            # and is major or minor after major was skipped:
            # Split and take first half of the packet
            log.debug(f"{(till_midnight, till_next_chunk)}")
            if till_next_chunk < self.unit_size:
                log.debug(
                    f"Splitting to next chunk: time till next chunk {till_next_chunk}"
                )
                split_offset = int(SPS * till_next_chunk)

                next_file.dset_split = next_file.dset[:split_offset, :]
                # Creating carry to use in the next chunk
                next_file.dset_carry = next_file.dset[split_offset:, :]

                next_file.is_chunk_end = True
                next_file.split_time = till_next_chunk

            # If time to split chunk to next day
            elif till_midnight <= self.unit_size:
                log.debug(f"Splitting to next day: time till midnight {till_midnight}")
                split_offset = int(SPS * till_midnight)

                next_file.dset_split = next_file.dset[:split_offset, :]
                # Creating carry to use in the next chunk
                next_file.dset_carry = next_file.dset[split_offset:, :]

                next_file.is_day_end = True
                next_file.split_time = till_midnight
            # Regular splitting within chunk otherwise
            else:
                packet_diff = int(np.round(next_file.packet_time - last_timestamp, 0))
                if packet_diff < TIME_DIFF_THRESHOLD:
                    log.debug(f"Splitting to next packet: packet diff {packet_diff}")
                    next_file.dset_split = next_file.dset[SPS * packet_diff :, :]
                    next_file.split_time = packet_diff
                else:
                    log.debug("No splitting")
                    next_file.split_time = self.unit_size

            # Main concatenation entry point
            start_chunk_time = self.concat_to_chunk_by_time(
                h5_file=next_file,
                start_chunk_time=start_chunk_time,
            )

            last_timestamp = next_file.packet_time
            # Save updated status data
            self.file_manager.save_status(
                filedir_r=working_dir_r,
                last_filename=next_file.file_name,
                last_filedir_r=next_file.file_dir,
                start_chunk_time=start_chunk_time,
            )

            # Prematurely exit main concatenation loop
            # Even if filenames left in major/minor list
            if next_file.is_day_end:
                return True

            del next_file  # Delete H5File object to free memory
        return True

    def run(self):
        start_time = datetime.now(tz=pytz.UTC)
        dirs = self.file_manager.get_sorted_dirs()
        for working_dir in dirs:
            log.info(f"Processing {working_dir}")
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
            self.file_manager.set_completed(working_dir)
        end_time = datetime.now(tz=pytz.UTC)
        print("Code finished in:", end_time - start_time)
