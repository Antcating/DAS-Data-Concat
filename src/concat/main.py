"""Main module for concatenating H5 files into chunks."""
from typing import Union, Tuple
from datetime import datetime, timedelta
import os
import json

import h5py
import numpy as np
import pytz

from log.main_logger import logger as log
from config import (
    CHUNK_SIZE,
    SPS,
    LOCAL_PATH,
    SAVE_PATH,
)


class Concatenator:
    """Class responsible for concatenating H5 files into chunks.

    Attributes:
        space_samples (int): Number of space samples.
        time_samples (int): Number of time samples.
        unit_size (int): Size of each unit.
    """

    def __init__(self):
        self.space_samples: int = 0
        self.time_samples: int = 0
        self.unit_size: int = 0
        self.packet_size: int = 0

        self.carry: Union[None, np.ndarray] = None
        self.old_carry: Union[None, np.ndarray] = None
        self.till_next_chunk: int = 0
        self.new_chunk: bool = False
        self.restored: bool = False

        self.chunk_time: float = 0
        self.chunk_time_str: Union[None, str] = None
        self.till_next_day: int = 0
        self.chunk_to_next_day: int = 0
        self.chunk_data_offset: int = 0

        self.attrs: dict = {}
        self.sps: int = 0
        self.time_seconds: int = 0

    def read_attrs(self, file_path: str) -> dict:
        """Read attributes from json file from working dir.

        Args:
            file_path (str): Path to the json file.

        Returns:
            dict: Dictionary containing the attributes.
        Raises:
            FileNotFoundError: If the file is not found.
        """
        try:
            with open(
                os.path.join(LOCAL_PATH, file_path), "r", encoding="utf-8"
            ) as json_file:
                attrs = json.load(fp=json_file)
            return attrs
        except FileNotFoundError as e:
            raise FileNotFoundError(f"File {file_path} not found") from e

    def _check_shape_consistency(self, data: np.ndarray) -> bool:
        """Check if the shape of the data is consistent with the expected shape.

        Args:
            data (np.ndarray): The data to be checked.

        Returns:
            bool: True if the shape is consistent, False otherwise.
        Raises:
            ValueError: If the data has wrong shape.
        """
        log.debug("Checking data shape against expected shape")
        if data.shape[0] != int(self.space_samples):
            log.critical(
                "Data has wrong shape: %s. Expected: %s, x",
                data.shape,
                int(self.space_samples),
            )
            raise ValueError("Data has wrong shape")
        if data.shape[1] != int(self.time_samples):
            log.critical(
                "Data has wrong shape: %s. Expected: x, %s",
                data.shape,
                int(self.time_samples),
            )
            raise ValueError("Data has wrong shape")
        log.debug("Data shape is consistent")
        return True

    def _get_next_packet_data(
        self, h5_files_list: list
    ) -> Tuple[Union[str, None], Union[np.ndarray, None], bool]:
        """Get the data from the next H5 file in the list.

        Args:
            h5_files_list (list): List of H5 file paths.

        Returns:
            tuple: A tuple containing the file name, data,
                and a flag indicating if there is a gap between files.
        """
        data: np.ndarray
        return_tuple = (None, None, False)  # name, data, gap
        if len(h5_files_list) > 1:
            file_path = h5_files_list[-1]
            self._calculate_attrs(
                file_path.replace(".h5", ".json").replace("das_SR_", "")
            )
            log.debug("Checking file: %s", file_path)
            file_timestamp = float(file_path.split("_")[-1].rsplit(".", maxsplit=1)[0])
            data = h5py.File(os.path.join(LOCAL_PATH, file_path), "r")["data_down"][
                ()
            ].T
            self._check_shape_consistency(data)

            next_file_name = h5_files_list[-2]
            next_file_timestamp = float(
                next_file_name.split("_")[-1].rsplit(".", maxsplit=1)[0]
            )
            if np.round(next_file_timestamp - file_timestamp) > self.time_seconds:
                log.critical(
                    "Data has gap between %s and %s", file_path, next_file_name
                )
                # h5_files_list.pop()
                return_tuple = (file_path, data, True)
            else:
                file_time_diff = int(np.round(next_file_timestamp - file_timestamp, 0))
                split_before = int(SPS * file_time_diff)
                log.debug(
                    "Cutting data after: %s seconds or %s samples",
                    file_time_diff,
                    split_before,
                )
                data = data[:, :split_before]
                log.debug("Data shape after cutting: %s", data.shape)
                # h5_files_list.pop()
                return_tuple = (file_path, data[:, :split_before], False)

            # raise NotImplementedError("Not implemented yet")

        else:
            file_path = h5_files_list[-1]
            self._calculate_attrs(
                file_path.replace(".h5", ".json").replace("das_SR_", "")
            )
            log.debug("Last file: %s", file_path)
            # h5_files_list.pop()
            data = h5py.File(os.path.join(LOCAL_PATH, file_path), "r")["data_down"][
                ()
            ].T
            self._check_shape_consistency(data)
            return_tuple = (file_path, data, True)

        return return_tuple

    def _calculate_attrs(self, file_path: str) -> None:
        """Calculate the attributes based on the file path.

        Args:
            file_path (str): The file path.

        Returns:
            None
        """
        log.debug("Calculating attrs for %s", file_path)
        if os.path.exists(os.path.join(LOCAL_PATH, file_path)):
            self.attrs = self.read_attrs(file_path)
        else:
            file_path = os.path.join(file_path.rsplit(os.sep, 1)[0], "attrs.json")
            log.warning("Working in legacy mode. Loading attrs from %s", file_path)
            self.attrs = self.read_attrs(file_path)

        self.space_samples = int(
            np.ceil((self.attrs["index"][1] + 1) / self.attrs["down_factor_space"])
        )
        self.time_samples = int(
            np.ceil((self.attrs["index"][3] + 1) / self.attrs["down_factor_time"])
        )
        self.time_seconds = (self.attrs["index"][3] + 1) / (
            1000 / self.attrs["spacing"][1]
        )
        log.debug("Expected data shape: %s, %s", self.space_samples, self.time_samples)
        self.sps = self.time_samples / self.time_seconds

    def _cut_chunk_to_size(self, chunk_data: np.ndarray) -> np.ndarray:
        """Cut the chunk data to the specified offset.

        Args:
            chunk_data (np.ndarray): The chunk data.

        Returns:
            np.ndarray: The cut chunk data.
        """
        log.debug("Cutting chunk data to size %s", self.chunk_data_offset)
        chunk_data = chunk_data[:, : self.chunk_data_offset]
        return chunk_data

    def _get_previous_file_data(self):
        if os.path.exists(os.path.join(SAVE_PATH, "last")):
            with open(os.path.join(SAVE_PATH, "last"), "r", encoding="utf-8") as f:
                chunk_time, chunk_data_offset = [x.strip() for x in f.readlines()]
                chunk_data_offset = int(chunk_data_offset)
                chunk_time = float(chunk_time)
                chunk_datetime = datetime.fromtimestamp(chunk_time, tz=pytz.UTC)
                chunk_end_time = chunk_time + (chunk_data_offset / SPS)
                next_day = (
                    chunk_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
                    + timedelta(days=1)
                ).timestamp()
                if (
                    chunk_data_offset == int(CHUNK_SIZE * SPS)
                    or chunk_end_time >= next_day
                ):
                    log.debug("Skipping restoration")
                    log.debug("Chunk time %s", chunk_time)
                    log.debug("Loading carry data")
                    if os.path.exists(os.path.join(SAVE_PATH, "carry.npy")):
                        log.debug("Loading carry data")
                        self.old_carry = np.load(os.path.join(SAVE_PATH, "carry.npy"))
                        self.carry = self.old_carry
                        os.remove(os.path.join(SAVE_PATH, "carry.npy"))
                        log.debug("Removing carry file")
                    else:
                        self.carry = None
                        self.old_carry = None
                        log.debug("No carry data found")
                else:
                    log.debug("Restoring previous chunk data")
                    log.debug("Loading last chunk data from file %s", chunk_time)
                    log.debug("Chunk data offset: %s", chunk_data_offset)
                    self.restored = True
        else:
            chunk_time = 0
            chunk_data_offset = 0
            log.debug("No last chunk data found")

        return chunk_time, chunk_data_offset

    def _allocate_empty_chunk(self):
        chunk_data = np.empty(
            (self.space_samples, int(CHUNK_SIZE * SPS)), dtype=np.float32
        )
        self.chunk_data_offset = 0
        self.till_next_chunk = CHUNK_SIZE
        self.new_chunk = True
        log.debug("New chunk data has shape: %s", chunk_data.shape)
        return chunk_data

    def _restore_previous_chunk(self, previous_chunk_time, previous_chunk_data_offset):
        previous_chunk_time = float(previous_chunk_time)
        chunk_datetime = datetime.fromtimestamp(previous_chunk_time, tz=pytz.UTC).date()
        chunk_date = chunk_datetime.strftime("%Y%m%d")
        chunk_year = chunk_datetime.strftime("%Y")
        chunk_path = os.path.join(
            SAVE_PATH, chunk_year, chunk_date, str(previous_chunk_time) + ".h5"
        )
        log.debug("Loading chunk data from %s", chunk_path)
        try:
            chunk_data: np.ndarray = h5py.File(chunk_path, "r")["data_down"][()]
            # Resize chunk to SPS * CHUNK_SIZE
            chunk_data = np.hstack(
                (
                    chunk_data,
                    np.zeros(
                        (
                            self.space_samples,
                            int((SPS * CHUNK_SIZE) - chunk_data.shape[1]),
                        ),
                        dtype=np.float32,
                    ),
                )
            )

            log.debug("Chunk data shape: %s", chunk_data.shape)

            next_day = (
                datetime.fromtimestamp(previous_chunk_time, tz=pytz.UTC).replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
                + timedelta(days=1)
            ).timestamp()
            self.chunk_to_next_day = next_day - previous_chunk_time

            self.chunk_data_offset = previous_chunk_data_offset
            self.chunk_time = previous_chunk_time
            self.chunk_time_str = str(previous_chunk_time)
            self.new_chunk = False
        except FileNotFoundError as e:
            log.error("Restored chunk data not found, starting new chunk")
            raise FileNotFoundError("Restored chunk data not found") from e
        self.restored = False

        return chunk_data

    def _get_chunk_data(self, previous_chunk_time, previous_chunk_data_offset):
        if self.restored:
            log.debug("Restoring previous chunk data")
            chunk_data = self._restore_previous_chunk(
                previous_chunk_time, previous_chunk_data_offset
            )
        else:
            log.debug("Creating new chunk data")
            chunk_data = self._allocate_empty_chunk()
        return chunk_data

    def _get_file_list(self, previous_chunk_time, previous_chunk_data_offset):
        today = datetime.now(tz=pytz.UTC).date().strftime("%Y%m%d")
        h5_files_list = []
        dirs = [
            dir
            for dir in os.listdir(LOCAL_PATH)
            if os.path.isdir(os.path.join(LOCAL_PATH, dir)) and dir != today
        ]

        for dir_path in sorted(dirs):
            for root, dirs, files in os.walk(os.path.join(LOCAL_PATH, dir_path)):
                files = [file for file in files if file.endswith(".h5")]
                for file in sorted(
                    files, key=lambda x: int(x.split("_")[-1].split(".")[0])
                ):
                    if file.endswith(".h5") and float(
                        file.split("_")[-1].rsplit(".", 1)[0]
                    ) >= np.floor(previous_chunk_time) + (
                        previous_chunk_data_offset / SPS
                    ):
                        h5_files_list.append(os.path.join(dir_path, file))
        # FIFO: reverse list
        h5_files_list = h5_files_list[::-1]
        return h5_files_list

    def _fill_chunk_data(
        self, h5_files_list, chunk_data, previous_chunk_time, previous_chunk_data_offset
    ):
        log.debug("Filling chunk data")
        while True:
            self.till_next_chunk = CHUNK_SIZE - self.chunk_data_offset / self.sps
            # Get next file data
            name, data, is_chunk_stop = self._get_next_packet_data(h5_files_list)
            if data.shape[0] != chunk_data.shape[0]:
                log.warning(
                    "Data shape mismatch: %s, %s",
                    data.shape[0],
                    chunk_data.shape[0],
                )
                log.debug("Creating new chunk data")
                break
            start_split_index = 0
            end_split_index = data.shape[1]
            if int(
                self.chunk_time
                + (self.chunk_data_offset / self.sps)
                - self.time_seconds
            ) >= int(name.split("_")[-1].rsplit(".")[0]):
                log.debug("Skipping %s", name)
                continue
            log.debug("Concatenating %s", name)
            if self.new_chunk:
                if self.carry is not None:
                    log.debug("Previous time: %s", previous_chunk_time)
                    log.debug("Previous offset: %s", previous_chunk_data_offset)
                    log.debug("Loaded time from carry data")
                    self.chunk_time = float(
                        previous_chunk_time + (previous_chunk_data_offset / SPS)
                    )
                    chunk_data[:, : self.carry.shape[1]] = self.carry
                    self.chunk_data_offset = self.carry.shape[1]
                    self.carry = None
                else:
                    log.debug("Loaded time from packet name")
                    self.chunk_time = float(
                        name.split("_")[-1].rsplit(".", maxsplit=1)[0]
                    )
                # Time drift correction
                self.chunk_time = (
                    np.floor(self.chunk_time)
                    + float(name.split("_")[-1].rsplit(".", maxsplit=1)[0])
                    - np.floor(float(name.split("_")[-1].rsplit(".", maxsplit=1)[0]))
                )

                next_day = (
                    datetime.fromtimestamp(self.chunk_time, tz=pytz.UTC).replace(
                        hour=0, minute=0, second=0
                    )
                    + timedelta(days=1)
                ).timestamp()
                log.debug("New chunk time: %s", self.chunk_time)
                log.debug(
                    "Chunk datetime: %s",
                    datetime.fromtimestamp(self.chunk_time, tz=pytz.UTC).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                )
                log.debug(
                    "Next day datetime: %s",
                    datetime.fromtimestamp(next_day, tz=pytz.UTC).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                )
                self.chunk_to_next_day = np.ceil(next_day - self.chunk_time)
                self.chunk_time_str = str(self.chunk_time)
                self.new_chunk = False

                date_datetime = datetime.fromtimestamp(
                    self.chunk_time, tz=pytz.UTC
                ).date()
                year = date_datetime.strftime("%Y")
                date = date_datetime.strftime("%Y%m%d")
                if not os.path.exists(os.path.join(SAVE_PATH, year, date)):
                    os.makedirs(os.path.join(SAVE_PATH, year, date))

                with open(
                    os.path.join(SAVE_PATH, year, date, self.chunk_time_str + ".json"),
                    "w",
                    encoding="utf-8",
                ) as f:
                    json.dump(self.attrs, f)

            self.till_next_day = round(
                self.chunk_to_next_day - self.chunk_data_offset / self.sps, 0
            )
            if round(self.chunk_time + (self.chunk_data_offset / self.sps)) > int(
                name.split("_")[-1].rsplit(".")[0]
            ):
                start_split_index = int(
                    self.sps
                    * (
                        int(self.chunk_time)
                        + (self.chunk_data_offset / self.sps)
                        - int(name.split("_")[-1].rsplit(".")[0])
                    )
                )
                log.debug(
                    "Splitting to next packet: start split offset %s",
                    start_split_index,
                )
            if self.till_next_day < data.shape[1] / self.sps:
                end_split_index = int(self.sps * self.till_next_day)
                log.debug(
                    "Splitting to next day: time till midnight %s",
                    self.till_next_day,
                )
                log.debug("Splitting to next day: split offset %s", end_split_index)
                self.carry = data[:, end_split_index:]
                is_chunk_stop = True
            elif self.till_next_chunk < data.shape[1] / self.sps:
                end_split_index = int(self.sps * self.till_next_chunk)
                log.debug(
                    "Splitting to next chunk: time till next chunk %s",
                    self.till_next_chunk,
                )
                log.debug("Splitting to next chunk: split offset %s", end_split_index)
                self.carry = data[:, end_split_index:]

                is_chunk_stop = True
            else:
                end_split_index = data.shape[1]

            log.debug(
                "Split data between %s and %s", start_split_index, end_split_index
            )
            data = data[:, start_split_index:end_split_index]
            log.debug("Data shape after split: %s", data.shape)

            if (
                np.floor(
                    self.chunk_time
                    + (self.chunk_data_offset / self.sps)
                    - (
                        float(name.split("_")[-1].rsplit(".")[0])
                        + start_split_index / self.sps
                    )
                )
                != 0
            ):
                raise ValueError("Inconsistency between chunk time and packet time")

            chunk_data[
                :,
                self.chunk_data_offset : self.chunk_data_offset
                + end_split_index
                - start_split_index,
            ] = data
            self.chunk_data_offset += end_split_index - start_split_index
            chunk_time_current = self.chunk_time + (self.chunk_data_offset / self.sps)

            h5_files_list.pop()
            log.debug("Data shape: %s", (chunk_data.shape[0], self.chunk_data_offset))
            log.debug("Time till next chunk: %s", self.till_next_chunk)
            log.debug("Time till next day: %s", self.till_next_day)
            log.debug("Chunk data offset: %s", self.chunk_data_offset)
            log.debug("Chunk time current: %s", chunk_time_current)
            log.debug("Is chunk stop: %s", is_chunk_stop)

            if is_chunk_stop:
                break
        return chunk_data

    def concat_files(self) -> Tuple[bool, Union[Exception, None]]:
        """Main entry point to packet concatenation.

        Returns:
            tuple: containing the status (bool) and the error (Exception or None).
        """
        previous_chunk_time, previous_chunk_data_offset = self._get_previous_file_data()
        # Getting all necessary environmental vars from status file (if exists)
        #  otherwise return default values
        h5_files_list = self._get_file_list(
            previous_chunk_time, previous_chunk_data_offset
        )
        if len(h5_files_list) == 0:
            log.warning("No new files found in %s", LOCAL_PATH)
            return
        log.debug("Files to process: %s", len(h5_files_list))

        # Check if there is a gap between last chunk and first file
        if self.carry is not None:
            time_diff = np.floor(
                float(h5_files_list[-1].split("_")[-1].rsplit(".", 1)[0])
                - float(
                    previous_chunk_time
                    + (previous_chunk_data_offset / SPS)
                    + (self.carry.shape[1] / SPS)
                )
            )
            if time_diff > 0:
                log.warning("Gap between last chunk and first file: %s", time_diff)
                self.carry = None

        while len(h5_files_list) > 0:
            self._calculate_attrs(
                # TODO: Dynamic loading of prefix
                h5_files_list[-1]
                .replace(".h5", ".json")
                .replace("das_SR_", "")
            )

            chunk_data = self._get_chunk_data(
                previous_chunk_time, previous_chunk_data_offset
            )

            chunk_data = self._fill_chunk_data(
                h5_files_list,
                chunk_data,
                previous_chunk_time,
                previous_chunk_data_offset,
            )
            # Cut chunk data to size
            chunk_data = self._cut_chunk_to_size(chunk_data)
            # Save chunk data to h5 file
            log.debug("Saving chunk data to %s.h5", self.chunk_time_str)
            date_datetime = datetime.fromtimestamp(
                float(self.chunk_time_str), tz=pytz.UTC
            ).date()
            year = date_datetime.strftime("%Y")
            date = date_datetime.strftime("%Y%m%d")
            save_path = os.path.join(SAVE_PATH, year, date)
            if not os.path.exists(save_path):
                os.makedirs(os.path.join(SAVE_PATH, year, date))

            h5py.File(os.path.join(save_path, self.chunk_time_str + ".h5"), "w")[
                "data_down"
            ] = chunk_data
            if os.path.exists(os.path.join(SAVE_PATH, "last")):
                os.remove(os.path.join(SAVE_PATH, "last"))
                log.debug("Removing last after saving chunk data")

            with open(os.path.join(SAVE_PATH, "last"), "w", encoding="utf-8") as f:
                f.writelines([f"{self.chunk_time}\n", f"{self.chunk_data_offset}\n"])
            if self.carry is not None:
                log.debug("Saving carry data to carry file")
                np.save(os.path.join(SAVE_PATH, "carry.npy"), self.carry)

            previous_chunk_time = self.chunk_time
            previous_chunk_data_offset = self.chunk_data_offset
        return

    def run(self):
        """Main entry point to the concatenation process."""
        start_time = datetime.now(tz=pytz.UTC)
        log.info("Starting concatenation at %s", start_time)
        self.concat_files()
        log.info("Finished in %s", datetime.now(tz=pytz.UTC) - start_time)
