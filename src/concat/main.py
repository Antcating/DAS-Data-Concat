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
        file_manager (FileManager): Object responsible for managing H5 files.
        space_samples (int): Number of space samples.
        time_samples (int): Number of time samples.
        unit_size (int): Size of each unit.
    """

    def __init__(self):
        self.space_samples = 0
        self.time_samples = 0
        self.unit_size = 0
        self.packet_size = 0

        self.carry = None
        self.start_chunk_offset = None
        self.till_next_chunk = 0
        self.new_chunk = False
        self.chunk_time = 0
        self.chunk_time_str = None
        self.till_next_day = 0
        self.chunk_to_next_day = 0
        self.chunk_data_offset = 0

        self.sps = 0
        self.time_seconds = 0
        self.restored = False

    def read_attrs(self, working_dir: str, filename: str = "attrs.json"):
        # Read attributes from json file from working dir
        try:
            with open(
                os.path.join(LOCAL_PATH, working_dir, filename), "r"
            ) as json_file:
                attrs = json.load(fp=json_file)
                self.write_attrs(attrs, working_dir)
            return attrs
        except FileNotFoundError as e:
            raise FileNotFoundError(
                f"File {filename} not found in {working_dir}"
            ) from e

    def write_attrs(self, attrs: dict, working_dir: str):
        # Write attributes to json file in save dir
        os.makedirs(
            os.path.join(SAVE_PATH, working_dir[:4], working_dir), exist_ok=True
        )
        with open(
            os.path.join(SAVE_PATH, working_dir[:4], working_dir, "attrs.json"),
            "w",
            encoding="utf-8",
        ) as f:
            json.dump(attrs, f)

    def check_shape_consistency(self, data):
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
        return True

    def get_next_file_data(self, h5_files_list: list):
        data: np.ndarray
        return_tuple = (None, None, False)  # name, data, gap
        if len(h5_files_list) > 1:
            file_path = h5_files_list[-1]
            self.calculate_attrs(file_path.rsplit("/", maxsplit=1)[0])
            log.debug("Processing file: %s", file_path)
            file_timestamp = float(file_path.split("_")[-1].rsplit(".", maxsplit=1)[0])
            data = h5py.File(os.path.join(LOCAL_PATH, file_path), "r")["data_down"][
                ()
            ].T
            self.check_shape_consistency(data)

            next_file_name = h5_files_list[-2]
            next_file_timestamp = float(
                next_file_name.split("_")[-1].rsplit(".", maxsplit=1)[0]
            )
            if np.round(next_file_timestamp - file_timestamp) > self.time_seconds:
                log.critical(
                    "Data has gap between %s and %s", file_path, next_file_name
                )
                h5_files_list.pop()
                return_tuple = (file_path, data, True)
            else:
                file_time_diff = int(np.round(next_file_timestamp - file_timestamp, 0))
                split_before = int(SPS * file_time_diff)
                log.debug("Splitting to next packet: packet diff %s", file_time_diff)
                log.debug("Splitting to next packet: split before %s", split_before)
                h5_files_list.pop()
                return_tuple = (file_path, data[:, :split_before], False)

            # raise NotImplementedError("Not implemented yet")

        else:
            file_path = h5_files_list[-1]
            self.calculate_attrs(file_path.rsplit("/", maxsplit=1)[0])
            log.debug("Last file: %s", file_path)
            h5_files_list.pop()
            data = h5py.File(os.path.join(LOCAL_PATH, file_path), "r")["data_down"][
                ()
            ].T
            self.check_shape_consistency(data)
            return_tuple = (file_path, data, True)

        return return_tuple

    def calculate_attrs(self, working_dir_r: str):
        attrs = self.read_attrs(working_dir_r)

        self.space_samples = int(
            np.ceil((attrs["index"][1] + 1) / attrs["down_factor_space"])
        )
        self.time_samples = int(
            np.ceil((attrs["index"][3] + 1) / attrs["down_factor_time"])
        )
        self.time_seconds = (attrs["index"][3] + 1) / (1000 / attrs["spacing"][1])
        log.debug(
            "Space samples: %s, Time samples: %s", self.space_samples, self.time_samples
        )
        self.sps = self.time_samples / self.time_seconds

    def concat_files(self) -> Tuple[bool, Union[Exception, None]]:
        """Main entry point to packet concatenation

        Args:
            curr_dir (str): Currently processed directory

        Returns:
            tuple[bool, Exception | None]: Returns bool status
                If error, also returns error attrs.
        """

        if os.path.exists("last"):
            with open("last", "r", encoding="utf-8") as f:
                chunk_time, chunk_data_offset = [x.strip() for x in f.readlines()]
                chunk_data_offset = int(chunk_data_offset)
                chunk_time = float(chunk_time)
                log.debug("Loading last chunk data from file %s", chunk_time)
                log.debug("Chunk data offset: %s", chunk_data_offset)
                self.restored = True
        else:
            chunk_time = 0
            chunk_data_offset = 0
            log.debug("No last chunk data found")

        # Getting all necessary environmental vars from status file (if exists)
        #  otherwise return default values
        h5_files_list = []
        dirs = [
            dir
            for dir in os.listdir(LOCAL_PATH)
            if os.path.isdir(os.path.join(LOCAL_PATH, dir))
        ]
        for dir_path in sorted(dirs):
            for root, dirs, files in os.walk(os.path.join(LOCAL_PATH, dir_path)):
                files = [file for file in files if file.endswith(".h5")]
                for file in sorted(
                    files, key=lambda x: int(x.split("_")[-1].split(".")[0])
                ):
                    # margin for several files upon restart
                    if (
                        file.endswith(".h5")
                        and float(file.split("_")[-1].split(".")[0])
                        >= chunk_time - CHUNK_SIZE
                    ):
                        h5_files_list.append(os.path.join(dir_path, file))
        if len(h5_files_list) == 0:
            log.warning("No new files found in %s", LOCAL_PATH)
            return
        # Main concatenation loop
        h5_files_list = h5_files_list[::-1]

        while len(h5_files_list) > 0:
            self.calculate_attrs(h5_files_list[-1].rsplit("/", maxsplit=1)[0])

            if self.restored:
                chunk_date = (
                    datetime.fromtimestamp(chunk_time, tz=pytz.UTC)
                    .date()
                    .strftime("%Y%m%d")
                )
                chunk_path = os.path.join("output", chunk_date, str(chunk_time) + ".h5")
                log.debug("Loading chunk data from %s", chunk_path)
                chunk_data: np.array = h5py.File(chunk_path, "r")["data_down"][()]
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
                    datetime.fromtimestamp(chunk_time, tz=pytz.UTC).replace(
                        hour=0, minute=0, second=0, microsecond=0
                    )
                    + timedelta(days=1)
                ).timestamp()
                self.chunk_to_next_day = next_day - chunk_time

                self.chunk_data_offset = chunk_data_offset
                self.chunk_time = chunk_time
                self.chunk_time_str = str(chunk_time)
                self.new_chunk = False
                self.restored = False

            else:
                chunk_data = np.empty(
                    (self.space_samples, int(CHUNK_SIZE * self.sps)), dtype=np.float32
                )
                self.chunk_data_offset = 0
                self.till_next_chunk = CHUNK_SIZE
                self.new_chunk = True
                log.debug("New chunk data has shape: %s", chunk_data.shape)

                if self.carry is not None:
                    log.debug("Carry shape: %s", self.carry.shape)
                    chunk_data[:, : self.carry.shape[1]] = self.carry
                    self.chunk_data_offset = self.carry.shape[1]
                    self.start_chunk_offset = self.carry.shape[1] / self.sps
                    self.carry = None
            while True:
                self.till_next_chunk = CHUNK_SIZE - self.chunk_data_offset / self.sps
                # Get next file data
                name, data, is_chunk_stop = self.get_next_file_data(h5_files_list)
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
                    self.chunk_time = float(
                        name.split("_")[-1].rsplit(".", maxsplit=1)[0]
                    )
                    if self.start_chunk_offset is not None:
                        self.chunk_time -= self.start_chunk_offset
                        self.start_chunk_offset = None

                    next_day = (
                        datetime.fromtimestamp(self.chunk_time, tz=pytz.UTC).replace(
                            hour=0, minute=0, second=0, microsecond=0
                        )
                        + timedelta(days=1)
                    ).timestamp()
                    log.debug(
                        "Chunk datetime: %s",
                        datetime.fromtimestamp(self.chunk_time, tz=pytz.UTC),
                    )
                    log.debug(
                        "Next day datetime: %s",
                        datetime.fromtimestamp(next_day, tz=pytz.UTC),
                    )
                    self.chunk_to_next_day = next_day - self.chunk_time
                    self.chunk_time_str = str(self.chunk_time)
                    self.new_chunk = False
                    log.debug("New chunk time: %s", self.chunk_time_str)
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
                    log.debug(
                        "Splitting to next chunk: split offset %s", end_split_index
                    )
                    self.carry = data[:, end_split_index:]

                    is_chunk_stop = True
                else:
                    end_split_index = data.shape[1]

                log.debug("Start split index: %s", start_split_index)
                log.debug("End split index: %s", end_split_index)
                log.debug("Data shape: %s", data.shape)
                log.debug(
                    "Data shape after split: %s",
                    data[:, start_split_index:end_split_index].shape,
                )

                chunk_data[
                    :,
                    self.chunk_data_offset : self.chunk_data_offset
                    + end_split_index
                    - start_split_index,
                ] = data[:, start_split_index:end_split_index]
                self.chunk_data_offset += end_split_index - start_split_index
                self.chunk_time_current = self.chunk_time + (
                    self.chunk_data_offset / self.sps
                )
                log.debug("Data shape: %s", chunk_data.shape)
                log.debug("Time till next chunk: %s", self.till_next_chunk)
                log.debug("Time till next day: %s", self.till_next_day)
                log.debug("Chunk data offset: %s", self.chunk_data_offset)
                log.debug("Chunk time current: %s", self.chunk_time_current)
                log.debug("Is chunk stop: %s", is_chunk_stop)

                if is_chunk_stop:
                    chunk_data = chunk_data[:, : self.chunk_data_offset]
                    break

            # Save chunk data to h5 file
            log.debug("Saving chunk data to %s.h5", self.chunk_time_str)
            date = (
                datetime.fromtimestamp(float(self.chunk_time_str), tz=pytz.UTC)
                .date()
                .strftime("%Y%m%d")
            )
            save_path = os.path.join("output", date)
            if not os.path.exists(save_path):
                os.makedirs(os.path.join("output", date))

            h5py.File(os.path.join(save_path, self.chunk_time_str + ".h5"), "w")[
                "data_down"
            ] = chunk_data
            if os.path.exists("last"):
                os.remove("last")
                log.debug("Removing last after saving chunk data")

            if self.chunk_data_offset != SPS * CHUNK_SIZE:
                with open(os.path.join("last"), "w", encoding="utf-8") as f:
                    f.writelines(
                        [f"{self.chunk_time}\n", f"{self.chunk_data_offset}\n"]
                    )
        return True

    def run(self):
        start_time = datetime.now(tz=pytz.UTC)
        self.concat_files()
        log.info("Finished in %s", datetime.now(tz=pytz.UTC) - start_time)
        return
