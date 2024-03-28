# FEBUS A1 Data Receiver

## Table of Contents

- [FEBUS A1 Data Receiver](#febus-a1-data-receiver)
  - [Table of Contents](#table-of-contents)
  - [About ](#about-)
  - [Getting Started ](#getting-started-)
    - [Configuration](#configuration)
      - [PATHs](#paths)
      - [Data characteristics](#data-characteristics)

## About <a name = "about"></a>

FEBUS A1 Data Receiver - receives, downsamples `ZMQ` packets from FEBUS A1 system and concatenates them to `hdf5` of desired length. It allows easy configuration for needs of the different systems (different packet length, packet redundancy, etc.). 

## Getting Started <a name = "getting_started"></a>

This project contains two parts: 
- Client - Receives packets using ZMQ and saving them as separate files. [See installation and setup guide](docs/client.md)
- Packet Concatenation - Combines packets in chunks of defined size. [See installation and setup guide](docs/concat.md)

### Configuration

Before running the project you have to configure parameters in the `config.ini` file.

#### PATHs

`LOCALPATH` is **absolute** PATH to the LOCAL directory, DAS client will write packets to `LOCALPATH/YYYYMMDD`.

`NASPATH_final` is **absolute** PATHs to the NAS directory which will contain concatenated `hdf5` files.

#### Data characteristics

`UNIT_SIZE` is length of the packet including redundancy (in second). By default 4
`SPS` is expected time frequency after data downsamling (in Hz). By default 100. 
`DX` is expected spatial spacing after data downsampling (in m). By default 9.6 