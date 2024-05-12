# Data preprocessor for DAS systems

## Table of Contents

- [Data preprocessor for DAS systems](#data-preprocessor-for-das-systems)
  - [Table of Contents](#table-of-contents)
  - [About ](#about-)
  - [Getting Started ](#getting-started-)
    - [Configuration](#configuration)
      - [System parameters](#system-parameters)
      - [PATHs](#paths)
      - [Data characteristics](#data-characteristics)
  - [Save format](#save-format)
    - [File naming](#file-naming)
    - [Data](#data)
    - [Metadata](#metadata)

## About <a name = "about"></a>

This project is a data preprocessor for DAS systems. It is designed to receive packets from DAS system, concatenate them in chunks of defined size and save them as separate files and.

## Getting Started <a name = "getting_started"></a>

To run the project you need to have Python 3.9 or higher installed on your machine.

### Configuration

Before running the project you have to configure parameters in the `config.ini` file:

#### System parameters

`NAME` - name of the DAS system (e.g. `Mekorot` or `Prisma`)

#### PATHs

`LOCALPATH` is **absolute** PATH to the LOCAL directory, DAS client will write packets to `LOCALPATH/YYYYMMDD`.

`NASPATH_final` is **absolute** PATHs to the NAS directory which will contain concatenated `hdf5` files.

#### Data characteristics

`CHUNK_SIZE` is the size of the chunk in seconds. By default 300.
`SPS` is expected time frequency after data downsamling (in Hz). By default 100. 
`DX` is expected spatial spacing after data downsampling (in m). By default 9.6 

## Save format

### File naming
- Files are named according to the following convention:
    - YYYY/YYYYMMDD/<timestamp>.h5
        - YYYY - year of the recording in UTC
        - YYYYMMDD - date of the recording in UTC
        - <timestamp> - timestamp of the beginning of the chunk
### Data
- Data is stored in .h5 format
    - Data is located in data_down dataset
        - Each point stored as float32
### Metadata
- Metadata saved in attributes. Contents of the metadata can vary depending on the system and date of recording.
    - Always present:
        - DX_down - spatial sampling rate after downsampling
        - PRR_down - temporal sampling rate after downsampling
        - down_factor_space, down_factor_time - downsampling factors
    - May be present:
        - Gauge_m - (Prisma  specific) gauge length of the system
        - Index, Origin, Spacing - (Mekorot specific) packet-wise original data descriptors