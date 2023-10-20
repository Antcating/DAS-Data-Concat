# FEBUS A1 Data Receiver

## Table of Contents

- [FEBUS A1 Data Receiver](#febus-a1-data-receiver)
  - [Table of Contents](#table-of-contents)
  - [About ](#about-)
  - [Getting Started ](#getting-started-)
    - [Installing](#installing)
    - [Configuration](#configuration)
      - [Necessary configurations](#necessary-configurations)
      - [Additional configurations](#additional-configurations)
  - [Usage ](#usage-)

## About <a name = "about"></a>

FEBUS A1 Data Receiver - concatenates data from FEBUS A1 system (to be done) once a day. It allows easy configuration for needs of the different systems (different packet length). 

## Getting Started <a name = "getting_started"></a>

Be sure, that you have `downsampled_reference.h5` - any h5 file created by `das_client.py` which was downsampled to wanted dimensions. 

### Installing

A step by step guide that tell you how to get a project running.

**Clone the project:**

```
git clone https://github.com/Antcating/DAS-FEBUS-Receiver.git
cd DAS-FEBUS-Receiver
```

**Create virtual environment for this project:**

```
python -m venv .
```
**Activate virtual environment (Linux + bash):**
```
source .venv/bin/activate
```
*See how to activate venv on any other system: [venv reference](https://docs.python.org/3/library/venv.html)*


**Install all the dependencies**

```
pip install -r requirement.txt
```

### Configuration

Before running the project you have to configure parameters in the `config.ini` file.

#### Necessary configurations

`LOCALPATH`, `NASPATH` are **absolute** PATHs to the LOCAL/NAS directory which contains downsampled h5 files from client in subsequent directories named by dates.

`LOCALPATH_final`, `NASPATH_final` are **absolute** PATHs to the LOCAL/NAS directory which would contain saves concatenated files.

#### Additional configurations

`TIME_DIFF_THRESHOLD` - minimal time threshold which requires use of packet without intersection. By default set to 3 (max difference between packets with intersection + 1).

`DATA_LOSE_THRESHOLD` - minimal time threshold which indicates unrecoverable loss of data. By default set to 5 (max difference between packets without intersection + 1).

`CONCAT_TIME` - defines size of chunks (in seconds) of the concatenated packages. By default set to 20.

## Usage <a name = "usage"></a>

To run the project, you have to provide `downsampled_reference.h5`, which has to be placed in the same directory as `concat.py`. From `downsampled_reference.h5` the program will calculate expected `DX`, `SPS`, and matrix `shape` for checking h5 files in the provided `{LOCAL/NAS}PATH/{date}`,


**Running the project**

```
python concat.py
```

> Important note: The concatenation program won't process directory named by today's date in format `YYYYMMDD` according to UTC to prevent processing directories which are in use by DAS Client!  

If project runs successfully, the concatenation has been done successfully. Otherwise the exception would be raised and (to be done) notification would be sent to the provided email.  

Logging of all actions is done to 3 locations: `{LOCAL/NAS}PATH/{date}/log` with `DEBUG` level, `{LOCAL/NAS}PATH_final/log` with `WARNING` level and to console with `INFO` level.
