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
    - [Manual use through Python](#manual-use-through-python)
    - [Scheduling on UNIX systems](#scheduling-on-unix-systems)
      - [Example using systemd:](#example-using-systemd)

## About <a name = "about"></a>

FEBUS A1 Data Receiver - concatenates data from FEBUS A1 system once a day. It allows easy configuration for needs of the different systems (different packet length). 

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

## Usage <a name = "usage"></a>

To run the project, you have to provide `downsampled_reference.h5`, which has to be placed in the same directory as `concat.py`. From `downsampled_reference.h5` the program will calculate expected `DX`, `SPS`, and matrix `shape` for checking h5 files in the provided `{LOCAL/NAS}PATH/{date}`,

Logging of all actions is done to 3 locations: `{LOCAL/NAS}PATH/{date}/log` with `DEBUG` level, `{LOCAL/NAS}PATH_final/log` with `WARNING` level and to console with `INFO` level.

> Important note: The concatenation program won't process directory named by today's date in format `YYYYMMDD` according to UTC to prevent processing directories which are in use by DAS Client!  
### Manual use through Python
```
python src/concat.py
```


If project runs successfully, the concatenation has been done successfully. Otherwise the exception would be raised and (to be done) notification would be sent to the provided email.  


### Scheduling on UNIX systems

This project contains `concat.sh` file, that is used as a wrapper for a `concat.py` file. 

Give `concat.sh` file executable permissions:

```
chmod u+x concat.sh
```

Before using it, you have to configure it by passing into it the **absolute** `PROJECT_PATH` to the project directory:

> concat.sh:
```
[...]
pushd PROJECT_PATH
[...]
```



#### Example using systemd: 

For the scheduling could be used any scheduling UNIX software. In this example I'll you **systemd Timers** (systemd is packed with most of the popular Linux distros nowadays).

Both files in the provided example are located in `systemd` directory of the project.

**Create concatenation systemd service and timer.**

Service:

> /etc/systemd/system/FebusConcatDaily.service

```
[Unit]
Description=Run concatenation of FEBUS data

[Service]
ExecStart=/bin/bash {PROJECT_PATH}/concat.sh

[Install]
WantedBy=multi-user.target
```

Timer:

> /etc/systemd/system/FebusConcatDaily.timer
```
[Unit]
Description=Run concatenation of FEBUS data daily at 3am.

[Timer]
OnCalendar=*-*-* 3:00:00
Persistent=true

[Install]
WantedBy=timers.target
```

> Note: We used `Persistent=true` to enable the timer to start the scheduled concatenation even if we lost power on the machine during the expected time period

**Using `systemctl` activate timer:**

```
sudo systemctl enable FebusConcatDaily.timer
sudo systemctl daemon-reload
sudo systemctl start FebusConcatDaily.timer
```

Hooray! Timer is set up and will automatically run concatenation once a day. To check that timer was successfully activated we can run: 

```
systemctl list-timers
```

Among the listed timers we would be able to see `FebusConcatDaily.timer`, which activates `FebusConcatDaily.service`