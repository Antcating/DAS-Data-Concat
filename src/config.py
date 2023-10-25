import configparser
import os

config = configparser.ConfigParser()
config.read("config.ini")

# PATHs to files and save
LOCALPATH = config["PATH"]["LOCALPATH"]
NASPATH = config["PATH"]["NASPATH"]
LOCALPATH_final = config["PATH"]["LOCALPATH_final"]
NASPATH_final = config["PATH"]["NASPATH_final"]

# Difference between PACKET and SAVE times
TIME_DIFF_THRESHOLD = int(config["CONSTANTS"]["TIME_DIFF_THRESHOLD"])
DATA_LOSE_THRESHOLD = int(config["CONSTANTS"]["DATA_LOSE_THRESHOLD"])

if os.path.isdir(NASPATH):
    PATH = NASPATH
else:
    PATH = LOCALPATH
if os.path.isdir(NASPATH_final):
    SAVE_PATH = NASPATH_final
else:
    SAVE_PATH = LOCALPATH_final