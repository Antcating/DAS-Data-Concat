import configparser
from os.path import isdir

config = configparser.ConfigParser()
config.read("config.ini")

# PATHs to files and save
LOCALPATH = config["PATH"]["LOCALPATH"]
LOCALPATH_final = config["PATH"]["LOCALPATH_final"]
NASPATH_final = config["PATH"]["NASPATH_final"]

# Difference between PACKET and SAVE times
TIME_DIFF_THRESHOLD = int(config["CONSTANTS"]["TIME_DIFF_THRESHOLD"])
DATA_LOSE_THRESHOLD = int(config["CONSTANTS"]["DATA_LOSE_THRESHOLD"])
# Concat time
CONCAT_TIME = int(config["CONSTANTS"]["CONCAT_TIME"])
# Packet length in seconds
UNIT_SIZE = int(config["CONSTANTS"]["UNIT_SIZE"])

if isdir(LOCALPATH):
    PATH = LOCALPATH
else:
    raise Exception("PATH is not accessible!")
if isdir(NASPATH_final):
    SAVE_PATH = NASPATH_final
elif isdir(LOCALPATH_final):
    SAVE_PATH = LOCALPATH_final
else:
    raise Exception("SAVE_PATH is not accessible!")
