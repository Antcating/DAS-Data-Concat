import configparser
from os.path import isdir

config_dict = configparser.ConfigParser()
config_dict.read("config.ini", encoding="UTF-8")

#
# CONCATENATION CHARACTERISTICS
#
# Difference between PACKET and SAVE times
TIME_DIFF_THRESHOLD = float(config_dict["CONSTANTS"]["TIME_DIFF_THRESHOLD"])
DATA_LOSE_THRESHOLD = int(config_dict["CONSTANTS"]["DATA_LOSE_THRESHOLD"])
# Concatenated chunk size (in seconds)
CHUNK_SIZE = int(config_dict["CONSTANTS"]["CONCAT_TIME"])

# PACKET CHARACTERISTICS
SPS = int(config_dict["CONSTANTS"]["SPS"])
DX = float(config_dict["CONSTANTS"]["DX"])

# PATHs to files and save
LOCALPATH = config_dict["PATH"]["LOCALPATH"]
NASPATH_final = config_dict["PATH"]["NASPATH_final"]

if isdir(LOCALPATH):
    PATH = LOCALPATH
else:
    raise Exception("PATH is not accessible!")
if isdir(NASPATH_final):
    SAVE_PATH = NASPATH_final
else:
    raise Exception("SAVE_PATH is not accessible!")
