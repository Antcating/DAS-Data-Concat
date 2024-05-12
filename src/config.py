import configparser
from os.path import isdir

config_dict = configparser.ConfigParser()
config_dict.read("config.ini", encoding="UTF-8")

#
# CONCATENATION CHARACTERISTICS
SYSTEM_NAME = config_dict["SYSTEM"]["NAME"]

# Concatenated chunk size (in seconds)
CHUNK_SIZE = int(config_dict["CONSTANTS"]["CONCAT_TIME"])

# PACKET CHARACTERISTICS
SPS = int(config_dict["CONSTANTS"]["SPS"])
DX = float(config_dict["CONSTANTS"]["DX"])

# PATHs to files and save
LOCALPATH = config_dict["PATH"]["LOCALPATH"]
NASPATH_final = config_dict["PATH"]["NASPATH_final"]

if isdir(LOCALPATH):
    LOCAL_PATH = LOCALPATH
else:
    raise Exception("PATH is not accessible!")
if isdir(NASPATH_final):
    SAVE_PATH = NASPATH_final
else:
    raise Exception("SAVE_PATH is not accessible!")
