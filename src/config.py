import configparser
from os.path import isdir

config = configparser.ConfigParser()
config.read("config.ini", encoding="UTF-8")

# PATHs to files and save
LOCALPATH = config["PATH"]["LOCALPATH"]
LOCALPATH_final = config["PATH"]["LOCALPATH_final"]
NASPATH_final = config["PATH"]["NASPATH_final"]

# Difference between PACKET and SAVE times
TIME_DIFF_THRESHOLD = int(config["CONSTANTS"]["TIME_DIFF_THRESHOLD"])
DATA_LOSE_THRESHOLD = int(config["CONSTANTS"]["DATA_LOSE_THRESHOLD"])
# Concatenated chunk size (in seconds)
CHUNK_SIZE = int(config["CONSTANTS"]["CONCAT_TIME"])
# Individual packet length (in seconds)
UNIT_SIZE = int(config["CONSTANTS"]["UNIT_SIZE"])

SPS = int(config["CONSTANTS"]["SPS"])

TIME_SAMPLES = SPS * UNIT_SIZE
SPACE_SAMPLES = int(config["CONSTANTS"]["SPACE_SAMPLES"])


# def get_dims(path_abs: str):
#     if isfile(path_abs):
#         with open(path_abs, "r") as attrs_json:
#             attrs: dict = json.load(attrs_json)
#             SPACE_SAMPLES = (attrs["index"][1] + 1) / attrs["down_factor_space"]
#             TIME_SAMPLES = (attrs["index"][1] + 1) / attrs["down_factor_time"]
#     else:
#         TIME_SAMPLES = SPS * UNIT_SIZE
#         SPACE_SAMPLES = SPS * UNIT_SIZE
#     return SPACE_SAMPLES, TIME_SAMPLES


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
