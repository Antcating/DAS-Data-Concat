import logging
from logging.handlers import TimedRotatingFileHandler
from os.path import join

from config import SAVE_PATH

# Logging
_LOG_LEVEL_STRINGS = ["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"]
LOCAL_LOG_FORMATTER = logging.Formatter(
    fmt="%(asctime)s.%(msecs)03d | %(name)s | %(levelname)s | %(message)s ",
    datefmt="%Y-%m-%dT%H:%M:%S",
)

LOG_MESSAGE = "{working_dir} | {file} | {msg}"
LOG_LEVEL = "INFO"


def set_logger(
    name: str = None, global_concat_log: bool = False, global_log_level: str = "INFO"
) -> logging.Logger:
    log = logging.getLogger(name)
    log.setLevel(global_log_level)  # set at default log level
    if global_concat_log:
        set_file_logger(
            log=log, log_level=global_log_level, log_file=join(SAVE_PATH, "log")
        )
    return log


def compose_log_message(
    working_dir: str = None, file: str = None, message: str = None
) -> str:
    return LOG_MESSAGE.format(working_dir=working_dir, file=file, msg=message)


def set_console_logger(log: logging.Logger, log_level: str) -> None:
    """Sets up console logger

    Args:
        log (logging.Logger): Existing instance of logger to connect to.
        log_level (str): Console output log level.
    """
    ch = logging.StreamHandler()
    ch.setFormatter(LOCAL_LOG_FORMATTER)
    ch.setLevel(log_level)
    log.addHandler(ch)

    log.info("Console handler added successfully")


def set_file_logger(log: logging.Logger, log_level: str, log_file: str | None) -> None:
    """Sets up file logger

    Args:
        log (logging.Logger): Existing instance of logger to connect to.
        log_level (str): Console output log level.
        log_file (str | None, optional): Logger file output path.
    """
    fh = TimedRotatingFileHandler(log_file, when="midnight", utc=True)
    fh.setLevel(log_level)
    fh.setFormatter(LOCAL_LOG_FORMATTER)

    log.addHandler(fh)

    # log.info("File handler added successfully, level:", log_level)
