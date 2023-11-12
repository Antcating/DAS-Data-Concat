import logging
from logging.handlers import TimedRotatingFileHandler
import os

from config import SAVE_PATH

# Logging
_LOG_LEVEL_STRINGS = ["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"]
LOCAL_LOG_FORMATTER = logging.Formatter(
    fmt="%(asctime)s.%(msecs)03d | %(name)s | %(levelname)s | %(message)s ",
    datefmt="%Y-%m-%dT%H:%M:%S",
)

LOG_MESSAGE = "{working_dir} | {file} | {msg}"
LOG_LEVEL = "INFO"


class Logger:
    def __init__(self, name: str = None) -> None:
        logging.basicConfig(level=logging.DEBUG)

        self.name = name
        self.log = self.get_logger(name)
        self.handlers: dict = {}

    def get_logger(self, name: str = None):
        return logging.getLogger(name)

    def get_instance(self):
        return self.log

    def attach_file_logger(
        self, handler_name: str = None, level: str = "DEBUG", filename: str = None
    ):
        if filename is None:
            logging.error("Filename for logging is not provided")
            return
        fh = TimedRotatingFileHandler(filename=filename, encoding="UTF-8", when="D")
        fh.setLevel(level=level)
        fh.setFormatter(LOCAL_LOG_FORMATTER)

        self.handlers[handler_name] = fh
        self.log.addHandler(fh)

        return handler_name

    def attach_console_logger(self, handler_name: str = None, level: str = "DEBUG"):
        ch = logging.StreamHandler()
        ch.setLevel(level=level)
        ch.setFormatter(LOCAL_LOG_FORMATTER)

        self.handlers[handler_name] = ch
        self.log.addHandler(ch)

        return handler_name

    def detach_handler(self, handler_name: str = None):
        try:
            handler = self.handlers[handler_name]
            self.log.removeHandler(handler)
            self.log.debug("Log handler successfully removed")
        except KeyError:
            self.log.error(
                "Unable to delete log handler: no handler with provided name"
            )


def get_global_logger(level: str = "DEBUG"):
    logger = Logger("CONCAT")
    logger.attach_file_logger(
        "global_concat_logger", level=level, filename=os.path.join(SAVE_PATH, "log")
    )
    logger.attach_console_logger("global_console_logger", level=level)
    return logger, logger.get_instance()


def compose_log_message(
    working_dir: str = None, file: str = None, message: str = None
) -> str:
    return LOG_MESSAGE.format(working_dir=working_dir, file=file, msg=message)


logger, log = get_global_logger(level="DEBUG")
