from concat.main import Concatenator
from log.main_logger import logger as log

try:
    Concatenator().run()
except Exception as err:
    msg = (
        "Unexpected error occurred during scheduled concatenation. Details:\n\n"
        + str(err)
    )
    log.exception(msg)

