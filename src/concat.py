from concat.main import main
from log.main_logger import logger as log

try:
    main()
except Exception as err:
    msg = (
        "Unexpected error occurred during scheduled concatenation. Details:\n\n"
        + str(err)
    )
    log.exception(msg)
