from concat.main import main
from log.telegram import send_telegram_error
from log.logger import set_logger, set_console_logger

import traceback

log = set_logger("MAIN", global_concat_log=True)
set_console_logger(log, log_level="ERROR")

try:
    main()
except Exception:
    msg = (
        "Unexpected error occurred during scheduled concatenation. Details:\n\n"
        + traceback.format_exc()
    )
    send_telegram_error(msg)
    log.exception(msg)
