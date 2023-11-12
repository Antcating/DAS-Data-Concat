from concat.main import main
from log.telegram import send_telegram_error
from log.logger import log
import traceback

try:
    main()
except Exception:
    msg = (
        "Unexpected error occurred during scheduled concatenation. Details:\n\n"
        + traceback.format_exc()
    )
    send_telegram_error(msg)
    log.exception(msg)
