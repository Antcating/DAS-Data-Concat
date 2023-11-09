from concat.main import main
from log.telegram import send_telegram_error

import traceback


try:
    main()
except Exception:
    msg = (
        "Error occurred during scheduled concatenation. Details:\n\n"
        + traceback.format_exc()
    )
    send_telegram_error(msg)
