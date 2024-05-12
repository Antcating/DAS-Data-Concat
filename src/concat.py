from concat.main import Concatenator
from log.main_logger import logger as log
import sys


try:
    log.info("Scheduled concatenation started.")
    if len(sys.argv) > 1:
        if "--num_threads" in sys.argv:
            num_threads = int(sys.argv[sys.argv.index("--num_threads") + 1])
            Concatenator(num_threads=num_threads)
    else:
        # Use default number of threads (4).
        # Calculated to be optimal for the current system (see test/test_num_of_threads.py)
        Concatenator()
except Exception as err:
    msg = (
        "Unexpected error occurred during scheduled concatenation. Details:\n\n"
        + str(err)
    )
    log.exception(msg)
