import logging
import time
import tracemalloc
from functools import wraps

from masters_project.config import settings

settings.setup_logging()

logger = logging.getLogger(__name__)


def time_track(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()

        result = func(*args, **kwargs)

        elapsed_time = time.perf_counter() - start_time

        logger.debug(
            f"Function '{func.__name__}' took {elapsed_time:.4f} seconds to run."
        )
        return result

    return wrapper


def measure_memory(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        tracemalloc.start()

        result = func(*args, **kwargs)

        current, peak = tracemalloc.get_traced_memory()

        tracemalloc.stop()

        logger.info(
            f"Memory for '{func.__name__}': "
            f"Peak: {peak / 10**6:.2f} MB | "
            f"Remaining: {current / 10**6:.2f} MB"
        )
        return result

    return wrapper
