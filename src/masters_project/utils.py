import logging
import threading
import time
import tracemalloc
from datetime import date, datetime
from functools import wraps

logger = logging.getLogger(__name__)

_memory_lock = threading.Lock()


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
        with _memory_lock:
            if not tracemalloc.is_tracing():
                tracemalloc.start()

        result = func(*args, **kwargs)

        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        logger.debug(
            f"Memory for '{func.__name__}': "
            f"Peak: {peak / 10**6:.2f} MB | "
            f"Remaining: {current / 10**6:.2f} MB"
        )
        return result

    return wrapper


def _parse_date(date_input: str | date) -> date:
    """Helper to convert strings to date objects consistently."""
    if isinstance(date_input, str):
        return datetime.strptime(date_input, "%Y-%m-%d").date()
    return date_input


def get_target_years(start_date: str | date, end_date: str | date) -> list[int]:
    """
    Converts start and end dates into a unique list of years.
    Example: '2023-11-01' to '2024-02-01' -> [2023, 2024]
    """
    start = _parse_date(start_date)
    end = _parse_date(end_date)

    return list(range(start.year, end.year + 1))


def get_target_year_months(
    start_date: str | date, end_date: str | date
) -> list[tuple[int, int]]:
    """
    Converts start and end dates into a sequential list of tuples.
    Example: '2023-11-15' to '2024-02-10' -> [(2023, 11), (2023, 12), (2024, 1), (2024, 2)]
    """
    start = _parse_date(start_date)
    end = _parse_date(end_date)

    year_months = []
    current_year = start.year
    current_month = start.month

    while (current_year < end.year) or (
        current_year == end.year and current_month <= end.month
    ):
        year_months.append((current_year, current_month))

        current_month += 1
        if current_month > 12:
            current_month = 1
            current_year += 1

    return year_months
