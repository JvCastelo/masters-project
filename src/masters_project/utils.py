import logging
import time
import tracemalloc
from datetime import date, datetime
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

        logger.info(
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


def get_target_years(start_date: str | date, end_date: str | date) -> list[int]:
    """
    Converts start and end dates into a unique list of years.
    Example: '2023-11-01' to '2024-02-01' -> [2023, 2024]
    """
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    return list(range(start_date.year, end_date.year + 1))


def get_target_year_months(
    start_date: str | date, end_date: str | date
) -> list[tuple[int, int]]:
    """
    Converts start and end dates into a sequential list of tuples.
    Example: '2023-11-15' to '2024-02-10' -> [(2023, 11), (2023, 12), (2024, 1), (2024, 2)]
    """
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    year_months = []
    current_year = start_date.year
    current_month = start_date.month

    while (current_year < end_date.year) or (
        current_year == end_date.year and current_month <= end_date.month
    ):
        year_months.append((current_year, current_month))

        current_month += 1
        if current_month > 12:
            current_month = 1
            current_year += 1

    return year_months
