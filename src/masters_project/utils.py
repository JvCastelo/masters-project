"""Small shared helpers."""

import logging
import time
from collections.abc import Callable
from functools import wraps
from typing import Any

logger = logging.getLogger(__name__)


def time_track(func: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator that logs the elapsed time of the wrapped function.

    Args:
        func: The function to wrap (callable).

    Returns:
        The wrapper function that invokes func and logs execution time.
    """

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        start_time = time.perf_counter()

        result = func(*args, **kwargs)

        elapsed_time = time.perf_counter() - start_time

        logger.debug(
            f"Function '{func.__name__}' took {elapsed_time:.4f} seconds to run."
        )
        return result

    return wrapper
