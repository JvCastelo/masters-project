"""Small shared helpers."""

import hashlib
import logging
import time
from collections.abc import Callable
from datetime import UTC, datetime
from functools import wraps
from pathlib import Path
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


def file_sha256(path: Path) -> str:
    """SHA-256 of a file, read in blocks so a large CSV is never loaded whole."""
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def utc_now_iso() -> str:
    """Current time as an ISO-8601 UTC string, to stamp metadata."""
    return datetime.now(UTC).isoformat(timespec="seconds")
