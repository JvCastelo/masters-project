"""One log file per pipeline run, with a header that says what produced it.

Every pipeline entry point wraps its work in :func:`run_logging`::

    with run_logging("etl_goes", period="2022-01-01..2022-12-31") as run:
        ...
        run.exit_code = 0

That writes ``data/logs/<pipeline>/<YYYY-MM-DD_HHMMSS>.log`` (``latest.log`` links to the newest
one), prints the same records to the console, and brackets the run with a header (command, git
revision, library versions, config, the summary you pass) and a footer (status, duration, exit
code). An unhandled exception is logged at CRITICAL with its traceback before it propagates.

Level convention used across the code base:

* ``INFO``: milestones of a stage (what was found, written or skipped, and how many).
* ``DEBUG``: per-file or per-period detail; only written when ``etl_config.log_level`` is DEBUG.
* ``WARNING``: recoverable data problems (a gap in the source, a defective sensor, dropped rows).
* ``ERROR``: one unit (a station, a day, a file) failed and the run goes on.
* ``logger.exception``: an ERROR with its traceback.

The handlers are added on entry and removed on exit; the root logger is never reset, so this
composes with other handlers (for example pytest's ``caplog``).
"""

import logging
import subprocess
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from importlib.metadata import version
from logging.handlers import RotatingFileHandler
from pathlib import Path

from tqdm.contrib.logging import logging_redirect_tqdm

from masters_project.settings import settings

logger = logging.getLogger(__name__)

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
# A run that logs at DEBUG for hours must not fill the disk: rotate within the run.
MAX_BYTES = 10 * 1024 * 1024
BACKUP_COUNT = 5
LATEST = "latest.log"

# Chatty at INFO and below; their records add nothing to a pipeline log.
NOISY_LIBRARIES = (
    "botocore",
    "urllib3",
    "s3fs",
    "h5netcdf",
    "fsspec",
    "httpx",
    "httpcore",
)


@dataclass
class RunLog:
    """Handle yielded by :func:`run_logging`."""

    pipeline: str
    path: Path
    #: Set by the entry point before leaving the block; ``None`` counts as success.
    exit_code: int | None = None


def new_log_path(pipeline: str) -> Path:
    """Path of this run's log: ``<LOG_PATH>/<pipeline>/<timestamp>.log`` (numbered if taken)."""
    directory = settings.LOG_PATH / pipeline
    directory.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    path, attempt = directory / f"{stamp}.log", 1
    while path.exists():
        attempt += 1
        path = directory / f"{stamp}-{attempt}.log"
    return path


def git_revision() -> str:
    """Short commit of the checkout, ``-dirty`` if tracked files changed; ``unknown`` without git."""
    try:
        run = {"cwd": settings.PROJECT_ROOT, "capture_output": True, "text": True}
        commit = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], check=True, timeout=5, **run
        ).stdout.strip()
        changed = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=no"],
            check=True,
            timeout=5,
            **run,
        ).stdout.strip()
    except OSError, subprocess.SubprocessError:
        return "unknown"
    return f"{commit}-dirty" if changed else commit


def _point_latest_at(path: Path) -> None:
    link = path.parent / LATEST
    try:
        link.unlink(missing_ok=True)
        link.symlink_to(path.name)
    except OSError:  # filesystems without symlinks: the dated file is still there
        logger.debug("Could not update %s", link)


def _format_duration(seconds: float) -> str:
    hours, rest = divmod(int(seconds), 3600)
    minutes, secs = divmod(rest, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def _log_header(pipeline: str, path: Path, summary: dict[str, object]) -> None:
    logger.info("Run started: pipeline=%s", pipeline)
    logger.info("Command: %s", " ".join(sys.argv))
    logger.info(
        "Revision: %s | Python %s | pandas %s | numpy %s",
        git_revision(),
        sys.version.split()[0],
        version("pandas"),
        version("numpy"),
    )
    logger.info("Config: %s", settings.PIPELINE_CONFIG_FILE)
    logger.info("Log: %s", path)
    for key, value in summary.items():
        logger.info("  %s: %s", key, value)


@contextmanager
def run_logging(pipeline: str, **summary: object) -> Iterator[RunLog]:
    """Log everything inside the block to the console and to this run's own file.

    Args:
        pipeline: Short name of the pipeline, also the log sub-directory (``etl_goes``, ``ml_train``...).
        **summary: Facts worth keeping with the log (period, stations, thresholds), one line each.

    Yields:
        A :class:`RunLog`; set ``exit_code`` on it before leaving the block.
    """
    run = RunLog(pipeline, new_log_path(pipeline))
    level = logging.DEBUG if settings.etl.log_level.upper() == "DEBUG" else logging.INFO

    formatter = logging.Formatter(LOG_FORMAT, DATE_FORMAT)
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(settings.etl.log_level)
    console.setFormatter(formatter)
    file_handler = RotatingFileHandler(
        run.path, maxBytes=MAX_BYTES, backupCount=BACKUP_COUNT, encoding="utf-8"
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)

    root = logging.getLogger()
    previous_level = root.level
    previous_libraries = {
        name: logging.getLogger(name).level for name in NOISY_LIBRARIES
    }
    root.setLevel(logging.DEBUG)
    root.addHandler(console)
    root.addHandler(file_handler)
    for name in NOISY_LIBRARIES:
        logging.getLogger(name).setLevel(logging.WARNING)
    _point_latest_at(run.path)

    started = time.monotonic()
    status = "failed"
    try:
        with logging_redirect_tqdm():
            _log_header(pipeline, run.path, summary)
            yield run
        status = "ok" if run.exit_code in (None, 0) else "failed"
    except KeyboardInterrupt:
        status = "interrupted"
        raise
    except Exception:
        logger.critical("Run aborted by an unhandled exception.", exc_info=True)
        raise
    finally:
        logger.log(
            logging.INFO if status == "ok" else logging.ERROR,
            "Run finished: status=%s duration=%s exit_code=%s",
            status,
            _format_duration(time.monotonic() - started),
            run.exit_code,
        )
        root.removeHandler(console)
        root.removeHandler(file_handler)
        file_handler.close()
        root.setLevel(previous_level)
        for name, library_level in previous_libraries.items():
            logging.getLogger(name).setLevel(library_level)
