"""Process-pool worker for the GOES ETL.

Kept in its own module, free of import-time side effects (no logging setup, no client
at module level): with the ``forkserver``/``spawn`` start methods each worker process
re-imports the function's module, and re-running the ETL module's top-level code there
would reconfigure logging and open S3 clients in every process.
"""

import gc
from dataclasses import dataclass

import numpy as np
import pandas as pd

from masters_project.etl.goes.client import GoesS3Client
from masters_project.etl.goes.reader import GoesProcessor

_client: GoesS3Client | None = None


@dataclass(frozen=True)
class ExtractionJob:
    """Everything a worker needs to extract the windows of one GOES file."""

    product_name: str
    bucket_name: str
    variable: str
    radius: int
    pixels: dict[str, tuple[int, int]]


@dataclass(frozen=True)
class FileExtraction:
    """Result of processing one file: windows per station, or an error message."""

    source_key: str
    timestamp: pd.Timestamp | None = None
    windows: dict[str, np.ndarray] | None = None
    error: str | None = None


def _get_client(job: ExtractionJob) -> GoesS3Client:
    """Return this process's S3 client, creating it on first use."""
    global _client
    if _client is None:
        _client = GoesS3Client(
            product_name=job.product_name, bucket_name=job.bucket_name
        )
    return _client


def extract_file(path: str, job: ExtractionJob) -> FileExtraction:
    """Read one GOES file with partial reads and extract the window of every station.

    Exceptions are returned as ``FileExtraction.error`` instead of raised: worker
    processes have no logging handlers, so the parent process logs the failure.

    Args:
        path: S3 object key of the NetCDF file.
        job: Extraction parameters shared by all files of the run.

    Returns:
        A ``FileExtraction`` with the timestamp and windows, or with ``error`` set.
    """
    try:
        with _get_client(job).open_partial(path) as file:
            timestamp, windows = GoesProcessor.read_windows(
                file, job.variable, job.radius, job.pixels
            )
        return FileExtraction(source_key=path, timestamp=timestamp, windows=windows)
    except Exception as e:  # noqa: BLE001
        return FileExtraction(source_key=path, error=f"{type(e).__name__}: {e}")
    finally:
        # The fsspec block cache of the closed file sits in a reference cycle. Its blocks
        # are big objects that barely move Python's allocation thresholds, so the automatic
        # collector lets them pile up (~3.6 MB per file, measured), and 24 workers reading
        # hundreds of thousands of files ran out of memory. Collecting after each file keeps
        # a worker flat at ~165 MB.
        gc.collect()
