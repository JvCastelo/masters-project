"""Tests for the GOES process-pool worker."""

from unittest.mock import MagicMock, patch

from masters_project.etl.goes import worker
from masters_project.etl.goes.worker import ExtractionJob, extract_file

JOB = ExtractionJob("ABI-L1b-RadF", "noaa-goes16", "Rad", 1, {"A": (5, 5)})


def test_extract_file_collects_garbage_after_every_file_even_on_error() -> None:
    """Regression: worker memory grew ~3.6 MB per file until the OOM killer stopped the run."""
    client = MagicMock()
    client.open_partial.side_effect = OSError("network down")

    with (
        patch.object(worker, "_get_client", return_value=client),
        patch.object(worker.gc, "collect") as collect,
    ):
        result = extract_file("noaa-goes16/x.nc", JOB)

    assert result.error == "OSError: network down"
    collect.assert_called_once()


def test_extract_file_collects_garbage_after_success() -> None:
    client = MagicMock()
    file = client.open_partial.return_value.__enter__.return_value

    with (
        patch.object(worker, "_get_client", return_value=client),
        patch.object(
            worker.GoesProcessor,
            "read_windows",
            return_value=("ts", {"A": [1.0]}),
        ) as read,
        patch.object(worker.gc, "collect") as collect,
    ):
        result = extract_file("noaa-goes16/x.nc", JOB)

    read.assert_called_once_with(file, "Rad", 1, {"A": (5, 5)})
    assert result.error is None and result.timestamp == "ts"
    collect.assert_called_once()


def test_each_process_creates_its_s3_client_only_once() -> None:
    worker._client = None
    try:
        with patch.object(worker, "GoesS3Client") as client_class:
            first = worker._get_client(JOB)
            second = worker._get_client(JOB)
    finally:
        worker._client = None

    assert first is second
    client_class.assert_called_once_with(
        product_name="ABI-L1b-RadF", bucket_name="noaa-goes16"
    )
