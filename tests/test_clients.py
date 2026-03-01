"""Tests for masters_project.clients: GoesS3Client and SondaClient."""

from collections.abc import Generator
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from masters_project.clients.goes_s3 import GoesS3Client
from masters_project.clients.sonda import SondaClient


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mock_goes_s3_client() -> Generator[GoesS3Client, None, None]:
    """Provide a GoesS3Client with a mocked S3 filesystem for tests."""
    with patch("s3fs.S3FileSystem") as mock_fs:
        client = GoesS3Client(product_name="ABI-L1b-RadC", bucket_name="noaa-goes16")
        client.fs = mock_fs.return_value
        yield client


# =============================================================================
# GoesS3Client Tests
# =============================================================================


def test_goes_s3_client_initialization_sets_attributes_correctly(
    mock_goes_s3_client: GoesS3Client,
) -> None:
    """Happy path: client stores product_name and bucket_name as configured."""
    assert mock_goes_s3_client.product_name == "ABI-L1b-RadC"
    assert mock_goes_s3_client.bucket_name == "noaa-goes16"


def test_goes_s3_client_get_files_path_start_after_end_raises_valueerror(
    mock_goes_s3_client: GoesS3Client,
) -> None:
    """Critical failure: start_date > end_date must raise ValueError to prevent invalid queries."""
    with pytest.raises(ValueError, match="start_date cannot be after end_date"):
        mock_goes_s3_client.get_files_path(
            channel="C01",
            start_date="2024-01-15",
            end_date="2024-01-10",
        )


def test_goes_s3_client_get_files_path_calls_glob_with_correct_pattern(
    mock_goes_s3_client: GoesS3Client,
) -> None:
    """Happy path: get_files_path builds bucket/product/year/doy pattern and calls fs.glob per day."""
    mock_goes_s3_client.fs.glob.return_value = []
    mock_goes_s3_client.get_files_path(
        channel="C01",
        start_date=date(2019, 1, 1),
        end_date=date(2019, 1, 1),
    )
    expected_pattern = "noaa-goes16/ABI-L1b-RadC/2019/001/*/*C01*.nc"
    mock_goes_s3_client.fs.glob.assert_called_with(expected_pattern)


def test_goes_s3_client_get_file_opens_with_s3_prefix(
    mock_goes_s3_client: GoesS3Client,
) -> None:
    """Happy path: get_file prepends s3:// to the path before calling fs.open."""
    mock_goes_s3_client.get_file("noaa-goes16/ABI-L1b-RadC/2019/001/12/file.nc")
    mock_goes_s3_client.fs.open.assert_called_once_with(
        "s3://noaa-goes16/ABI-L1b-RadC/2019/001/12/file.nc"
    )


def test_sonda_client_invalid_station_raises_valueerror() -> None:
    """Critical failure: unknown station name must raise ValueError with valid options listed."""
    with pytest.raises(ValueError, match="not recognized"):
        SondaClient(station="INVALID_STATION", data_type="BSRN")
