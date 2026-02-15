from unittest.mock import patch

import pytest

from masters_project.clients.goes_s3 import GoesS3Client


@pytest.fixture
def mock_goes_s3_client():
    """
    Provides a GoesS3Client with a mocked S3 filesystem.
    """
    with patch("s3fs.S3FileSystem") as mock_fs:
        client = GoesS3Client(product_name="ABI-L1b-RadF", bucket_name="noaa-goes16")
        client.fs = mock_fs.return_value
        yield client


def test_initialization_goes_s3(mock_goes_s3_client):
    """
    Check if attributes are assigned correctly.
    """
    assert mock_goes_s3_client.product_name == "ABI-L1b-RadF"
    assert mock_goes_s3_client.bucket_name == "noaa-goes16"


def test_invalid_day_range_raises_error_goes_s3(mock_goes_s3_client):
    """
    Test that start_day > end_day raises ValueError.
    """
    with pytest.raises(ValueError, match="Invalid range"):
        mock_goes_s3_client.get_files_path_by_day(start_day=100, end_day=50, year=2023)


def test_leap_year_validation_goes_s3(mock_goes_s3_client):
    """
    Test that day 366 is rejected for non-leap years.
    """
    with pytest.raises(ValueError, match="not a leap year"):
        mock_goes_s3_client.get_files_path_by_day(start_day=366, end_day=366, year=2023)


def test_glob_called_correctly_goes_s3(mock_goes_s3_client):
    """
    Verify that the S3 path string is built correctly before calling glob.
    """

    mock_goes_s3_client.get_files_path_by_day(start_day=1, end_day=1, year=2019)
    expected_pattern = "noaa-goes16/ABI-L1b-RadF/2019/001/*/*.nc"
    mock_goes_s3_client.fs.glob.assert_called_with(expected_pattern)


def test_get_file_uri_format_goes_s3(mock_goes_s3_client):
    """
    Verify get_file add the s3://prefix.
    """
    file_path = "bucket/prod/year/day/file.nc"
    mock_goes_s3_client.get_file(file_path)
    mock_goes_s3_client.fs.open.assert_called_with(f"s3://{file_path}")
