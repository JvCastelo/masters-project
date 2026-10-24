"""Tests for masters_project.etl.goes.client: GoesS3Client listing and partial opens."""

from collections.abc import Generator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest

from masters_project.etl.goes.client import GoesS3Client

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


def _key(channel: str, hour: str = "00") -> str:
    return (
        f"noaa-goes16/ABI-L1b-RadC/2019/001/{hour}/"
        f"OR_ABI-L1b-RadC-M6{channel}_G16_s20190010000206_e20190010009514_c20190010009567.nc"
    )


def test_list_files_by_channel_splits_one_listing_per_day_by_channel(
    mock_goes_s3_client: GoesS3Client,
) -> None:
    """One glob per day (all channels), then keys are split by the channel in the file name."""
    mock_goes_s3_client.fs.glob.return_value = [
        _key("C01"),
        _key("C03"),
        _key("C03", "01"),
        _key("C13"),
    ]
    result = mock_goes_s3_client.list_files_by_channel(
        channels=["C03", "C01", "C05"],
        start_date=date(2019, 1, 1),
        end_date=date(2019, 1, 1),
    )
    mock_goes_s3_client.fs.glob.assert_called_once_with(
        "noaa-goes16/ABI-L1b-RadC/2019/001/*/*.nc"
    )
    assert result == {
        "C03": [_key("C03"), _key("C03", "01")],
        "C01": [_key("C01")],
        "C05": [],
    }


def test_list_files_by_channel_start_after_end_raises_valueerror(
    mock_goes_s3_client: GoesS3Client,
) -> None:
    with pytest.raises(ValueError, match="start_date cannot be after end_date"):
        mock_goes_s3_client.list_files_by_channel(
            ["C01"], start_date="2024-01-15", end_date="2024-01-10"
        )


def test_list_files_by_channel_caches_closed_days_and_skips_s3_next_time(
    mock_goes_s3_client: GoesS3Client, tmp_path: Path
) -> None:
    """A past day is listed once; the second call is served from the on-disk listing."""
    mock_goes_s3_client.fs.glob.return_value = [_key("C03")]
    kwargs = {
        "channels": ["C03"],
        "start_date": date(2019, 1, 1),
        "end_date": date(2019, 1, 1),
        "cache_dir": tmp_path,
    }
    first = mock_goes_s3_client.list_files_by_channel(**kwargs)
    second = mock_goes_s3_client.list_files_by_channel(**kwargs)

    assert first == second == {"C03": [_key("C03")]}
    assert mock_goes_s3_client.fs.glob.call_count == 1
    assert (tmp_path / "2019" / "001.json").exists()


def test_list_files_by_channel_does_not_cache_today_empty_or_failed_listings(
    mock_goes_s3_client: GoesS3Client, tmp_path: Path
) -> None:
    """Today's listing still grows, and an empty/failed listing must not be remembered."""
    today = datetime.now(UTC).date()

    mock_goes_s3_client.fs.glob.return_value = [_key("C03")]
    mock_goes_s3_client.list_files_by_channel(["C03"], today, today, cache_dir=tmp_path)

    old = today - timedelta(days=30)
    mock_goes_s3_client.fs.glob.return_value = []
    mock_goes_s3_client.list_files_by_channel(["C03"], old, old, cache_dir=tmp_path)

    mock_goes_s3_client.fs.glob.side_effect = OSError("network down")
    older = today - timedelta(days=31)
    result = mock_goes_s3_client.list_files_by_channel(
        ["C03"], older, older, cache_dir=tmp_path
    )

    assert result == {"C03": []}
    assert list(tmp_path.rglob("*.json")) == []


def test_list_files_by_channel_only_days_skips_other_days(
    mock_goes_s3_client: GoesS3Client,
) -> None:
    """Days outside ``only_days`` are not even requested from S3."""
    mock_goes_s3_client.fs.glob.return_value = []
    mock_goes_s3_client.list_files_by_channel(
        ["C03"],
        date(2019, 1, 1),
        date(2019, 1, 5),
        only_days={date(2019, 1, 2), date(2019, 1, 4), date(2020, 1, 1)},
    )
    patterns = [c.args[0] for c in mock_goes_s3_client.fs.glob.call_args_list]
    assert sorted(patterns) == [
        "noaa-goes16/ABI-L1b-RadC/2019/002/*/*.nc",
        "noaa-goes16/ABI-L1b-RadC/2019/004/*/*.nc",
    ]


def test_open_partial_uses_a_block_cache_so_only_needed_ranges_are_fetched(
    mock_goes_s3_client: GoesS3Client,
) -> None:
    """Regression: the s3fs default read 402 MB of a 47 MB file; 512 KB blocks read ~10%."""
    mock_goes_s3_client.open_partial("noaa-goes16/ABI-L1b-RadC/2019/001/12/file.nc")

    mock_goes_s3_client.fs.open.assert_called_once_with(
        "s3://noaa-goes16/ABI-L1b-RadC/2019/001/12/file.nc",
        cache_type="blockcache",
        block_size=512 * 1024,
    )
