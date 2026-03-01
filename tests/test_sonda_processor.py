"""Tests for masters_project.processors: SondaProcessor (extract, load, format)."""

from pathlib import Path
from zipfile import BadZipFile, ZipFile

import pandas as pd
import pytest

from masters_project.processors.sonda import SondaProcessor


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def valid_zip_path(tmp_path: Path) -> Path:
    """Create a minimal valid ZIP containing a .dat file for SondaProcessor tests."""
    zip_path = tmp_path / "sonda_2023.zip"
    dat_content = """skip
timestamp,glo_avg,acronym,year,day,min
2023-01-01 00:00:00,100.5,x,2023,1,0
2023-01-01 00:10:00,105.2,x,2023,1,10"""
    dat_path = tmp_path / "station_2023.dat"
    dat_path.write_text(dat_content)
    with ZipFile(zip_path, "w") as zf:
        zf.write(dat_path, dat_path.name)
    dat_path.unlink()
    return zip_path


@pytest.fixture
def zip_without_dat(tmp_path: Path) -> Path:
    """ZIP containing no .dat file to trigger FileNotFoundError."""
    zip_path = tmp_path / "empty.zip"
    with ZipFile(zip_path, "w") as zf:
        zf.writestr("readme.txt", "no dat here")
    return zip_path


@pytest.fixture
def raw_sonda_dataframe() -> pd.DataFrame:
    """Raw DataFrame mimicking create_dataframe output (with header row and columns to drop)."""
    return pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2023-01-01 00:00:00", "2023-01-01 00:10:00"]),
            "glo_avg": ["100.5", "105.2"],
            "acronym": ["x", "x"],
            "year": [2023, 2023],
            "day": [1, 1],
            "min": [0, 10],
        }
    )


# =============================================================================
# Tests
# =============================================================================


def test_sonda_processor_extract_zip_valid_zip_returns_extraction_dir(
    valid_zip_path: Path,
) -> None:
    """Happy path: valid ZIP is extracted to a directory with the same base name."""
    result = SondaProcessor.extract_zip(valid_zip_path, delete_zip=True)
    assert result == valid_zip_path.with_suffix("")
    assert result.exists()
    assert any(result.glob("*.dat"))
    assert not valid_zip_path.exists()


def test_sonda_processor_extract_zip_corrupted_file_raises_badzipfile(
    tmp_path: Path,
) -> None:
    """Failure mode: corrupted or non-ZIP file must raise BadZipFile."""
    bad_zip = tmp_path / "corrupt.zip"
    bad_zip.write_bytes(b"not a zip file")
    with pytest.raises(BadZipFile):
        SondaProcessor.extract_zip(bad_zip)


def test_sonda_processor_create_dataframe_missing_dat_raises_filenotfounderror(
    zip_without_dat: Path,
) -> None:
    """Critical failure: no .dat file in extraction dir must raise FileNotFoundError."""
    extraction_dir = SondaProcessor.extract_zip(zip_without_dat, delete_zip=False)
    with pytest.raises(FileNotFoundError, match="Missing .dat file"):
        SondaProcessor.create_dataframe(extraction_dir)


def test_sonda_processor_create_dataframe_valid_dat_returns_dataframe(
    valid_zip_path: Path,
) -> None:
    """Happy path: valid .dat file is loaded into a DataFrame with timestamp column."""
    extraction_dir = SondaProcessor.extract_zip(valid_zip_path, delete_zip=False)
    df = SondaProcessor.create_dataframe(extraction_dir)
    assert isinstance(df, pd.DataFrame)
    assert "timestamp" in df.columns
    assert len(df) >= 1
    assert not extraction_dir.exists()


def test_sonda_processor_format_dataframe_drops_columns_and_localizes_utc(
    raw_sonda_dataframe: pd.DataFrame,
) -> None:
    """Happy path: format_dataframe drops unused columns, coerces glo_avg, and localizes timestamps to UTC."""
    result = SondaProcessor.format_dataframe(raw_sonda_dataframe)
    assert "acronym" not in result.columns
    assert "year" not in result.columns
    assert "glo_avg" in result.columns
    assert pd.api.types.is_numeric_dtype(result["glo_avg"])
    assert result["timestamp"].dt.tz is not None
