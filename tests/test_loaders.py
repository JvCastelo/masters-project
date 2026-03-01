"""Tests for masters_project.loaders: DataExporter, CSVExporter, ParquetExporter."""

from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from masters_project.loaders.csv import CSVExporter
from masters_project.loaders.parquet import ParquetExporter


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    """Non-empty DataFrame for export happy-path tests."""
    return pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})


@pytest.fixture
def empty_dataframe() -> pd.DataFrame:
    """Empty DataFrame for skip-on-empty behavior test."""
    return pd.DataFrame()


@pytest.fixture
def tmp_output_path(tmp_path: Path) -> Path:
    """Temporary directory for writing export output files."""
    return tmp_path / "output"


# =============================================================================
# Tests
# =============================================================================


def test_csv_exporter_export_non_empty_dataframe_writes_file(
    sample_dataframe: pd.DataFrame, tmp_output_path: Path
) -> None:
    """Happy path: CSVExporter writes a valid CSV file with expected content."""
    dest = tmp_output_path / "data.csv"
    CSVExporter().export(sample_dataframe, dest)
    assert dest.exists()
    loaded = pd.read_csv(dest)
    assert len(loaded) == 3
    assert list(loaded.columns) == ["a", "b"]


def test_csv_exporter_export_empty_dataframe_skips_without_error(
    empty_dataframe: pd.DataFrame, tmp_output_path: Path
) -> None:
    """Edge case: empty DataFrame must be skipped (no write, no exception) to avoid corrupt outputs."""
    dest = tmp_output_path / "empty.csv"
    CSVExporter().export(empty_dataframe, dest)
    assert not dest.exists()


def test_exporter_creates_parent_directory_when_missing(
    sample_dataframe: pd.DataFrame, tmp_path: Path
) -> None:
    """Base export logic creates parent directories; tested via CSVExporter (same for ParquetExporter)."""
    nested = tmp_path / "nested" / "dir" / "out.csv"
    CSVExporter().export(sample_dataframe, nested)
    assert nested.parent.exists()
    assert nested.exists()


@patch("pandas.DataFrame.to_parquet")
def test_parquet_exporter_export_calls_to_parquet_with_correct_args(
    mock_to_parquet: patch, sample_dataframe: pd.DataFrame, tmp_output_path: Path
) -> None:
    """Happy path: ParquetExporter delegates to df.to_parquet with destination and index=False."""
    dest = tmp_output_path / "data.parquet"
    dest.parent.mkdir(parents=True, exist_ok=True)
    ParquetExporter().export(sample_dataframe, dest)
    mock_to_parquet.assert_called_once_with(dest, index=False)


def test_csv_exporter_save_to_disk_called_with_correct_arguments(
    sample_dataframe: pd.DataFrame, tmp_output_path: Path
) -> None:
    """Verifies export delegates to _save_to_disk with the same df and destination."""
    dest = tmp_output_path / "delegate.csv"
    exporter = CSVExporter()
    with patch.object(exporter, "_save_to_disk", wraps=exporter._save_to_disk) as mock_save:
        exporter.export(sample_dataframe, dest)
        mock_save.assert_called_once()
        call_args = mock_save.call_args
        assert call_args[0][0].equals(sample_dataframe)
        assert call_args[0][1] == dest
