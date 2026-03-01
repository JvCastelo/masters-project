"""Tests for masters_project.processors.merger: GOES–SONDA dataset merge logic."""

import pandas as pd
import pytest

from masters_project.processors.merger import DatasetMerger


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def df_goes_with_timestamps() -> pd.DataFrame:
    """GOES DataFrame with UTC timestamps for merge happy path."""
    return pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2024-01-01 12:00:00", "2024-01-01 12:10:00", "2024-01-01 12:20:00"],
                utc=True,
            ),
            "rad_C01": [100.0, 101.0, 102.0],
        }
    )


@pytest.fixture
def df_sonda_with_timestamps() -> pd.DataFrame:
    """SONDA DataFrame with matching timestamps for merge happy path."""
    return pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2024-01-01 12:00:00", "2024-01-01 12:10:00"],
                utc=True,
            ),
            "glo_avg": [500.0, 510.0],
        }
    )


@pytest.fixture
def df_sonda_no_overlap() -> pd.DataFrame:
    """SONDA DataFrame with timestamps that do not overlap GOES (merge yields 0 rows)."""
    return pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2024-01-02 00:00:00", "2024-01-02 00:10:00"],
                utc=True,
            ),
            "glo_avg": [600.0, 610.0],
        }
    )


# =============================================================================
# Tests
# =============================================================================


def test_merge_goes_sonda_matching_timestamps_returns_merged_dataframe(
    df_goes_with_timestamps: pd.DataFrame,
    df_sonda_with_timestamps: pd.DataFrame,
) -> None:
    """Happy path: inner join on matching timestamps produces merged DataFrame with expected shape."""
    result = DatasetMerger.merge_goes_sonda(
        df_goes=df_goes_with_timestamps,
        df_sonda=df_sonda_with_timestamps,
    )
    assert len(result) == 2
    assert "rad_C01" in result.columns and "glo_avg" in result.columns
    assert result["glo_avg"].tolist() == [500.0, 510.0]


def test_merge_goes_sonda_no_matching_timestamps_raises_valueerror(
    df_goes_with_timestamps: pd.DataFrame,
    df_sonda_no_overlap: pd.DataFrame,
) -> None:
    """Critical failure: empty inner merge must raise ValueError to prevent silent data loss."""
    with pytest.raises(ValueError, match="Merged dataset is empty"):
        DatasetMerger.merge_goes_sonda(
            df_goes=df_goes_with_timestamps,
            df_sonda=df_sonda_no_overlap,
        )


def test_merge_goes_sonda_empty_goes_dataframe_raises_valueerror(
    df_sonda_with_timestamps: pd.DataFrame,
) -> None:
    """Edge case: merging with empty GOES DataFrame yields 0 rows and must raise ValueError."""
    df_goes_empty = pd.DataFrame(columns=["timestamp", "rad_C01"])
    with pytest.raises(ValueError, match="Merged dataset is empty"):
        DatasetMerger.merge_goes_sonda(
            df_goes=df_goes_empty,
            df_sonda=df_sonda_with_timestamps,
        )


@pytest.mark.parametrize(
    ("time_col_goes", "time_col_sonda"),
    [
        ("timestamp", "timestamp"),
        ("ts_goes", "ts_sonda"),
    ],
    ids=["default_columns", "custom_columns"],
)
def test_merge_goes_sonda_custom_time_columns_merges_correctly(
    time_col_goes: str,
    time_col_sonda: str,
) -> None:
    """Parametrized: default and custom timestamp column names both produce correct merge."""
    df_goes = pd.DataFrame(
        {time_col_goes: pd.to_datetime(["2024-01-01 12:00:00"], utc=True), "x": [1]}
    )
    df_sonda = pd.DataFrame(
        {time_col_sonda: pd.to_datetime(["2024-01-01 12:00:00"], utc=True), "y": [2]}
    )
    result = DatasetMerger.merge_goes_sonda(
        df_goes=df_goes,
        df_sonda=df_sonda,
        time_col_goes=time_col_goes,
        time_col_sonda=time_col_sonda,
    )
    assert len(result) == 1
    assert result["x"].iloc[0] == 1 and result["y"].iloc[0] == 2


def test_merge_goes_sonda_invalid_timestamp_format_raises_exception() -> None:
    """Failure mode: unparseable timestamp strings cause pd.to_datetime to raise, re-raised by merger."""
    df_goes = pd.DataFrame({"timestamp": ["not-a-date"], "x": [1]})
    df_sonda = pd.DataFrame({"timestamp": ["2024-01-01 12:00:00"], "y": [2]})
    with pytest.raises(Exception):
        DatasetMerger.merge_goes_sonda(
            df_goes=df_goes,
            df_sonda=df_sonda,
        )
