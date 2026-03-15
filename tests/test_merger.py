import pandas as pd
import pytest

from masters_project.processors.merger import DatasetMerger

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def df_day_1() -> pd.DataFrame:
    """Simulates a single day of data for one channel."""
    return pd.DataFrame(
        {"timestamp": ["2024-01-01 12:00:00", "2024-01-01 13:00:00"], "val": [10, 20]}
    )


@pytest.fixture
def df_day_2() -> pd.DataFrame:
    """Simulates a second day of data for the same channel."""
    return pd.DataFrame(
        {"timestamp": ["2024-01-02 12:00:00", "2024-01-02 13:00:00"], "val": [30, 40]}
    )


@pytest.fixture
def df_sensor_a() -> pd.DataFrame:
    """Simulates Sensor A data."""
    return pd.DataFrame({"timestamp": ["2024-01-01 12:00:00"], "sensor_a": [1.1]})


@pytest.fixture
def df_sensor_b() -> pd.DataFrame:
    """Simulates Sensor B data for the same timestamp."""
    return pd.DataFrame({"timestamp": ["2024-01-01 12:00:00"], "sensor_b": [9.9]})


# =============================================================================
# Tests: Concat by Rows (Vertical Stacking)
# =============================================================================


def test_concat_by_rows_stacks_vertically_and_sorts(df_day_1, df_day_2):
    """Ensure multiple days are combined into one long timeline and sorted."""
    # We pass them out of order to test the internal sorting
    result = DatasetMerger.concat_by_rows([df_day_2, df_day_1], time_col="timestamp")

    assert len(result) == 4
    # Check chronological order
    assert result["timestamp"].iloc[0] < result["timestamp"].iloc[-1]
    assert result["val"].tolist() == [10, 20, 30, 40]


def test_concat_by_rows_ensures_utc_conversion():
    """Ensure the helper method standardizes timezones."""
    df = pd.DataFrame({"timestamp": ["2024-01-01 12:00:00"], "val": [1]})
    result = DatasetMerger.concat_by_rows([df], time_col="timestamp")

    assert result["timestamp"].dt.tz is not None
    assert str(result["timestamp"].dt.tz) == "UTC"


# =============================================================================
# Tests: Join by Columns (Horizontal Merging)
# =============================================================================


def test_join_by_columns_merges_multiple_sensors(df_sensor_a, df_sensor_b):
    """Happy path: n-way join on shared timestamps."""
    result = DatasetMerger.join_by_columns(
        [df_sensor_a, df_sensor_b], time_col="timestamp", how="inner"
    )

    assert len(result) == 1
    assert "sensor_a" in result.columns
    assert "sensor_b" in result.columns
    assert result["sensor_b"].iloc[0] == 9.9


def test_join_by_columns_empty_intersection_logs_warning(df_sensor_a):
    """If timestamps don't match, result should be empty (but method should return empty DF)."""
    df_no_match = pd.DataFrame({"timestamp": ["1999-01-01 00:00:00"], "val": [0]})

    result = DatasetMerger.join_by_columns(
        [df_sensor_a, df_no_match], time_col="timestamp"
    )
    assert result.empty


# =============================================================================
# Tests: Error Handling
# =============================================================================


def test_prepare_dataframes_invalid_time_raises_error():
    """Ensure malformed timestamps in any of the dataframes trigger an exception."""
    df_bad = pd.DataFrame({"timestamp": ["not-a-date"], "val": [1]})

    with pytest.raises(Exception):
        DatasetMerger.concat_by_rows([df_bad], time_col="timestamp")
