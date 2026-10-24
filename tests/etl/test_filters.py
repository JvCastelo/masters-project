"""Tests for masters_project.etl.filters: daytime and frozen-sensor filters."""

import logging

import pandas as pd
import pytest

from masters_project.etl.filters import drop_frozen, keep_daytime
from masters_project.etl.solar import solar_elevation

# Ourinhos (SP). On 2022-06-21 solar noon is ~15:20 UTC, so 15:00-16:00 UTC is full day
# and 03:00-04:00 UTC is the middle of the night.
LAT, LON = -23.0, -49.844
DAY = "2022-06-21 15:00"
NIGHT = "2022-06-21 03:00"


def _series(start: str, values: list[float | None]) -> pd.DataFrame:
    """One sample per minute starting at ``start`` (UTC)."""
    timestamps = pd.date_range(start, periods=len(values), freq="1min", tz="UTC")
    return pd.DataFrame({"timestamp": timestamps, "glo_avg": values})


def _varied(n: int, start: float = 100.0) -> list[float]:
    """``n`` strictly different values, so they never form a run."""
    return [start + i for i in range(n)]


# =============================================================================
# keep_daytime
# =============================================================================


def test_keep_daytime_drops_the_night_and_resets_the_index() -> None:
    df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2022-03-20 00:00", "2022-03-20 12:00", "2022-03-20 23:00"], utc=True
            ),
            "val": [1, 2, 3],
        }
    )
    kept = keep_daytime(df, 0.0, 0.0, min_elevation=10)
    assert kept["val"].tolist() == [2]
    assert kept.index.tolist() == [0]


def test_keep_daytime_with_no_threshold_returns_the_same_frame() -> None:
    """Failure mode: ``None`` must leave the dataset exactly as it was (filter disabled)."""
    df = _series(NIGHT, _varied(5))
    assert keep_daytime(df, LAT, LON, min_elevation=None) is df


def test_keep_daytime_threshold_is_strict() -> None:
    """Rows are kept only when the elevation is greater than the threshold."""
    df = _series(DAY, [1.0])
    noon = float(solar_elevation(df["timestamp"], LAT, LON)[0])
    assert len(keep_daytime(df, LAT, LON, noon - 0.01)) == 1
    assert len(keep_daytime(df, LAT, LON, noon)) == 0


def test_keep_daytime_logs_how_many_rows_were_kept(
    caplog: pytest.LogCaptureFixture,
) -> None:
    df = pd.concat([_series(NIGHT, _varied(10)), _series(DAY, _varied(10))])
    with caplog.at_level(logging.INFO):
        keep_daytime(df, LAT, LON, 5)
    assert "kept 10 of 20 rows" in caplog.text


# =============================================================================
# drop_frozen
# =============================================================================


def test_drop_frozen_removes_a_long_run_of_identical_daytime_values() -> None:
    values = [*_varied(5), *[500.0] * 6, *_varied(5, start=200.0)]
    df = _series(DAY, values)
    kept = drop_frozen(df, "glo_avg", min_run=5, lat=LAT, lon=LON)
    assert 500.0 not in kept["glo_avg"].tolist()
    assert len(kept) == len(df) - 6
    assert kept.index.tolist() == list(range(len(kept)))


def test_drop_frozen_keeps_a_run_shorter_than_the_threshold() -> None:
    df = _series(DAY, [*_varied(5), *[500.0] * 4, *_varied(5, start=200.0)])
    assert len(drop_frozen(df, "glo_avg", 5, LAT, LON)) == len(df)


def test_drop_frozen_run_exactly_as_long_as_the_threshold_is_removed() -> None:
    df = _series(DAY, [*_varied(3), *[500.0] * 5, *_varied(3, start=200.0)])
    assert len(drop_frozen(df, "glo_avg", 5, LAT, LON)) == len(df) - 5


def test_drop_frozen_never_touches_the_night() -> None:
    """At night the target is quantized sensor offset: long equal runs are legitimate."""
    df = _series(NIGHT, [-1.2] * 40)
    assert len(drop_frozen(df, "glo_avg", 5, LAT, LON)) == 40


def test_drop_frozen_a_time_gap_ends_the_run() -> None:
    """Equal values separated by missing minutes are two short runs, not one long run."""
    first = _series(DAY, [500.0] * 3)
    second = _series("2022-06-21 15:30", [500.0] * 3)
    df = pd.concat([first, second], ignore_index=True)
    assert len(drop_frozen(df, "glo_avg", 5, LAT, LON)) == 6


def test_drop_frozen_missing_values_are_kept_and_do_not_join_runs() -> None:
    df = _series(DAY, [500.0] * 3 + [None] + [500.0] * 3)
    kept = drop_frozen(df, "glo_avg", 5, LAT, LON)
    assert len(kept) == 7
    assert kept["glo_avg"].isna().sum() == 1


def test_drop_frozen_with_no_threshold_returns_the_same_frame() -> None:
    df = _series(DAY, [500.0] * 30)
    assert drop_frozen(df, "glo_avg", None, LAT, LON) is df


def test_drop_frozen_with_no_daytime_rows_returns_the_frame_unchanged() -> None:
    df = _series(NIGHT, [-1.0] * 30)
    assert len(drop_frozen(df, "glo_avg", 5, LAT, LON)) == 30


def test_drop_frozen_logs_how_many_rows_were_dropped(
    caplog: pytest.LogCaptureFixture,
) -> None:
    df = _series(DAY, [*_varied(5), *[500.0] * 6])
    with caplog.at_level(logging.INFO):
        drop_frozen(df, "glo_avg", 5, LAT, LON)
    assert "dropped 6 of 11 rows" in caplog.text
