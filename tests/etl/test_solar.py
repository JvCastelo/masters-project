"""Tests for masters_project.etl.solar: solar elevation."""

import numpy as np
import pandas as pd
import pytest

from masters_project.etl.solar import solar_elevation

# Ourinhos (SP), the station used for the 2023 data.
LAT, LON = -23.0, -49.844


def _utc(*values: str) -> pd.DatetimeIndex:
    return pd.DatetimeIndex(pd.to_datetime(list(values), utc=True))


def test_sun_is_overhead_at_the_equator_on_the_equinox_noon() -> None:
    """Lat 0, lon 0, 2022-03-20 12:00 UTC: within 3 degrees of the zenith.

    The equation of time is about -7 min on that date, so solar noon is ~12:07 UTC and the
    Sun is still ~2 degrees short of the zenith at 12:00.
    """
    elevation = solar_elevation(_utc("2022-03-20 12:00"), 0.0, 0.0)
    assert elevation[0] == pytest.approx(90, abs=3)


def test_sun_is_far_below_the_horizon_at_local_midnight() -> None:
    """Ourinhos at 03:00 UTC (about midnight local solar time) is deep in the night."""
    assert solar_elevation(_utc("2022-06-21 03:00"), LAT, LON)[0] < -40


def test_noon_elevation_follows_latitude_and_season() -> None:
    """Ourinhos is south of the Tropic: higher Sun at the December solstice than in June."""
    # Local solar noon at lon -49.844 is about 15:20 UTC.
    summer = solar_elevation(_utc("2022-12-21 15:20"), LAT, LON)[0]
    winter = solar_elevation(_utc("2022-06-21 15:20"), LAT, LON)[0]
    assert summer == pytest.approx(90 - abs(LAT - (-23.44)), abs=1.5)
    assert winter == pytest.approx(90 - abs(LAT - 23.44), abs=1.5)
    assert summer > winter


def test_elevation_accepts_a_series_and_returns_one_value_per_row() -> None:
    series = pd.Series(_utc("2022-03-20 12:00", "2022-03-20 00:00"))
    elevation = solar_elevation(series, 0.0, 0.0)
    assert elevation.shape == (2,)
    assert elevation[0] > 80 > 0 > elevation[1]


def test_elevation_never_leaves_the_physical_range() -> None:
    index = pd.date_range("2022-01-01", periods=24 * 60, freq="1min", tz="UTC")
    elevation = solar_elevation(index, LAT, LON)
    assert np.all((elevation >= -90) & (elevation <= 90))
