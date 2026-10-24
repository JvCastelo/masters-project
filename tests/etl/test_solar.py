"""Tests for masters_project.etl.solar: solar elevation and the daytime filter."""

import numpy as np
import pandas as pd
import pytest
from pydantic import ValidationError

from masters_project.etl.solar import SolarProcessor
from masters_project.settings import EtlConfig

# Ourinhos (SP), the station used for the 2023 data.
LAT, LON = -23.0, -49.844


def _utc(*values: str) -> pd.DatetimeIndex:
    return pd.DatetimeIndex(pd.to_datetime(list(values), utc=True))


def test_sun_is_overhead_at_the_equator_on_the_equinox_noon() -> None:
    """Lat 0, lon 0, 2022-03-20 12:00 UTC: within 3 degrees of the zenith.

    The equation of time is about -7 min on that date, so solar noon is ~12:07 UTC and the
    Sun is still ~2 degrees short of the zenith at 12:00.
    """
    elevation = SolarProcessor.solar_elevation(_utc("2022-03-20 12:00"), 0.0, 0.0)
    assert elevation[0] == pytest.approx(90, abs=3)


def test_sun_is_far_below_the_horizon_at_local_midnight() -> None:
    """Ourinhos at 03:00 UTC (about midnight local solar time) is deep in the night."""
    elevation = SolarProcessor.solar_elevation(_utc("2022-06-21 03:00"), LAT, LON)
    assert elevation[0] < -40


def test_noon_elevation_follows_latitude_and_season() -> None:
    """Ourinhos is south of the Tropic: higher Sun at the December solstice than in June."""
    # Local solar noon at lon -49.844 is about 15:20 UTC.
    summer = SolarProcessor.solar_elevation(_utc("2022-12-21 15:20"), LAT, LON)[0]
    winter = SolarProcessor.solar_elevation(_utc("2022-06-21 15:20"), LAT, LON)[0]
    assert summer == pytest.approx(90 - abs(LAT - (-23.44)), abs=1.5)
    assert winter == pytest.approx(90 - abs(LAT - 23.44), abs=1.5)
    assert summer > winter


def test_elevation_accepts_a_series_and_returns_one_value_per_row() -> None:
    series = pd.Series(_utc("2022-03-20 12:00", "2022-03-20 00:00"))
    elevation = SolarProcessor.solar_elevation(series, 0.0, 0.0)
    assert elevation.shape == (2,)
    assert elevation[0] > 80 > 0 > elevation[1]


def test_keep_daytime_drops_the_night_and_resets_the_index() -> None:
    df = pd.DataFrame(
        {
            "timestamp": _utc(
                "2022-03-20 00:00", "2022-03-20 12:00", "2022-03-20 23:00"
            ),
            "val": [1, 2, 3],
        }
    )
    kept = SolarProcessor.keep_daytime(df, 0.0, 0.0, min_elevation=10)
    assert kept["val"].tolist() == [2]
    assert kept.index.tolist() == [0]


def test_keep_daytime_with_no_threshold_returns_every_row() -> None:
    """Failure mode: ``None`` must leave the dataset exactly as it was (filter disabled)."""
    df = pd.DataFrame({"timestamp": _utc("2022-03-20 00:00", "2022-03-20 12:00")})
    assert SolarProcessor.keep_daytime(df, 0.0, 0.0, min_elevation=None) is df


def test_keep_daytime_threshold_is_strict() -> None:
    """Rows are kept only when the elevation is greater than the threshold."""
    df = pd.DataFrame({"timestamp": _utc("2022-03-20 12:00")})
    noon = float(SolarProcessor.solar_elevation(df["timestamp"], 0.0, 0.0)[0])
    assert SolarProcessor.keep_daytime(df, 0.0, 0.0, noon - 0.01).shape[0] == 1
    assert SolarProcessor.keep_daytime(df, 0.0, 0.0, noon).shape[0] == 0


def test_elevation_never_leaves_the_physical_range() -> None:
    index = pd.date_range("2022-01-01", periods=24 * 60, freq="1min", tz="UTC")
    elevation = SolarProcessor.solar_elevation(index, LAT, LON)
    assert np.all((elevation >= -90) & (elevation <= 90))


def test_min_solar_elevation_must_be_a_valid_angle() -> None:
    """Failure mode: an angle outside [-90, 90] is rejected when the config is loaded."""
    with pytest.raises(ValidationError, match="min_solar_elevation"):
        EtlConfig(
            start_date="2022-01-01",
            end_date="2022-01-02",
            min_solar_elevation=120,
            stations={},
            goes={
                "bucket_name": "b",
                "product_name": "p",
                "variable": "Rad",
                "channels": ["C01"],
                "pixel_radius": 1,
            },
            sonda={"data_type": "solarimetricos", "target_variable": "glo_avg"},
        )
