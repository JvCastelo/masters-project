"""Sun geometry for a fixed location, used to tell day from night."""

import numpy as np
import pandas as pd


def solar_elevation(
    timestamps: pd.Series | pd.DatetimeIndex, lat: float, lon: float
) -> np.ndarray:
    """Elevation of the Sun above the horizon, in degrees, for UTC timestamps.

    Uses the Spencer (1971) Fourier series for the declination and the equation of
    time, as in the NOAA solar calculator. No atmospheric refraction is applied; the
    error is well below a degree, which is enough to separate day from night.

    Args:
        timestamps: UTC-aware timestamps (a Series or a DatetimeIndex).
        lat: Latitude in degrees (north positive).
        lon: Longitude in degrees (east positive).

    Returns:
        Array of elevations in degrees: 90 at the zenith, 0 at the horizon, negative
        at night.
    """
    index = pd.DatetimeIndex(timestamps)
    doy = index.dayofyear.to_numpy()
    hour = (
        index.hour.to_numpy()
        + index.minute.to_numpy() / 60
        + index.second.to_numpy() / 3600
    )

    # Fractional year in radians.
    g = 2 * np.pi / 365 * (doy - 1 + (hour - 12) / 24)
    eq_time = 229.18 * (
        0.000075
        + 0.001868 * np.cos(g)
        - 0.032077 * np.sin(g)
        - 0.014615 * np.cos(2 * g)
        - 0.040849 * np.sin(2 * g)
    )  # minutes
    declination = (
        0.006918
        - 0.399912 * np.cos(g)
        + 0.070257 * np.sin(g)
        - 0.006758 * np.cos(2 * g)
        + 0.000907 * np.sin(2 * g)
        - 0.002697 * np.cos(3 * g)
        + 0.00148 * np.sin(3 * g)
    )

    hour_angle = np.radians((hour * 60 + eq_time + 4 * lon) / 4 - 180)
    lat_rad = np.radians(lat)
    sin_elevation = np.sin(lat_rad) * np.sin(declination) + np.cos(lat_rad) * np.cos(
        declination
    ) * np.cos(hour_angle)
    return np.degrees(np.arcsin(np.clip(sin_elevation, -1.0, 1.0)))
