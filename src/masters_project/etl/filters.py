"""Row filters applied to the SONDA series before it is joined with the GOES features.

Each filter is a pure function ``DataFrame -> DataFrame`` that logs how many rows it
removed, and ``None`` as its threshold disables it. They are composed explicitly in
:mod:`masters_project.etl.build_dataset`. The raw CSVs are never filtered: filtering is a
modelling choice, so changing a threshold only needs a rebuild, not a new collection.
"""

import logging

import pandas as pd

from masters_project.etl.solar import solar_elevation

logger = logging.getLogger(__name__)

# A run of identical values is only suspicious while the Sun is above the horizon: at night
# the target is a few quantized levels of sensor offset and repeats legitimately.
HORIZON_ELEVATION = 0.0
# A run is broken by a time gap larger than this many median sampling steps, so two equal
# values separated by missing data (or by the night) are never counted as one run.
MAX_GAP_IN_STEPS = 1.5


def keep_daytime(
    df: pd.DataFrame,
    lat: float,
    lon: float,
    min_elevation: float | None,
    time_col: str = "timestamp",
) -> pd.DataFrame:
    """Keep only the rows on which the Sun is higher than ``min_elevation`` degrees.

    The criterion depends only on the timestamp and the station position, never on the
    target, so it can be applied the same way at prediction time.

    Args:
        df: DataFrame with a UTC-aware ``time_col``.
        lat: Station latitude in degrees.
        lon: Station longitude in degrees.
        min_elevation: Minimum solar elevation in degrees (strictly greater is kept).
            ``None`` disables the filter and returns ``df`` unchanged.
        time_col: Name of the timestamp column.

    Returns:
        The filtered DataFrame with a fresh index.
    """
    if min_elevation is None:
        return df

    elevation = solar_elevation(df[time_col], lat, lon)
    kept = df[elevation > min_elevation].reset_index(drop=True)
    logger.info(
        f"Solar elevation > {min_elevation}°: kept {len(kept)} of {len(df)} rows."
    )
    return kept


def drop_frozen(
    df: pd.DataFrame,
    column: str,
    min_run: int | None,
    lat: float,
    lon: float,
    time_col: str = "timestamp",
) -> pd.DataFrame:
    """Drop daytime rows where ``column`` repeats the same value ``min_run`` or more times in a row.

    A sensor or logger that stops updating keeps reporting its last value. Measured on the
    SONDA 1-minute series (2022-2023, three stations) the longest natural run of identical
    daytime values is 5 samples, so this is a safeguard that removes nothing from today's
    data. Night rows and missing values are never counted nor removed, and a time gap
    larger than ``MAX_GAP_IN_STEPS`` median steps ends a run. Meant for the native
    resolution of the series, before it is resampled or joined.

    Args:
        df: DataFrame sorted by ``time_col`` (UTC-aware) with ``column``.
        column: Column to inspect (the target, e.g. ``glo_avg``).
        min_run: Smallest number of consecutive identical samples that counts as frozen.
            ``None`` disables the filter and returns ``df`` unchanged.
        lat: Station latitude in degrees (to tell day from night).
        lon: Station longitude in degrees.
        time_col: Name of the timestamp column.

    Returns:
        The DataFrame without the frozen rows, with a fresh index.
    """
    if min_run is None:
        return df

    df = df.reset_index(drop=True)
    elevation = solar_elevation(df[time_col], lat, lon)
    day = df[(elevation > HORIZON_ELEVATION) & df[column].notna()]
    if day.empty:
        return df

    step = day[time_col].diff()
    new_run = (day[column] != day[column].shift()) | (
        step > MAX_GAP_IN_STEPS * step.median()
    )
    run_length = day.groupby(new_run.cumsum())[column].transform("size")
    frozen = day.index[run_length >= min_run]

    kept = df.drop(index=frozen).reset_index(drop=True)
    logger.info(
        f"Frozen '{column}' (>= {min_run} identical daytime samples in a row): "
        f"dropped {len(frozen)} of {len(df)} rows."
    )
    return kept
