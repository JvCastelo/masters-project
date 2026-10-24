"""Per-day cache of GOES pixel windows, so a file on S3 is read at most once.

Layout (one CSV per channel and day, long format: one row per file x station)::

    source_key, station, pixel_i, pixel_j, timestamp, radius=1_C03_11, ..., radius=1_C03_33

The cache is independent of the execution dates and of the selected stations: runs with
overlapping windows reuse it, and a new station only costs a re-read of the files that
lack it. The target pixel is stored with each row so a changed station coordinate (or
window) is detected instead of silently serving stale values.
"""

import json
import logging
import os
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path

import pandas as pd

from masters_project.etl.goes.worker import FileExtraction

logger = logging.getLogger(__name__)

KEY_COLUMNS = ["source_key", "station", "pixel_i", "pixel_j", "timestamp"]

Day = tuple[int, int]  # (year, day of year)


def group_by_day(paths: list[str]) -> dict[Day, list[str]]:
    """Group S3 keys ``bucket/product/{year}/{doy}/{hour}/file.nc`` by (year, day of year)."""
    days: dict[Day, list[str]] = defaultdict(list)
    for path in paths:
        _, _, year, doy, *_ = path.split("/")
        days[(int(year), int(doy))].append(path)
    return dict(sorted(days.items()))


def load_day(path: Path, window_columns: list[str]) -> pd.DataFrame:
    """Load one day's cache, or an empty frame with the expected columns if missing.

    Args:
        path: Cache CSV path for the day.
        window_columns: Names of the flattened window columns.

    Returns:
        DataFrame with ``KEY_COLUMNS + window_columns`` (possibly empty).
    """
    columns = KEY_COLUMNS + window_columns
    if not path.exists():
        return pd.DataFrame(columns=columns)

    df = pd.read_csv(path)
    if list(df.columns) != columns:
        logger.warning(
            f"Cache schema mismatch in {path.name}; ignoring this day's cache."
        )
        return pd.DataFrame(columns=columns)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df


def extractions_to_frame(
    extractions: Iterable[FileExtraction],
    pixels: dict[str, tuple[int, int]],
    window_columns: list[str],
) -> pd.DataFrame:
    """Turn successful worker results into cache rows (one per file x station).

    Args:
        extractions: Results from the worker; entries with ``error`` are skipped.
        pixels: Station -> (i, j) used for the extraction, stored for later validation.
        window_columns: Names of the flattened window columns.

    Returns:
        DataFrame with ``KEY_COLUMNS + window_columns``.
    """
    rows = []
    for item in extractions:
        if item.error is not None or item.windows is None:
            continue
        for station, window in item.windows.items():
            i, j = pixels[station]
            rows.append([item.source_key, station, i, j, item.timestamp, *window])
    return pd.DataFrame(rows, columns=KEY_COLUMNS + window_columns)


def save_day(path: Path, cached: pd.DataFrame, new_rows: pd.DataFrame) -> pd.DataFrame:
    """Merge ``new_rows`` into the day's cache and write it atomically.

    Rows in ``cached`` for the same (source_key, station) as a new row are replaced.
    The file is written to a temporary name and renamed, so an interruption never
    leaves a truncated cache.

    Args:
        path: Cache CSV path for the day.
        cached: Rows currently in the cache.
        new_rows: Freshly extracted rows.

    Returns:
        The merged day frame that was written.
    """
    if new_rows.empty:
        return cached

    keys = ["source_key", "station"]
    if cached.empty:
        merged = new_rows
    else:
        stale = cached.set_index(keys).index.isin(new_rows.set_index(keys).index)
        merged = pd.concat([cached[~stale], new_rows], ignore_index=True)
    merged = merged.sort_values(["timestamp", "station"]).reset_index(drop=True)

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".csv.tmp")
    merged.to_csv(tmp_path, index=False)
    os.replace(tmp_path, path)
    return merged


def find_pending_files(
    cached: pd.DataFrame,
    files: list[str],
    pixels: dict[str, tuple[int, int]],
) -> list[str]:
    """Decide which S3 files still have to be read for this day.

    Args:
        cached: Rows currently in the day's cache (columns ``KEY_COLUMNS`` + window
            columns; empty if nothing was cached yet).
        files: All S3 keys listed for the day and channel.
        pixels: Station -> (i, j) requested in this run.

    Returns:
        The subset of ``files`` (keeping their order) that must be processed.
    """
    if cached.empty:
        return list(files)

    # A cached row is only valid for the pixel it was extracted at: a station that moved
    # (or a new station) must not be served from a stale or missing row.
    cached_rows = set(
        zip(
            cached["source_key"],
            cached["station"],
            cached["pixel_i"].astype(int),
            cached["pixel_j"].astype(int),
        )
    )

    def is_fully_cached(source_key: str) -> bool:
        return all(
            (source_key, station, i, j) in cached_rows
            for station, (i, j) in pixels.items()
        )

    return [path for path in files if not is_fully_cached(path)]


def load_pixels(
    path: Path, stations: dict[str, tuple[float, float]]
) -> dict[str, tuple[int, int]] | None:
    """Load the cached target pixels of a channel's grid if they match the stations' coordinates.

    The pixel of a (lat, lon) only depends on the channel's fixed grid, so it never has to
    be recomputed (which needs opening a file) unless a station is added or moved.

    Args:
        path: JSON file written by :func:`save_pixels`.
        stations: Station -> (latitude, longitude) currently configured.

    Returns:
        Station -> (i, j), or None if the file is missing or any station is new/changed.
    """
    if not path.exists():
        return None

    stored = json.loads(path.read_text())
    pixels = {}
    for name, (lat, lon) in stations.items():
        entry = stored.get(name)
        if entry is None or (entry["latitude"], entry["longitude"]) != (lat, lon):
            return None
        pixels[name] = (entry["i"], entry["j"])
    return pixels


def save_pixels(
    path: Path,
    stations: dict[str, tuple[float, float]],
    pixels: dict[str, tuple[int, int]],
) -> None:
    """Write the target pixels, with the coordinates they were computed from, atomically."""
    payload = {
        name: {
            "latitude": lat,
            "longitude": lon,
            "i": pixels[name][0],
            "j": pixels[name][1],
        }
        for name, (lat, lon) in stations.items()
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".json.tmp")
    tmp_path.write_text(json.dumps(payload, indent=2))
    os.replace(tmp_path, path)
