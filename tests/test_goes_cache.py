"""Tests for the per-day GOES cache and the multi-station extraction helpers."""

from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from masters_project.loaders.goes_cache import (
    KEY_COLUMNS,
    extractions_to_frame,
    find_pending_files,
    group_by_day,
    load_day,
    load_pixels,
    save_day,
    save_pixels,
)
from masters_project.processors.goes import GoesProcessor
from masters_project.processors.goes_extract import FileExtraction

WINDOW_COLS = GoesProcessor.window_columns(radius=1, channel="C03")
PIXELS = {"A": (10, 20), "B": (30, 40)}
TS = pd.Timestamp("2022-02-01 00:00:00", tz="UTC")


def _extraction(
    key: str, ts: pd.Timestamp = TS, error: str | None = None
) -> FileExtraction:
    if error:
        return FileExtraction(source_key=key, error=error)
    windows = {s: np.arange(9, dtype=float) for s in PIXELS}
    return FileExtraction(source_key=key, timestamp=ts, windows=windows)


def _frame(
    keys: list[str], pixels: dict[str, tuple[int, int]] = PIXELS
) -> pd.DataFrame:
    return extractions_to_frame([_extraction(k) for k in keys], pixels, WINDOW_COLS)


# =============================================================================
# Window helpers
# =============================================================================


def test_window_columns_radius_1_matches_legacy_naming() -> None:
    """Names follow radius={r}_{channel}_{row}{col}, row-major, same as extract_window_to_df."""
    assert WINDOW_COLS[0] == "radius=1_C03_11"
    assert WINDOW_COLS[4] == "radius=1_C03_22"
    assert WINDOW_COLS[-1] == "radius=1_C03_33"
    assert len(WINDOW_COLS) == 9


def test_extract_windows_multiple_stations_matches_single_station_extraction() -> None:
    """Each station's window equals what the single-station extractor returns."""
    data = np.arange(100, dtype=float).reshape(10, 10)
    ds = xr.Dataset({"Rad": (["y", "x"], data)})
    windows = GoesProcessor.extract_windows(ds, "Rad", 1, {"A": (2, 3), "B": (7, 5)})

    np.testing.assert_array_equal(windows["A"], data[1:4, 2:5].flatten())
    np.testing.assert_array_equal(windows["B"], data[6:9, 4:7].flatten())


# =============================================================================
# Cache IO
# =============================================================================


def test_load_day_missing_file_returns_empty_frame_with_schema(tmp_path: Path) -> None:
    df = load_day(tmp_path / "001.csv", WINDOW_COLS)
    assert df.empty
    assert list(df.columns) == KEY_COLUMNS + WINDOW_COLS


def test_save_then_load_roundtrip_preserves_values_and_utc_timestamps(
    tmp_path: Path,
) -> None:
    path = tmp_path / "goes" / "001.csv"
    new = _frame(["k1", "k2"])
    save_day(path, load_day(path, WINDOW_COLS), new)

    loaded = load_day(path, WINDOW_COLS)
    assert len(loaded) == 4  # 2 files x 2 stations
    assert str(loaded["timestamp"].dt.tz) == "UTC"
    np.testing.assert_array_equal(
        loaded[WINDOW_COLS].to_numpy()[0], np.arange(9, dtype=float)
    )


def test_save_day_replaces_rows_with_same_file_and_station(tmp_path: Path) -> None:
    path = tmp_path / "001.csv"
    first = save_day(path, load_day(path, WINDOW_COLS), _frame(["k1"]))
    updated = _frame(["k1"])
    updated[WINDOW_COLS] = 99.0
    merged = save_day(path, first, updated)

    assert len(merged) == 2  # not duplicated
    assert (merged[WINDOW_COLS] == 99.0).all().all()


def test_save_day_leaves_no_temp_file_and_skips_empty_update(tmp_path: Path) -> None:
    path = tmp_path / "001.csv"
    save_day(path, load_day(path, WINDOW_COLS), _frame(["k1"]))
    assert not path.with_suffix(".csv.tmp").exists()

    empty = _frame([])
    before = path.read_text()
    save_day(path, load_day(path, WINDOW_COLS), empty)
    assert path.read_text() == before


def test_extractions_to_frame_skips_failed_files() -> None:
    df = extractions_to_frame(
        [_extraction("ok"), _extraction("bad", error="boom")], PIXELS, WINDOW_COLS
    )
    assert set(df["source_key"]) == {"ok"}


def test_group_by_day_splits_keys_by_year_and_day_of_year() -> None:
    paths = [
        "noaa-goes16/ABI-L1b-RadF/2022/032/01/a.nc",
        "noaa-goes16/ABI-L1b-RadF/2022/032/00/b.nc",
        "noaa-goes16/ABI-L1b-RadF/2022/033/00/c.nc",
    ]
    days = group_by_day(paths)
    assert list(days) == [(2022, 32), (2022, 33)]
    assert len(days[(2022, 32)]) == 2


# =============================================================================
# find_pending_files contract (implemented by the user)
# =============================================================================


def test_pending_nothing_cached_returns_all_files() -> None:
    cached = load_day(Path("/nonexistent"), WINDOW_COLS)
    assert find_pending_files(cached, ["k1", "k2"], PIXELS) == ["k1", "k2"]


def test_pending_fully_cached_returns_nothing() -> None:
    cached = _frame(["k1", "k2"])
    assert find_pending_files(cached, ["k1", "k2"], PIXELS) == []


def test_pending_only_uncached_files_are_returned_in_order() -> None:
    cached = _frame(["k1"])
    assert find_pending_files(cached, ["k1", "k2", "k3"], PIXELS) == ["k2", "k3"]


def test_pending_new_station_forces_reread_of_cached_files() -> None:
    cached = _frame(["k1"])
    cached = cached[cached["station"] == "A"]
    assert find_pending_files(cached, ["k1"], PIXELS) == ["k1"]


@pytest.mark.parametrize(
    "changed", [{"A": (11, 20), "B": (30, 40)}, {"A": (10, 21), "B": (30, 40)}]
)
def test_pending_changed_pixel_invalidates_cached_file(
    changed: dict[str, tuple[int, int]],
) -> None:
    cached = _frame(["k1"])
    assert find_pending_files(cached, ["k1"], changed) == ["k1"]


# =============================================================================
# Pixel cache
# =============================================================================

COORDS = {"A": (-23.0, -49.8), "B": (-22.7, -45.0)}


def test_pixels_roundtrip_returns_same_indices(tmp_path: Path) -> None:
    path = tmp_path / "pixels" / "C03.json"
    save_pixels(path, COORDS, PIXELS)
    assert load_pixels(path, COORDS) == PIXELS


def test_load_pixels_missing_file_returns_none(tmp_path: Path) -> None:
    assert load_pixels(tmp_path / "nope.json", COORDS) is None


def test_load_pixels_new_station_returns_none(tmp_path: Path) -> None:
    path = tmp_path / "C03.json"
    save_pixels(path, COORDS, PIXELS)
    assert load_pixels(path, {**COORDS, "C": (-10.0, -50.0)}) is None


def test_load_pixels_moved_station_returns_none(tmp_path: Path) -> None:
    path = tmp_path / "C03.json"
    save_pixels(path, COORDS, PIXELS)
    assert load_pixels(path, {**COORDS, "A": (-23.1, -49.8)}) is None
