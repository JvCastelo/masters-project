"""Tests for masters_project.etl.goes.reader: dataset opening, projection, and window extraction."""

from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from masters_project.etl.goes.reader import GoesProcessor


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mock_goes_dataset_valid_projection() -> xr.Dataset:
    """Minimal xarray Dataset with goes_imager_projection and x/y coords for get_target_indices."""
    # GOES-16 typical values (simplified)
    proj = xr.DataArray(
        0,
        attrs={
            "semi_major_axis": 6378137.0,
            "semi_minor_axis": 6356752.31414,
            "perspective_point_height": 42164160.0,
            "longitude_of_projection_origin": -75.0,
        },
    )
    # Create a small grid; target_y, target_x will be found via argmin
    ds = xr.Dataset(
        coords={
            "y": (["y"], np.linspace(-0.1, 0.1, 50)),
            "x": (["x"], np.linspace(-0.1, 0.1, 50)),
        },
        attrs={"goes_imager_projection": proj},
    )
    ds["goes_imager_projection"] = proj
    return ds


@pytest.fixture
def mock_goes_dataset_behind_earth() -> xr.Dataset:
    """Dataset configured so a specific (lat, lon) falls behind Earth from satellite view."""
    proj = xr.DataArray(
        0,
        attrs={
            "semi_major_axis": 6378137.0,
            "semi_minor_axis": 6356752.31414,
            "perspective_point_height": 42164160.0,
            "longitude_of_projection_origin": -75.0,
        },
    )
    ds = xr.Dataset(
        coords={
            "y": (["y"], np.linspace(-0.1, 0.1, 10)),
            "x": (["x"], np.linspace(-0.1, 0.1, 10)),
        },
        attrs={"goes_imager_projection": proj},
    )
    ds["goes_imager_projection"] = proj
    return ds


# =============================================================================
# Tests
# =============================================================================


@patch("masters_project.etl.goes.reader.xr.open_dataset")
def test_open_as_dataset_file_like_object_returns_dataset(
    mock_open: MagicMock,
) -> None:
    """Happy path: open_as_dataset delegates to xr.open_dataset with h5netcdf engine."""
    mock_ds = xr.Dataset()
    mock_open.return_value = mock_ds
    fake_file = MagicMock()
    result = GoesProcessor.open_as_dataset(fake_file)
    mock_open.assert_called_once_with(fake_file, engine="h5netcdf", cache=False)
    assert result is mock_ds


def test_get_target_indices_valid_coords_returns_integer_indices(
    mock_goes_dataset_valid_projection: xr.Dataset,
) -> None:
    """Happy path: valid (lat, lon) returns (i, j) indices within dataset bounds."""
    ds = mock_goes_dataset_valid_projection
    i, j = GoesProcessor.get_target_indices(ds, lat=-15.0, lon=-48.0)
    assert isinstance(i, int) and isinstance(j, int)
    assert 0 <= i < len(ds.y) and 0 <= j < len(ds.x)


def test_get_target_indices_point_behind_earth_raises_valueerror(
    mock_goes_dataset_behind_earth: xr.Dataset,
) -> None:
    """Failure mode: (lat, lon) behind Earth from satellite view must raise ValueError."""
    ds = mock_goes_dataset_behind_earth
    # Use coords that trigger the visibility check (opposite side of Earth from -75°)
    with pytest.raises(ValueError, match="hidden behind the Earth"):
        GoesProcessor.get_target_indices(ds, lat=0.0, lon=105.0)


# =============================================================================
# read_windows_h5: direct HDF5 reader must match the xarray path exactly
# =============================================================================


@pytest.fixture
def packed_netcdf(tmp_path):
    """A NetCDF4 file packed like GOES Rad: int16, scale_factor/add_offset float32, _FillValue."""
    rng = np.random.default_rng(0)
    raw = rng.integers(0, 1022, size=(20, 20)).astype(np.int16)
    raw[5, 5] = 1023  # fill value
    ds = xr.Dataset(
        {"Rad": (["y", "x"], raw.astype(np.float32) * 0.37691253 - 12.037643)},
        coords={"y": np.arange(20.0), "x": np.arange(20.0)},
        attrs={"time_coverage_start": "2022-02-01T00:04:27.6Z"},
    )
    path = tmp_path / "goes_like.nc"
    ds.to_netcdf(
        path,
        engine="h5netcdf",
        encoding={
            "Rad": {
                "dtype": "int16",
                "scale_factor": np.float32(0.37691253),
                "add_offset": np.float32(-12.037643),
                "_FillValue": np.int16(1023),
                "chunksizes": (8, 8),
            }
        },
    )
    # Re-encode the fill pixel explicitly: write raw ints through h5py to be exact.
    import h5py

    with h5py.File(path, "r+") as h5:
        h5["Rad"][...] = raw
    return path


def test_read_windows_h5_matches_xarray_including_fill_values(packed_netcdf) -> None:
    pixels = {"A": (5, 5), "B": (10, 3), "EDGE": (0, 19)}
    radius = 1

    with open(packed_netcdf, "rb") as f:
        timestamp, windows = GoesProcessor.read_windows_h5(f, "Rad", radius, pixels)

    with xr.open_dataset(packed_netcdf, engine="h5netcdf") as ds:
        for name, (i, j) in pixels.items():
            expected = (
                ds["Rad"]
                .isel(
                    y=slice(max(i - radius, 0), i + radius + 1),
                    x=slice(max(j - radius, 0), j + radius + 1),
                )
                .values.flatten()
            )
            assert windows[name].dtype == expected.dtype == np.float32
            np.testing.assert_array_equal(windows[name], expected)

    assert np.isnan(windows["A"][4])  # center pixel (5, 5) is the fill value
    assert timestamp == pd.Timestamp("2022-02-01 00:00:00", tz="UTC")


def test_parse_timestamp_accepts_bytes_and_rounds_to_ten_minutes() -> None:
    assert GoesProcessor.parse_timestamp(b"2022-02-01T00:09:51.4Z") == pd.Timestamp(
        "2022-02-01 00:10:00", tz="UTC"
    )
