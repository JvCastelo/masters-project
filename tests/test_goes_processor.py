"""Tests for masters_project.processors.goes: GOES NetCDF metadata, projection, and window extraction."""

from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from masters_project.processors.goes import GoesProcessor


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


@pytest.fixture
def mock_goes_dataset_with_window() -> xr.Dataset:
    """Dataset with Rad variable and target indices in attrs for extract_window_to_df."""
    radius = 1
    dim = 2 * radius + 1  # 3
    data = np.arange(dim * dim, dtype=float).reshape(dim, dim)
    ds = xr.Dataset(
        {"Rad": (["y", "x"], data)},
        coords={
            "y": np.arange(dim, dtype=float),
            "x": np.arange(dim, dtype=float),
        },
        attrs={
            "target_pixel_i": 1,
            "target_pixel_j": 1,
            "channel": "C01",
            "timestamp": pd.Timestamp("2024-01-01 12:00:00", tz="UTC"),
        },
    )
    return ds


# =============================================================================
# Tests
# =============================================================================


@patch("masters_project.processors.goes.xr.open_dataset")
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


@pytest.mark.parametrize(
    ("dataset_name", "expected_channel"),
    [
        ("OR_ABI-L1b-RadM1-C01_G16_s20240011200000_e20240011205999_c20240011205999", "C01"),
        ("", "UNK"),
        ("malformed", "UNK"),
    ],
    ids=["valid_abi_filename", "empty_dataset_name", "malformed_dataset_name"],
)
def test_add_metadata_various_dataset_names_sets_channel_correctly(
    dataset_name: str, expected_channel: str
) -> None:
    """Parametrized: valid ABI filename, empty, and malformed dataset_name produce correct channel or UNK fallback."""
    ds = xr.Dataset(
        attrs={
            "dataset_name": dataset_name,
            "time_coverage_start": "2024-01-01T12:00:00Z",
        }
    )
    result = GoesProcessor.add_metadata(ds)
    assert result.attrs["channel"] == expected_channel
    assert "timestamp" in result.attrs


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


def test_extract_window_to_df_missing_target_indices_raises_valueerror() -> None:
    """Failure mode: missing target_pixel_i or target_pixel_j in attrs must raise ValueError."""
    ds = xr.Dataset(
        {"Rad": (["y", "x"], np.zeros((3, 3)))},
        coords={"y": np.arange(3), "x": np.arange(3)},
        attrs={"channel": "C01"},  # no target_pixel_i, target_pixel_j
    )
    with pytest.raises(ValueError, match="target_pixel_i and target_pixel_j"):
        GoesProcessor.extract_window_to_df(ds, variable="Rad", radius=1)
