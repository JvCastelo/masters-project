import logging
from typing import BinaryIO

import h5py
import numpy as np
import pandas as pd
import xarray as xr

logger = logging.getLogger(__name__)


class GoesProcessor:
    """Read GOES ABI files: project lat/lon to grid indices and read pixel windows straight from HDF5."""

    @staticmethod
    def open_as_dataset(file_obj: BinaryIO) -> xr.Dataset:
        """Open a file-like object as an xarray Dataset using the h5netcdf engine.

        Args:
            file_obj: Binary file-like object (e.g. BytesIO) containing NetCDF bytes.

        Returns:
            xarray Dataset for the GOES NetCDF.
        """
        logger.debug("Opening file-like object as NetCDF dataset.")
        return xr.open_dataset(file_obj, engine="h5netcdf", cache=False)

    @staticmethod
    def get_target_indices(ds: xr.Dataset, lat: float, lon: float) -> tuple[int, int]:
        """Compute dataset (i, j) indices for a given latitude/longitude using GOES imager projection.

        Args:
            ds: GOES xarray Dataset with goes_imager_projection and x, y coordinates.
            lat: Target latitude in degrees.
            lon: Target longitude in degrees.

        Returns:
            (i, j) indices into the dataset grid (y, x) for the nearest pixel.

        Raises:
            ValueError: If (lat, lon) is behind the Earth from the satellite view.
        """
        logger.info(f"Calculating inverse projection for Lat: {lat}, Lon: {lon}")

        proj = ds.goes_imager_projection
        r_eq = proj.attrs["semi_major_axis"]
        r_pol = proj.attrs["semi_minor_axis"]
        h_sat = proj.attrs["perspective_point_height"]
        l_0 = proj.attrs["longitude_of_projection_origin"] * (np.pi / 180.0)

        H = r_eq + h_sat

        lat_rad = lat * (np.pi / 180.0)
        lon_rad = lon * (np.pi / 180.0)

        # NOAA PUG Inverse Projection Math
        e_sq = 1.0 - (r_pol**2 / r_eq**2)
        phi_c = np.arctan((r_pol**2 / r_eq**2) * np.tan(lat_rad))
        r_c = r_pol / np.sqrt(1.0 - e_sq * np.cos(phi_c) ** 2)

        s_x = H - r_c * np.cos(phi_c) * np.cos(lon_rad - l_0)
        s_y = -r_c * np.cos(phi_c) * np.sin(lon_rad - l_0)
        s_z = r_c * np.sin(phi_c)

        if H * (H - s_x) < (s_y**2 + (r_eq**2 / r_pol**2) * s_z**2):
            raise ValueError(
                f"Coordinate ({lat}, {lon}) is hidden behind the Earth from this satellite."
            )

        target_x = np.arcsin(-s_y / np.sqrt(s_x**2 + s_y**2 + s_z**2))
        target_y = np.arctan(s_z / s_x)

        i = np.argmin(np.abs(ds.y.values - target_y))
        j = np.argmin(np.abs(ds.x.values - target_x))

        logger.debug(f"Target found at indices: i={i}, j={j}")
        return int(i), int(j)

    @staticmethod
    def window_columns(radius: int, channel: str) -> list[str]:
        """Column names of a flattened (2*radius+1)^2 window: ``radius={r}_{channel}_{row}{col}``.

        Args:
            radius: Half-width of the square window in pixels.
            channel: ABI channel id (e.g. 'C03').

        Returns:
            Column names in the same order as the flattened window values.
        """
        dim = 2 * radius + 1
        return [
            f"radius={radius}_{channel}_{idx // dim + 1}{idx % dim + 1}"
            for idx in range(dim * dim)
        ]

    @staticmethod
    def parse_timestamp(time_str: str | bytes) -> pd.Timestamp:
        """Parse ``time_coverage_start`` into a UTC timestamp rounded to 10 minutes.

        Args:
            time_str: ISO-8601 string (or bytes, as returned by h5py) such as
                '2022-02-01T00:00:20.6Z'.

        Returns:
            Timezone-aware UTC timestamp rounded to the nearest 10 minutes.
        """
        if isinstance(time_str, bytes):
            time_str = time_str.decode()
        return pd.to_datetime(time_str).round("10min").tz_convert("UTC")

    @staticmethod
    def read_windows_h5(
        file_obj: BinaryIO,
        variable: str,
        radius: int,
        pixels: dict[str, tuple[int, int]],
    ) -> tuple[pd.Timestamp, dict[str, np.ndarray]]:
        """Read only the pixel windows of ``variable`` straight from the HDF5 file.

        Skips xarray, which parses the metadata of every variable in the file (more
        requests and CPU per file over S3). Applies the CF packing the same way xarray
        does: ``_FillValue`` becomes NaN, then ``value * scale_factor + add_offset`` in
        the attribute's dtype, so the values are identical to the xarray path.

        Args:
            file_obj: Seekable binary file (e.g. an s3fs file opened with a block cache).
            variable: Name of the data variable to read (e.g. 'Rad').
            radius: Half-width of the square window in pixels.
            pixels: Mapping station name -> (i, j) target pixel indices.

        Returns:
            (timestamp, windows): the file's rounded UTC timestamp and a mapping
            station name -> 1-D array with (2*radius+1)^2 values.
        """
        with h5py.File(file_obj, "r") as h5:
            data = h5[variable]
            scale = _scalar_attr(data, "scale_factor")
            offset = _scalar_attr(data, "add_offset")
            fill = _scalar_attr(data, "_FillValue")

            windows = {}
            for station, (i_center, j_center) in pixels.items():
                i_slice = slice(max(i_center - radius, 0), i_center + radius + 1)
                j_slice = slice(max(j_center - radius, 0), j_center + radius + 1)
                raw = data[i_slice, j_slice].flatten()
                windows[station] = _decode_packed(raw, scale, offset, fill)

            timestamp = GoesProcessor.parse_timestamp(h5.attrs["time_coverage_start"])

        return timestamp, windows


def _scalar_attr(data: h5py.Dataset, name: str) -> np.generic | None:
    """Return an HDF5 attribute as a numpy scalar (they are stored as 1-element arrays)."""
    if name not in data.attrs:
        return None
    return np.ravel(data.attrs[name])[0]


def _decode_packed(
    raw: np.ndarray,
    scale: np.generic | None,
    offset: np.generic | None,
    fill: np.generic | None,
) -> np.ndarray:
    """Undo CF packing: mask the fill value, then apply ``scale_factor`` and ``add_offset``."""
    if scale is None and offset is None and fill is None:
        return raw

    values = raw.astype(np.float32)
    if fill is not None:
        values[raw == fill] = np.nan
    if scale is not None:
        values = values * scale
    if offset is not None:
        values = values + offset
    return values
