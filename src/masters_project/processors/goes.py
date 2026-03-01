import logging

import numpy as np
import pandas as pd
import xarray as xr

logger = logging.getLogger(__name__)


class GoesProcessor:
    """Process GOES NetCDF: open as xarray, add channel/timestamp metadata, project lat/lon to indices, extract pixel windows."""

    @staticmethod
    def open_as_dataset(file_obj) -> xr.Dataset:
        """Open a file-like object as an xarray Dataset using the h5netcdf engine.

        Args:
            file_obj: File-like object (e.g. BytesIO) containing NetCDF bytes.

        Returns:
            xarray Dataset for the GOES NetCDF.
        """
        logger.debug("Opening file-like object as NetCDF dataset.")
        return xr.open_dataset(file_obj, engine="h5netcdf", cache=False)

    @staticmethod
    def add_metadata(ds: xr.Dataset) -> xr.Dataset:
        """Parse channel and time from dataset attributes and attach to ds.attrs.

        Args:
            ds: GOES xarray Dataset with dataset_name and time_coverage_start.

        Returns:
            The same Dataset with attrs['channel'] and attrs['timestamp'] set.
        """
        logger.debug("Extracting metadata from dataset attributes.")
        file_name = ds.attrs.get("dataset_name", "")

        try:
            channel = file_name.split("_")[1].split("-")[-1][-3:]
            logger.debug(f"Detected channel: {channel}")
        except Exception:
            channel = "UNK"
            logger.exception(f"Error catching channel name. Defaulting to {channel}")

        time_str = ds.attrs.get("time_coverage_start")
        timestamp = pd.to_datetime(time_str).round("10min").tz_convert("UTC")
        logger.debug(f"Computed timestamp: {timestamp}")

        ds.attrs["channel"] = channel
        ds.attrs["timestamp"] = timestamp

        return ds

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
    def extract_window_to_df(
        ds: xr.Dataset, variable: str, radius: int
    ) -> pd.DataFrame:
        """Extract a (2*radius+1)^2 window around the target pixel and flatten to a one-row DataFrame.

        Expects ds.attrs to contain target_pixel_i, target_pixel_j, and channel.

        Args:
            ds: GOES Dataset with the variable and target indices in attrs.
            variable: Name of the data variable to extract (e.g. 'Rad').
            radius: Half-width of the square window in pixels (window size = 2*radius+1).

        Returns:
            DataFrame with one row: flattened window columns (radius={r}_{channel}_{row}{col}) and timestamp.

        Raises:
            ValueError: If target_pixel_i or target_pixel_j are missing from ds.attrs.
        """
        logger.debug(f"Extracting {variable} window with radius {radius}.")

        i_center = ds.attrs.get("target_pixel_i")
        j_center = ds.attrs.get("target_pixel_j")
        channel = ds.attrs.get("channel")

        if i_center is None or j_center is None:
            logger.error(
                "Target indices missing from dataset attributes. Extraction failed."
            )
            raise ValueError(
                "target_pixel_i and target_pixel_j must be defined in ds.attrs"
            )
        i_slice = slice(max(i_center - radius, 0), i_center + radius + 1)
        j_slice = slice(max(j_center - radius, 0), j_center + radius + 1)

        window_data = ds[variable].isel(y=i_slice, x=j_slice).values

        window_flat = window_data.flatten()

        # Generate column names (radius={radius}_{channel}_{row}{col})
        matrix_dim = 2 * radius + 1
        cols = []
        for idx in range(len(window_flat)):
            row, col = (idx // matrix_dim) + 1, (idx % matrix_dim) + 1
            cols.append(f"radius={radius}_{channel}_{row}{col}")

        df = pd.DataFrame([window_flat], columns=cols)
        df["timestamp"] = ds.attrs.get("timestamp")

        logger.debug(f"Successfully created DataFrame with {len(cols)} window columns.")
        return df
