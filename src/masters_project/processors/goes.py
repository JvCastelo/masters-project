import io
import logging

import numpy as np
import pandas as pd
import xarray as xr

from masters_project.utils import measure_memory, time_track

logger = logging.getLogger(__name__)


class GoesProcessor:
    @staticmethod
    @measure_memory
    @time_track
    def open_as_dataset(file_obj) -> xr.Dataset:
        files_bytes = file_obj.read()
        virtual_file = io.BytesIO(files_bytes)
        logger.info("Opening file-like object as NetCDF dataset.")
        return xr.open_dataset(virtual_file, engine="h5netcdf")

    @staticmethod
    @measure_memory
    @time_track
    def add_metadata(ds: xr.Dataset) -> xr.Dataset:
        logger.info("Extracting metadata from dataset attributes.")
        file_name = ds.attrs.get("dataset_name", "")

        try:
            channel = file_name.split("_")[1].split("-")[-1][-3:]
            logger.debug(f"Detected channel: {channel}")
        except Exception as e:
            channel = "UNK"
            logger.exception(f"Error catching name: {e}, defined as {channel}")

        time_str = ds.attrs.get("time_coverage_start")
        timestamp = pd.to_datetime(time_str).round("10min").tz_convert("UTC")
        logger.debug(f"Computed timestamp: {timestamp}")

        ds.attrs["channel"] = channel
        ds.attrs["timestamp"] = timestamp

        return ds

    @staticmethod
    @measure_memory
    @time_track
    def get_target_indices(ds: xr.Dataset, lat: float, lon: float) -> tuple[int, int]:
        """
        Inverse GOES Imager Projection: Converts a target Lat/Lon into dataset i, j indices.
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

        logger.info(f"Target found at indices: i={i}, j={j}")
        return int(i), int(j)

    @staticmethod
    @measure_memory
    @time_track
    def extract_window_to_df(
        ds: xr.Dataset, variable: str, radius: int
    ) -> pd.DataFrame:
        logger.info(f"Extracting {variable} window with radius {radius}.")

        i_center = ds.attrs.get("target_pixel_i")
        j_center = ds.attrs.get("target_pixel_j")
        channel = ds.attrs.get("channel")
        timestamp = ds.attrs.get("timestamp")

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
        df["timestamp"] = timestamp

        logger.info(f"Successfully created DataFrame with {len(cols)} window columns.")
        return df
