import logging

import numpy as np
import pandas as pd
import xarray as xr

logger = logging.getLogger(__name__)


class GoesProcessor:
    @staticmethod
    def open_as_dataset(file_obj) -> xr.Dataset:
        logger.info("Opening file-like object as NetCDF dataset.")
        return xr.open_dataset(file_obj, engine="h5netcdf")

    @staticmethod
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
        timestamp = pd.to_datetime(time_str).ceil("15min").tz_convert("UTC")
        logger.debug(f"Computed timestamp: {timestamp}")

        ds.attrs["channel"] = channel
        ds.attrs["timestamp"] = timestamp

        return ds

    @staticmethod
    def add_lat_lon_dimensions(ds: xr.Dataset) -> xr.Dataset:
        """
        The math for this function was taken from:
        https://makersportal.com/blog/2018/11/25/goes-r-satellite-latitude-and-longitude-grid-projection-algorithm
        """
        logger.info("Starting Latitude/Longitude grid projection algorithm.")

        x = ds.x
        y = ds.y
        goes_imager_projection = ds.goes_imager_projection

        logger.debug(f"Meshgrid shape: {len(y)}x{len(x)}")
        x, y = np.meshgrid(x, y)

        r_eq = goes_imager_projection.attrs["semi_major_axis"]
        r_pol = goes_imager_projection.attrs["semi_minor_axis"]
        l_0 = goes_imager_projection.attrs["longitude_of_projection_origin"] * (
            np.pi / 180
        )
        h_sat = goes_imager_projection.attrs["perspective_point_height"]
        H = r_eq + h_sat

        a = np.sin(x) ** 2 + (
            np.cos(x) ** 2 * (np.cos(y) ** 2 + (r_eq**2 / r_pol**2) * np.sin(y) ** 2)
        )
        b = -2 * H * np.cos(x) * np.cos(y)
        c = H**2 - r_eq**2

        r_s = (-b - np.sqrt(b**2 - 4 * a * c)) / (2 * a)

        s_x = r_s * np.cos(x) * np.cos(y)
        s_y = -r_s * np.sin(x)
        s_z = r_s * np.cos(x) * np.sin(y)

        lat = np.arctan(
            (r_eq**2 / r_pol**2) * (s_z / np.sqrt((H - s_x) ** 2 + s_y**2))
        ) * (180 / np.pi)
        lon = (l_0 - np.arctan(s_y / (H - s_x))) * (180 / np.pi)

        ds = ds.assign_coords({"lat": (["y", "x"], lat), "lon": (["y", "x"], lon)})
        ds.lat.attrs["units"] = "degrees_north"
        ds.lon.attrs["units"] = "degrees_east"

        logger.info("Lat/Lon dimensions successfully assigned to dataset.")
        return ds

    @staticmethod
    def get_nearest_xy_from_latlon(ds, lat_target, lon_target) -> xr.Dataset:
        logger.info(
            f"Searching for nearest pixel to Lat: {lat_target}, Lon: {lon_target}"
        )
        lat = ds.lat.data
        lon = ds.lon.data

        valid_pixels = ~np.isnan(lat) & ~np.isnan(lon)

        distance = np.full(lat.shape, np.inf)
        distance[valid_pixels] = np.sqrt(
            (lat[valid_pixels] - lat_target) ** 2
            + (lon[valid_pixels] - lon_target) ** 2
        )

        i, j = np.unravel_index(np.argmin(distance), distance.shape)
        ds.attrs["target_pixel_i"], ds.attrs["target_pixel_j"] = i, j

        logger.info(f"Nearest pixel found at indices: i={i}, j={j}")
        return ds

    @staticmethod
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
