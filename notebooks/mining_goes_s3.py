import logging

import pandas as pd

from masters_project.clients.goes_s3 import GoesS3Client
from masters_project.config import settings
from masters_project.processors.goes import GoesProcessor

settings.setup_logging()

logger = logging.getLogger(__name__)

client = GoesS3Client(
    product_name=settings.PRODUCT_NAME, bucket_name=settings.BUCKET_NAME
)

list_files_path = client.get_files_path_by_day(year=2019)
list_files_path = list_files_path[:3]
print(list_files_path)

target_indices = None
all_dfs = []

for path in list_files_path:
    with client.get_file(path) as file:
        ds = GoesProcessor.open_as_dataset(file)

        ds = GoesProcessor.add_metadata(ds)

        if target_indices is None:
            logger.info("First file: Calculating Lat/Lon grid and target indices...")
            ds = GoesProcessor.add_lat_lon_dimensions(ds)
            ds = GoesProcessor.get_nearest_xy_from_latlon(
                ds,
                lat_target=settings.TARGET_LATITUDE,
                lon_target=settings.TARGET_LONGITUDE,
            )
            target_indices = (ds.attrs["target_pixel_i"], ds.attrs["target_pixel_j"])
        else:
            ds.attrs["target_pixel_i"], ds.attrs["target_pixel_j"] = target_indices

        try:
            df_row = GoesProcessor.extract_window_to_df(
                ds, variable=settings.GOES_VARIABLE, radius=settings.RADIUS
            )
            all_dfs.append(df_row)
        except Exception as e:
            logger.error(f"Failed to process {path}: {e}")

df_satellite = pd.concat(all_dfs, ignore_index=True)

df_satellite.to_csv("data/satellite.csv", index=False)
