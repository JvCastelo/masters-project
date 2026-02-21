import logging

import pandas as pd

from masters_project.clients.goes_s3 import GoesS3Client
from masters_project.config import GeneralConfig, GoesConfig, SondaConfig, settings
from masters_project.enums import GoesChannelEnums
from masters_project.loaders.csv import CSVExporter
from masters_project.processors.goes import GoesProcessor

settings.setup_logging()

logger = logging.getLogger(__name__)

client = GoesS3Client(
    product_name=GoesConfig.PRODUCT_NAME, bucket_name=GoesConfig.BUCKET_NAME
)

# channels = [channel.value for channel in GoesChannelEnums] ## All channels
# channels = [channel.value for channel in GoesChannelEnums if channel.value != 'C02'] ## All channels but C02
channels = [GoesChannelEnums.C01.value]  ## Only one channel

list_files_path = client.get_files_path_by_day(
    year=GeneralConfig.YEAR, channels=channels
)

target_indices = None

for channel in channels:
    all_dfs = []

    for path in list_files_path:
        with client.get_file(path) as file:
            ds = GoesProcessor.open_as_dataset(file)

            ds = GoesProcessor.add_metadata(ds)

            if target_indices is None:
                logger.info(
                    "First file: Calculating Lat/Lon grid and target indices..."
                )
                ds = GoesProcessor.add_lat_lon_dimensions(ds)
                ds = GoesProcessor.get_nearest_xy_from_latlon(
                    ds,
                    lat_target=GoesConfig.TARGET_LATITUDE,
                    lon_target=GoesConfig.TARGET_LONGITUDE,
                )
                target_indices = (
                    ds.attrs["target_pixel_i"],
                    ds.attrs["target_pixel_j"],
                )
            else:
                ds.attrs["target_pixel_i"], ds.attrs["target_pixel_j"] = target_indices

            try:
                df_row = GoesProcessor.extract_window_to_df(
                    ds, variable=GoesConfig.GOES_VARIABLE, radius=GoesConfig.RADIUS
                )
                all_dfs.append(df_row)
            except Exception as e:
                logger.error(f"Failed to process {path}: {e}")

    df_goes = pd.concat(all_dfs, ignore_index=True)

    base_path = (
        GoesConfig.BASE_PATH_FILE
        / f"goes_{channel}_{GeneralConfig.YEAR}_{SondaConfig.STATION}.csv"
    )

    CSVExporter().export(df_goes, base_path)
