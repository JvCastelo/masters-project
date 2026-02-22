import io
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import pandas as pd
from tqdm import tqdm

from masters_project.clients.goes_s3 import GoesS3Client
from masters_project.config import GeneralConfig, GoesConfig, SondaConfig, settings
from masters_project.enums import GoesChannelEnums
from masters_project.loaders.csv import CSVExporter
from masters_project.processors.goes import GoesProcessor

settings.setup_logging()
logger = logging.getLogger(__name__)

hdf5_lock = Lock()

client = GoesS3Client(
    product_name=GoesConfig.PRODUCT_NAME, bucket_name=GoesConfig.BUCKET_NAME
)
channels = [GoesChannelEnums.C01.value]  ## Only one channel
# channels = [channel.value for channel in GoesChannelEnums] ## All channels
# channels = [channel.value for channel in GoesChannelEnums if channel.value != 'C02'] ## All channels but C02


def process_single_file(path: str, target_i: int, target_j: int) -> pd.DataFrame:
    try:
        with client.get_file(path) as file:
            file_bytes = file.read()

        virtual_file = io.BytesIO(file_bytes)

        with hdf5_lock:
            with GoesProcessor.open_as_dataset(virtual_file) as ds:
                ds = ds.pipe(GoesProcessor.add_metadata)
                ds.attrs["target_pixel_i"] = target_i
                ds.attrs["target_pixel_j"] = target_j

                return GoesProcessor.extract_window_to_df(
                    ds, variable=GoesConfig.GOES_VARIABLE, radius=GoesConfig.RADIUS
                )
    except Exception as e:
        logger.error(f"Failed to process {path}: {e}")
        return None


for channel in channels:
    logger.info(f"--- Starting processing for channel {channel} ---")

    list_files_path = client.get_files_path(
        start_date=GeneralConfig.START_DATE,
        # end_date=GeneralConfig.END_DATE,
        channel=channel,
    )

    if not list_files_path:
        logger.warning(f"No files found for {channel}. Skipping. ")

    first_file_path, *remaining_files_path = list_files_path

    all_dfs = []

    logger.info("Setting up grid coordinates using the first file...")
    with client.get_file(first_file_path) as file:
        file_bytes = file.read()
    virtual_file = io.BytesIO(file_bytes)

    with GoesProcessor.open_as_dataset(virtual_file) as ds:
        ds = ds.pipe(GoesProcessor.add_metadata)

        target_index_i, target_index_j = GoesProcessor.get_target_indices(
            ds, lat=GoesConfig.TARGET_LATITUDE, lon=GoesConfig.TARGET_LONGITUDE
        )
        ds.attrs["target_pixel_i"] = target_index_i
        ds.attrs["target_pixel_j"] = target_index_j

        first_df = GoesProcessor.extract_window_to_df(
            ds, variable=GoesConfig.GOES_VARIABLE, radius=GoesConfig.RADIUS
        )

        all_dfs.append(first_df)

    logger.info(
        f"Target locked at i={target_index_i}, j={target_index_j}. Commencing threaded downloads."
    )

    with ThreadPoolExecutor(max_workers=GoesConfig.MAX_WORKERS) as executor:
        future_to_path = {
            executor.submit(
                process_single_file, path, target_index_i, target_index_j
            ): path
            for path in remaining_files_path
        }

        for future in tqdm(
            as_completed(future_to_path),
            total=len(remaining_files_path),
            desc=f"Processing {channel}",
        ):
            df_row = future.result()
            if df_row is not None:
                all_dfs.append(df_row)

    if all_dfs:
        df_goes = pd.concat(all_dfs, ignore_index=True)

        base_path = (
            GoesConfig.BASE_PATH_FILE
            / f"goes_{channel}_st_{GeneralConfig.START_DATE}_et_{GeneralConfig.END_DATE}_{SondaConfig.STATION}.csv"
        )

        CSVExporter().export(df_goes, base_path)
        logger.info(f"Successfully exported data for {channel}.")
    else:
        logger.warning(f"No data was extracted for channel {channel}. Skipping export.")
