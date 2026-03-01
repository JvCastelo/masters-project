import io
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import pandas as pd
from tqdm import tqdm

from masters_project.clients.goes_s3 import GoesS3Client
from masters_project.enums import GoesChannelEnums
from masters_project.loaders.csv import CSVExporter
from masters_project.processors.goes import GoesProcessor
from masters_project.settings import settings

settings.setup_logging("goes_etl")
logger = logging.getLogger(__name__)

hdf5_lock = Lock()

client = GoesS3Client(
    product_name=settings.general.goes_product_name,
    bucket_name=settings.general.goes_bucket_name,
)
channels = [GoesChannelEnums.C01.value]  ## Only one channel
# channels = [channel.value for channel in GoesChannelEnums] ## All channels
# channels = [channel.value for channel in GoesChannelEnums if channel.value != 'C02'] ## All channels but C02


def process_single_file(path: str, target_i: int, target_j: int) -> pd.DataFrame | None:
    """Download one GOES NetCDF from S3, extract a pixel window, and return a one-row DataFrame.

    Args:
        path: S3 object key for the NetCDF file.
        target_i: Row index of the target pixel in the dataset grid.
        target_j: Column index of the target pixel in the dataset grid.

    Returns:
        A DataFrame with one row of window features and timestamp, or None on failure.
    """
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
                    ds,
                    variable=settings.general.goes_variable,
                    radius=settings.general.pixel_radius,
                )
    except Exception:
        logger.exception(f"Failed to process {path}")
        return None


def main() -> None:
    """Run the GOES ETL: discover files by channel/date, extract windows at station coords, export CSV per channel."""
    for channel in channels:
        logger.info(f"--- Starting processing for channel {channel} ---")

        list_files_path = client.get_files_path(
            start_date=settings.general.start_date,
            # end_date=settings.general.end_date,
            channel=channel,
        )

        if not list_files_path:
            logger.warning(f"No files found for {channel}. Skipping to next channel.")
            continue

        first_file_path, *remaining_files_path = list_files_path

        all_dfs = []

        logger.info("Setting up grid coordinates using the first file...")
        try:
            with client.get_file(first_file_path) as file:
                file_bytes = file.read()
            virtual_file = io.BytesIO(file_bytes)

            with GoesProcessor.open_as_dataset(virtual_file) as ds:
                ds = ds.pipe(GoesProcessor.add_metadata)

                target_index_i, target_index_j = GoesProcessor.get_target_indices(
                    ds, lat=settings.station.latitude, lon=settings.station.longitude
                )
                ds.attrs["target_pixel_i"] = target_index_i
                ds.attrs["target_pixel_j"] = target_index_j

                first_df = GoesProcessor.extract_window_to_df(
                    ds,
                    variable=settings.general.goes_variable,
                    radius=settings.general.pixel_radius,
                )

                all_dfs.append(first_df)
        except Exception:
            logger.exception(
                "Failed to establish grid coordinates from the first file. Halting channel."
            )
            continue

        logger.info(
            f"Target locked at i={target_index_i}, j={target_index_j}. Commencing threaded downloads."
        )

        with ThreadPoolExecutor(max_workers=settings.general.max_workers) as executor:
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
            logger.info("Concatenating and sorting dataset chronologically...")
            df_goes = pd.concat(all_dfs, ignore_index=True)

            df_goes = df_goes.sort_values(by="timestamp").reset_index(drop=True)

            base_path = (
                settings.RAW_PATH
                / "goes"
                / f"goes_{channel}_st_{settings.general.start_date}_et_{settings.general.end_date}_{settings.station.name}.csv"
            )

            CSVExporter().export(df_goes, base_path)
        else:
            logger.warning(
                f"No valid data was extracted for channel {channel}. Skipping export."
            )


if __name__ == "__main__":
    main()
