import logging
from collections import defaultdict

import pandas as pd

from masters_project.enums import GoesChannelEnums
from masters_project.file_paths import file_paths
from masters_project.loaders.csv import CSVExporter
from masters_project.processors.merger import DatasetMerger
from masters_project.settings import settings

settings.setup_logging("building_model_input")
logger = logging.getLogger(__name__)


def build_model_input() -> None:
    """
    1. List all GOES files.
    2. Group and concat by row for each channel.
    3. Join channels together by timestamp.
    4. Join with SONDA data.
    """
    goes_dir = settings.RAW_PATH / "goes"

    channel_groups = defaultdict(list)

    logger.info(f"Scanning directory: {goes_dir}")

    for file_path in goes_dir.glob("goes_*.csv"):
        for channel in GoesChannelEnums:
            if f"goes_{channel.value}_" in file_path.name:
                logger.debug(f"Found file for {channel.value}: {file_path.name}")
                df = pd.read_csv(file_path)
                channel_groups[channel.value].append(df)
                break

    if not channel_groups:
        logger.error("No GOES files found. Check paths and naming conventions.")
        return

    channel_dfs = []
    for channel_val, df_list in channel_groups.items():
        logger.info(f"Concatenating {len(df_list)} files for channel {channel_val}")
        df_chan_stacked = DatasetMerger.concat_by_rows(df_list, time_col="timestamp")
        channel_dfs.append(df_chan_stacked)

    logger.info("Joining all GOES channels into a single feature set.")
    df_goes_combined = DatasetMerger.join_by_columns(channel_dfs, time_col="timestamp")

    sonda_path = file_paths.raw_sonda()

    if not sonda_path.exists():
        logger.error(f"SONDA file not found: {sonda_path}")
        return

    df_sonda = pd.read_csv(sonda_path)

    logger.info("Merging GOES features with SONDA ground targets...")
    df_final = DatasetMerger.join_by_columns(
        [df_goes_combined, df_sonda], time_col="timestamp", how="inner"
    )

    initial_len = len(df_final)
    df_final = df_final.dropna().reset_index(drop=True)

    logger.info(
        f"Final dataset ready. Dropped {initial_len - len(df_final)} rows with NaNs."
    )

    output_path = file_paths.model_input()

    CSVExporter().export(df_final, output_path)


if __name__ == "__main__":
    build_model_input()
