import logging

import pandas as pd

from masters_project.enums import GoesChannelEnums
from masters_project.loaders.csv import CSVExporter
from masters_project.processors.merger import DatasetMerger
from masters_project.settings import settings

settings.setup_logging("building_features")
logger = logging.getLogger(__name__)


def build_final_dataset() -> None:
    """Load GOES CSVs (all channels), merge with SONDA CSV, align by timestamp, drop NaNs, export final features CSV.

    Reads paths from settings; merges GOES channels on timestamp, then inner-joins with SONDA.
    Writes the result to the processed path. Exits early if GOES or SONDA data is missing.
    """
    df_goes_combined = None

    for channel in GoesChannelEnums:
        channel_val = channel.value
        goes_path = (
            settings.RAW_PATH
            / "goes"
            / f"goes_{channel_val}_st_{settings.general.start_date}_et_{settings.general.end_date}_{settings.station.name}.csv"
        )

        if goes_path.exists():
            logger.debug(f"Loading GOES dataset for channel: {channel_val}")
            df_channel = pd.read_csv(goes_path, parse_dates=["timestamp"])

            if df_goes_combined is None:
                df_goes_combined = df_channel
                logger.info(
                    f"Initialized GOES base with {channel_val}. Shape: {df_goes_combined.shape}"
                )
            else:
                initial_rows = len(df_goes_combined)
                df_goes_combined = pd.merge(
                    df_goes_combined, df_channel, on="timestamp", how="inner"
                )
                logger.info(
                    f"Merged {channel_val}. Shape: {df_goes_combined.shape}. "
                    f"Dropped {initial_rows - len(df_goes_combined)} unaligned rows."
                )
        else:
            logger.warning(f"No file found for {channel_val}. Skipping this channel.")

    if df_goes_combined is None or df_goes_combined.empty:
        logger.error("No valid GOES data was loaded! Halting pipeline.")
        return

    sonda_path = (
        settings.RAW_PATH
        / "sonda"
        / f"sonda_{settings.general.sonda_data_type}_st_{settings.general.start_date}_et_{settings.general.end_date}_{settings.station.name}.csv"
    )

    if not sonda_path.exists():
        logger.error(f"SONDA file not found at: {sonda_path}. Halting pipeline.")
        return

    logger.info(f"Loading SONDA dataset from: {sonda_path}")
    df_sonda = pd.read_csv(sonda_path, parse_dates=["timestamp"])

    logger.info("Aligning GOES satellite features with SONDA ground targets...")
    df_final = DatasetMerger.merge_goes_sonda(
        df_goes=df_goes_combined,
        df_sonda=df_sonda,
        time_col_goes="timestamp",
        time_col_sonda="timestamp",
    )

    initial_final_rows = len(df_final)
    logger.info("Purging rows with missing target values (NaNs)...")
    df_final = df_final.dropna().reset_index(drop=True)

    dropped_nans = initial_final_rows - len(df_final)
    if dropped_nans > 0:
        logger.warning(
            f"Dropped {dropped_nans} rows containing NaNs. These cannot be used for ML training."
        )

    output_path = (
        settings.PROCESSED_PATH
        / f"final_features_st_{settings.general.start_date}_et_{settings.general.end_date}_{settings.station.name}.csv"
    )

    CSVExporter().export(df_final, output_path)


if __name__ == "__main__":
    build_final_dataset()
