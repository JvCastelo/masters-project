import logging
from functools import reduce

import pandas as pd

from masters_project.config import GeneralConfig, GoesConfig, SondaConfig, settings
from masters_project.enums import GoesChannelEnums
from masters_project.loaders.csv import CSVExporter
from masters_project.processors.merger import DatasetMerger

settings.setup_logging()
logger = logging.getLogger(__name__)


def build_final_dataset():
    goes_dfs = []

    for channel in GoesChannelEnums:
        channel_val = channel.value
        goes_path = (
            GoesConfig.BASE_PATH_FILE
            / f"goes_{channel_val}_st_{GeneralConfig.START_DATE}_et_{GeneralConfig.END_DATE}_{SondaConfig.STATION}.csv"
        )

        if goes_path.exists():
            logger.info(f"Loading GOES dataset for channel: {channel_val}")
            df_channel = pd.read_csv(goes_path)

            goes_dfs.append(df_channel)
        else:
            logger.debug(f"No file found for {channel_val}. Skipping.")

    if not goes_dfs:
        logger.error("No GOES files were found in the raw directory! Halting.")
        return

    logger.info("Merging all collected GOES channels into a single dataframe...")
    df_goes_combined = reduce(
        lambda left, right: pd.merge(left, right, on="timestamp", how="inner"), goes_dfs
    )

    sonda_path = (
        SondaConfig.BASE_PATH_FILE
        / f"sonda_{SondaConfig.DATA_TYPE}_st_{GeneralConfig.START_DATE}_et_{GeneralConfig.END_DATE}_{SondaConfig.STATION}.csv"
    )

    if not sonda_path.exists():
        logger.error(f"SONDA file not found at: {sonda_path}. Halting.")
        return

    logger.info(f"Loading SONDA dataset from: {sonda_path}")
    df_sonda = pd.read_csv(sonda_path)

    logger.info("Merging combined GOES features with SONDA ground truth targets...")
    df_final = DatasetMerger.merge_goes_sonda(
        df_goes=df_goes_combined,
        df_sonda=df_sonda,
        time_col_goes="timestamp",
        time_col_sonda="timestamp",
    )

    GeneralConfig.BASE_PATH_FILE_INPUT_MODEL.mkdir(parents=True, exist_ok=True)

    output_path = (
        GeneralConfig.BASE_PATH_FILE_INPUT_MODEL
        / f"final_features_st_{GeneralConfig.START_DATE}_et_{GeneralConfig.END_DATE}_{SondaConfig.STATION}.csv"
    )

    CSVExporter().export(df_final, output_path)
    logger.info(f"Success! Model-ready dataset saved to: {output_path}")


if __name__ == "__main__":
    build_final_dataset()
