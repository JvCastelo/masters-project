"""Build merged model input per station: stack GOES channels, join SONDA, drop NaNs, keep daytime, export CSV."""

import logging
import sys
from collections import defaultdict

import pandas as pd

from masters_project.enums import GoesChannelEnums
from masters_project.file_paths import file_paths
from masters_project.exporters import CSVExporter
from masters_project.etl.merger import DatasetMerger
from masters_project.etl.solar import SolarProcessor
from masters_project.settings import settings

settings.setup_logging("building_model_input")
logger = logging.getLogger(__name__)


def build_station_input(station: str) -> bool:
    """Concatenate ``station``'s GOES CSVs per channel, join channels on time, join SONDA, drop NaNs.

    Only GOES files whose name ends with ``_{station}.csv`` are used. Writes the result to
    :func:`~masters_project.file_paths.FilePathBuilder.model_input`.

    Args:
        station: Station key (a key of ``etl_config.stations``).

    Returns:
        True if the model input was written, False if inputs were missing.
    """
    goes_dir = settings.RAW_PATH / "goes"

    channel_groups = defaultdict(list)

    logger.info(f"[{station}] Scanning directory: {goes_dir}")

    for file_path in goes_dir.glob(f"goes_*_{station}.csv"):
        for channel in GoesChannelEnums:
            if f"goes_{channel.value}_" in file_path.name:
                logger.debug(f"Found file for {channel.value}: {file_path.name}")
                df = pd.read_csv(file_path)
                channel_groups[channel.value].append(df)
                break

    if not channel_groups:
        logger.error(
            f"[{station}] No GOES files found. Check paths and naming conventions."
        )
        return False

    channel_dfs = []
    for channel_val, df_list in channel_groups.items():
        logger.info(
            f"[{station}] Concatenating {len(df_list)} files for channel {channel_val}"
        )
        df_chan_stacked = DatasetMerger.concat_by_rows(df_list, time_col="timestamp")
        channel_dfs.append(df_chan_stacked)

    logger.info(f"[{station}] Joining all GOES channels into a single feature set.")
    df_goes_combined = DatasetMerger.join_by_columns(channel_dfs, time_col="timestamp")

    sonda_path = file_paths.raw_sonda(station)

    if not sonda_path.exists():
        logger.error(f"[{station}] SONDA file not found: {sonda_path}")
        return False

    df_sonda = pd.read_csv(sonda_path)

    logger.info(f"[{station}] Merging GOES features with SONDA ground targets...")
    df_final = DatasetMerger.join_by_columns(
        [df_goes_combined, df_sonda], time_col="timestamp", how="inner"
    )

    initial_len = len(df_final)
    df_final = df_final.dropna().reset_index(drop=True)

    logger.info(f"[{station}] Dropped {initial_len - len(df_final)} rows with NaNs.")

    coordinates = settings.stations[station]
    df_final = SolarProcessor.keep_daytime(
        df_final,
        lat=coordinates.latitude,
        lon=coordinates.longitude,
        min_elevation=settings.etl.min_solar_elevation,
    )
    logger.info(f"[{station}] Final dataset ready: {len(df_final)} rows.")

    CSVExporter().export(df_final, file_paths.model_input(station))
    return True


def build_model_input() -> int:
    """Build the merged model input for every station in ``etl_config.stations``.

    Returns:
        0 if every station was built, 1 if any station lacked its GOES or SONDA input.
    """
    failed = [s for s in settings.stations if not build_station_input(s)]
    if failed:
        logger.error(f"Model input could not be built for: {failed}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(build_model_input())
