"""Build merged model input per station: stack GOES channels, filter and join SONDA, drop NaNs, export CSV."""

import logging
import sys
from collections import defaultdict

import pandas as pd

from masters_project.enums import GoesChannel
from masters_project.etl.filters import drop_frozen, keep_daytime
from masters_project.etl.merger import DatasetMerger
from masters_project.etl.schemas import DataValidationError, validate_model_input
from masters_project.exporters import CSVExporter
from masters_project.file_paths import file_paths
from masters_project.settings import settings

logger = logging.getLogger(__name__)


def build_station_input(station: str) -> bool:
    """Build and write the model input of one station.

    Steps: stack the station's GOES CSVs of each channel (overlapping periods are merged,
    repeated timestamps dropped), join the channels side by side, filter the SONDA target
    (daytime and frozen-sensor filters from ``etl_config.filters``), inner-join it with the
    GOES features, drop rows with missing values, validate the result and write it.

    Only GOES files in ``data/raw/goes`` whose name ends with ``_{station}.csv`` are used.
    The output goes to :func:`~masters_project.file_paths.FilePathBuilder.model_input`.

    Args:
        station: Station key (a key of ``etl_config.stations``).

    Returns:
        True if the model input was written; False if the GOES or SONDA input is missing,
        or the result fails validation (nothing is written then).
    """
    goes_dir = settings.RAW_PATH / "goes"

    channel_groups = defaultdict(list)

    logger.info(f"[{station}] Scanning directory: {goes_dir}")

    for file_path in goes_dir.glob(f"goes_*_{station}.csv"):
        for channel in GoesChannel:
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

    df_sonda = pd.read_csv(sonda_path, parse_dates=["timestamp"])
    df_sonda["timestamp"] = pd.to_datetime(df_sonda["timestamp"], utc=True)

    # Filters act on the SONDA series at its native 1-minute resolution, before the join.
    station_position = settings.stations[station]
    filters = settings.etl.filters
    df_sonda = keep_daytime(
        df_sonda,
        station_position.latitude,
        station_position.longitude,
        filters.min_solar_elevation,
    )
    df_sonda = drop_frozen(
        df_sonda,
        settings.etl.sonda.target_variable,
        filters.frozen_min_run,
        station_position.latitude,
        station_position.longitude,
    )

    logger.info(f"[{station}] Merging GOES features with SONDA ground targets...")
    df_final = DatasetMerger.join_by_columns(
        [df_goes_combined, df_sonda], time_col="timestamp", how="inner"
    )

    initial_len = len(df_final)
    df_final = df_final.dropna().reset_index(drop=True)
    logger.info(
        f"[{station}] Final dataset ready: {len(df_final)} rows "
        f"({initial_len - len(df_final)} dropped for NaNs)."
    )

    try:
        validate_model_input(
            df_final, settings.etl.sonda.target_variable, label=f"[{station}]"
        )
    except DataValidationError:
        logger.exception(f"[{station}] The model input is not valid; not written.")
        return False

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
    settings.setup_logging("building_model_input")
    sys.exit(build_model_input())
