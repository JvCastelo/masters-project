"""Build merged model input per station: stack GOES channels, filter and join SONDA, drop NaNs, export CSV."""

import argparse
import logging
import sys
from collections import defaultdict
from collections.abc import Sequence

import pandas as pd

from masters_project.enums import GoesChannel
from masters_project.etl.filters import drop_frozen, keep_daytime
from masters_project.etl.merger import DatasetMerger
from masters_project.etl.schemas import DataValidationError, validate_model_input
from masters_project.exporters import CSVExporter, JSONExporter
from masters_project.file_paths import file_paths
from masters_project.logging_setup import git_revision, run_logging
from masters_project.settings import settings
from masters_project.utils import file_sha256, utc_now_iso

# Run with ``python -m``, ``__name__`` is "__main__"; ``__spec__.name`` is the real module name.
logger = logging.getLogger(__spec__.name if __spec__ else __name__)


def write_manifest(station: str, df: pd.DataFrame, goes_dir: str | None) -> None:
    """Record next to the model input how it was built: filters, hash, revision and extent.

    The file name of the CSV does not say which filters were applied, and a model fitted on it
    later needs to know. The manifest sits beside the CSV as ``<name>.meta.json``.
    """
    JSONExporter().export(
        {
            "station": station,
            "period": {
                "start_date": settings.etl.start_date,
                "end_date": settings.etl.end_date,
            },
            "target": settings.etl.sonda.target_variable,
            "filters": settings.etl.filters.model_dump(),
            "goes_dir": goes_dir or file_paths.station_dir(station),
            "rows": len(df),
            "columns": len(df.columns),
            "first_timestamp": df["timestamp"].min().isoformat(),
            "last_timestamp": df["timestamp"].max().isoformat(),
            "csv_sha256": file_sha256(file_paths.model_input(station)),
            "git_revision": git_revision(),
            "created_at": utc_now_iso(),
        },
        file_paths.model_input_manifest(station),
    )


def build_station_input(station: str, goes_dir: str | None = None) -> bool:
    """Build and write the model input of one station.

    Steps: stack the station's GOES CSVs of each channel (overlapping periods are merged,
    repeated timestamps dropped), join the channels side by side, filter the SONDA target
    (daytime and frozen-sensor filters from ``etl_config.filters``), inner-join it with the
    GOES features, drop rows with missing values, validate the result and write it.

    The GOES CSVs are read from the station's own directory, ``data/raw/goes/<station>/``,
    and only files named ``goes_<channel>_..._<STATION>.csv`` are used. The output goes to
    :func:`~masters_project.file_paths.FilePathBuilder.model_input`.

    Args:
        station: Station key (a key of ``etl_config.stations``).
        goes_dir: Name of another directory inside ``data/raw/goes`` to read instead of the
            station's own (for example a set of CSVs kept apart for an experiment).

    Returns:
        True if the model input was written; False if the GOES directory or the SONDA
        input is missing, or the result fails validation (nothing is written then).
    """
    goes_path = file_paths.raw_goes_dir(station, goes_dir)
    if not goes_path.is_dir():
        logger.error(f"[{station}] GOES directory not found: {goes_path}")
        return False

    channel_groups = defaultdict(list)

    logger.info(f"[{station}] Scanning directory: {goes_path}")

    # sorted(): glob returns the file system's order, which would decide the order of the feature
    # columns and which of two scans rounded to the same timestamp is kept. Sorted names give
    # C01..C16 and the periods of one channel in chronological order.
    for file_path in sorted(goes_path.glob(f"goes_*_{station}.csv")):
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

    output = file_paths.model_input(station)
    CSVExporter().export(df_final, output)
    if output.exists():
        write_manifest(station, df_final, goes_dir)
    return True


def build_model_input(
    stations: Sequence[str] | None = None, goes_dir: str | None = None
) -> int:
    """Build the model input of several stations, one after the other.

    A station that cannot be built is logged and does not stop the others.

    Args:
        stations: Station keys to build; ``None`` means every station in ``etl_config.stations``.
        goes_dir: Directory name inside ``data/raw/goes`` to read instead of the station's own.
            Only meaningful for a single station, so it is rejected for several.

    Returns:
        0 if every station was built, 1 if any station lacked its GOES or SONDA input or
        produced an invalid table.

    Raises:
        ValueError: If ``goes_dir`` is given for more than one station.
    """
    targets = list(stations) if stations else list(settings.stations)
    if goes_dir is not None and len(targets) != 1:
        raise ValueError(
            "goes_dir applies to a single station; select it with --station."
        )

    failed = [s for s in targets if not build_station_input(s, goes_dir)]
    if failed:
        logger.error(f"Model input could not be built for: {failed}")
        return 1
    return 0


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Command-line options of ``python -m masters_project.etl.build_dataset``."""
    parser = argparse.ArgumentParser(
        description="Build the model input: GOES features joined with the SONDA target."
    )
    parser.add_argument(
        "--station",
        choices=list(settings.stations),
        help="build only this station (default: every station in etl_config.stations)",
    )
    parser.add_argument(
        "--goes-dir",
        metavar="NAME",
        help="read the GOES CSVs from data/raw/goes/NAME instead of the station's own "
        "directory (requires --station)",
    )
    args = parser.parse_args(argv)
    if args.goes_dir and not args.station:
        parser.error("--goes-dir requires --station")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    """Build the model input inside its own run log (``data/logs/etl_build_dataset/``)."""
    args = parse_args(argv)
    filters = settings.etl.filters
    with run_logging(
        "etl_build_dataset",
        period=f"{settings.etl.start_date}..{settings.etl.end_date}",
        stations=[args.station] if args.station else list(settings.stations),
        goes_dir=args.goes_dir or "<station>",
        min_solar_elevation=filters.min_solar_elevation,
        frozen_min_run=filters.frozen_min_run,
    ) as log:
        log.exit_code = build_model_input(
            [args.station] if args.station else None, args.goes_dir
        )
    return log.exit_code


if __name__ == "__main__":
    sys.exit(main())
