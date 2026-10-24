"""GOES ETL: list NetCDF keys on S3, extract pixel windows for all stations and channels, export CSVs.

Each S3 file is read at most once: windows are cached per channel and day (see
:mod:`masters_project.etl.goes.cache`), files are read with partial range requests, and
the work runs in a process pool (h5py holds a global lock, so threads do not scale). All
channels in ``etl_config.goes.channels`` are collected in a single run.
"""

import logging
from collections import defaultdict
from collections.abc import Callable, Iterator
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from masters_project.etl.goes.cache import (
    Day,
    extractions_to_frame,
    find_pending_files,
    group_by_day,
    load_day,
    load_pixels,
    save_day,
    save_pixels,
)
from masters_project.etl.goes.client import GoesS3Client
from masters_project.etl.goes.reader import GoesProcessor
from masters_project.etl.goes.worker import (
    ExtractionJob,
    FileExtraction,
    extract_file,
)
from masters_project.etl.sonda.processor import SondaProcessor
from masters_project.exporters import CSVExporter
from masters_project.file_paths import file_paths
from masters_project.settings import settings

logger = logging.getLogger(__name__)

# Listing days is network latency, not CPU, so it can use many more threads than cores.
LISTING_WORKERS = 16
# Tasks in flight per worker process: enough to keep workers busy without creating one
# future per file of a whole year.
IN_FLIGHT_PER_WORKER = 4
# Recycle each worker process after this many files, as a second line of defence against
# any slow memory growth over a very long collection.
MAX_TASKS_PER_CHILD = 1000


@dataclass
class ChannelPlan:
    """What a run has to do for one channel: files, target pixels, per-day cache state."""

    channel: str
    files: list[str]
    pixels: dict[str, tuple[int, int]]
    window_columns: list[str]
    cache_paths: dict[Day, Path]
    cached: dict[Day, pd.DataFrame]
    pending: dict[Day, list[str]] = field(default_factory=dict)


def locate_pixels(client: GoesS3Client, first_file: str) -> dict[str, tuple[int, int]]:
    """Compute the target pixel (i, j) of every station in the config from one file's grid."""
    with client.open_partial(first_file) as file:
        with GoesProcessor.open_as_dataset(file) as ds:
            return {
                name: GoesProcessor.get_target_indices(
                    ds, lat=station.latitude, lon=station.longitude
                )
                for name, station in settings.stations.items()
            }


def resolve_pixels(
    client: GoesS3Client, channel: str, first_file: str
) -> dict[str, tuple[int, int]]:
    """Target pixels for a channel, from the cache when the stations did not change.

    Opening a file just to find the pixels is only needed the first time (or after a
    station is added or moved): the grid of a channel is fixed.
    """
    goes = settings.etl.goes
    coords = {
        name: (station.latitude, station.longitude)
        for name, station in settings.stations.items()
    }
    cache_path = file_paths.goes_pixels(goes.bucket_name, goes.product_name, channel)

    pixels = load_pixels(cache_path, coords)
    if pixels is None:
        pixels = locate_pixels(client, first_file)
        save_pixels(cache_path, coords, pixels)
    return pixels


def plan_channel(client: GoesS3Client, channel: str, files: list[str]) -> ChannelPlan:
    """Resolve pixels and work out, per day, which files are not in the cache yet."""
    goes = settings.etl.goes
    pixels = resolve_pixels(client, channel, files[0])
    logger.info(f"[{channel}] Target pixels (i, j) per station: {pixels}")

    window_columns = GoesProcessor.window_columns(goes.pixel_radius, channel)
    days = group_by_day(files)
    cache_paths = {
        day: file_paths.goes_cache_day(
            goes.product_name, goes.variable, goes.pixel_radius, channel, *day
        )
        for day in days
    }
    cached = {day: load_day(cache_paths[day], window_columns) for day in days}
    pending = {
        day: find_pending_files(cached[day], day_files, pixels)
        for day, day_files in days.items()
    }

    plan = ChannelPlan(
        channel=channel,
        files=files,
        pixels=pixels,
        window_columns=window_columns,
        cache_paths=cache_paths,
        cached=cached,
        pending={day: paths for day, paths in pending.items() if paths},
    )
    to_read = sum(len(paths) for paths in plan.pending.values())
    logger.info(
        f"[{channel}] {len(files)} files in {len(days)} days; "
        f"{to_read} to read, {len(files) - to_read} served from cache."
    )
    return plan


def iter_tasks(
    plans: list[ChannelPlan],
) -> Iterator[tuple[str, ExtractionJob, tuple[str, Day]]]:
    """Yield (path, job, (channel, day)) for every pending file, channel by channel, day by day."""
    goes = settings.etl.goes
    for plan in plans:
        job = ExtractionJob(
            product_name=goes.product_name,
            bucket_name=goes.bucket_name,
            variable=goes.variable,
            radius=goes.pixel_radius,
            pixels=plan.pixels,
        )
        for day, paths in plan.pending.items():
            for path in paths:
                yield path, job, (plan.channel, day)


def run_bounded(
    pool: ProcessPoolExecutor,
    tasks: Iterator[tuple[str, ExtractionJob, tuple[str, Day]]],
    on_result: Callable[[tuple[str, Day], FileExtraction], None],
    window: int,
) -> None:
    """Run tasks keeping at most ``window`` in flight, calling ``on_result`` as each finishes."""
    in_flight = {}

    def submit_next() -> None:
        task = next(tasks, None)
        if task is not None:
            path, job, key = task
            in_flight[pool.submit(extract_file, path, job)] = key

    for _ in range(window):
        submit_next()

    while in_flight:
        done, _ = wait(in_flight, return_when=FIRST_COMPLETED)
        for future in done:
            key = in_flight.pop(future)
            on_result(key, future.result())
            submit_next()


def collect(plans: list[ChannelPlan]) -> None:
    """Read every pending file in one process pool and persist each (channel, day) when complete."""
    by_channel = {plan.channel: plan for plan in plans}
    total = sum(len(p) for plan in plans for p in plan.pending.values())
    if total == 0:
        return

    results: dict[tuple[str, Day], list[FileExtraction]] = defaultdict(list)
    failed: list[str] = []
    progress = tqdm(total=total, desc="Reading GOES files")

    def on_result(key: tuple[str, Day], item: FileExtraction) -> None:
        channel, day = key
        plan = by_channel[channel]
        progress.update(1)

        if item.error is not None:
            logger.error(f"Failed to process {item.source_key}: {item.error}")
            failed.append(item.source_key)
        results[key].append(item)

        if len(results[key]) == len(plan.pending[day]):
            # Day complete: persist now so an interruption keeps finished days.
            new_rows = extractions_to_frame(
                results.pop(key), plan.pixels, plan.window_columns
            )
            plan.cached[day] = save_day(
                plan.cache_paths[day], plan.cached[day], new_rows
            )

    workers = settings.etl.max_workers
    with ProcessPoolExecutor(
        max_workers=workers, max_tasks_per_child=MAX_TASKS_PER_CHILD
    ) as pool:
        run_bounded(
            pool, iter_tasks(plans), on_result, window=workers * IN_FLIGHT_PER_WORKER
        )
    progress.close()

    if failed:
        logger.warning(f"{len(failed)} files failed and will be retried next run.")


def export_channel(plan: ChannelPlan) -> None:
    """Write one CSV per station for the channel from its cached days."""
    day_frames = [df for df in plan.cached.values() if not df.empty]
    if not day_frames:
        logger.warning(f"[{plan.channel}] No valid data extracted. Skipping export.")
        return

    rows = pd.concat(day_frames, ignore_index=True)
    rows = rows[rows["source_key"].isin(set(plan.files))]

    for station, (i, j) in plan.pixels.items():
        df_station = rows[
            (rows["station"] == station)
            & (rows["pixel_i"] == i)
            & (rows["pixel_j"] == j)
        ]
        if df_station.empty:
            logger.warning(
                f"[{plan.channel}] No valid data for {station}. Skipping export."
            )
            continue

        df_out = (
            df_station[[*plan.window_columns, "timestamp"]]
            .sort_values(by="timestamp")
            .reset_index(drop=True)
        )
        CSVExporter().export(df_out, file_paths.raw_goes(plan.channel, station))


def target_days() -> set[date] | None:
    """Days on which some station has a SONDA target, or None if that cannot be known.

    GOES days without any target are dropped by the final join, so they are not worth
    downloading. Needs the SONDA CSVs of every configured station (run the SONDA ETL
    first); if any is missing, nothing is filtered and a warning says why.
    """
    paths = {name: file_paths.raw_sonda(name) for name in settings.stations}
    missing = [name for name, path in paths.items() if not path.exists()]
    if missing:
        logger.warning(
            f"SONDA CSV missing for {missing}: collecting every day. Run the SONDA ETL "
            "first to skip days without a target."
        )
        return None

    return SondaProcessor.days_with_target(
        paths.values(), settings.etl.sonda.target_variable
    )


def main() -> None:
    """Run the GOES ETL for every configured channel and export one CSV per channel and station."""
    settings.setup_logging("goes_etl")
    goes = settings.etl.goes
    client = GoesS3Client(product_name=goes.product_name, bucket_name=goes.bucket_name)

    logger.info(f"--- Starting GOES ETL for channels {goes.channels} ---")

    only_days = target_days() if goes.skip_days_without_target else None
    if only_days is not None:
        logger.info(
            f"Collecting only the {len(only_days)} days that have a SONDA target."
        )

    files_by_channel = client.list_files_by_channel(
        channels=goes.channels,
        start_date=settings.etl.start_date,
        end_date=settings.etl.end_date,
        max_workers=LISTING_WORKERS,
        cache_dir=file_paths.goes_listing_dir(goes.bucket_name, goes.product_name),
        only_days=only_days,
    )

    plans = []
    for channel, files in files_by_channel.items():
        if not files:
            logger.warning(f"[{channel}] No files found. Skipping.")
            continue
        try:
            plans.append(plan_channel(client, channel, files))
        except Exception:
            logger.exception(f"[{channel}] Could not plan this channel. Skipping.")

    collect(plans)

    for plan in plans:
        export_channel(plan)
