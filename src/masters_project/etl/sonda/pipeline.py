"""SONDA ETL: download yearly or monthly ZIPs for every station, export one trimmed CSV per station.

Run it before the GOES ETL: the GOES step reads these CSVs to skip days on which no station
has a target value.
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path

import httpx
import pandas as pd

from masters_project.etl.schemas import validate_sonda
from masters_project.etl.sonda.client import SondaClient
from masters_project.etl.sonda.periods import format_period, get_target_periods
from masters_project.etl.sonda.processor import SondaProcessor
from masters_project.exporters import CSVExporter
from masters_project.file_paths import file_paths
from masters_project.logging_setup import run_logging
from masters_project.settings import settings

logger = logging.getLogger(__name__)


@dataclass
class StationResult:
    """Outcome of the SONDA ETL for one station."""

    station: str
    missing: list[str] = field(
        default_factory=list
    )  # periods SONDA does not publish (HTTP 404)
    failed: list[str] = field(
        default_factory=list
    )  # periods that failed for another reason

    @property
    def ok(self) -> bool:
        """True when every requested period was downloaded and processed."""
        return not self.missing and not self.failed


def download_period(client: SondaClient, period: tuple[int, int | None]) -> Path:
    """Download the ZIP of one period (a year, or a year and month) into ``data/raw/sonda``."""
    year, month = period
    destination = settings.RAW_PATH / "sonda"
    if month is None:
        return client.download_file_by_year(year, destination)
    return client.download_file_by_year_month(year, month, destination)


def process_station(station: str, granularity: str) -> StationResult:
    """Download, merge and export the SONDA data of one station for the configured period.

    Periods that SONDA does not publish (HTTP 404) or that fail for other reasons are
    recorded in the result instead of being silently skipped; the CSV is still written
    with whatever was available.
    """
    client = SondaClient(station=station, data_type=settings.etl.sonda.data_type)
    periods = get_target_periods(
        settings.etl.start_date, settings.etl.end_date, granularity
    )
    logger.info(
        f"[{station}] Fetching {granularity}ly SONDA data for: "
        f"{[format_period(p) for p in periods]}"
    )

    result = StationResult(station)
    frames = []

    for period in periods:
        label = format_period(period)
        try:
            zip_path = download_period(client, period)
            extraction_dir = SondaProcessor.extract_zip(zip_path)
            frames.append(
                SondaProcessor.create_dataframe(extraction_dir).pipe(
                    SondaProcessor.format_dataframe
                )
            )
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                result.missing.append(label)
            else:
                result.failed.append(label)
                logger.exception(f"[{station}] HTTP error for {label}")
        except Exception:
            result.failed.append(label)
            logger.exception(f"[{station}] Failed to process {label}")

    if not frames:
        logger.error(f"[{station}] No SONDA data could be extracted.")
        return result

    df_sonda = SondaProcessor.trim_to_period(
        pd.concat(frames, ignore_index=True),
        settings.etl.start_date,
        settings.etl.end_date,
    )

    target = settings.etl.sonda.target_variable
    valid_days, total_days = SondaProcessor.coverage_days(df_sonda, target)
    logger.info(f"[{station}] Days with a valid '{target}': {valid_days}/{total_days}")

    validate_sonda(df_sonda, target, label=f"[{station}]")

    CSVExporter().export(df_sonda, file_paths.raw_sonda(station))
    return result


def run(granularity: str = "year") -> int:
    """Run the SONDA ETL for every station in ``etl_config.stations``.

    Args:
        granularity: 'year' (one ZIP per year, fewer requests) or 'month'.

    Returns:
        0 if every period of every station was obtained, 1 otherwise. A station that
        raises does not stop the others.
    """
    results = []
    for station in settings.stations:
        try:
            results.append(process_station(station, granularity))
        except Exception:
            logger.exception(f"[{station}] SONDA ETL failed")
            results.append(StationResult(station, failed=["all"]))

    problems = [r for r in results if not r.ok]
    for r in problems:
        if r.missing:
            logger.error(
                f"[{r.station}] NOT PUBLISHED by SONDA for: {r.missing}. "
                "The exported CSV has a gap in those periods."
            )
        if r.failed:
            logger.error(f"[{r.station}] FAILED for: {r.failed}. Run again to retry.")

    return 1 if problems else 0


def main(granularity: str = "year") -> int:
    """Run the SONDA ETL inside its own run log (``data/logs/etl_sonda/``).

    Args:
        granularity: 'year' (one ZIP per year, fewer requests) or 'month'.

    Returns:
        The exit code of :func:`run`.
    """
    with run_logging(
        "etl_sonda",
        period=f"{settings.etl.start_date}..{settings.etl.end_date}",
        stations=list(settings.stations),
        granularity=granularity,
        data_type=settings.etl.sonda.data_type,
    ) as log:
        log.exit_code = run(granularity)
    return log.exit_code
