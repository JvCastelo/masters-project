"""SONDA ETL (monthly): download monthly ZIPs, export trimmed CSV."""

import logging

import pandas as pd

from masters_project.clients.sonda import SondaClient
from masters_project.file_paths import file_paths
from masters_project.loaders.csv import CSVExporter
from masters_project.processors.sonda import SondaProcessor
from masters_project.settings import settings
from masters_project.utils import get_target_year_months

settings.setup_logging("'sonda_etl_year_month")
logger = logging.getLogger(__name__)


def main() -> None:
    """Run the monthly SONDA ETL: download ZIPs by year-month, extract, merge and trim to config dates, export CSV."""
    client = SondaClient(
        station=settings.execution.selected_station,
        data_type=settings.etl.sonda.data_type,
    )

    year_months_to_fetch = get_target_year_months(
        start_date=settings.execution.start_date, end_date=settings.execution.end_date
    )

    logger.info(
        f"--- STARTING SONDA EXTRACTION ---\n"
        f"Target Station: {settings.execution.selected_station}\n"
        f"Required Year-Months: {year_months_to_fetch}"
    )

    all_dfs = []

    for year, month in year_months_to_fetch:
        try:
            logger.info(f"Processing SONDA data for year-month: {year}-{month:02d}")

            zip_path = client.download_file_by_year_month(
                year, month, settings.RAW_PATH / "sonda"
            )

            extraction_dir = SondaProcessor.extract_zip(zip_path)

            df_year_month = SondaProcessor.create_dataframe(extraction_dir).pipe(
                SondaProcessor.format_dataframe
            )

            all_dfs.append(df_year_month)
        except Exception:
            logger.exception(f"Failed to process SONDA data for {year}-{month}")

    if all_dfs:
        logger.info("Concatenating all monthly datasets...")
        df_sonda = pd.concat(all_dfs, ignore_index=True)

        logger.info(
            f"Trimming dataset strictly to bounds: {settings.execution.start_date} to {settings.execution.end_date}"
        )

        start_bound = pd.to_datetime(settings.execution.start_date).tz_localize("UTC")
        end_bound = pd.to_datetime(settings.execution.end_date).tz_localize(
            "UTC"
        ) + pd.Timedelta(days=1, seconds=-1)

        df_sonda = df_sonda[
            (df_sonda["timestamp"] >= start_bound)
            & (df_sonda["timestamp"] <= end_bound)
        ]

        logger.info("Sorting chronological order...")
        df_sonda = df_sonda.sort_values(by="timestamp").reset_index(drop=True)

        output_path = file_paths.raw_sonda()

        CSVExporter().export(df_sonda, output_path)

    else:
        logger.error(
            "No valid SONDA data was extracted during this run. Pipeline halted."
        )


if __name__ == "__main__":
    main()
