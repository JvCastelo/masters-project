import logging

import pandas as pd

from masters_project.clients.sonda import SondaClient
from masters_project.loaders.csv import CSVExporter
from masters_project.processors.sonda import SondaProcessor
from masters_project.settings import settings
from masters_project.utils import get_target_years

settings.setup_logging("sonda_etl_yearly")
logger = logging.getLogger(__name__)


def main() -> None:
    """Run the yearly SONDA ETL: download ZIPs by year, extract, merge and trim to config dates, export CSV."""
    client = SondaClient(
        station=settings.execution.selected_station,
        data_type=settings.etl.sonda.data_type,
    )

    years_to_fetch = get_target_years(
        start_date=settings.execution.start_date, end_date=settings.execution.end_date
    )

    logger.info(
        f"--- STARTING YEARLY SONDA EXTRACTION ---\n"
        f"Target Station: {settings.execution.selected_station}\n"
        f"Required Years: {years_to_fetch}"
    )

    all_dfs = []

    for year in years_to_fetch:
        try:
            logger.info(f"Processing SONDA data for year: {year}")

            zip_path = client.download_file_by_year(year, settings.RAW_PATH / "sonda")

            extraction_dir = SondaProcessor.extract_zip(zip_path)

            df_year = SondaProcessor.create_dataframe(extraction_dir).pipe(
                SondaProcessor.format_dataframe
            )

            all_dfs.append(df_year)

        except Exception:
            logger.exception(f"Failed to process SONDA data for year {year}")

    if all_dfs:
        logger.info("Concatenating all yearly datasets...")
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

        base_path = (
            settings.RAW_PATH
            / "sonda"
            / f"sonda_{settings.etl.sonda.data_type}_st_{settings.execution.start_date}_et_{settings.execution.end_date}_{settings.execution.selected_station}.csv"
        )

        CSVExporter().export(df_sonda, base_path)

    else:
        logger.error(
            "No valid SONDA data was extracted during this run. Pipeline halted."
        )


if __name__ == "__main__":
    main()
