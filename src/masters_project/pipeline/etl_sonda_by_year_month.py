import logging

import pandas as pd

from masters_project.clients.sonda import SondaClient
from masters_project.config import GeneralConfig, SondaConfig, settings
from masters_project.loaders.csv import CSVExporter
from masters_project.processors.sonda import SondaProcessor
from masters_project.utils import get_target_year_months

settings.setup_logging("'sonda_etl_year_month")
logger = logging.getLogger(__name__)


def main():

    client = SondaClient(station=SondaConfig.STATION, data_type=SondaConfig.DATA_TYPE)

    year_months_to_fetch = get_target_year_months(
        start_date=GeneralConfig.START_DATE, end_date=GeneralConfig.END_DATE
    )

    logger.info(
        f"--- STARTING SONDA EXTRACTION ---\n"
        f"Target Station: {SondaConfig.STATION}\n"
        f"Required Year-Months: {year_months_to_fetch}"
    )

    all_dfs = []

    for year, month in year_months_to_fetch:
        try:
            logger.info(f"Processing SONDA data for year-month: {year}-{month:02d}")

            zip_path = client.download_file_by_year_month(
                year, month, SondaConfig.BASE_PATH_FILE
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
            f"Trimming dataset strictly to bounds: {GeneralConfig.START_DATE} to {GeneralConfig.END_DATE}"
        )

        start_bound = pd.to_datetime(GeneralConfig.START_DATE).tz_localize("UTC")
        end_bound = pd.to_datetime(GeneralConfig.END_DATE).tz_localize(
            "UTC"
        ) + pd.Timedelta(days=1, seconds=-1)

        df_sonda = df_sonda[
            (df_sonda["timestamp"] >= start_bound)
            & (df_sonda["timestamp"] <= end_bound)
        ]

        logger.info("Sorting chronological order...")
        df_sonda = df_sonda.sort_values(by="timestamp").reset_index(drop=True)

        base_path = (
            SondaConfig.BASE_PATH_FILE
            / f"sonda_{SondaConfig.DATA_TYPE}_st_{GeneralConfig.START_DATE}_et_{GeneralConfig.END_DATE}_{SondaConfig.STATION}.csv"
        )

        CSVExporter().export(df_sonda, base_path)

    else:
        logger.error(
            "No valid SONDA data was extracted during this run. Pipeline halted."
        )


if __name__ == "__main__":
    main()
