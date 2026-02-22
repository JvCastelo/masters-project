import logging

import pandas as pd

from masters_project.clients.sonda import SondaClient
from masters_project.config import GeneralConfig, SondaConfig, settings
from masters_project.loaders.csv import CSVExporter
from masters_project.processors.sonda import SondaProcessor
from masters_project.utils import get_target_years

settings.setup_logging()
logger = logging.getLogger(__name__)
client = SondaClient(station=SondaConfig.STATION, data_type=SondaConfig.DATA_TYPE)

years_to_fetch = get_target_years(
    start_date=GeneralConfig.START_DATE, end_date=GeneralConfig.END_DATE
)
logger.info(f"Starting SONDA extraction for years: {years_to_fetch}")

all_dfs = []

for year in years_to_fetch:
    try:
        logger.info(f"Processing SONDA data for year: {year}")

        zip_path = client.download_file_by_year(year, SondaConfig.BASE_PATH_FILE)

        extraction_dir = SondaProcessor.extract_zip(zip_path)

        df_year = SondaProcessor.create_dataframe(extraction_dir).pipe(
            SondaProcessor.format_dataframe
        )

        all_dfs.append(df_year)
    except Exception as e:
        logger.error(f"Failed to process SONDA data for {year}: {e}")

if all_dfs:
    df_sonda = pd.concat(all_dfs, ignore_index=True)

    base_path = (
        SondaConfig.BASE_PATH_FILE
        / f"sonda_{SondaConfig.DATA_TYPE}_st_{GeneralConfig.START_DATE}_et_{GeneralConfig.END_DATE}_{SondaConfig.STATION}.csv"
    )

    CSVExporter().export(df_sonda, base_path)
    logger.info("Successfully exported combined SONDA data.")
else:
    logger.warning("No SONDA data was extracted.")
