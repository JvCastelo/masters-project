from masters_project.clients.sonda import SondaClient
from masters_project.config import GeneralConfig, SondaConfig, settings
from masters_project.loaders.csv import CSVExporter
from masters_project.processors.sonda import SondaProcessor

settings.setup_logging()

client = SondaClient(station=SondaConfig.STATION, data_type=SondaConfig.DATA_TYPE)

zip_path = client.download_file_by_year(GeneralConfig.YEAR)

extraction_dir = SondaProcessor.extract_zip(zip_path)

df_sonda = SondaProcessor.create_dataframe(extraction_dir).pipe(
    SondaProcessor.format_dataframe
)

base_path = (
    SondaConfig.BASE_PATH_FILE / f"sonda_{GeneralConfig.YEAR}_{SondaConfig.STATION}.csv"
)

CSVExporter().export(df_sonda, base_path)
