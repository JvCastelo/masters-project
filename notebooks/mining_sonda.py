from masters_project.clients.sonda import SondaClient
from masters_project.config import settings
from masters_project.processors.sonda import SondaProcessor

settings.setup_logging()

client = SondaClient(station=settings.STATION)

zip_path = client.download_file_by_year(2024)

extraction_dir = SondaProcessor.extract_zip(zip_path)

df_sonda = SondaProcessor.create_dataframe(extraction_dir).pipe(
    SondaProcessor.format_dataframe
)

print(df_sonda)
