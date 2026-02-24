import json
import logging
import sys
from pathlib import Path

from pydantic import BaseModel, ValidationError


class GeneralSettings(BaseModel):
    start_date: str
    end_date: str
    pixel_radius: int
    sonda_data_type: str
    goes_product_name: str
    goes_bucket_name: str
    goes_variable: str
    max_workers: int = 10
    log_level: str = "INFO"


class StationSettings(BaseModel):
    name: str
    latitude: float
    longitude: float


class PipelineConfig(BaseModel):
    general: GeneralSettings
    stations: list[StationSettings]
    active_station: str


class Settings:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]

    PIPELINE_CONFIG_FILE = PROJECT_ROOT / "config" / "pipeline.json"
    DATA_PATH = PROJECT_ROOT / "data"
    LOG_PATH = DATA_PATH / "logs"
    RAW_PATH = DATA_PATH / "raw"
    PROCESSED_PATH = DATA_PATH / "processed"
    MODELS_PATH = DATA_PATH / "models"

    def __init__(self):
        self._config = self._load_config()

    def _load_config(self) -> PipelineConfig:
        if not self.PIPELINE_CONFIG_FILE.exists():
            print(f"CRITICAL: Config file not found at {self.PIPELINE_CONFIG_FILE}")
            sys.exit(1)

        try:
            with open(self.PIPELINE_CONFIG_FILE, "r") as f:
                data = json.load(f)

            return PipelineConfig(**data)

        except (ValidationError, json.JSONDecodeError) as e:
            print(f"CONFIGURATION ERROR: {e}")
            sys.exit(1)

    @property
    def general(self) -> GeneralSettings:
        return self._config.general

    @property
    def station(self) -> StationSettings:
        target_name = self._config.active_station

        for station in self._config.stations:
            if station.name == target_name:
                return station

        raise ValueError(
            f"Active station '{target_name}' is not in your stations list."
        )

    def setup_logging(self, log_name: str):
        self.LOG_PATH.mkdir(parents=True, exist_ok=True)

        formatter = logging.Formatter(
            "%(asctime)s :: %(levelname)-8s :: %(name)s :: %(message)s"
        )

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(self.general.log_level)
        console_handler.setFormatter(formatter)

        file_path = self.LOG_PATH / f"{log_name}.log"
        file_handler = logging.FileHandler(file_path, mode="a")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)

        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)

        if root_logger.hasHandlers():
            root_logger.handlers.clear()

        root_logger.addHandler(console_handler)
        root_logger.addHandler(file_handler)

        logging.getLogger("botocore").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("s3fs").setLevel(logging.WARNING)
        logging.getLogger("h5netcdf").setLevel(logging.WARNING)
        logging.getLogger("fsspec").setLevel(logging.WARNING)


settings = Settings()
