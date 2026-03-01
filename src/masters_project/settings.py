import json
import logging
import sys
from pathlib import Path

from pydantic import BaseModel, ValidationError


class GeneralSettings(BaseModel):
    """Pipeline-wide configuration (dates, GOES/SONDA params, concurrency, logging)."""

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
    """Configuration for a single station (name and coordinates)."""

    name: str
    latitude: float
    longitude: float


class PipelineConfig(BaseModel):
    """Root configuration model: general settings, station list, and active station."""

    general: GeneralSettings
    stations: list[StationSettings]
    active_station: str


class Settings:
    """Application settings loader: reads pipeline config from JSON and exposes paths and station config."""

    PROJECT_ROOT = Path(__file__).resolve().parents[2]

    PIPELINE_CONFIG_FILE = PROJECT_ROOT / "config" / "pipeline.json"
    DATA_PATH = PROJECT_ROOT / "data"
    LOG_PATH = DATA_PATH / "logs"
    RAW_PATH = DATA_PATH / "raw"
    PROCESSED_PATH = DATA_PATH / "processed"
    MODELS_PATH = DATA_PATH / "models"

    def __init__(self) -> None:
        """Load pipeline configuration from the configured JSON file."""
        self._config = self._load_config()

    def _load_config(self) -> PipelineConfig:
        """Load and validate pipeline configuration from JSON.

        Returns:
            Validated PipelineConfig instance.

        Note:
            Calls sys.exit(1) if the config file is missing or invalid.
        """
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
        """General pipeline settings (dates, GOES/SONDA, workers, log level)."""
        return self._config.general

    @property
    def station(self) -> StationSettings:
        """Station settings for the active station (name, latitude, longitude).

        Returns:
            The StationSettings matching active_station in the config.

        Raises:
            ValueError: If active_station is not found in the stations list.
        """
        target_name = self._config.active_station

        for station in self._config.stations:
            if station.name == target_name:
                return station

        raise ValueError(
            f"Active station '{target_name}' is not in your stations list."
        )

    def setup_logging(self, log_name: str) -> None:
        """Configure root logger with console and file handlers, and quiet third-party loggers.

        Args:
            log_name: Base name for the log file (e.g. 'goes_etl' -> goes_etl.log).
        """
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
