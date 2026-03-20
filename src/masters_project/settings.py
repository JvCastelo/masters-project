import json
import logging
import sys
from pathlib import Path

from pydantic import BaseModel, ValidationError


class ExecutionSettings(BaseModel):
    """Pipeline execution controls (dates, selections, resources)."""

    start_date: str
    end_date: str
    selected_station: str
    selected_model: str
    max_workers: int = 10
    log_level: str = "INFO"


class GoesConfig(BaseModel):
    """Configuration specific to the GOES-16 satellite ETL process."""

    bucket_name: str
    product_name: str
    variable: str
    selected_channel: str
    pixel_radius: int


class SondaConfig(BaseModel):
    """Configuration specific to the SONDA ground station ETL process."""

    data_type: str
    target_variable: str


class EtlConfig(BaseModel):
    """Wrapper for all ETL related configurations."""

    goes: GoesConfig
    sonda: SondaConfig


class ModelSettings(BaseModel):
    """Configuration for a specific Machine Learning model."""

    n_neighbors: int | None = None
    n_jobs: int | None = None
    n_estimators: int | None = None
    learning_rate: float | None = None


class MlConfig(BaseModel):
    """Wrapper for all Machine Learning related configurations."""

    test_size: float
    models: dict[str, ModelSettings]


class StationSettings(BaseModel):
    """Configuration for a single station's coordinates."""

    latitude: float
    longitude: float


class PipelineConfig(BaseModel):
    """Root configuration model containing all sub-configurations."""

    execution: ExecutionSettings
    etl_config: EtlConfig
    ml_config: MlConfig
    stations: dict[str, StationSettings]


class Settings:
    """Application settings loader: reads pipeline config from JSON and exposes paths and config."""

    PROJECT_ROOT = Path(__file__).resolve().parents[2]

    PIPELINE_CONFIG_FILE = PROJECT_ROOT / "config" / "pipeline.json"
    DATA_PATH = PROJECT_ROOT / "data"
    LOG_PATH = DATA_PATH / "logs"
    RAW_PATH = DATA_PATH / "raw"
    PROCESSED_PATH = DATA_PATH / "processed"
    MODELS_PATH = PROJECT_ROOT / "models"
    RESULTS_PATH = PROJECT_ROOT / "results"

    def __init__(self) -> None:
        """Load pipeline configuration from the configured JSON file."""
        self._config = self._load_config()

    def _load_config(self) -> PipelineConfig:
        """Load and validate pipeline configuration from JSON."""
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
    def execution(self) -> ExecutionSettings:
        return self._config.execution

    @property
    def etl(self) -> EtlConfig:
        return self._config.etl_config

    @property
    def ml(self) -> MlConfig:
        return self._config.ml_config

    @property
    def station(self) -> StationSettings:
        """Instantly looks up the selected station without needing a loop!"""
        target_name = self.execution.selected_station
        if target_name not in self._config.stations:
            raise ValueError(
                f"Selected station '{target_name}' is not in your stations list."
            )
        return self._config.stations[target_name]

    @property
    def model(self) -> ModelSettings:
        """Instantly looks up the selected model without needing a loop!"""
        target_name = self.execution.selected_model
        if target_name not in self.ml.models:
            raise ValueError(
                f"Selected model '{target_name}' is not in your models list."
            )
        return self.ml.models[target_name]

    def setup_logging(self, log_name: str) -> None:
        """Configure root logger with console and file handlers."""
        self.LOG_PATH.mkdir(parents=True, exist_ok=True)

        formatter = logging.Formatter(
            "%(asctime)s :: %(levelname)-8s :: %(name)s :: %(message)s"
        )

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(self.execution.log_level)
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
