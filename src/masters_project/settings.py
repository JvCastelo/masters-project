import json
import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

from pydantic import (
    BaseModel,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

from masters_project.enums import GoesChannel


class GoesConfig(BaseModel):
    """Configuration specific to the GOES-16 satellite ETL process."""

    bucket_name: str
    product_name: str
    variable: str
    channels: list[str]
    pixel_radius: int
    skip_days_without_target: bool = True

    @field_validator("channels")
    @classmethod
    def _channels_are_valid_abi_channels(cls, channels: list[str]) -> list[str]:
        if not channels:
            raise ValueError("goes.channels must list at least one channel.")
        invalid = [c for c in channels if c not in GoesChannel.__members__]
        if invalid:
            raise ValueError(f"Unknown GOES channels: {invalid}. Use C01 to C16.")
        return channels


class SondaConfig(BaseModel):
    """Configuration specific to the SONDA ground station ETL process."""

    data_type: str
    target_variable: str


class StationSettings(BaseModel):
    """Configuration for a single station's coordinates."""

    latitude: float
    longitude: float


class FiltersConfig(BaseModel):
    """Row filters applied when building the model input; ``None`` turns a filter off."""

    # The 5 degrees in config/pipeline.json was chosen arbitrarily, not tuned or derived.
    min_solar_elevation: float | None = Field(default=None, ge=-90, le=90)
    # Consecutive identical daytime target samples that count as a frozen sensor. The
    # 10 in config/pipeline.json leaves a margin over the longest natural run measured (5).
    frozen_min_run: int | None = Field(default=None, ge=2)


class EtlConfig(BaseModel):
    """ETL configuration: period, resources, stations, and the GOES/SONDA sources.

    Every station in ``stations`` is processed by each pipeline step.
    """

    start_date: str
    end_date: str
    max_workers: int = 12
    log_level: str = "INFO"
    filters: FiltersConfig = Field(default_factory=FiltersConfig)
    stations: dict[str, StationSettings]
    goes: GoesConfig
    sonda: SondaConfig


class ModelSettings(BaseModel):
    """Configuration for a specific Machine Learning model."""

    n_neighbors: int | None = None
    n_jobs: int | None = None
    n_estimators: int | None = None
    learning_rate: float | None = None


class MlConfig(BaseModel):
    """Machine learning configuration, including which model the pipelines run."""

    selected_model: str
    test_size: float
    evaluation_metrics: list[str]
    models: dict[str, ModelSettings]

    @model_validator(mode="after")
    def _selected_model_is_configured(self) -> "MlConfig":
        if self.selected_model not in self.models:
            raise ValueError(
                f"Selected model '{self.selected_model}' is not in your models list."
            )
        return self


class TuningConfig(BaseModel):
    """Configuration for hyperparameter tuning execution."""

    n_iter: int
    cv: int
    scoring: str
    n_jobs: int = -1
    verbose: int = 2


class PipelineConfig(BaseModel):
    """Root configuration model containing all sub-configurations."""

    etl_config: EtlConfig
    ml_config: MlConfig
    tuning_config: TuningConfig


class Settings:
    """Application settings loader: reads pipeline config from JSON and exposes paths and config."""

    PROJECT_ROOT = Path(__file__).resolve().parents[2]

    # ``MASTERS_PIPELINE_CONFIG`` points the pipeline at another config file, so a run can use
    # different dates/stations/channels without editing ``config/pipeline.json``.
    PIPELINE_CONFIG_FILE = Path(
        os.environ.get(
            "MASTERS_PIPELINE_CONFIG", PROJECT_ROOT / "config" / "pipeline.json"
        )
    )
    DATA_PATH = PROJECT_ROOT / "data"
    LOG_PATH = DATA_PATH / "logs"
    RAW_PATH = DATA_PATH / "raw"
    PROCESSED_PATH = DATA_PATH / "processed"
    MODELS_PATH = DATA_PATH / "models"
    RESULTS_PATH = DATA_PATH / "results"
    CACHE_PATH = DATA_PATH / "cache"

    def __init__(self) -> None:
        """Load pipeline configuration from the configured JSON file."""
        self._config = self._load_config()

    def _load_config(self) -> PipelineConfig:
        """Load and validate pipeline configuration from JSON.

        Returns:
            Parsed and validated ``PipelineConfig``.

        Note:
            Exits the process with code 1 if the file is missing or invalid JSON/Pydantic.
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
    def etl(self) -> EtlConfig:
        """ETL configuration: period, workers, stations, GOES and SONDA sources."""
        return self._config.etl_config

    @property
    def ml(self) -> MlConfig:
        """Machine learning configuration: selected model, test split, metrics, hyperparameters."""
        return self._config.ml_config

    @property
    def tuning(self) -> TuningConfig:
        """Hyperparameter search settings (RandomizedSearchCV-style)."""
        return self._config.tuning_config

    @property
    def stations(self) -> dict[str, StationSettings]:
        """All configured stations (name -> coordinates); every pipeline step runs for each."""
        return self.etl.stations

    @property
    def model(self) -> ModelSettings:
        """Hyperparameters for ``ml_config.selected_model`` from ``ml_config.models``."""
        return self.ml.models[self.ml.selected_model]

    def setup_logging(self, log_name: str) -> None:
        """Configure root logger with console and rotating file handlers.

        The console shows ``etl_config.log_level``. The file keeps INFO and above (DEBUG
        only if ``log_level`` is DEBUG) and rotates at 10 MB, keeping 5 backups, so a long
        run cannot fill the disk.
        """
        self.LOG_PATH.mkdir(parents=True, exist_ok=True)

        formatter = logging.Formatter(
            "%(asctime)s :: %(levelname)-8s :: %(name)s :: %(message)s"
        )

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(self.etl.log_level)
        console_handler.setFormatter(formatter)

        file_path = self.LOG_PATH / f"{log_name}.log"
        file_handler = RotatingFileHandler(
            file_path, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
        )
        file_handler.setLevel(
            logging.DEBUG if self.etl.log_level.upper() == "DEBUG" else logging.INFO
        )
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
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("httpcore").setLevel(logging.WARNING)


settings = Settings()
