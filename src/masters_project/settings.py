"""Pipeline configuration (``config/pipeline.json``) validated with pydantic, plus the project paths."""

import json
import os
import sys
from datetime import date
from pathlib import Path
from typing import Any, Literal

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


class PeriodConfig(BaseModel):
    """An inclusive range of days, ``YYYY-MM-DD``."""

    start_date: str
    end_date: str

    @model_validator(mode="after")
    def _start_is_not_after_end(self) -> "PeriodConfig":
        start, end = (
            date.fromisoformat(self.start_date),
            date.fromisoformat(self.end_date),
        )
        if start > end:
            raise ValueError(
                f"start_date {self.start_date} is after end_date {self.end_date}."
            )
        return self


class EnsembleConfig(BaseModel):
    """Several models whose predictions are combined into one."""

    members: list[str] = Field(min_length=2)
    #: ``idw``: inverse-distance weights from the target station; ``equal``: plain mean.
    weights: Literal["idw", "equal"] = "idw"


class PredictConfig(PeriodConfig):
    """What ``python -m masters_project.ml.predict`` evaluates: models and ensembles on one station.

    The period (inherited ``start_date``/``end_date``) is where the target station is evaluated.
    """

    target_station: str
    #: Stations whose trained model is evaluated on the target station's data.
    models: list[str] = Field(min_length=1)
    #: Name -> ensemble of some of ``models``.
    ensembles: dict[str, EnsembleConfig] = Field(default_factory=dict)
    #: Model to use; defaults to ``ml_config.selected_model``.
    model_name: str | None = None

    @model_validator(mode="after")
    def _models_and_ensembles_are_consistent(self) -> "PredictConfig":
        if len(set(self.models)) != len(self.models):
            raise ValueError("predict.models lists a station twice.")
        for name, ensemble in self.ensembles.items():
            outside = [m for m in ensemble.members if m not in self.models]
            if outside:
                raise ValueError(
                    f"Ensemble '{name}' uses {outside}, which are not in predict.models."
                )
            if name in self.models:
                raise ValueError(f"Ensemble '{name}' has the name of a model.")
        return self


class MlConfig(BaseModel):
    """Machine learning configuration, including which model the pipelines run."""

    selected_model: str
    #: Fraction of the training period held out (the most recent days) to evaluate the model.
    test_size: float
    #: Period of the model input used to train and tune; ``None`` uses the whole dataset.
    train: PeriodConfig | None = None
    #: Evaluation of trained models on another station/period; ``None`` if not used.
    predict: PredictConfig | None = None
    evaluation_metrics: list[str]
    #: Model name -> constructor arguments of its estimator (see ``masters_project.ml.models``).
    models: dict[str, dict[str, Any]]

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
    def train_period(self) -> PeriodConfig:
        """Days used to train and tune: ``ml_config.train``, else the whole ETL period."""
        return self.ml.train or PeriodConfig(
            start_date=self.etl.start_date, end_date=self.etl.end_date
        )

    @property
    def model(self) -> dict[str, Any]:
        """Hyperparameters for ``ml_config.selected_model`` from ``ml_config.models``."""
        return self.ml.models[self.ml.selected_model]


settings = Settings()
