"""Tests for masters_project.settings: config structure and validation."""

import json

import pytest
from pydantic import ValidationError

from masters_project.settings import FiltersConfig, GoesConfig, MlConfig, settings


def test_pipeline_json_has_only_the_three_top_level_sections() -> None:
    """The config is organised as etl_config, ml_config and tuning_config, nothing else."""
    with open(settings.PIPELINE_CONFIG_FILE) as f:
        data = json.load(f)
    assert set(data) == {"etl_config", "ml_config", "tuning_config"}


def test_etl_config_holds_period_workers_logging_and_stations() -> None:
    """Fields that used to live in ``execution`` and ``stations`` are now in etl_config."""
    etl = settings.etl
    assert etl.start_date <= etl.end_date
    assert etl.max_workers >= 1
    assert etl.log_level
    assert len(etl.stations) >= 1


def test_stations_property_returns_every_configured_station() -> None:
    """Pipeline steps run for every station in etl_config.stations."""
    assert settings.stations == settings.etl.stations


def test_selected_model_lives_in_ml_config_and_resolves_hyperparameters() -> None:
    """``settings.model`` returns the hyperparameters of ml_config.selected_model."""
    assert settings.model == settings.ml.models[settings.ml.selected_model]


def test_ml_config_unknown_selected_model_raises_validation_error() -> None:
    """Failure mode: selected_model must be one of the configured models."""
    with pytest.raises(ValidationError, match="ATLANTIS"):
        MlConfig(
            selected_model="ATLANTIS",
            test_size=0.2,
            evaluation_metrics=["rmse"],
            models={"KNN": {"n_neighbors": 5}},
        )


def _goes_config(channels: list[str]) -> GoesConfig:
    return GoesConfig(
        bucket_name="noaa-goes16",
        product_name="ABI-L1b-RadF",
        variable="Rad",
        channels=channels,
        pixel_radius=1,
    )


def test_goes_config_accepts_several_channels() -> None:
    assert _goes_config(["C01", "C03", "C16"]).channels == ["C01", "C03", "C16"]


@pytest.mark.parametrize("channels", [[], ["C17"], ["C03", "banana"]])
def test_goes_config_rejects_empty_or_unknown_channels(channels: list[str]) -> None:
    with pytest.raises(ValidationError):
        _goes_config(channels)


@pytest.fixture
def restore_root_logger():
    """setup_logging replaces the root logger's handlers: put the originals back."""
    import logging

    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    yield
    for handler in list(root.handlers):
        handler.close()
    root.handlers[:] = handlers
    root.setLevel(level)


def test_setup_logging_uses_rotating_file_handler_at_info(
    tmp_path, monkeypatch: pytest.MonkeyPatch, restore_root_logger
) -> None:
    import logging
    from logging.handlers import RotatingFileHandler

    monkeypatch.setattr(settings, "LOG_PATH", tmp_path)
    monkeypatch.setattr(settings.etl, "log_level", "INFO")

    settings.setup_logging("rotation_test")

    file_handlers = [
        h for h in logging.getLogger().handlers if isinstance(h, RotatingFileHandler)
    ]
    assert len(file_handlers) == 1
    assert file_handlers[0].maxBytes == 10 * 1024 * 1024
    assert file_handlers[0].backupCount == 5
    assert file_handlers[0].level == logging.INFO


def test_pipeline_config_path_can_be_overridden_by_environment(tmp_path) -> None:
    """MASTERS_PIPELINE_CONFIG makes a fresh interpreter load another config file."""
    import json
    import subprocess
    import sys

    with open(settings.PIPELINE_CONFIG_FILE) as f:
        data = json.load(f)
    data["etl_config"]["start_date"] = "2031-05-06"
    other = tmp_path / "other.json"
    other.write_text(json.dumps(data))

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from masters_project.settings import settings; print(settings.etl.start_date)",
        ],
        env={**__import__("os").environ, "MASTERS_PIPELINE_CONFIG": str(other)},
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout.strip() == "2031-05-06"


def test_filters_default_to_disabled() -> None:
    """With no ``filters`` section every filter is off (``None``)."""
    filters = FiltersConfig()
    assert filters.min_solar_elevation is None
    assert filters.frozen_min_run is None


@pytest.mark.parametrize(
    ("field", "value"),
    [("min_solar_elevation", 120), ("min_solar_elevation", -91), ("frozen_min_run", 1)],
)
def test_filters_reject_out_of_range_values(field: str, value: float) -> None:
    """Failure mode: an impossible angle, or a run of 1 (every row), is rejected at load time."""
    with pytest.raises(ValidationError, match=field):
        FiltersConfig(**{field: value})
