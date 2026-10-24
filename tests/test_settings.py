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


@pytest.mark.parametrize(
    ("content", "message"),
    [
        (None, "Config file not found"),
        ("{not json", "CONFIGURATION ERROR"),
        ('{"etl_config": {}}', "CONFIGURATION ERROR"),
    ],
    ids=["missing_file", "invalid_json", "invalid_schema"],
)
def test_unusable_config_file_exits_with_code_1(
    tmp_path, monkeypatch: pytest.MonkeyPatch, capsys, content: str | None, message: str
) -> None:
    """Failure mode: a bad config stops the program before any long run starts."""
    from masters_project.settings import Settings

    path = tmp_path / "pipeline.json"
    if content is not None:
        path.write_text(content)
    monkeypatch.setattr(Settings, "PIPELINE_CONFIG_FILE", path)

    with pytest.raises(SystemExit) as exit_info:
        Settings()

    assert exit_info.value.code == 1
    assert message in capsys.readouterr().out


def test_period_config_rejects_a_start_after_the_end() -> None:
    from masters_project.settings import PeriodConfig

    with pytest.raises(ValidationError, match="is after end_date"):
        PeriodConfig(start_date="2023-01-01", end_date="2022-12-31")


def test_period_config_rejects_a_date_that_is_not_iso() -> None:
    from masters_project.settings import PeriodConfig

    with pytest.raises(ValidationError):
        PeriodConfig(start_date="01/01/2022", end_date="2022-12-31")


def test_train_period_is_the_configured_one_or_else_the_etl_period(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from masters_project.settings import PeriodConfig

    configured = PeriodConfig(start_date="2022-01-01", end_date="2022-12-31")
    monkeypatch.setattr(settings.ml, "train", configured)
    assert settings.train_period == configured

    monkeypatch.setattr(settings.ml, "train", None)
    assert settings.train_period == PeriodConfig(
        start_date=settings.etl.start_date, end_date=settings.etl.end_date
    )


# =============================================================================
# ml_config.predict
# =============================================================================


def _predict(**overrides):
    from masters_project.settings import PredictConfig

    values = {
        "target_station": "OURINHOS",
        "start_date": "2023-01-01",
        "end_date": "2023-12-31",
        "models": ["CACHOEIRA_PAULISTA", "OURINHOS", "SAO_MARTINHO_DA_SERRA"],
        "ensembles": {
            "CPA_SMS_IDW": {
                "members": ["CACHOEIRA_PAULISTA", "SAO_MARTINHO_DA_SERRA"],
                "weights": "idw",
            }
        },
    }
    return PredictConfig(**{**values, **overrides})


def test_predict_config_accepts_the_experiment_of_the_project() -> None:
    config = _predict()
    assert config.ensembles["CPA_SMS_IDW"].weights == "idw"
    assert config.model_name is None


def test_predict_config_weights_default_to_idw() -> None:
    config = _predict(ensembles={"E": {"members": ["OURINHOS", "CACHOEIRA_PAULISTA"]}})
    assert config.ensembles["E"].weights == "idw"


def test_predict_config_inherits_the_period_check() -> None:
    with pytest.raises(ValidationError, match="is after end_date"):
        _predict(start_date="2024-01-01")


def test_predict_config_rejects_an_ensemble_member_that_is_not_a_model() -> None:
    with pytest.raises(
        ValidationError, match=r"uses \['GHOST'\], which are not in predict.models"
    ):
        _predict(ensembles={"E": {"members": ["GHOST", "OURINHOS"]}})


def test_predict_config_rejects_a_station_listed_twice() -> None:
    with pytest.raises(ValidationError, match="lists a station twice"):
        _predict(models=["OURINHOS", "OURINHOS"])


def test_predict_config_rejects_an_ensemble_named_like_a_model() -> None:
    with pytest.raises(ValidationError, match="has the name of a model"):
        _predict(
            ensembles={
                "OURINHOS": {"members": ["CACHOEIRA_PAULISTA", "SAO_MARTINHO_DA_SERRA"]}
            }
        )


def test_predict_config_rejects_unknown_weights_and_single_member_ensembles() -> None:
    with pytest.raises(ValidationError):
        _predict(
            ensembles={
                "E": {
                    "members": ["OURINHOS", "CACHOEIRA_PAULISTA"],
                    "weights": "median",
                }
            }
        )
    with pytest.raises(ValidationError):
        _predict(ensembles={"E": {"members": ["OURINHOS"]}})


def test_predict_config_is_optional_in_ml_config() -> None:
    from masters_project.settings import MlConfig

    config = MlConfig(
        selected_model="KNN",
        test_size=0.2,
        evaluation_metrics=["rmse"],
        models={"KNN": {}},
    )
    assert config.predict is None and config.train is None
