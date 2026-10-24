"""Tests for masters_project.ml.train and masters_project.ml.tune: end to end on tiny data."""

import json

import numpy as np
import pandas as pd
import pytest

from masters_project.file_paths import file_paths
from masters_project.ml import train, tune
from masters_project.ml.artifacts import card_path, load_model
from masters_project.settings import PeriodConfig, settings
from masters_project.utils import file_sha256

TRAIN_START, TRAIN_END = "2022-01-01", "2022-12-31"


def _segment(start: str, n: int, seed: int) -> pd.DataFrame:
    """A learnable segment: the target is a function of one pixel."""
    rng = np.random.default_rng(seed)
    pixel = rng.uniform(0, 100, n)
    return pd.DataFrame(
        {
            "timestamp": pd.date_range(start, periods=n, freq="10min", tz="UTC"),
            "radius=1_C03_11": pixel,
            "radius=1_C03_12": rng.uniform(0, 1, n),
            "glo_avg": pixel * 5.0,
        }
    )


@pytest.fixture
def model_input(data_tree, one_station, monkeypatch: pytest.MonkeyPatch) -> str:
    """80 rows in 2022 (the training period) and 40 in 2023, which must stay out of training."""
    frame = pd.concat(
        [_segment("2022-06-21 12:00", 80, 0), _segment("2023-06-21 12:00", 40, 1)]
    )
    path = file_paths.model_input(one_station)
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)
    monkeypatch.setattr(
        settings.ml, "train", PeriodConfig(start_date=TRAIN_START, end_date=TRAIN_END)
    )
    return one_station


@pytest.fixture
def fast_search(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings.tuning, "n_iter", 2)
    monkeypatch.setattr(settings.tuning, "cv", 2)
    monkeypatch.setattr(settings.tuning, "n_jobs", 1)
    monkeypatch.setattr(settings.tuning, "verbose", 0)


def _model_path(station: str):
    return file_paths.model_save(
        station, settings.ml.selected_model, TRAIN_START, TRAIN_END
    )


def _json(path):
    return json.loads(path.read_text())


# =============================================================================
# train
# =============================================================================


def test_the_model_file_is_named_after_the_training_period(model_input: str) -> None:
    train.train_station(model_input)

    path = _model_path(model_input)
    assert path.exists()
    assert "_st_2022-01-01_et_2022-12-31_" in path.name


def test_training_uses_only_the_training_period_and_holds_out_its_last_days(
    model_input: str,
) -> None:
    train.train_station(model_input)

    card = _json(card_path(_model_path(model_input)))
    assert card["rows"] == {
        "train": 64,
        "test": 16,
    }  # 80 rows of 2022; the 2023 rows are out
    assert card["train_period"]["requested"] == {
        "start_date": TRAIN_START,
        "end_date": TRAIN_END,
    }
    assert card["train_period"]["last"].startswith("2022-06-21")


def test_the_card_records_what_produced_the_model(model_input: str) -> None:
    train.train_station(model_input)

    card = _json(card_path(_model_path(model_input)))
    assert card["model_type"] == settings.ml.selected_model
    assert card["station"] == model_input
    assert card["target"] == "glo_avg"
    assert card["hyperparameters"]["n_neighbors"] == settings.model["n_neighbors"]
    assert card["features"] == {
        "count": 2,
        "names": ["radius=1_C03_11", "radius=1_C03_12"],
    }
    assert card["dataset"]["sha256"] == file_sha256(file_paths.model_input(model_input))
    assert {"git_revision", "created_at", "versions"} <= set(card)


def test_the_saved_model_predicts_with_the_columns_it_was_trained_on(
    model_input: str,
) -> None:
    train.train_station(model_input)

    model, card = load_model(_model_path(model_input))

    assert list(model.feature_names_in_) == card["features"]["names"]
    frame = pd.read_csv(file_paths.model_input(model_input))
    assert model.predict(frame[card["features"]["names"]]).shape == (len(frame),)


def test_the_metrics_report_says_what_was_evaluated_and_on_which_model(
    model_input: str,
) -> None:
    train.train_station(model_input)

    report = _json(
        file_paths.model_metrics(
            model_input, settings.ml.selected_model, TRAIN_START, TRAIN_END
        )
    )
    assert set(settings.ml.evaluation_metrics) <= set(report["metrics"])
    assert (
        report["evaluation"]["kind"] == "holdout" and report["evaluation"]["rows"] == 16
    )
    assert report["model"].endswith(_model_path(model_input).name)
    assert report["dataset"]["sha256"] == file_sha256(
        file_paths.model_input(model_input)
    )
    assert report["station"] == model_input


def test_without_a_train_period_the_etl_period_is_used(
    model_input: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The ETL period here is the single day 2022-06-21: of the 80 rows, 72 fall on it."""
    monkeypatch.setattr(settings.ml, "train", None)

    train.train_station(model_input)

    path = file_paths.model_save(
        model_input, settings.ml.selected_model, "2022-06-21", "2022-06-21"
    )
    assert _json(card_path(path))["rows"] == {"train": 57, "test": 15}


def test_train_main_returns_zero_when_every_station_trains(model_input: str) -> None:
    assert train.main() == 0


def test_train_main_returns_one_when_a_station_has_no_input(
    data_tree, one_station
) -> None:
    """Failure mode: a missing model input is logged and reflected in the exit code."""
    assert train.main() == 1


# =============================================================================
# tune
# =============================================================================


def test_tune_station_writes_the_best_parameters_and_what_produced_them(
    model_input: str, fast_search: None
) -> None:
    tune.tune_station(model_input)

    result = _json(
        file_paths.model_tuning(
            model_input, settings.ml.selected_model, TRAIN_START, TRAIN_END
        )
    )
    assert result["station"] == model_input
    assert result["best_rmse"] >= 0
    # Parameters come without the pipeline prefix, ready to paste under ml_config.models.
    assert result["best_parameters"] and not any(
        name.startswith("model__") for name in result["best_parameters"]
    )
    assert result["search"]["n_iter"] == 2 and result["search"]["cv"] == 2
    assert set(result["search"]["space"]) >= set(result["best_parameters"])
    assert result["rows"] == 64  # the training part of 2022 only
    assert result["train_period"] == {"start_date": TRAIN_START, "end_date": TRAIN_END}
    assert result["dataset"]["sha256"] == file_sha256(
        file_paths.model_input(model_input)
    )


def test_tune_main_returns_zero_on_success_and_one_on_failure(
    model_input: str, fast_search: None
) -> None:
    assert tune.main() == 0

    file_paths.model_input(model_input).unlink()
    assert tune.main() == 1
