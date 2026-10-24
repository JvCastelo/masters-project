"""Tests for masters_project.ml.train and masters_project.ml.tune: end to end on tiny data."""

import json

import numpy as np
import pandas as pd
import pytest

from masters_project.file_paths import file_paths
from masters_project.ml import train, tune
from masters_project.settings import settings


@pytest.fixture
def model_input(data_tree, one_station) -> str:
    """A small, learnable model input for the station: the target is a function of a pixel."""
    rng = np.random.default_rng(0)
    n = 80
    pixel = rng.uniform(0, 100, n)
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range(
                "2022-06-21 12:00", periods=n, freq="10min", tz="UTC"
            ),
            "radius=1_C03_11": pixel,
            "radius=1_C03_12": rng.uniform(0, 1, n),
            "glo_avg": pixel * 5.0,
        }
    )
    path = file_paths.model_input(one_station)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    return one_station


@pytest.fixture
def fast_search(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings.tuning, "n_iter", 2)
    monkeypatch.setattr(settings.tuning, "cv", 2)
    monkeypatch.setattr(settings.tuning, "n_jobs", 1)
    monkeypatch.setattr(settings.tuning, "verbose", 0)


def test_train_station_saves_a_model_and_a_metrics_report(model_input: str) -> None:
    train.train_station(model_input)

    model_name = settings.ml.selected_model
    assert file_paths.model_save(model_input, model_name, "v1").exists()
    report = json.loads(
        file_paths.model_metrics(model_input, model_name, "v1").read_text()
    )
    assert report["station"] == model_input
    assert set(settings.ml.evaluation_metrics) <= set(report)


def test_train_main_returns_zero_when_every_station_trains(model_input: str) -> None:
    assert train.main() == 0


def test_train_main_returns_one_when_a_station_has_no_input(
    data_tree, one_station
) -> None:
    """Failure mode: a missing model input is logged and reflected in the exit code."""
    assert train.main() == 1


def test_tune_station_writes_the_best_parameters(
    model_input: str, fast_search: None
) -> None:
    tune.tune_station(model_input)

    model_name = settings.ml.selected_model
    result = json.loads(
        file_paths.model_tuning(model_input, model_name, "v1").read_text()
    )
    assert result["station"] == model_input
    assert result["best_rmse"] >= 0
    assert result["best_parameters"]


def test_tune_main_returns_zero_on_success_and_one_on_failure(
    model_input: str, fast_search: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert tune.main() == 0

    file_paths.model_input(model_input).unlink()
    assert tune.main() == 1
