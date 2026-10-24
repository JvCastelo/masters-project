"""Tests for masters_project.ml.evaluate: metric registry and the JSON report."""

import json
import logging

import numpy as np
import pytest

from masters_project.file_paths import file_paths
from masters_project.ml.evaluate import METRIC_REGISTRY, evaluate_performance
from masters_project.settings import settings

Y_TRUE = np.array([1.0, 2.0, 3.0])
Y_PRED = np.array([2.0, 2.0, 4.0])


def _report(station: str = "OURINHOS") -> dict:
    return json.loads(file_paths.model_metrics(station, "KNN", "v1").read_text())


def test_report_has_the_configured_metrics_with_known_values(
    data_tree, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings.ml, "evaluation_metrics", ["rmse", "r2", "mae", "mbe"])
    evaluate_performance(Y_TRUE, Y_PRED, "KNN", "OURINHOS")

    report = _report()
    assert report["model_type"] == "KNN"
    assert report["station"] == "OURINHOS"
    assert report["rmse"] == pytest.approx(0.816, abs=1e-3)  # sqrt(2/3)
    assert report["mae"] == pytest.approx(0.667, abs=1e-3)
    assert report["mbe"] == pytest.approx(0.667, abs=1e-3)  # predictions run high
    assert report["r2"] == pytest.approx(0.0, abs=1e-3)


def test_metric_names_are_case_insensitive(
    data_tree, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings.ml, "evaluation_metrics", ["RMSE"])
    evaluate_performance(Y_TRUE, Y_PRED, "KNN", "OURINHOS")
    assert "rmse" in _report()


def test_unsupported_metric_is_skipped_with_a_warning(
    data_tree, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(settings.ml, "evaluation_metrics", ["rmse", "nope"])
    with caplog.at_level(logging.WARNING):
        evaluate_performance(Y_TRUE, Y_PRED, "KNN", "OURINHOS")

    assert "nope" not in _report()
    assert "Metric 'nope' is not supported" in caplog.text


def test_registry_covers_every_metric_named_in_the_default_config() -> None:
    assert set(settings.ml.evaluation_metrics) <= set(METRIC_REGISTRY)
