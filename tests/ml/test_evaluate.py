"""Tests for masters_project.ml.evaluate: the metric registry and the JSON report."""

import json
import logging

import numpy as np
import pytest

from masters_project.ml.evaluate import (
    METRIC_REGISTRY,
    compute_metrics,
    write_metrics_report,
)
from masters_project.settings import settings

Y_TRUE = np.array([1.0, 2.0, 3.0])
Y_PRED = np.array([2.0, 2.0, 4.0])


def test_metrics_have_known_values() -> None:
    metrics = compute_metrics(Y_TRUE, Y_PRED, ["rmse", "r2", "mae", "mbe"])

    assert metrics["rmse"] == pytest.approx(0.816, abs=1e-3)  # sqrt(2/3)
    assert metrics["mae"] == pytest.approx(0.667, abs=1e-3)
    assert metrics["mbe"] == pytest.approx(0.667, abs=1e-3)  # predictions run high
    assert metrics["r2"] == pytest.approx(0.0, abs=1e-3)


def test_metrics_are_plain_floats_that_json_accepts() -> None:
    metrics = compute_metrics(Y_TRUE, Y_PRED, ["rmse", "mbe"])

    assert all(type(value) is float for value in metrics.values())
    json.dumps(metrics)


def test_metric_names_are_case_insensitive() -> None:
    assert set(compute_metrics(Y_TRUE, Y_PRED, ["RMSE", "Mae"])) == {"rmse", "mae"}


def test_without_names_the_configured_metrics_are_used(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings.ml, "evaluation_metrics", ["mae"])
    assert set(compute_metrics(Y_TRUE, Y_PRED)) == {"mae"}


def test_unsupported_metric_is_skipped_with_a_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.WARNING):
        metrics = compute_metrics(Y_TRUE, Y_PRED, ["rmse", "nope"])

    assert set(metrics) == {"rmse"}
    assert "Metric 'nope' is not supported" in caplog.text


def test_registry_covers_every_metric_named_in_the_default_config() -> None:
    assert set(settings.ml.evaluation_metrics) <= set(METRIC_REGISTRY)


def test_write_metrics_report_creates_the_directory_and_keeps_the_content(
    tmp_path,
) -> None:
    path = tmp_path / "results" / "ourinhos" / "report.json"
    report = {"station": "OURINHOS", "metrics": {"rmse": 1.5}}

    write_metrics_report(path, report)

    assert json.loads(path.read_text()) == report
