"""Score predictions with the configured metrics and write the JSON report of a run."""

import logging
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import numpy as np
from numpy.typing import ArrayLike
from sklearn.metrics import (
    mean_absolute_error,
    mean_absolute_percentage_error,
    r2_score,
    root_mean_squared_error,
)

from masters_project.exporters import JSONExporter
from masters_project.settings import settings

logger = logging.getLogger(__name__)

METRIC_REGISTRY = {
    "rmse": root_mean_squared_error,
    "r2": r2_score,
    "mae": mean_absolute_error,
    "mbe": lambda y_target, y_predicted: np.mean(y_predicted - y_target),
    "mape": mean_absolute_percentage_error,
}


def compute_metrics(
    y_target: ArrayLike,
    y_predicted: ArrayLike,
    names: Sequence[str] | None = None,
) -> dict[str, float]:
    """Compute metrics of predictions against the observed values.

    Args:
        y_target: Ground-truth target values (array-like).
        y_predicted: Model predictions (array-like, same length as ``y_target``).
        names: Metrics to compute; defaults to ``ml_config.evaluation_metrics``. Names are
            case-insensitive and an unsupported one is skipped with a warning.

    Returns:
        Metric name (lower case) -> value rounded to 3 decimals.
    """
    results: dict[str, float] = {}
    for metric_name in names if names is not None else settings.ml.evaluation_metrics:
        key = metric_name.lower()
        if key in METRIC_REGISTRY:
            results[key] = round(float(METRIC_REGISTRY[key](y_target, y_predicted)), 3)
        else:
            logger.warning(
                f"Metric '{metric_name}' is not supported in the registry. Skipping."
            )
    return results


def write_metrics_report(path: Path, report: dict[str, Any]) -> None:
    """Write a report (metrics plus the metadata that explains them) as JSON."""
    JSONExporter().export(report, path)
    logger.info(f"Metrics report saved to {path}")
