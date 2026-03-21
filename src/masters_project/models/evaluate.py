import logging

import numpy as np
from sklearn.metrics import (
    mean_absolute_error,
    mean_absolute_percentage_error,
    r2_score,
    root_mean_squared_error,
)

from masters_project.file_paths import file_paths
from masters_project.loaders.json import JSONExporter
from masters_project.settings import settings

logger = logging.getLogger(__name__)

METRIC_REGISTRY = {
    "rmse": root_mean_squared_error,
    "r2": r2_score,
    "mae": mean_absolute_error,
    "mbe": lambda y_target, y_predicted: np.mean(y_predicted - y_target),
    "mape": mean_absolute_percentage_error,
}


def evaluate_performance(y_target, y_predicted, model_name: str):
    """
    Calculates ML metrics (RMSE, R2) and saves them to a JSON report.
    """
    logger.info("Calculating requested performance metrics...")

    results = {
        "model_type": model_name,
        "station": settings.execution.selected_station,
        "start_date": settings.execution.start_date,
        "end_date": settings.execution.end_date,
    }

    requested_metrics = settings.ml.evaluation_metrics

    for metric_name in requested_metrics:
        metric_key = metric_name.lower()

        if metric_key in METRIC_REGISTRY:
            metric_function = METRIC_REGISTRY[metric_key]
            score = metric_function(y_target, y_predicted)
            results[metric_key] = round(score, 3)
        else:
            logger.warning(
                f"Metric '{metric_name}' is not supported in the registry. Skipping."
            )

    logger.info(f"FINAL RESULTS: {results}")

    output_path = file_paths.model_metrics(model_name, "v1")
    exporter = JSONExporter()
    exporter.export(data=results, destination=output_path)

    logger.info(f"Metrics report saved to {output_path}")

    return
