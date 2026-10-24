"""Evaluate trained models, and ensembles of them, on one station and period.

``python -m masters_project.ml.predict`` reads ``ml_config.predict``: the *target* station and
period, the stations whose trained models are tried on the target's data, and the ensembles
to build from them. Each model predicts with the columns it was trained on, selected by name.

Two files are written to the target station's results directory: the predictions
(``timestamp``, observed ``y_true``, one column per model and ensemble) and a report with
the metrics of every column plus what produced them (models, datasets, weights, revision).
"""

import logging
import sys
from typing import Any

import numpy as np
import pandas as pd
from numpy.typing import NDArray

from masters_project.exporters import CSVExporter
from masters_project.file_paths import file_paths
from masters_project.logging_setup import run_logging
from masters_project.ml.artifacts import (
    dataset_fingerprint,
    display_path,
    load_model,
    run_metadata,
)
from masters_project.ml.data import load_model_input_data, select_period
from masters_project.ml.ensemble import combine, ensemble_weights
from masters_project.ml.evaluate import compute_metrics, write_metrics_report
from masters_project.settings import PredictConfig, settings

# Run with ``python -m``, ``__name__`` is "__main__"; ``__spec__.name`` is the real module name.
logger = logging.getLogger(__spec__.name if __spec__ else __name__)

OBSERVED = "y_true"


def _filters(dataset: dict[str, Any]) -> dict[str, Any] | None:
    manifest = dataset.get("manifest")
    return manifest.get("filters") if manifest else None


def _check_consistency(
    station: str,
    card: dict[str, Any],
    target: str,
    target_dataset: dict[str, Any],
    config: PredictConfig,
) -> None:
    """Warn about anything that makes a model's score hard to trust (it never raises)."""
    trained_with, evaluated_with = (
        _filters(card.get("dataset", {})),
        _filters(target_dataset),
    )
    if trained_with is None or evaluated_with is None:
        logger.warning(
            f"[{station}] The filters of the training or of the evaluation dataset are "
            "unknown (no manifest): cannot check that both used the same ones."
        )
    elif trained_with != evaluated_with:
        logger.warning(
            f"[{station}] Trained with filters {trained_with} but evaluated on a dataset "
            f"built with {evaluated_with}: the scores are not comparable."
        )

    if station == target:
        period = card.get("train_period", {})
        first, last = period.get("first", "")[:10], period.get("last", "")[:10]
        if first and first <= config.end_date and last >= config.start_date:
            logger.warning(
                f"[{station}] The evaluation period {config.start_date}..{config.end_date} overlaps "
                f"the days this model was trained on ({first}..{last}): its scores are not "
                "out-of-sample."
            )


def _predict_with(
    station: str, frame: pd.DataFrame, pipeline: Any
) -> NDArray[np.float64]:
    names = list(pipeline.feature_names_in_)
    missing = [name for name in names if name not in frame.columns]
    if missing:
        raise ValueError(
            f"The {station} model needs {len(missing)} columns that the target dataset "
            f"does not have, e.g. {missing[:5]}."
        )
    return pipeline.predict(frame[names])


def predict() -> None:
    """Predict with every configured model and ensemble, then write the predictions and report.

    Raises:
        ValueError: If ``ml_config.predict`` is missing, the period has no rows, a model's
            columns are absent from the target dataset, or an ensemble cannot be weighted.
        FileNotFoundError: If a model has not been trained.
    """
    config = settings.ml.predict
    if config is None:
        raise ValueError("ml_config.predict is not configured; see the README.")

    model_name = config.model_name or settings.ml.selected_model
    train_period = settings.train_period
    target = config.target_station
    target_variable = settings.etl.sonda.target_variable

    frame = select_period(
        load_model_input_data(target), config.start_date, config.end_date
    )
    if frame.empty:
        raise ValueError(
            f"[{target}] The model input has no rows between {config.start_date} and {config.end_date}."
        )
    target_dataset = dataset_fingerprint(target)
    logger.info(
        f"[{target}] Evaluating on {len(frame)} rows, {config.start_date}..{config.end_date}."
    )

    predictions: dict[str, NDArray[np.float64]] = {}
    model_info: dict[str, Any] = {}
    for station in config.models:
        path = file_paths.model_save(
            station, model_name, train_period.start_date, train_period.end_date
        )
        pipeline, card = load_model(path)
        if card is None:
            logger.warning(
                f"[{station}] No card beside {path}: cannot check how it was trained."
            )
        else:
            _check_consistency(station, card, target, target_dataset, config)
        predictions[station] = _predict_with(station, frame, pipeline)
        model_info[station] = {
            "path": display_path(path),
            "train_period": (card or {}).get("train_period"),
            "dataset_sha256": ((card or {}).get("dataset") or {}).get("sha256"),
            "filters": _filters((card or {}).get("dataset") or {}),
        }
        logger.info(f"[{station}] Predicted with {display_path(path)}")

    ensemble_info: dict[str, Any] = {}
    for name, ensemble in config.ensembles.items():
        weights = ensemble_weights(
            ensemble.weights, target, ensemble.members, settings.etl.stations
        )
        predictions[name] = combine(predictions, weights)
        ensemble_info[name] = {
            "members": ensemble.members,
            "scheme": ensemble.weights,
            "weights": weights,
        }
        logger.info(f"[{target}] Ensemble {name}: {ensemble.weights} weights {weights}")

    observed = frame[target_variable].to_numpy()
    metrics = {
        name: compute_metrics(observed, values) for name, values in predictions.items()
    }
    for name, values in metrics.items():
        logger.info(f"[{target}] {name}: {values}")

    output = pd.DataFrame(
        {"timestamp": frame["timestamp"], OBSERVED: observed, **predictions}
    )
    CSVExporter().export(
        output,
        file_paths.predictions(target, model_name, config.start_date, config.end_date),
    )
    write_metrics_report(
        file_paths.predictions_report(
            target, model_name, config.start_date, config.end_date
        ),
        {
            "target_station": target,
            "model_type": model_name,
            "evaluation": {
                "start_date": config.start_date,
                "end_date": config.end_date,
                "rows": len(frame),
                "first": frame["timestamp"].min().isoformat(),
                "last": frame["timestamp"].max().isoformat(),
            },
            "metrics": metrics,
            "models": model_info,
            "ensembles": ensemble_info,
            "dataset": target_dataset,
            **run_metadata(),
        },
    )


def run() -> int:
    """Run :func:`predict`, turning an expected failure into a clear message and exit code 1."""
    logger.info("--- Starting ML Prediction ---")
    try:
        predict()
    except (FileNotFoundError, ValueError) as error:
        logger.error(str(error))
        return 1
    logger.info("--- ML Prediction Complete! ---")
    return 0


def main() -> int:
    """Run the prediction inside its own run log (``data/logs/ml_predict/``)."""
    config = settings.ml.predict
    with run_logging(
        "ml_predict",
        model=(config.model_name if config else None) or settings.ml.selected_model,
        target_station=config.target_station if config else None,
        period=f"{config.start_date}..{config.end_date}" if config else None,
        models=config.models if config else None,
        ensembles=list(config.ensembles) if config else None,
        train_period=settings.train_period.model_dump(),
    ) as log:
        log.exit_code = run()
    return log.exit_code


if __name__ == "__main__":
    sys.exit(main())
