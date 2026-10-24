"""Train the selected model on each station's training period; save it with its card and report."""

import logging
import sys

import pandas as pd

from masters_project.file_paths import file_paths
from masters_project.logging_setup import run_logging
from masters_project.ml.artifacts import (
    dataset_fingerprint,
    display_path,
    estimator_parameters,
    run_metadata,
    save_model,
)
from masters_project.ml.data import get_train_test_splits, training_frame
from masters_project.ml.evaluate import compute_metrics, write_metrics_report
from masters_project.ml.models import build_model
from masters_project.settings import settings

# Run with ``python -m``, ``__name__`` is "__main__"; ``__spec__.name`` is the real module name.
logger = logging.getLogger(__spec__.name if __spec__ else __name__)


def time_span(timestamps: pd.Series) -> dict[str, str]:
    """First and last timestamp of a series, as ISO strings."""
    return {"first": timestamps.min().isoformat(), "last": timestamps.max().isoformat()}


def train_station(station: str) -> None:
    """Fit the selected model on ``station``'s training period and evaluate it on the held-out days.

    The training period is ``ml_config.train`` (the whole dataset if absent). Inside it the
    most recent ``test_size`` fraction is held out, never shuffled. Writes the model and its
    card under ``data/models/<station>/`` and the evaluation report under
    ``data/results/<station>/``.
    """
    model_name = settings.ml.selected_model
    period = settings.train_period

    logger.info(f"[{station}] Loading model input...")
    frame = training_frame(station)
    X_train, X_test, y_train, y_test = get_train_test_splits(frame)

    logger.info(f"[{station}] Training {model_name} on {len(X_train)} rows...")
    model = build_model(model_name, **settings.model)
    model.fit(X_train, y_train)

    dataset = dataset_fingerprint(station)
    metadata = run_metadata()
    train_span = time_span(frame.loc[X_train.index, "timestamp"])
    test_span = time_span(frame.loc[X_test.index, "timestamp"])

    model_path = file_paths.model_save(
        station, model_name, period.start_date, period.end_date
    )
    save_model(
        model,
        model_path,
        {
            "model_type": model_name,
            "station": station,
            "target": settings.etl.sonda.target_variable,
            "hyperparameters": estimator_parameters(model),
            "train_period": {"requested": period.model_dump(), **train_span},
            "rows": {"train": len(X_train), "test": len(X_test)},
            "features": {"count": X_train.shape[1], "names": list(X_train.columns)},
            "dataset": dataset,
            **metadata,
        },
    )

    logger.info(f"[{station}] Evaluating on the {len(X_test)} held-out rows...")
    metrics = compute_metrics(y_test, model.predict(X_test))
    logger.info(f"[{station}] {metrics}")
    write_metrics_report(
        file_paths.model_metrics(
            station, model_name, period.start_date, period.end_date
        ),
        {
            "model_type": model_name,
            "station": station,
            "metrics": metrics,
            "evaluation": {"kind": "holdout", "rows": len(X_test), **test_span},
            "train_period": {"requested": period.model_dump(), **train_span},
            "model": display_path(model_path),
            "dataset": dataset,
            **metadata,
        },
    )


def run() -> int:
    """Train and evaluate the selected model for every station in ``etl_config.stations``.

    A failure in one station is logged and does not stop the others.

    Returns:
        0 if every station was trained, 1 otherwise.
    """
    logger.info("--- Starting ML Training Pipeline ---")

    failed = []
    for station in settings.stations:
        try:
            train_station(station)
        except Exception:
            logger.exception(f"Training failed for station {station}")
            failed.append(station)

    if failed:
        logger.error(f"Training failed for: {failed}")
        return 1

    logger.info("--- ML Training Pipeline Complete! ---")
    return 0


def main() -> int:
    """Run training inside its own run log (``data/logs/ml_train/``).

    Returns:
        The exit code of :func:`run`.
    """
    with run_logging(
        "ml_train",
        model=settings.ml.selected_model,
        period=f"{settings.etl.start_date}..{settings.etl.end_date}",
        stations=list(settings.stations),
    ) as log:
        log.exit_code = run()
    return log.exit_code


if __name__ == "__main__":
    sys.exit(main())
