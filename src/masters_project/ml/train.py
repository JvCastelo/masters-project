"""Train the selected model on each station's model input, save joblib bundles, evaluate on test sets."""

import logging
import sys

import joblib

from masters_project.file_paths import file_paths
from masters_project.logging_setup import run_logging
from masters_project.ml.data import (
    get_train_test_splits,
    load_model_input_data,
)
from masters_project.ml.evaluate import evaluate_performance
from masters_project.ml.models import build_model
from masters_project.settings import settings

# Run with ``python -m``, ``__name__`` is "__main__"; ``__spec__.name`` is the real module name.
logger = logging.getLogger(__spec__.name if __spec__ else __name__)


def train_station(station: str) -> None:
    """Fit the selected model for ``station``, persist to ``data/models``, run metrics, write JSON report."""
    model_name = settings.ml.selected_model

    logger.info(f"[{station}] Loading model input...")
    X_train, X_test, y_train, y_test = load_model_input_data(station).pipe(
        get_train_test_splits
    )

    logger.info(f"[{station}] Initializing the Machine Learning algorithm...")

    model = build_model(model_name, **settings.model)
    model.fit(X_train, y_train)

    model_path = file_paths.model_save(station, model_name, "v1")
    model_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, model_path)
    logger.info(f"[{station}] Model saved to {model_path}")

    logger.info(f"[{station}] Generating predictions on unseen test data...")
    predictions = model.predict(X_test)

    logger.info(f"[{station}] Grading the model's performance...")

    evaluate_performance(
        y_target=y_test,
        y_predicted=predictions,
        model_name=model_name,
        station=station,
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
