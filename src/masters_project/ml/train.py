"""Train the selected model on each station's model input, save joblib bundles, evaluate on test sets."""

import logging
import sys

from masters_project.file_paths import file_paths
from masters_project.ml.evaluate import evaluate_performance
from masters_project.ml.factory import build_model
from masters_project.ml.data import (
    get_train_test_splits,
    load_model_input_data,
)
from masters_project.settings import settings

settings.setup_logging("training_pipeline")
logger = logging.getLogger(__name__)


def train_station(station: str) -> None:
    """Fit the selected model for ``station``, persist to ``data/models``, run metrics, write JSON report."""
    model_name = settings.ml.selected_model

    logger.info(f"[{station}] Loading model input...")
    X_train, X_test, y_train, y_test = load_model_input_data(station).pipe(
        get_train_test_splits
    )

    logger.info(f"[{station}] Initializing the Machine Learning algorithm...")

    hyperparameters = settings.model.model_dump(exclude_none=True)
    model = build_model(model_name=model_name, **hyperparameters)

    model.train(X_train, y_train)

    model_path = file_paths.model_save(station, model_name, "v1")
    model.save(model_path)

    logger.info(f"[{station}] Generating predictions on unseen test data...")
    predictions = model.predict(X_test)

    logger.info(f"[{station}] Grading the model's performance...")

    evaluate_performance(
        y_target=y_test,
        y_predicted=predictions,
        model_name=model_name,
        station=station,
    )


def main() -> int:
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


if __name__ == "__main__":
    sys.exit(main())
