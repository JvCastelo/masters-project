"""Hyperparameter search with RandomizedSearchCV for each station; save best params to JSON."""

import logging
import sys

from sklearn.model_selection import RandomizedSearchCV

from masters_project.exporters import JSONExporter
from masters_project.file_paths import file_paths
from masters_project.logging_setup import run_logging
from masters_project.ml.data import (
    get_train_test_splits,
    load_model_input_data,
)
from masters_project.ml.models import build_model, search_space
from masters_project.settings import settings

# Run with ``python -m``, ``__name__`` is "__main__"; ``__spec__.name`` is the real module name.
logger = logging.getLogger(__spec__.name if __spec__ else __name__)


def tune_station(station: str) -> None:
    """Run randomized search for ``ml_config.selected_model`` on ``station``; export tuning JSON."""
    model_name = settings.ml.selected_model

    X_train, X_test, y_train, y_test = load_model_input_data(station).pipe(
        get_train_test_splits
    )

    pipeline = build_model(model_name, n_jobs=-1)
    param_grid = search_space(model_name)

    logger.info(f"[{station}] Initializing Search with grid: {list(param_grid.keys())}")

    tuner = RandomizedSearchCV(
        estimator=pipeline,
        param_distributions=param_grid,
        n_iter=settings.tuning.n_iter,
        scoring=settings.tuning.scoring,
        cv=settings.tuning.cv,
        verbose=settings.tuning.verbose,
        n_jobs=settings.tuning.n_jobs,
        random_state=42,
    )

    logger.info(f"[{station}] Starting the tuning process...")

    tuner.fit(X_train, y_train)

    best_params = tuner.best_params_
    best_score = abs(tuner.best_score_)

    logger.info(
        f"[{station}] BEST RMSE ACHIEVED DURING CROSS-VALIDATION: {best_score:.3f} W/m²"
    )

    tuning_results = {
        "model_type": model_name,
        "station": station,
        "start_time": settings.etl.start_date,
        "end_time": settings.etl.end_date,
        "best_rmse": round(best_score, 3),
        "best_parameters": best_params,
    }

    output_path = file_paths.model_tuning(station, model_name, "v1")

    JSONExporter().export(tuning_results, output_path)

    logger.info(f"[{station}] Tuning results permanently saved to {output_path}")


def run() -> int:
    """Tune the selected model for every station in ``etl_config.stations``.

    A failure in one station is logged and does not stop the others.

    Returns:
        0 if every station was tuned, 1 otherwise.
    """
    logger.info("--- Starting ML Tuning Pipeline ---")

    failed = []
    for station in settings.stations:
        try:
            tune_station(station)
        except Exception:
            logger.exception(f"Tuning failed for station {station}")
            failed.append(station)

    if failed:
        logger.error(f"Tuning failed for: {failed}")
        return 1

    logger.info("--- Tuning Complete! ---")
    return 0


def main() -> int:
    """Run tuning inside its own run log (``data/logs/ml_tune/``).

    Returns:
        The exit code of :func:`run`.
    """
    with run_logging(
        "ml_tune",
        model=settings.ml.selected_model,
        period=f"{settings.etl.start_date}..{settings.etl.end_date}",
        stations=list(settings.stations),
        n_iter=settings.tuning.n_iter,
        cv=settings.tuning.cv,
        scoring=settings.tuning.scoring,
    ) as log:
        log.exit_code = run()
    return log.exit_code


if __name__ == "__main__":
    sys.exit(main())
