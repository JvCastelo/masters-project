"""Hyperparameter search with RandomizedSearchCV for each station; save best params to JSON."""

import logging
import sys

from sklearn.model_selection import RandomizedSearchCV

from masters_project.exporters import JSONExporter
from masters_project.file_paths import file_paths
from masters_project.logging_setup import run_logging
from masters_project.ml.artifacts import dataset_fingerprint, jsonable, run_metadata
from masters_project.ml.data import get_train_test_splits, training_frame
from masters_project.ml.models import ESTIMATOR_STEP, build_model, search_space
from masters_project.settings import settings

# Run with ``python -m``, ``__name__`` is "__main__"; ``__spec__.name`` is the real module name.
logger = logging.getLogger(__spec__.name if __spec__ else __name__)

RANDOM_STATE = 42


def tune_station(station: str) -> None:
    """Search the selected model's hyperparameters on ``station``'s training rows.

    The search runs on the same pipeline that training builds (scaler included), over the
    training part of ``ml_config.train``; the held-out days are never touched. Writes the best
    parameters, ready to paste under ``ml_config.models``, and what produced them.
    """
    model_name = settings.ml.selected_model
    period = settings.train_period

    frame = training_frame(station)
    X_train, _, y_train, _ = get_train_test_splits(frame)

    pipeline = build_model(model_name, n_jobs=-1)
    space = search_space(model_name)
    logger.info(f"[{station}] Initializing Search with grid: {list(space)}")

    tuner = RandomizedSearchCV(
        estimator=pipeline,
        param_distributions=space,
        n_iter=settings.tuning.n_iter,
        scoring=settings.tuning.scoring,
        cv=settings.tuning.cv,
        verbose=settings.tuning.verbose,
        n_jobs=settings.tuning.n_jobs,
        random_state=RANDOM_STATE,
    )

    logger.info(f"[{station}] Starting the tuning process on {len(X_train)} rows...")
    tuner.fit(X_train, y_train)

    best_score = abs(tuner.best_score_)
    logger.info(
        f"[{station}] BEST RMSE ACHIEVED DURING CROSS-VALIDATION: {best_score:.3f} W/m²"
    )

    prefix = f"{ESTIMATOR_STEP}__"
    JSONExporter().export(
        jsonable(
            {
                "model_type": model_name,
                "station": station,
                "best_rmse": round(best_score, 3),
                "best_parameters": {
                    name.removeprefix(prefix): value
                    for name, value in tuner.best_params_.items()
                },
                "search": {
                    "space": {n.removeprefix(prefix): v for n, v in space.items()},
                    "n_iter": settings.tuning.n_iter,
                    "cv": settings.tuning.cv,
                    "scoring": settings.tuning.scoring,
                    "random_state": RANDOM_STATE,
                },
                "train_period": period.model_dump(),
                "rows": len(X_train),
                "dataset": dataset_fingerprint(station),
                **run_metadata(),
            }
        ),
        file_paths.model_tuning(
            station, model_name, period.start_date, period.end_date
        ),
    )
    logger.info(f"[{station}] Tuning results saved.")


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
