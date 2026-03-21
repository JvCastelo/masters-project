import logging

from sklearn.model_selection import RandomizedSearchCV

from masters_project.file_paths import file_paths
from masters_project.loaders.json import JSONExporter
from masters_project.models.tuning import get_tuning_config
from masters_project.pipeline.dataset_prep import (
    get_train_test_splits,
    load_model_input_data,
)
from masters_project.settings import settings

settings.setup_logging("tuning_pipeline")
logger = logging.getLogger(__name__)


def main():

    logger.info("--- Starting ML Tuning Pipeline ---")

    model_name = settings.execution.selected_model

    X_train, X_test, y_train, y_test = load_model_input_data().pipe(
        get_train_test_splits
    )

    base_estimator, param_grid = get_tuning_config(model_name)

    logger.info(f"Initializing Search with grid: {list(param_grid.keys())}")

    tuner = RandomizedSearchCV(
        estimator=base_estimator,
        param_distributions=param_grid,
        n_iter=settings.tuning.n_iter,
        scoring=settings.tuning.scoring,
        cv=settings.tuning.cv,
        verbose=settings.tuning.verbose,
        n_jobs=settings.tuning.n_jobs,
        random_state=42,
    )

    logger.info("Starting the tuning process...")

    tuner.fit(X_train, y_train)

    logger.info("--- Tuning Complete! ---")

    best_params = tuner.best_params_
    best_score = abs(tuner.best_score_)

    logger.info(f"BEST RMSE ACHIEVED DURING CROSS-VALIDATION: {best_score:.3f} W/m²")

    tuning_results = {
        "model_type": model_name,
        "station": settings.execution.active_station,
        "start_time": settings.execution.start_date,
        "end_time": settings.execution.end_date,
        "best_rmse": round(best_score, 3),
        "best_parameters": best_params,
    }

    output_path = file_paths.model_tuning(model_name, "v1")

    exporter = JSONExporter()
    exporter.export(data=tuning_results, destination=output_path)

    logger.info(f"Tuning results permanently saved to {output_path}")


if __name__ == "__main__":
    main()
