import logging

from masters_project.file_paths import file_paths
from masters_project.models.evaluate import evaluate_performance
from masters_project.models.factory import build_model
from masters_project.pipeline.dataset_prep import (
    get_train_test_splits,
    load_model_input_data,
)
from masters_project.settings import settings

settings.setup_logging("training_pipeline")
logger = logging.getLogger(__name__)


def main():
    logger.info("--- Starting ML Training Pipeline ---")

    model_name = settings.execution.selected_model

    X_train, X_test, y_train, y_test = load_model_input_data().pipe(
        get_train_test_splits
    )

    logger.info("Initializing the Machine Learning algorithm...")

    hyperparameters = settings.model.model_dump(exclude_none=True)
    model = build_model(model_name=model_name, **hyperparameters)

    model.train(X_train, y_train)

    model_path = file_paths.model_save(model_name, "v1")
    model.save(model_path)

    logger.info("Generating predictions on unseen test data...")
    predictions = model.predict(X_test)

    logger.info("Grading the model's performance...")

    evaluate_performance(
        y_target=y_test, y_predicted=predictions, model_name=model_name
    )

    logger.info("--- ML Training Pipeline Complete! ---")


if __name__ == "__main__":
    main()
