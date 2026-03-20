import logging

import pandas as pd
from sklearn.model_selection import train_test_split

from masters_project.models.evaluate import evaluate_performance
from masters_project.models.factory import build_model
from masters_project.settings import settings

settings.setup_logging("training_pipeline")
logger = logging.getLogger(__name__)


def main():
    logger.info("--- Starting ML Training Pipeline ---")

    data_path = (
        settings.PROCESSED_PATH
        / f"final_features_st_{settings.execution.start_date}_et_{settings.execution.end_date}_{settings.execution.selected_station}.csv"
    )
    logger.info(f"Loading data from {data_path}")

    df = pd.read_csv(data_path, parse_dates=["timestamp"])

    df = df.sort_values("timestamp").reset_index(drop=True)
    target_col = settings.etl.sonda.target_variable
    feature_cols = [col for col in df.columns if col not in ["timestamp", target_col]]

    X = df[feature_cols]
    y = df[target_col]

    logger.info("Splitting data: 80% Train (Past), 20% Test (Future)...")
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=settings.ml.test_size, shuffle=False
    )

    logger.info("Initializing the Machine Learning algorithm...")

    model_name = settings.execution.selected_model
    hyperparameters = settings.model.model_dump(exclude_none=True)
    model = build_model(model_name=model_name, **hyperparameters)

    model.train(X_train, y_train)

    model_path = f"models/{model_name}_st_{settings.execution.start_date}_et_{settings.execution.end_date}_{settings.execution.selected_station}_v1.joblib"
    model.save(model_path)

    logger.info("Generating predictions on unseen test data...")
    predictions = model.predict(X_test)

    logger.info("Grading the model's performance...")

    evaluate_performance(
        y_actual=y_test, y_predicted=predictions, model_name=model_name
    )

    logger.info("--- ML Training Pipeline Complete! ---")


if __name__ == "__main__":
    main()
