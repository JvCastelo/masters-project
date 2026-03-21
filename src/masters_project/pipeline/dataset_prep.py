import logging

import pandas as pd
from sklearn.model_selection import train_test_split

from masters_project.file_paths import file_paths
from masters_project.settings import settings

logger = logging.getLogger(__name__)


def load_model_input_data() -> pd.DataFrame:
    """1. ONLY loads the data from the disk and sorts it."""
    data_path = file_paths.model_input()
    logger.info(f"Loading data from {data_path}")

    df = pd.read_csv(data_path, parse_dates=["timestamp"])
    return df.sort_values("timestamp").reset_index(drop=True)


def get_train_test_splits(df: pd.DataFrame):
    """2. ONLY handles separating the columns and splitting the rows."""
    logger.info("Splitting data into X and y...")

    target_col = settings.etl.sonda.target_variable
    feature_cols = [col for col in df.columns if col not in ["timestamp", target_col]]

    X = df[feature_cols]
    y = df[target_col]

    logger.info(
        f"Splitting data chronologically (Test Size: {settings.ml.test_size})..."
    )
    return train_test_split(X, y, test_size=settings.ml.test_size, shuffle=False)
