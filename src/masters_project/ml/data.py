"""Load a station's model input, pick the training period and split it chronologically."""

import logging

import pandas as pd
from sklearn.model_selection import train_test_split

from masters_project.file_paths import file_paths
from masters_project.settings import settings

logger = logging.getLogger(__name__)


def load_model_input_data(station: str) -> pd.DataFrame:
    """Load ``station``'s ``model_input`` CSV from disk and sort by ``timestamp``."""
    data_path = file_paths.model_input(station)
    logger.info(f"Loading data from {data_path}")

    df = pd.read_csv(data_path)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df.sort_values("timestamp").reset_index(drop=True)


def select_period(df: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    """Rows from ``start_date`` 00:00 to ``end_date`` 23:59:59 (UTC), both days included."""
    first = pd.Timestamp(start_date, tz="UTC")
    last = pd.Timestamp(end_date, tz="UTC") + pd.Timedelta(days=1)
    kept = df[(df["timestamp"] >= first) & (df["timestamp"] < last)]
    return kept.reset_index(drop=True)


def training_frame(station: str) -> pd.DataFrame:
    """The station's model input restricted to the training period (``settings.train_period``).

    Raises:
        ValueError: If the model input has no rows in that period.
    """
    period = settings.train_period
    frame = select_period(
        load_model_input_data(station), period.start_date, period.end_date
    )
    if frame.empty:
        raise ValueError(
            f"[{station}] The model input has no rows between {period.start_date} "
            f"and {period.end_date}."
        )
    logger.info(
        f"[{station}] Training period {period.start_date}..{period.end_date}: "
        f"{len(frame)} rows."
    )
    return frame


def get_train_test_splits(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
    """Split features and target; chronological ``train_test_split`` (no shuffle)."""
    logger.info("Splitting data into X and y...")

    target_col = settings.etl.sonda.target_variable
    feature_cols = [col for col in df.columns if col not in ["timestamp", target_col]]

    X = df[feature_cols]
    y = df[target_col]

    logger.info(
        f"Splitting data chronologically (Test Size: {settings.ml.test_size})..."
    )
    return train_test_split(X, y, test_size=settings.ml.test_size, shuffle=False)
