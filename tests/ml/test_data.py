"""Tests for masters_project.ml.data: loading the model input and the chronological split."""

import pandas as pd
import pytest

from masters_project.file_paths import file_paths
from masters_project.ml.data import get_train_test_splits, load_model_input_data
from masters_project.settings import settings

TARGET = "glo_avg"


def _frame(n: int = 10) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.date_range(
                "2022-06-21 12:00", periods=n, freq="10min", tz="UTC"
            ),
            "radius=1_C03_11": range(n),
            "radius=1_C03_12": range(n, 2 * n),
            TARGET: [float(i * 10) for i in range(n)],
        }
    )


def test_load_model_input_data_reads_the_station_file_sorted_by_time(data_tree) -> None:
    path = file_paths.model_input("OURINHOS")
    path.parent.mkdir(parents=True, exist_ok=True)
    _frame().iloc[::-1].to_csv(path, index=False)  # written out of order

    df = load_model_input_data("OURINHOS")

    assert df["timestamp"].is_monotonic_increasing
    assert df.index.tolist() == list(range(10))


def test_load_model_input_data_missing_file_raises(data_tree) -> None:
    with pytest.raises(FileNotFoundError):
        load_model_input_data("OURINHOS")


def test_split_is_chronological_and_follows_test_size(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Failure mode guarded: shuffling would leak the future into the training set."""
    monkeypatch.setattr(settings.ml, "test_size", 0.2)
    X_train, X_test, y_train, y_test = get_train_test_splits(_frame(10))

    assert (len(X_train), len(X_test)) == (8, 2)
    assert y_train.tolist() == [0.0, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0]
    assert y_test.tolist() == [80.0, 90.0]


def test_split_features_exclude_timestamp_and_target() -> None:
    X_train, X_test, _, _ = get_train_test_splits(_frame())
    assert list(X_train.columns) == ["radius=1_C03_11", "radius=1_C03_12"]
    assert list(X_test.columns) == list(X_train.columns)
