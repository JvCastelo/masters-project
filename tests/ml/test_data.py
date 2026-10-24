"""Tests for masters_project.ml.data: loading the model input and the chronological split."""

import pandas as pd
import pytest

from masters_project.file_paths import file_paths
from masters_project.ml.data import (
    get_train_test_splits,
    load_model_input_data,
    select_period,
    training_frame,
)
from masters_project.settings import PeriodConfig, settings

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


def test_loaded_timestamps_are_utc_aware(data_tree) -> None:
    path = file_paths.model_input("OURINHOS")
    path.parent.mkdir(parents=True, exist_ok=True)
    _frame().to_csv(path, index=False)

    assert str(load_model_input_data("OURINHOS")["timestamp"].dtype.tz) == "UTC"


def _two_years() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2021-12-31 23:50",
                    "2022-01-01 00:00",
                    "2022-12-31 23:50",
                    "2023-01-01 00:00",
                ],
                utc=True,
            ),
            TARGET: [1.0, 2.0, 3.0, 4.0],
        }
    )


def test_select_period_includes_both_boundary_days_entirely() -> None:
    kept = select_period(_two_years(), "2022-01-01", "2022-12-31")

    assert kept[TARGET].tolist() == [2.0, 3.0]
    assert kept.index.tolist() == [0, 1]


def test_select_period_outside_the_data_is_empty() -> None:
    assert select_period(_two_years(), "2030-01-01", "2030-12-31").empty


def test_training_frame_keeps_only_the_training_period(
    data_tree, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The dataset spans 2022-2023 but the model must not see 2023."""
    path = file_paths.model_input("OURINHOS")
    path.parent.mkdir(parents=True, exist_ok=True)
    _two_years().to_csv(path, index=False)
    monkeypatch.setattr(
        settings.ml,
        "train",
        PeriodConfig(start_date="2022-01-01", end_date="2022-12-31"),
    )

    assert training_frame("OURINHOS")[TARGET].tolist() == [2.0, 3.0]


def test_training_frame_without_a_train_period_uses_the_whole_etl_period(
    data_tree, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings.ml, "train", None)
    monkeypatch.setattr(settings.etl, "start_date", "2021-01-01")
    monkeypatch.setattr(settings.etl, "end_date", "2023-12-31")
    path = file_paths.model_input(
        "OURINHOS"
    )  # its name carries the ETL dates set above
    path.parent.mkdir(parents=True, exist_ok=True)
    _two_years().to_csv(path, index=False)

    assert len(training_frame("OURINHOS")) == 4


def test_training_frame_with_no_rows_in_the_period_names_the_station_and_dates(
    data_tree, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = file_paths.model_input("OURINHOS")
    path.parent.mkdir(parents=True, exist_ok=True)
    _two_years().to_csv(path, index=False)
    monkeypatch.setattr(
        settings.ml,
        "train",
        PeriodConfig(start_date="2030-01-01", end_date="2030-12-31"),
    )

    with pytest.raises(ValueError, match=r"OURINHOS.*2030-01-01.*2030-12-31"):
        training_frame("OURINHOS")
