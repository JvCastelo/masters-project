"""Tests for masters_project.ml.predict: trained models and an ensemble evaluated on another station."""

import json
import logging

import numpy as np
import pandas as pd
import pytest

from masters_project.file_paths import file_paths
from masters_project.ml import predict, train
from masters_project.ml.ensemble import ensemble_weights
from masters_project.ml.evaluate import compute_metrics
from masters_project.settings import (
    EnsembleConfig,
    PeriodConfig,
    PredictConfig,
    settings,
)
from masters_project.utils import file_sha256

TARGET = "OURINHOS"
CPA, SMS = "CACHOEIRA_PAULISTA", "SAO_MARTINHO_DA_SERRA"
STATIONS = [CPA, TARGET, SMS]
FILTERS = {"min_solar_elevation": 5.0, "frozen_min_run": 10}
ROWS_2022, ROWS_2023 = 120, 48


def _dataset(offset: float, start: str, n: int, seed: int) -> pd.DataFrame:
    """The target is a function of the pixels; each station's model learns its own offset."""
    rng = np.random.default_rng(seed)
    pixel = rng.uniform(0, 100, n)
    return pd.DataFrame(
        {
            "timestamp": pd.date_range(start, periods=n, freq="10min", tz="UTC"),
            "radius=1_C03_11": pixel,
            "radius=1_C03_12": rng.uniform(0, 1, n),
            "glo_avg": pixel * 5.0 + offset,
        }
    )


def _write_manifest(station: str, filters: dict | None) -> None:
    path = file_paths.model_input_manifest(station)
    path.write_text(json.dumps({"station": station, "filters": filters}))


@pytest.fixture
def experiment(data_tree, monkeypatch: pytest.MonkeyPatch) -> None:
    """Three stations trained on 2022; Ourinhos also has 2023, which is the evaluation period."""
    monkeypatch.setattr(settings.etl, "start_date", "2022-01-01")
    monkeypatch.setattr(settings.etl, "end_date", "2023-12-31")
    monkeypatch.setattr(
        settings.ml,
        "train",
        PeriodConfig(start_date="2022-01-01", end_date="2022-12-31"),
    )
    monkeypatch.setattr(
        settings.ml,
        "predict",
        PredictConfig(
            target_station=TARGET,
            start_date="2023-01-01",
            end_date="2023-12-31",
            models=STATIONS,
            ensembles={
                "CPA_SMS_IDW": EnsembleConfig(members=[CPA, SMS], weights="idw")
            },
        ),
    )

    for seed, (station, offset) in enumerate(zip(STATIONS, (10.0, 0.0, -10.0))):
        frames = [_dataset(offset, "2022-06-21 12:00", ROWS_2022, seed)]
        if station == TARGET:
            frames.append(_dataset(offset, "2023-06-21 12:00", ROWS_2023, 99))
        path = file_paths.model_input(station)
        path.parent.mkdir(parents=True, exist_ok=True)
        pd.concat(frames).to_csv(path, index=False)
        _write_manifest(station, FILTERS)
        train.train_station(station)


def _report():
    return json.loads(
        file_paths.predictions_report(
            TARGET, "KNN", "2023-01-01", "2023-12-31"
        ).read_text()
    )


def _predictions() -> pd.DataFrame:
    return pd.read_csv(
        file_paths.predictions(TARGET, "KNN", "2023-01-01", "2023-12-31"),
        parse_dates=["timestamp"],
    )


# =============================================================================
# outputs
# =============================================================================


def test_one_column_per_model_and_ensemble_for_the_evaluation_period_only(
    experiment,
) -> None:
    assert predict.run() == 0

    output = _predictions()
    assert list(output.columns) == [
        "timestamp",
        "y_true",
        CPA,
        TARGET,
        SMS,
        "CPA_SMS_IDW",
    ]
    assert len(output) == ROWS_2023  # the 2022 rows are not evaluated
    assert output["timestamp"].dt.year.unique().tolist() == [2023]


def test_y_true_is_the_observed_target_of_the_evaluation_rows(experiment) -> None:
    predict.run()

    frame = pd.read_csv(file_paths.model_input(TARGET), parse_dates=["timestamp"])
    expected = frame[frame["timestamp"].dt.year == 2023]["glo_avg"].to_numpy()
    np.testing.assert_allclose(_predictions()["y_true"], expected)


def test_the_ensemble_is_the_idw_combination_of_its_members(experiment) -> None:
    predict.run()

    output = _predictions()
    weights = ensemble_weights("idw", TARGET, [CPA, SMS], settings.etl.stations)
    expected = weights[CPA] * output[CPA] + weights[SMS] * output[SMS]
    np.testing.assert_allclose(output["CPA_SMS_IDW"], expected)
    assert _report()["ensembles"]["CPA_SMS_IDW"]["weights"] == pytest.approx(weights)


def test_the_report_has_the_metrics_of_every_column_computed_from_the_csv(
    experiment,
) -> None:
    predict.run()

    output, report = _predictions(), _report()
    assert set(report["metrics"]) == {CPA, TARGET, SMS, "CPA_SMS_IDW"}
    for name, metrics in report["metrics"].items():
        assert metrics == compute_metrics(output["y_true"], output[name])


def test_the_report_says_what_produced_the_numbers(experiment) -> None:
    predict.run()

    report = _report()
    assert report["target_station"] == TARGET and report["model_type"] == "KNN"
    assert report["evaluation"]["rows"] == ROWS_2023
    assert report["evaluation"]["start_date"] == "2023-01-01"
    assert report["dataset"]["sha256"] == file_sha256(file_paths.model_input(TARGET))
    assert set(report["models"]) == set(STATIONS)
    assert report["models"][CPA]["train_period"]["requested"] == {
        "start_date": "2022-01-01",
        "end_date": "2022-12-31",
    }
    assert report["models"][CPA]["filters"] == FILTERS
    assert {"git_revision", "created_at", "versions"} <= set(report)


def test_equal_weights_give_the_plain_mean(
    experiment, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        settings.ml.predict,
        "ensembles",
        {
            "MEAN": EnsembleConfig(members=[CPA, SMS], weights="equal"),
        },
    )
    predict.run()

    output = _predictions()
    np.testing.assert_allclose(output["MEAN"], (output[CPA] + output[SMS]) / 2)


def test_columns_are_selected_by_name_whatever_their_order_in_the_dataset(
    experiment,
) -> None:
    predict.run()
    before = _predictions()

    path = file_paths.model_input(TARGET)
    frame = pd.read_csv(path)
    frame[["glo_avg", "radius=1_C03_12", "radius=1_C03_11", "timestamp"]].to_csv(
        path, index=False
    )
    predict.run()

    pd.testing.assert_frame_equal(_predictions(), before)


# =============================================================================
# failures are clear and exit 1
# =============================================================================


def test_a_model_that_was_not_trained_says_how_to_train_it(
    experiment, caplog: pytest.LogCaptureFixture
) -> None:
    next(
        file_paths.model_save(SMS, "KNN", "2022-01-01", "2022-12-31").parent.glob(
            "*.joblib"
        )
    ).unlink()

    with caplog.at_level(logging.ERROR):
        assert predict.run() == 1

    assert "No trained model at" in caplog.text and "ml.train" in caplog.text


def test_a_missing_feature_column_names_the_model_and_the_column(
    experiment, caplog: pytest.LogCaptureFixture
) -> None:
    path = file_paths.model_input(TARGET)
    pd.read_csv(path).drop(columns=["radius=1_C03_11"]).to_csv(path, index=False)

    with caplog.at_level(logging.ERROR):
        assert predict.run() == 1

    assert "needs 1 columns" in caplog.text and "radius=1_C03_11" in caplog.text


def test_an_evaluation_period_without_rows_is_an_error(
    experiment, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(settings.ml.predict, "start_date", "2030-01-01")
    monkeypatch.setattr(settings.ml.predict, "end_date", "2030-12-31")

    with caplog.at_level(logging.ERROR):
        assert predict.run() == 1

    assert "no rows between 2030-01-01 and 2030-12-31" in caplog.text


def test_an_idw_ensemble_containing_the_target_is_rejected(
    experiment, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(
        settings.ml.predict,
        "ensembles",
        {
            "BAD": EnsembleConfig(members=[TARGET, CPA], weights="idw"),
        },
    )

    with caplog.at_level(logging.ERROR):
        assert predict.run() == 1

    assert "it is the target station" in caplog.text


def test_without_ml_config_predict_the_run_fails_with_a_clear_message(
    data_tree, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(settings.ml, "predict", None)

    with caplog.at_level(logging.ERROR):
        assert predict.run() == 1

    assert "ml_config.predict is not configured" in caplog.text


# =============================================================================
# warnings that make a score hard to trust
# =============================================================================


def test_evaluating_a_model_on_its_own_training_days_is_flagged(
    experiment, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(settings.ml.predict, "start_date", "2022-01-01")
    monkeypatch.setattr(settings.ml.predict, "end_date", "2022-12-31")

    with caplog.at_level(logging.WARNING):
        predict.run()

    assert (
        "[OURINHOS] The evaluation period 2022-01-01..2022-12-31 overlaps"
        in caplog.text
    )
    assert "not out-of-sample" in caplog.text
    assert (
        "[CACHOEIRA_PAULISTA] The evaluation period" not in caplog.text
    )  # another place: no leakage


def test_evaluating_on_other_days_than_the_training_raises_no_overlap_warning(
    experiment, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.WARNING):
        predict.run()

    assert "overlaps" not in caplog.text


def test_different_filters_in_training_and_evaluation_are_flagged(
    experiment, caplog: pytest.LogCaptureFixture
) -> None:
    _write_manifest(TARGET, {"min_solar_elevation": None, "frozen_min_run": None})

    with caplog.at_level(logging.WARNING):
        predict.run()

    assert "the scores are not comparable" in caplog.text


def test_a_dataset_without_a_manifest_is_flagged(
    experiment, caplog: pytest.LogCaptureFixture
) -> None:
    file_paths.model_input_manifest(TARGET).unlink()

    with caplog.at_level(logging.WARNING):
        predict.run()

    assert "cannot check that both used the same ones" in caplog.text


def test_a_model_without_a_card_still_predicts_but_says_it_cannot_check(
    experiment, caplog: pytest.LogCaptureFixture
) -> None:
    model = file_paths.model_save(CPA, "KNN", "2022-01-01", "2022-12-31")
    model.with_suffix(".json").unlink()

    with caplog.at_level(logging.WARNING):
        assert predict.run() == 0

    assert "cannot check how it was trained" in caplog.text


# =============================================================================
# entry point
# =============================================================================


def test_main_writes_a_run_log_that_names_the_experiment(
    experiment, isolated_logs
) -> None:
    assert predict.main() == 0

    log = (isolated_logs / "ml_predict" / "latest.log").read_text()
    assert "target_station: OURINHOS" in log
    assert "period: 2023-01-01..2023-12-31" in log
    assert "ensembles: ['CPA_SMS_IDW']" in log
    assert "status=ok" in log


def test_main_without_a_predict_section_exits_with_one(
    data_tree, monkeypatch: pytest.MonkeyPatch, isolated_logs
) -> None:
    monkeypatch.setattr(settings.ml, "predict", None)

    assert predict.main() == 1
    assert "status=failed" in (isolated_logs / "ml_predict" / "latest.log").read_text()
