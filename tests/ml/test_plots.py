"""Tests for masters_project.ml.plots: the figures of a prediction run."""

import json
import logging

import numpy as np
import pandas as pd
import pytest
from matplotlib.figure import Figure

from masters_project.file_paths import file_paths
from masters_project.ml import plots
from masters_project.ml.plots import (
    PredictionRun,
    break_at_gaps,
    densest_window,
    load_run,
    metrics_figure,
    save_figures,
    timeseries_figure,
    window_figure,
)
from masters_project.settings import EnsembleConfig, PredictConfig, settings

TARGET = "OURINHOS"
CPA, SMS = "CACHOEIRA_PAULISTA", "SAO_MARTINHO_DA_SERRA"
START, END = "2023-01-01", "2023-01-20"
PNG = b"\x89PNG\r\n\x1a\n"


def _daytime(days: pd.DatetimeIndex, rows_per_day: int = 24) -> pd.DatetimeIndex:
    """``rows_per_day`` samples every 10 minutes from 12:00 UTC on each day: no night."""
    return pd.DatetimeIndex(
        [
            day + pd.Timedelta(hours=12, minutes=10 * i)
            for day in days
            for i in range(rows_per_day)
        ]
    )


def _predictions() -> pd.DataFrame:
    """20 days (2023-01-01..20) of 24 samples each, from 12:00 UTC: every night is a gap."""
    stamps = _daytime(pd.date_range(START, END, freq="D", tz="UTC"))
    n = len(stamps)
    rng = np.random.default_rng(0)
    observed = rng.uniform(0, 800, n)
    return pd.DataFrame(
        {
            "timestamp": stamps,
            "y_true": observed,
            CPA: observed + rng.normal(0, 30, n),
            TARGET: observed + rng.normal(0, 20, n),
            SMS: observed + rng.normal(0, 40, n),
            "CPA_SMS_IDW": observed + rng.normal(0, 25, n),
        }
    )


@pytest.fixture
def run_outputs(data_tree, monkeypatch: pytest.MonkeyPatch) -> PredictionRun:
    """What ml.predict would have written, for the config below."""
    config = PredictConfig(
        target_station=TARGET,
        start_date=START,
        end_date=END,
        models=[CPA, TARGET, SMS],
        ensembles={"CPA_SMS_IDW": EnsembleConfig(members=[CPA, SMS])},
    )
    monkeypatch.setattr(settings.ml, "predict", config)
    frame = _predictions()
    args = (TARGET, "KNN", START, END)
    csv = file_paths.predictions(*args)
    csv.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(csv, index=False)
    metrics = {
        name: {"rmse": 10.0 + i, "r2": 0.9 - i / 10, "mbe": -1.5 * i}
        for i, name in enumerate([CPA, TARGET, SMS, "CPA_SMS_IDW"])
    }
    file_paths.predictions_report(*args).write_text(
        json.dumps({"metrics": metrics, "git_revision": "abc"})
    )
    return load_run()


# =============================================================================
# loading
# =============================================================================


def test_load_run_reads_the_predictions_and_the_report(
    run_outputs: PredictionRun,
) -> None:
    assert run_outputs.model_name == "KNN"
    assert run_outputs.columns == [CPA, TARGET, SMS, "CPA_SMS_IDW"]
    assert str(run_outputs.predictions["timestamp"].dt.tz) == "UTC"
    assert run_outputs.report["git_revision"] == "abc"
    assert run_outputs.stem == "knn_predict_2023-01-01_2023-01-20"
    assert run_outputs.directory == settings.RESULTS_PATH / "ourinhos"


def test_the_metrics_table_has_one_row_per_model_and_one_column_per_metric(
    run_outputs: PredictionRun,
) -> None:
    assert list(run_outputs.metrics.index) == [CPA, TARGET, SMS, "CPA_SMS_IDW"]
    assert list(run_outputs.metrics.columns) == ["rmse", "r2", "mbe"]
    assert run_outputs.metrics.loc[SMS, "rmse"] == 12.0


def test_load_run_without_ml_config_predict_fails_clearly(
    data_tree, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings.ml, "predict", None)
    with pytest.raises(ValueError, match="ml_config.predict is not configured"):
        load_run()


def test_load_run_before_ml_predict_says_to_run_it(
    data_tree, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        settings.ml,
        "predict",
        PredictConfig(
            target_station=TARGET, start_date=START, end_date=END, models=[TARGET]
        ),
    )
    with pytest.raises(
        FileNotFoundError, match=r"No prediction output at .*ml\.predict"
    ):
        load_run()


# =============================================================================
# gaps and the zoom window
# =============================================================================


def test_break_at_gaps_inserts_an_empty_row_only_where_time_jumps() -> None:
    df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2023-01-01 12:00",
                    "2023-01-01 12:10",
                    "2023-01-02 12:00",
                    "2023-01-02 12:10",
                ],
                utc=True,
            ),
            "y_true": [1.0, 2.0, 3.0, 4.0],
        }
    )

    result = break_at_gaps(df)

    assert len(result) == 5
    assert result["y_true"].isna().tolist() == [False, False, True, False, False]
    assert result["timestamp"].is_monotonic_increasing
    assert result.loc[2, "timestamp"] == pd.Timestamp("2023-01-01 12:11", tz="UTC")


def test_break_at_gaps_leaves_a_continuous_series_alone() -> None:
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2023-01-01", periods=5, freq="10min", tz="UTC"),
            "y_true": range(5),
        }
    )
    assert break_at_gaps(df) is df


def test_the_default_window_is_the_week_with_the_most_data() -> None:
    days = pd.date_range("2023-01-01", periods=30, freq="D", tz="UTC")
    rows = [
        pd.date_range(
            day + pd.Timedelta(hours=12),
            periods=60 if 10 <= i < 17 else 6,
            freq="10min",
        )
        for i, day in enumerate(days)
    ]
    frame = pd.DataFrame({"timestamp": rows[0].append(rows[1:])})

    start, end = densest_window(frame)

    assert start == pd.Timestamp("2023-01-11", tz="UTC")
    assert end == pd.Timestamp("2023-01-17 23:59:59", tz="UTC")


def test_the_window_of_a_short_series_is_the_series_itself() -> None:
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range(
                "2023-01-01 12:00", periods=10, freq="10min", tz="UTC"
            )
        }
    )
    start, end = densest_window(frame, days=7)

    assert start <= frame["timestamp"].min() and end >= frame["timestamp"].max()


# =============================================================================
# figures
# =============================================================================


def test_the_time_series_figure_has_one_panel_per_model_and_ensemble(
    run_outputs: PredictionRun,
) -> None:
    fig = timeseries_figure(run_outputs)

    assert isinstance(fig, Figure) and len(fig.axes) == 4
    titles = [ax.get_title() for ax in fig.axes]
    assert titles == [f"Observed vs {c}" for c in run_outputs.columns]
    for ax in fig.axes:
        assert [line.get_label() for line in ax.get_lines()][0] == "Observed"


def test_the_lines_break_overnight_instead_of_crossing_it(
    run_outputs: PredictionRun,
) -> None:
    observed = timeseries_figure(run_outputs).axes[0].get_lines()[0].get_ydata()

    assert int(np.isnan(observed).sum()) == 19  # 20 days: 19 nights between them


def test_the_window_figure_draws_observed_and_every_model_on_one_panel(
    run_outputs: PredictionRun,
) -> None:
    fig = window_figure(run_outputs)

    assert len(fig.axes) == 1
    labels = [line.get_label() for line in fig.axes[0].get_lines()]
    assert labels == ["Observed", *run_outputs.columns]


def test_the_window_figure_accepts_an_explicit_window(
    run_outputs: PredictionRun,
) -> None:
    start = pd.Timestamp("2023-01-05", tz="UTC")
    fig = window_figure(run_outputs, start, start + pd.Timedelta(days=1))

    assert fig.axes[0].get_title() == "2023-01-05 to 2023-01-06"


def test_an_empty_window_is_an_error(run_outputs: PredictionRun) -> None:
    start = pd.Timestamp("2030-01-01", tz="UTC")
    with pytest.raises(ValueError, match="no predictions between"):
        window_figure(run_outputs, start, start + pd.Timedelta(days=1))


def test_the_metrics_figure_has_one_chart_per_metric_with_the_reported_values(
    run_outputs: PredictionRun,
) -> None:
    fig = metrics_figure(run_outputs)

    assert [ax.get_title() for ax in fig.axes] == ["RMSE", "R2", "MBE"]
    heights = [patch.get_height() for patch in fig.axes[0].patches]
    assert heights == [10.0, 11.0, 12.0, 13.0]


# =============================================================================
# saving and the command
# =============================================================================


def test_save_figures_writes_three_png_files_next_to_the_predictions(
    run_outputs: PredictionRun,
) -> None:
    paths = save_figures(run_outputs)

    assert [p.name for p in paths] == [
        "knn_predict_2023-01-01_2023-01-20_timeseries.png",
        "knn_predict_2023-01-01_2023-01-20_window.png",
        "knn_predict_2023-01-01_2023-01-20_metrics.png",
    ]
    for path in paths:
        assert path.parent == run_outputs.directory
        assert path.read_bytes().startswith(PNG) and path.stat().st_size > 5_000


def test_run_saves_the_figures_and_exits_zero(run_outputs: PredictionRun) -> None:
    assert plots.run() == 0
    assert len(list(run_outputs.directory.glob("*.png"))) == 3


def test_run_before_ml_predict_logs_the_fix_and_exits_one(
    data_tree, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(
        settings.ml,
        "predict",
        PredictConfig(
            target_station=TARGET, start_date=START, end_date=END, models=[TARGET]
        ),
    )
    with caplog.at_level(logging.ERROR):
        assert plots.run() == 1

    assert "ml.predict" in caplog.text


def test_main_writes_a_run_log_for_the_experiment(
    run_outputs: PredictionRun, isolated_logs
) -> None:
    assert plots.main() == 0

    log = (isolated_logs / "ml_plots" / "latest.log").read_text()
    assert "target_station: OURINHOS" in log and "status=ok" in log


def test_main_without_a_predict_section_exits_with_one(
    data_tree, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings.ml, "predict", None)
    assert plots.main() == 1
