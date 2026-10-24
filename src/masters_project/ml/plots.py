"""Figures of a prediction run: the observed target against every model and ensemble.

``python -m masters_project.ml.plots`` reads what ``ml.predict`` wrote for the experiment in
``ml_config.predict`` (the predictions CSV and the metrics report) and saves three figures
next to them:

* ``<stem>_timeseries.png``: observed against each model, over the whole evaluation period;
* ``<stem>_window.png``: the same, zoomed on one week (by default the week with most data);
* ``<stem>_metrics.png``: one bar chart per metric.

The figures are built with matplotlib's object API, not ``pyplot``: no display, no global
state, and the same functions serve a notebook (display the returned ``Figure``) and the
command line. Nothing is recomputed here; the numbers are the ones ``ml.predict`` reported.
"""

import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from matplotlib import colormaps
from matplotlib.figure import Figure

from masters_project.file_paths import file_paths
from masters_project.logging_setup import run_logging
from masters_project.ml.predict import OBSERVED
from masters_project.settings import PredictConfig, settings

# Run with ``python -m``, ``__name__`` is "__main__"; ``__spec__.name`` is the real module name.
logger = logging.getLogger(__spec__.name if __spec__ else __name__)

DPI = 150
#: Days shown by the zoomed figure.
WINDOW_DAYS = 7
#: A gap longer than this is drawn as a break, not as a line across the night.
MAX_GAP = pd.Timedelta("30min")
PALETTE = colormaps["tab10"]


@dataclass(frozen=True)
class PredictionRun:
    """What ``ml.predict`` wrote for one experiment."""

    config: PredictConfig
    model_name: str
    predictions: pd.DataFrame
    report: dict[str, Any]

    @property
    def columns(self) -> list[str]:
        """The model and ensemble columns, in the order ``ml.predict`` wrote them."""
        return [c for c in self.predictions.columns if c not in ("timestamp", OBSERVED)]

    @property
    def metrics(self) -> pd.DataFrame:
        """Metrics of every model and ensemble: one row each, one column per metric."""
        return pd.DataFrame(self.report["metrics"]).T

    @property
    def stem(self) -> str:
        """Common start of the names of the figures."""
        return (
            f"{self.model_name.lower()}_predict_"
            f"{self.config.start_date}_{self.config.end_date}"
        )

    @property
    def directory(self) -> Path:
        """The target station's results directory, where the figures are saved."""
        return file_paths.predictions(
            self.config.target_station,
            self.model_name,
            self.config.start_date,
            self.config.end_date,
        ).parent


def load_run() -> PredictionRun:
    """Read the predictions and the report of the experiment in ``ml_config.predict``.

    Raises:
        ValueError: If ``ml_config.predict`` is not configured.
        FileNotFoundError: If ``ml.predict`` has not been run for it.
    """
    config = settings.ml.predict
    if config is None:
        raise ValueError("ml_config.predict is not configured; see the README.")

    model_name = config.model_name or settings.ml.selected_model
    args = (config.target_station, model_name, config.start_date, config.end_date)
    csv, report = file_paths.predictions(*args), file_paths.predictions_report(*args)
    for path in (csv, report):
        if not path.exists():
            raise FileNotFoundError(
                f"No prediction output at {path}. Run "
                "`python -m masters_project.ml.predict` first."
            )

    predictions = pd.read_csv(csv)
    predictions["timestamp"] = pd.to_datetime(predictions["timestamp"], utc=True)
    return PredictionRun(
        config=config,
        model_name=model_name,
        predictions=predictions,
        report=json.loads(report.read_text(encoding="utf-8")),
    )


def break_at_gaps(df: pd.DataFrame, max_gap: pd.Timedelta = MAX_GAP) -> pd.DataFrame:
    """Insert an empty row wherever time jumps by more than ``max_gap``, so a line breaks there.

    The daytime filter leaves no rows at night; without this, matplotlib would draw a straight
    line from the last sample of one day to the first of the next.
    """
    gap_starts = df["timestamp"].shift(1)[df["timestamp"].diff() > max_gap]
    if gap_starts.empty:
        return df
    breaks = pd.DataFrame({"timestamp": gap_starts + pd.Timedelta("1min")})
    return (
        pd.concat([df, breaks])
        .sort_values("timestamp", kind="stable")
        .reset_index(drop=True)
    )


def densest_window(
    predictions: pd.DataFrame, days: int = WINDOW_DAYS
) -> tuple[pd.Timestamp, pd.Timestamp]:
    """First and last instant of the ``days`` consecutive days with the most rows."""
    per_day = predictions.set_index("timestamp").resample("D").size()
    end_day = per_day.rolling(days, min_periods=1).sum().idxmax()
    start_day = end_day - pd.Timedelta(days=days - 1)
    return start_day, end_day + pd.Timedelta(days=1) - pd.Timedelta("1s")


def _comparison(
    ax: Any, data: pd.DataFrame, column: str, color: Any, observed_width: float
) -> None:
    ax.plot(
        data["timestamp"],
        data[OBSERVED],
        color="black",
        linewidth=observed_width,
        label="Observed",
    )
    ax.plot(
        data["timestamp"],
        data[column],
        color=color,
        linewidth=0.9,
        alpha=0.85,
        label=column,
    )
    ax.set_title(f"Observed vs {column}", fontsize=13)
    ax.set_ylabel("Irradiance (W/m²)")
    ax.grid(alpha=0.3)
    ax.legend(loc="upper right")


def timeseries_figure(run: PredictionRun) -> Figure:
    """Observed against each model and ensemble: one panel each, the whole period."""
    data = break_at_gaps(run.predictions)
    fig = Figure(figsize=(16, 3.8 * len(run.columns)), layout="constrained")
    axes = fig.subplots(len(run.columns), 1, sharex=True, squeeze=False)[:, 0]
    for index, (ax, column) in enumerate(zip(axes, run.columns)):
        _comparison(ax, data, column, PALETTE(index % 10), observed_width=1.0)
    axes[-1].set_xlabel("Time (UTC)")
    return fig


def window_figure(
    run: PredictionRun,
    start: pd.Timestamp | None = None,
    end: pd.Timestamp | None = None,
) -> Figure:
    """Observed and every model on one panel, zoomed on a few days.

    Args:
        run: The loaded run.
        start: First instant (UTC); defaults to the start of the densest week.
        end: Last instant (UTC); defaults to the end of the densest week.

    Raises:
        ValueError: If the window holds no rows.
    """
    if start is None or end is None:
        start, end = densest_window(run.predictions)
    window = run.predictions[run.predictions["timestamp"].between(start, end)]
    if window.empty:
        raise ValueError(f"There are no predictions between {start} and {end}.")

    data = break_at_gaps(window)
    fig = Figure(figsize=(16, 5.5), layout="constrained")
    ax = fig.subplots()
    ax.plot(
        data["timestamp"],
        data[OBSERVED],
        color="black",
        linewidth=2.2,
        label="Observed",
    )
    for index, column in enumerate(run.columns):
        ax.plot(
            data["timestamp"],
            data[column],
            color=PALETTE(index % 10),
            linewidth=1.1,
            alpha=0.9,
            label=column,
        )
    ax.set_title(f"{start:%Y-%m-%d} to {end:%Y-%m-%d}", fontsize=13)
    ax.set_ylabel("Irradiance (W/m²)")
    ax.set_xlabel("Time (UTC)")
    ax.grid(alpha=0.3)
    ax.legend(loc="upper right")
    return fig


def metrics_figure(run: PredictionRun) -> Figure:
    """One bar chart per metric, one bar per model and ensemble."""
    metrics = run.metrics
    fig = Figure(figsize=(4.6 * len(metrics.columns), 5), layout="constrained")
    axes = fig.subplots(1, len(metrics.columns), squeeze=False)[0]
    colors = [PALETTE(i % 10) for i in range(len(metrics))]
    for ax, metric in zip(axes, metrics.columns):
        bars = ax.bar(metrics.index, metrics[metric], color=colors)
        ax.bar_label(bars, fmt="%.3f", padding=3)
        ax.set_title(metric.upper(), fontsize=13)
        ax.grid(axis="y", alpha=0.3)
        ax.tick_params(axis="x", rotation=30)
        for label in ax.get_xticklabels():
            label.set_horizontalalignment("right")
    return fig


def save_figures(run: PredictionRun) -> list[Path]:
    """Save the three figures of a run next to its predictions; return their paths."""
    run.directory.mkdir(parents=True, exist_ok=True)
    figures = {
        "timeseries": timeseries_figure(run),
        "window": window_figure(run),
        "metrics": metrics_figure(run),
    }
    paths = []
    for name, figure in figures.items():
        path = run.directory / f"{run.stem}_{name}.png"
        figure.savefig(path, dpi=DPI)
        paths.append(path)
        logger.info(f"Saved {path}")
    return paths


def run() -> int:
    """Draw and save the figures, turning an expected failure into a message and exit code 1."""
    logger.info("--- Starting ML Plots ---")
    try:
        save_figures(load_run())
    except (FileNotFoundError, ValueError) as error:
        logger.error(str(error))
        return 1
    logger.info("--- ML Plots Complete! ---")
    return 0


def main() -> int:
    """Run the plots inside their own run log (``data/logs/ml_plots/``)."""
    config = settings.ml.predict
    with run_logging(
        "ml_plots",
        model=(config.model_name if config else None) or settings.ml.selected_model,
        target_station=config.target_station if config else None,
        period=f"{config.start_date}..{config.end_date}" if config else None,
    ) as log:
        log.exit_code = run()
    return log.exit_code


if __name__ == "__main__":
    sys.exit(main())
