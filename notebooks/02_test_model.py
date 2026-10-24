# %%
"""Plots of an experiment run by ``python -m masters_project.ml.predict``.

The notebook only reads what ``ml.predict`` wrote (predictions and metrics); it computes
nothing itself. Start the kernel with the same config as the run, for example
``MASTERS_PIPELINE_CONFIG=config/experiments/ourinhos-2023.json``.
"""

import json

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from masters_project.file_paths import file_paths
from masters_project.settings import settings

sns.set_theme(style="whitegrid")

# %%
predict = settings.ml.predict
if predict is None:
    raise SystemExit(
        "ml_config.predict is not configured: start the kernel with the experiment's config."
    )

model_name = predict.model_name or settings.ml.selected_model
run = (predict.target_station, model_name, predict.start_date, predict.end_date)

predictions_path = file_paths.predictions(*run)
report_path = file_paths.predictions_report(*run)
figures = predictions_path.parent

predictions = pd.read_csv(predictions_path, parse_dates=["timestamp"])
report = json.loads(report_path.read_text())
columns = [c for c in predictions.columns if c not in ("timestamp", "y_true")]

print(
    f"{predict.target_station} {predict.start_date}..{predict.end_date}: {len(predictions)} rows"
)
print("models and ensembles:", columns)
print("revision:", report["git_revision"], "| created:", report["created_at"])

# %%
# Metrics of every model and ensemble, side by side.
metrics = pd.DataFrame(report["metrics"]).T
metrics

# %%
# Observed value against each model, over the whole evaluation period.
fig, axes = plt.subplots(len(columns), 1, figsize=(16, 4 * len(columns)), sharex=True)
for ax, column in zip(axes, columns):
    ax.plot(
        predictions["timestamp"],
        predictions["y_true"],
        label="Observed",
        color="black",
        linewidth=1.5,
    )
    ax.plot(predictions["timestamp"], predictions[column], label=column, alpha=0.7)
    ax.set_title(f"Observed vs {column}", fontsize=14)
    ax.set_ylabel("Irradiance (W/m²)")
    ax.legend(loc="upper right")
axes[-1].set_xlabel("Time (UTC)")
plt.tight_layout()
plt.savefig(
    figures
    / f"{model_name.lower()}_predict_{predict.start_date}_{predict.end_date}_timeseries.png",
    dpi=300,
    bbox_inches="tight",
)
plt.show()

# %%
# A closer look at a few days (change the window to see another season).
ZOOM = (
    pd.Timestamp(predict.start_date, tz="UTC") + pd.Timedelta(days=60),
    pd.Timestamp(predict.start_date, tz="UTC") + pd.Timedelta(days=67),
)
window = predictions[predictions["timestamp"].between(*ZOOM)]

fig, ax = plt.subplots(figsize=(16, 5))
ax.plot(
    window["timestamp"], window["y_true"], label="Observed", color="black", linewidth=2
)
for column in columns:
    ax.plot(window["timestamp"], window[column], label=column, alpha=0.8)
ax.set_title(f"{ZOOM[0]:%Y-%m-%d} to {ZOOM[1]:%Y-%m-%d}")
ax.set_ylabel("Irradiance (W/m²)")
ax.legend(loc="upper right")
plt.tight_layout()
plt.show()

# %%
# One bar chart per metric.
fig, axes = plt.subplots(
    1, len(metrics.columns), figsize=(4.4 * len(metrics.columns), 5)
)
for ax, metric in zip(axes, metrics.columns):
    bars = ax.bar(
        metrics.index, metrics[metric], color=sns.color_palette("deep", len(metrics))
    )
    ax.bar_label(bars, fmt="%.3f", padding=3)
    ax.set_title(metric.upper())
    ax.tick_params(axis="x", rotation=30)
    for label in ax.get_xticklabels():
        label.set_horizontalalignment("right")
plt.tight_layout()
plt.savefig(
    figures
    / f"{model_name.lower()}_predict_{predict.start_date}_{predict.end_date}_metrics.png",
    dpi=300,
    bbox_inches="tight",
)
plt.show()
