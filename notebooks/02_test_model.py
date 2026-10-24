# %%
"""Look at an experiment run by ``python -m masters_project.ml.predict``.

The figures come from ``masters_project.ml.plots``, the same functions that
``python -m masters_project.ml.plots`` uses to save them as PNG files. Start the kernel with
the config of the run, for example ``MASTERS_PIPELINE_CONFIG=config/experiments/ourinhos-2023.json``.
"""

from IPython.display import display

from masters_project.ml import plots

# %%
run = plots.load_run()
print(
    f"{run.config.target_station} {run.config.start_date}..{run.config.end_date}: {len(run.predictions)} rows"
)
print("models and ensembles:", run.columns)
print("revision:", run.report["git_revision"], "| created:", run.report["created_at"])

# One row per model and ensemble.
run.metrics

# %%
# Observed against each model, over the whole evaluation period.
display(plots.timeseries_figure(run))

# %%
# A week in detail: by default the one with the most data. To choose another
# (with `import pandas as pd`):
# plots.window_figure(run, pd.Timestamp("2023-03-01", tz="UTC"), pd.Timestamp("2023-03-08", tz="UTC"))
display(plots.window_figure(run))

# %%
display(plots.metrics_figure(run))

# %%
# Save the three figures next to the predictions (what `python -m masters_project.ml.plots` does).
# plots.save_figures(run)
