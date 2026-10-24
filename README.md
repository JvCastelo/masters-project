# Masters Project

A data pipeline that predicts ground solar radiation from **GOES-R** satellite imagery (NOAA, on AWS S3) and the **SONDA** ground-station network (INPE). It collects both sources, builds one ML-ready table per station, trains and tunes models on it, and evaluates the trained models (and ensembles of them) on another station or period.

## Overview

```
SONDA ZIPs ──► etl.sonda ──► data/raw/sonda/<station>/*.csv ─┐
                                                    ├─► etl.build_dataset ──► data/processed/<station>/model_input_*.csv ──► ml.train / ml.tune ──► ml.predict ──► ml.plots
GOES S3 ─────► etl.goes ───► data/raw/goes/<station>/*.csv ──┘
```

1. **SONDA ETL** (`etl.sonda`): downloads the SONDA ZIP archives (by year or year-month) of every configured station, extracts the `.dat` files and exports one CSV per station with the target (`glo_avg`, W/m²). Run it **first**: it reports periods SONDA does not publish, and the GOES step uses it to skip days with no target.
2. **GOES ETL** (`etl.goes`): reads GOES-R ABI NetCDF files from NOAA's Open Data Registry with partial range requests, extracts the pixel window of **all configured stations and channels** in one pass, caches it per day, and exports one CSV per channel and station.
3. **Build model input** (`etl.build_dataset`): stacks the GOES channels, filters the SONDA target (daytime and frozen-sensor filters), joins both on timestamp, drops missing values, validates the result with pandera and writes the table the ML stage reads.
4. **Training, tuning, prediction and plots** (`ml.train`, `ml.tune`, `ml.predict`, `ml.plots`): fit and evaluate the selected model on a training period for every station, search its hyperparameters, evaluate the trained models (and ensembles of them) on another station or period, and draw the comparison.

| Command | Reads | Writes |
|---|---|---|
| `etl.sonda` | SONDA server | `data/raw/sonda/<station>/` |
| `etl.goes` | NOAA S3 and the SONDA CSVs (to skip days without a target) | `data/raw/goes/<station>/`, `data/cache/goes/` |
| `etl.build_dataset` | the GOES and SONDA CSVs of the station | `data/processed/<station>/model_input_*.csv` and its manifest |
| `ml.train` | `model_input_*.csv` | `data/models/<station>/` (model and card), `data/results/<station>/..._metrics.json` |
| `ml.tune` | `model_input_*.csv` | `data/results/<station>/..._tuning.json` |
| `ml.predict` | the target station's `model_input_*.csv` and the trained models | `data/results/<target>/..._predictions.csv` and `..._metrics.json` |
| `ml.plots` | the files `ml.predict` wrote | `data/results/<target>/*.png` |

Every command writes its own log under `data/logs/<pipeline>/` (see [Logs](#logs)).

`etl/` and `ml/` never import each other. Their only contract is the `model_input_*.csv` file, whose structure `etl/schemas.py` checks before it is written.

## Requirements

- Python ≥ 3.14
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

## Installation

```bash
git clone <repository-url>
cd masters_project

uv sync                      # runtime (including matplotlib) + dev tools (pytest, ruff, pytest-cov, ipykernel)
```

## Configuration

Edit `config/pipeline.json` before running the pipeline. It has three top-level sections, validated with pydantic when the program starts (an invalid file stops it with exit code 1):

| Section | Description |
|---------|-------------|
| `etl_config` | `start_date`, `end_date` (YYYY-MM-DD), `max_workers` (GOES worker **processes**; 24 was the best measured on a 12-core machine, 36 was already slower), `log_level` |
| `etl_config.stations` | Object mapping station name to `{ "latitude", "longitude" }`. Every pipeline step runs for **all** stations listed here, so keep only the stations you want for the period (e.g. only `OURINHOS` for 2023) |
| `etl_config.goes` | `bucket_name`, `product_name`, `variable`, `channels` (list, e.g. `["C01", "C03"]`; all are collected in one run), `pixel_radius`, `skip_days_without_target` (default `true`: collect only days on which some station has a SONDA target; needs the SONDA ETL to have run) |
| `etl_config.sonda` | `data_type`, `target_variable` |
| `etl_config.filters` | Row filters applied when the model input is built; `null` turns a filter off (default). `min_solar_elevation`: keep rows with the Sun higher than this many degrees. `frozen_min_run`: drop daytime target samples that repeat the same value this many times in a row. See [Filters](#filters) |
| `ml_config` | `selected_model` (key in `models`), `test_size`, `evaluation_metrics`, `models` (per-model constructor arguments of the estimator, e.g. `KNN`, `XGBoost`; a plain dict, so a new model needs no change here) |
| `ml_config.train` | Optional `start_date`/`end_date`: the days of the model input used to train and tune. Without it the whole ETL period is used. Inside it the most recent `test_size` fraction is held out |
| `ml_config.predict` | Optional, read by `ml.predict`: `target_station`, `start_date`/`end_date` (where it is evaluated), `models` (stations whose trained model is tried), `ensembles` (`{name: {members, weights: "idw"\|"equal"}}`) and an optional `model_name` |
| `tuning_config` | `n_iter`, `cv`, `scoring`, `n_jobs`, `verbose` |

To run with a different configuration without editing this file, set `MASTERS_PIPELINE_CONFIG=/path/to/other.json` (same structure).

There is no "selected station": GOES, SONDA, dataset building, training and tuning each process every station in `etl_config.stations` and write one output per station (the station name is part of the file name). The exceptions are `ml.predict` and `ml.plots`, which work on the one **target station** named in `ml_config.predict`, and `build_dataset --station`.

## Running the Pipeline

Use the same date range in `etl_config` for GOES, SONDA, and `build_dataset`. The `ml` commands find the `model_input_*.csv` by that same range (it is part of the file name), so they need the config that built it; the days they train on are chosen separately by `ml_config.train`.

### 1. SONDA ground data (run first)

```bash
# Yearly ZIPs (default, fewer requests: about 1 s per station-year)
uv run python -m masters_project.etl.sonda

# Or monthly ZIPs
uv run python -m masters_project.etl.sonda --by month
```

- One CSV per station, in `data/raw/sonda/<station>/`. The log shows how many days have a valid target (e.g. `Days with a valid 'glo_avg': 196/365`).
- SONDA has real gaps (e.g. Ourinhos had no data from 2023-01-01 to 2023-01-22). A period that SONDA does not publish (HTTP 404) is reported as an error, the CSV is still written with the rest, and the command **exits with code 1**.
- Before writing, the frame is [validated](#validation): a broken structure fails the station, while values outside a physical range only produce a warning (a defective sensor is a property of the data).

### 2. GOES satellite data

```bash
uv run python -m masters_project.etl.goes
```

How it works:

- **Only days with a target.** With `skip_days_without_target` the GOES days are limited to those on which at least one configured station has a valid SONDA value (those days would be dropped by the final join anyway). If a station's SONDA CSV is missing, every day is collected and a warning says so.
- **All stations at once.** Each run extracts the pixel window of *every* station in `etl_config.stations` and writes one CSV per channel and station, in `data/raw/goes/<station>/`. Reading other stations' pixels from a file that is already open is almost free, so the S3 cost is paid once per channel and period.
- **All channels in one run.** `etl_config.goes.channels` lists the channels to collect. Files are listed with one S3 call per day for all channels (instead of one per channel and day), days are listed in parallel, and listings of days that are already closed are cached in `data/cache/goes/listing/` (the NOAA archive does not change). The target pixels of each channel are cached in `data/cache/goes/pixels/`, so no file is opened just to locate stations.
- **Per-day cache.** Results are stored in `data/cache/goes/{product}/{variable}_r{radius}/{channel}/{year}/{day}.csv`, one row per file and station, together with the target pixel. A day is saved once all its files finish, so an interrupted run only redoes the day in progress. Files that fail are retried on the next run. The cache does not depend on the dates, so overlapping periods reuse it.
- **What the cache does and does not save.** It is a checkpoint and a memo for the *same channel and stations*: resuming after an interruption, repeating a run, overlapping periods, retrying failed files and re-exporting CSVs. It does **not** help a new channel (each `.nc` holds a single channel) or a new station: a cached file is skipped only if *every* station in `etl_config.stations` is present with the same target pixel, so adding or moving a station reads all the files of that channel again. Delete the cache folder to force a full re-read.
- **Partial reads and processes.** Only the needed `Rad` chunks are read, straight from the HDF5 file with `h5py` (no xarray) and a 512 KB block cache, so under 10% of each Full Disk file is transferred. Values are decoded exactly as xarray does (verified by a test). Files are processed in a process pool, because `h5py` holds a global lock that prevents threads from scaling (listing days, which only waits on the network, does use threads).

### 3. Build merged model input

```bash
uv run python -m masters_project.etl.build_dataset                       # every station
uv run python -m masters_project.etl.build_dataset --station OURINHOS    # one station
uv run python -m masters_project.etl.build_dataset --station OURINHOS --goes-dir ourinhos_experiment
```

Each station is built from its own directory, `data/raw/goes/<station>/` (the lower-case station key, e.g. `ourinhos`), using the files named `goes_<channel>_..._<STATION>.csv`; CSVs of several periods are stacked and repeated timestamps dropped. `--goes-dir NAME` reads `data/raw/goes/NAME/` instead (it requires `--station`), which lets you build from a different set of CSVs without moving files. A missing directory is logged with the path that was expected and the command exits with code 1.

The result does not depend on the order in which the file system lists the files: channels come out as `C01`..`C16`, and when two scans round to the same 10-minute timestamp the one from the later period wins. (A dataset built before 2026-10-24 has its channel columns in the file system's arbitrary order, different for each station, so a model fitted on it needs its columns selected by name.)

#### Filters

Applied to the SONDA series at its native 1-minute resolution, before the join (`etl/filters.py`; the raw CSVs are never filtered, so changing a threshold only needs a rebuild, not a new collection):

- **`min_solar_elevation`** removes night and twilight. About 48% of the unfiltered rows have `glo_avg ≤ 0`, which makes MAPE unstable and lets a model score well by predicting "night". It depends only on timestamp and station position, never on the target. The 5° in `config/pipeline.json` was chosen **arbitrarily**; it is not tuned or derived. At 0° / 5° / 10° the model input keeps about 49% / 46% / 43% of the rows.
- **`frozen_min_run`** removes daytime samples where the target repeats the same value that many times in a row (a stuck sensor or logger). Night rows and missing values are never counted, and a time gap ends a run. It is a safeguard: on SONDA 2022-2023 (three stations) the longest natural run of identical daytime values is 5 samples, so `frozen_min_run = 10` removes nothing today. GOES features are not filtered for repetition: the 1.38 µm channel (C04) legitimately repeats values through quantization.

#### Validation

`etl/schemas.py` checks the SONDA CSV and the model input with [pandera](https://pandera.readthedocs.io):

- **Structure** (violations raise `DataValidationError`, and the file is not written): expected columns, timezone-aware UTC timestamps that are unique and sorted, numeric target, no missing values and complete GOES windows (every channel has all `(2·radius+1)²` pixels) in the model input.
- **Quality** (violations only log a warning with the offending range; the rows are kept): the target inside `[-100, 2000]` W/m². On the real data this flags exactly one thing: a defective Cachoeira Paulista sensor (2023-10-03 to 12, down to −765 W/m²). Isolated midday peaks up to 1657 W/m² are real cloud-enhancement events and are not flagged.

### 4. Training, tuning, prediction and plots

```bash
uv run python -m masters_project.ml.train      # one model per station
uv run python -m masters_project.ml.tune       # search the hyperparameters
uv run python -m masters_project.ml.predict    # evaluate trained models on another station/period
uv run python -m masters_project.ml.plots      # draw the comparison
```

Each model is a scikit-learn `Pipeline` (`StandardScaler`, then the estimator). Training and tuning build the **same** pipeline, so the scaler is part of every search. A new model is one entry in `ml/models.py` (`MODELS`: the estimator and its search space).

**Train and test.** `ml.train` filters the station's model input by `ml_config.train`, holds out the most recent `test_size` of those days (never shuffled) and fits on the rest. It writes the model, a **model card** beside it and an evaluation report on the held-out days:

- `data/models/<station>/<model>_st_<train start>_et_<train end>_<STATION>_v1.joblib`: the fitted pipeline. Its name carries the **training** period, not the ETL period.
- `...v1.json` (the card): model type, every estimator parameter, requested and actual training period, train/test rows, feature names, dataset (path, sha256 and the dataset manifest with the filters it was built with), git revision and library versions.
- `data/results/<station>/..._metrics.json`: the metrics plus the same context.

`ml.tune` searches on the same training rows and writes `..._tuning.json`: the best parameters (ready to paste under `ml_config.models`) and the search space, `n_iter`, `cv`, `scoring` and dataset that produced them.

Every `model_input_*.csv` that `build_dataset` writes has a manifest, `model_input_*.meta.json`: filters, period, rows, first/last timestamp, the CSV's sha256 and the git revision. A model card copies it, so the filters a model was trained with are never a guess.

**Predict.** `ml.predict` reads `ml_config.predict`. For the target station and period it loads the model trained on each listed station (the artifact of `ml_config.train`), predicts with the columns that model was trained on **selected by name**, builds each ensemble, and scores every model and ensemble against the observed target. Ensemble members are weighted by inverse distance to the target station (`idw`; with two members, `d2 / (d1 + d2)`) or equally (`equal`); the target station itself cannot be a member of an `idw` ensemble. It writes to the target station's results directory:

- `<model>_predict_st_<start>_et_<end>_<TARGET>_predictions.csv`: `timestamp`, `y_true`, one column per model and ensemble.
- `..._metrics.json`: the metrics of every column, the models used and their training periods, the ensemble weights, the dataset hash and the revision.

It only warns, never fails, about what makes a score hard to trust: a model evaluated on its own training days, training and evaluation datasets built with different filters, or a dataset or model without a manifest/card. Expected failures (a model that was not trained, a missing column, an empty period, no `ml_config.predict`) are logged with the fix and the command exits with code 1.

**Example: train on 2022, test on Ourinhos 2023.** `config/experiments/ourinhos-2023.json` trains one model per station on 2022 and evaluates the three models and a CPA+SMS inverse-distance ensemble on Ourinhos 2023:

```bash
export MASTERS_PIPELINE_CONFIG=config/experiments/ourinhos-2023.json
uv run python -m masters_project.etl.build_dataset
uv run python -m masters_project.ml.train
uv run python -m masters_project.ml.predict
uv run python -m masters_project.ml.plots
```

Do not run the GOES ETL with this config: its period covers 2023 for all three stations.

**Plots.** `ml.plots` reads what `ml.predict` wrote for the experiment in `ml_config.predict` and saves three PNG files next to it, in the target station's results directory (nothing is recomputed; the numbers are the ones `ml.predict` reported):

- `<model>_predict_<start>_<end>_timeseries.png`: observed against each model and ensemble, over the whole period (one panel each). A gap longer than 30 minutes, such as the night, is drawn as a break, not as a line across it.
- `..._window.png`: all of them on one panel for one week, by default the week with the most data.
- `..._metrics.png`: one bar chart per metric.

The figures are built with matplotlib's object API (no `pyplot`, no display), so the same functions serve a notebook: `notebooks/02_test_model.py` calls `plots.timeseries_figure`, `plots.window_figure` and `plots.metrics_figure` and displays the result; pass your own window to `window_figure` to look at another season. Like `ml.predict`, a missing output or config is logged with the fix and exits with code 1.

### Recommended Execution Workflow

1. Set the period and `etl_config.stations` in `pipeline.json` (only the stations you need for that period).
2. Run the **SONDA ETL**, then the **GOES ETL**. A whole year can run at once: files are read with bounded memory and each channel and day is cached as soon as it finishes, so an interrupted run resumes where it stopped. List every channel you want in `goes.channels`.
3. Run `build_dataset` with the **full span** (see the note below). It overwrites the station's `model_input_*.csv`, so keep a copy of any version you still need: the file name does not say which filters built it, but its manifest does.
4. Run `ml.train` (and `ml.tune` if you want to search hyperparameters). For an experiment that trains on one period and tests on another, put `ml_config.train` and `ml_config.predict` in a file under `config/experiments/` (see the example above) and point `MASTERS_PIPELINE_CONFIG` at it.
5. Run `ml.predict`, then `ml.plots`.

**Several periods (e.g. 2022 with three stations, then 2023 with one).** Collect each period with its own `start_date`/`end_date` and its own `etl_config.stations`. The GOES CSVs of every period in the station's directory are stacked by `build_dataset`, but the SONDA CSV is read **only for the configured span**, so rows of the other period would be silently dropped by the join. Before `build_dataset`, set `start_date`/`end_date` to the full span (e.g. `2022-01-01` to `2023-12-31`), run the SONDA ETL again (about a second per station-year) and then `build_dataset`. The GOES cache is reused for overlapping days, and a station with no GOES for a period simply contributes no rows for it.

## Outputs

Everything that belongs to one station lives in a directory named after it (lower-case station key), below the stage directory. File names keep the station and the period, so a copied file still says what it is. Under `data/` (git-ignored):

```
data/
├── raw/
│   ├── goes/<station>/      goes_<channel>_st_<start>_et_<end>_<STATION>.csv
│   └── sonda/<station>/     sonda_<type>_st_<start>_et_<end>_<STATION>.csv
├── cache/goes/              listing, pixel and per-day caches (per channel and day; safe to delete, rebuilt from S3)
├── processed/<station>/     model_input_st_<start>_et_<end>_<STATION>.csv and its manifest (.meta.json)
├── models/<station>/        <model>_st_<train period>_<STATION>_v1.joblib and its card (.json)
├── results/<station>/       ..._metrics.json, ..._tuning.json, <model>_predict_..._predictions.csv, ..._metrics.json, ..._*.png
├── logs/<pipeline>/         <YYYY-MM-DD_HHMMSS>.log, latest.log
└── samples/                 tests, samples and experiments, in the same tree (kept apart from the real runs;
                             models saved before the pipeline refactor are in an old format and cannot be loaded)
```

`raw/` and `processed/` hold only complete collection periods; anything that is a test or a sample (single months, January slices, models trained on them, notebook outputs) goes under `data/samples/`.

## Logs

Every pipeline run writes its own file, `data/logs/<pipeline>/<YYYY-MM-DD_HHMMSS>.log` (`etl_sonda`, `etl_goes`, `etl_build_dataset`, `ml_train`, `ml_tune`, `ml_predict`, `ml_plots`), and the same lines go to the console. `latest.log` points to the newest one: `tail -f data/logs/etl_goes/latest.log`.

A run starts with a header (command, git revision with `-dirty` if tracked files changed, Python/pandas/numpy versions, config file, and a summary of period, stations, channels and thresholds) and ends with a footer (`status=ok|failed|interrupted`, duration, exit code), so a log says what produced it. An unhandled exception is logged at `CRITICAL` with its traceback before the program stops. Line format: `2026-10-24T16:33:28-0300 | INFO | masters_project.etl.build_dataset | message`.

Levels: `INFO` marks the milestones of a stage; `DEBUG` is per-file detail and is written only when `etl_config.log_level` is `DEBUG`; `WARNING` is a recoverable data problem (a gap in the source, a defective sensor, dropped rows); `ERROR` is one unit (a station, a day, a file) that failed while the run goes on. A single file rotates at 10 MB, so a long `DEBUG` run cannot fill the disk. Old logs are not deleted automatically.

## Project Structure

```
masters_project/
├── config/pipeline.json        # development defaults
├── config/experiments/         # one file per experiment (period, training window, predict section)
├── notebooks/                  # 02_test_model.py: looks at a prediction run through ml.plots
├── src/masters_project/
│   ├── settings.py             # pydantic config and project paths
│   ├── logging_setup.py        # one log file per run, with header and footer
│   ├── file_paths.py           # canonical paths, one directory per station
│   ├── enums.py                # SondaStation, GoesChannel
│   ├── exporters.py            # CSV and JSON writers
│   ├── utils.py                # time_track decorator
│   ├── etl/
│   │   ├── sonda/              # client, processor, periods, pipeline
│   │   ├── goes/               # client, reader, worker (process pool), cache, pipeline
│   │   ├── merger.py           # stack/join time series without duplicating rows
│   │   ├── solar.py            # solar elevation
│   │   ├── filters.py          # daytime and frozen-sensor filters
│   │   ├── schemas.py          # pandera data contracts
│   │   └── build_dataset.py
│   └── ml/                     # models, artifacts (model cards), evaluate, data, ensemble, train, tune, predict, plots
└── tests/                      # mirrors src/
```

## Testing

```bash
uv run pytest                   # all tests
uv run pytest --cov             # with coverage; fails below 90% (currently ~100%)
uv run ruff check src tests     # lint
uv run ruff format src tests    # format
```

Tests never touch `data/`: `tests/conftest.py` points every path in `settings` (including the logs) at a temporary directory, and S3, the SONDA server and the GOES process pool are replaced by fakes.

## License

See repository for license details.
