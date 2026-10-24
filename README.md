# Masters Project

A data pipeline that predicts ground solar radiation from **GOES-R** satellite imagery (NOAA, on AWS S3) and the **SONDA** ground-station network (INPE). It collects both sources, builds one ML-ready table per station, and trains and tunes models on it.

## Overview

```
SONDA ZIPs ──► etl.sonda ──► data/raw/sonda/*.csv ─┐
                                                    ├─► etl.build_dataset ──► data/processed/model_input_*.csv ──► ml.train / ml.tune
GOES S3 ─────► etl.goes ───► data/raw/goes/*.csv ──┘
```

1. **SONDA ETL** (`etl.sonda`): downloads the SONDA ZIP archives (by year or year-month) of every configured station, extracts the `.dat` files and exports one CSV per station with the target (`glo_avg`, W/m²). Run it **first**: it reports periods SONDA does not publish, and the GOES step uses it to skip days with no target.
2. **GOES ETL** (`etl.goes`): reads GOES-R ABI NetCDF files from NOAA's Open Data Registry with partial range requests, extracts the pixel window of **all configured stations and channels** in one pass, caches it per day, and exports one CSV per channel and station.
3. **Build model input** (`etl.build_dataset`): stacks the GOES channels, filters the SONDA target (daytime and frozen-sensor filters), joins both on timestamp, drops missing values, validates the result with pandera and writes the table the ML stage reads.
4. **Training and tuning** (`ml.train`, `ml.tune`): fit and evaluate the selected model, or search its hyperparameters, for every station.

`etl/` and `ml/` never import each other. Their only contract is the `model_input_*.csv` file, whose structure `etl/schemas.py` checks before it is written.

## Requirements

- Python ≥ 3.14
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

## Installation

```bash
git clone <repository-url>
cd masters_project

uv sync                      # runtime + dev tools (pytest, ruff, pytest-cov)
uv sync --group notebooks    # also matplotlib, seaborn, geopy for the notebooks
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
| `ml_config` | `selected_model` (key in `models`), `test_size`, `evaluation_metrics`, `models` (per-model hyperparameters, e.g. `KNN`, `XGBoost`) |
| `tuning_config` | `n_iter`, `cv`, `scoring`, `n_jobs`, `verbose` |

To run with a different configuration without editing this file, set `MASTERS_PIPELINE_CONFIG=/path/to/other.json` (same structure).

There is no "selected station": GOES, SONDA, dataset building, training and tuning each process every station in `etl_config.stations` and write one output per station (the station name is part of the file name).

## Running the Pipeline

Use the same date range in `etl_config` for GOES, SONDA, and downstream steps.

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

### 4. Training and tuning (optional)

```bash
uv run python -m masters_project.ml.train
uv run python -m masters_project.ml.tune
```

Both fit one model per station on a chronological split (no shuffling, so the test set is the most recent data) and exit with code 1 if any station failed.

### Recommended Execution Workflow

1. Set the period and `etl_config.stations` in `pipeline.json` (only the stations you need for that period).
2. Run the **SONDA ETL**, then the **GOES ETL**. A whole year can run at once: files are read with bounded memory and each channel and day is cached as soon as it finishes, so an interrupted run resumes where it stopped. List every channel you want in `goes.channels`.
3. Run `build_dataset`, then training and tuning.

**Several periods (e.g. 2022 with three stations, then 2023 with one).** Collect each period with its own `start_date`/`end_date` and its own `etl_config.stations`. The GOES CSVs of every period in the station's directory are stacked by `build_dataset`, but the SONDA CSV is read **only for the configured span**, so rows of the other period would be silently dropped by the join. Before `build_dataset`, set `start_date`/`end_date` to the full span (e.g. `2022-01-01` to `2023-12-31`), run the SONDA ETL again (about a second per station-year) and then `build_dataset`. The GOES cache is reused for overlapping days, and a station with no GOES for a period simply contributes no rows for it.

## Outputs

Everything that belongs to one station lives in a directory named after it (lower-case station key), below the stage directory. File names keep the station and the period, so a copied file still says what it is. Under `data/` (git-ignored):

```
data/
├── raw/
│   ├── goes/<station>/      goes_<channel>_st_<start>_et_<end>_<STATION>.csv
│   └── sonda/<station>/     sonda_<type>_st_<start>_et_<end>_<STATION>.csv
├── cache/goes/              listing, pixel and per-day caches (per channel and day; safe to delete, rebuilt from S3)
├── processed/<station>/     model_input_st_<start>_et_<end>_<STATION>.csv
├── models/<station>/        <model>_st_..._<STATION>_v1.joblib
├── results/<station>/       <model>_st_..._<STATION>_v1_metrics.json, ..._tuning.json
├── logs/<pipeline>/         <YYYY-MM-DD_HHMMSS>.log, latest.log
└── samples/                 tests, samples and experiments, in the same tree (kept apart from the real runs)
```

`raw/` and `processed/` hold only complete collection periods; anything that is a test or a sample (single months, January slices, models trained on them, notebook outputs) goes under `data/samples/`.

## Logs

Every pipeline run writes its own file, `data/logs/<pipeline>/<YYYY-MM-DD_HHMMSS>.log` (`etl_sonda`, `etl_goes`, `etl_build_dataset`, `ml_train`, `ml_tune`), and the same lines go to the console. `latest.log` points to the newest one: `tail -f data/logs/etl_goes/latest.log`.

A run starts with a header (command, git revision with `-dirty` if tracked files changed, Python/pandas/numpy versions, config file, and a summary of period, stations, channels and thresholds) and ends with a footer (`status=ok|failed|interrupted`, duration, exit code), so a log says what produced it. An unhandled exception is logged at `CRITICAL` with its traceback before the program stops. Line format: `2026-10-24T16:33:28-0300 | INFO | masters_project.etl.build_dataset | message`.

Levels: `INFO` marks the milestones of a stage; `DEBUG` is per-file detail and is written only when `etl_config.log_level` is `DEBUG`; `WARNING` is a recoverable data problem (a gap in the source, a defective sensor, dropped rows); `ERROR` is one unit (a station, a day, a file) that failed while the run goes on. A single file rotates at 10 MB, so a long `DEBUG` run cannot fill the disk. Old logs are not deleted automatically.

## Project Structure

```
masters_project/
├── config/pipeline.json
├── notebooks/                  # exploratory analysis (needs --group notebooks)
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
│   └── ml/                     # models (scaler + estimator pipelines), evaluate, data, train, tune
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
