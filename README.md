# Masters Project

A data pipeline for building ML-ready features by combining **GOES-R** satellite imagery (from AWS S3) with **SONDA** ground-based solar radiation measurements (from INPE's Brazilian network).

## Overview

The pipeline:

1. **SONDA ETL** — Downloads SONDA ZIP archives (by year or year-month) for every configured station, extracts the `.dat` files, and exports a formatted CSV per station. Run it **first**: it reports periods SONDA does not publish, and the GOES step uses it to skip days with no target.
2. **GOES ETL** — Reads GOES-R ABI NetCDF files from NOAA's Open Data Registry (S3) with partial range requests, extracts pixel windows for **all configured stations and channels** in one pass, caches them per file/day, and exports per-channel, per-station CSVs.
3. **Build model input** — Merges GOES channels and SONDA on timestamp, drops rows with NaNs, and exports the processed CSV for ML.
4. **Training / tuning** (optional) — Train or tune models on the merged dataset; metrics and artifacts are written under `data/`.

## Requirements

- Python ≥ 3.14
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd masters_project

# Install with uv
uv sync

# Or with pip
pip install -e .
```

## Configuration

Edit `config/pipeline.json` before running the pipeline.

`pipeline.json` has three top-level sections:

| Section | Description |
|---------|-------------|
| `etl_config` | `start_date`, `end_date` (YYYY-MM-DD), `max_workers` (GOES worker **processes**; 24 was the best measured on a 12-core machine, 36 was already slower), `log_level` |
| `etl_config.stations` | Object mapping station name to `{ "latitude", "longitude" }`. Every pipeline step runs for **all** stations listed here, so keep only the stations you want for the period (e.g. only `OURINHOS` for 2023) |
| `etl_config.goes` | `bucket_name`, `product_name`, `variable`, `channels` (list, e.g. `["C01", "C03"]`; all are collected in one run), `pixel_radius`, `skip_days_without_target` (default `true`: collect only days on which some station has a SONDA target; needs the SONDA ETL to have run) |
| `etl_config.sonda` | `data_type`, `target_variable` |
| `ml_config` | `selected_model` (key in `models`), `test_size`, `evaluation_metrics`, `models` (per-model hyperparameters, e.g. `KNN`, `XGBoost`) |
| `tuning_config` | `n_iter`, `cv`, `scoring`, `n_jobs`, `verbose` |

To run with a different configuration without editing this file, set `MASTERS_PIPELINE_CONFIG=/path/to/other.json` (same structure).

There is no "selected station": GOES, SONDA, dataset building, training and tuning each process every station in `etl_config.stations` and write one output per station (the station name is part of the file name).

## Running the Pipeline

Use the same date range in `etl_config` for GOES, SONDA, and downstream steps.

### 1. SONDA ground data (run first)

```bash
# Yearly ZIPs (default, fewer requests: about 1 s per station-year)
uv run python -m masters_project.pipeline.etl_sonda

# Or monthly ZIPs
uv run python -m masters_project.pipeline.etl_sonda --by month
```

`etl_sonda_by_year` and `etl_sonda_by_year_month` are kept as shortcuts for `--by year` / `--by month`.

- One CSV per station. The log shows how many days have a valid target (e.g. `Days with a valid 'glo_avg': 196/365`).
- SONDA has real gaps (e.g. Ourinhos had no data from 2023-01-01 to 2023-01-22). A period that SONDA does not publish (HTTP 404) is reported as an error, the CSV is still written with the rest, and the command **exits with code 1**.

### 2. GOES satellite data

```bash
uv run python -m masters_project.pipeline.etl_goes_s3
```

How it works:

- **Only days with a target.** With `skip_days_without_target` the GOES days are limited to those on which at least one configured station has a valid SONDA value (those days would be dropped by the final join anyway). If a station's SONDA CSV is missing, every day is collected and a warning says so.
- **All stations at once.** Each run extracts the pixel window of *every* station in `etl_config.stations` and writes one CSV per station. Reading other stations' pixels from a file that is already open is almost free, so the S3 cost is paid once per channel and period. Repeating the run (e.g. after an interruption, or for an overlapping period) is served from the cache.
- **All channels in one run.** `etl_config.goes.channels` lists the channels to collect. Files are listed with one S3 call per day for all channels (instead of one per channel and day), days are listed in parallel, and listings of days that are already closed are cached in `data/cache/goes/listing/` (the NOAA archive does not change). The target pixels of each channel are cached in `data/cache/goes/pixels/`, so no file is opened just to locate stations.
- **Per-day cache.** Results are stored in `data/cache/goes/{product}/{variable}_r{radius}/{channel}/{year}/{day}.csv`, one row per file and station, together with the target pixel. A day is saved once all its files finish, so an interrupted run only redoes the day in progress. Files that fail are retried on the next run. The cache does not depend on the dates, so overlapping periods reuse it.
- **Cache invalidation.** A cached file is skipped only if every station in `etl_config.stations` is present with the same target pixel. Adding a station to `etl_config.stations` (or changing its coordinates) makes the affected files be read again. Delete the cache folder to force a full re-read.
- **Partial reads and processes.** Only the needed `Rad` chunks are read, straight from the HDF5 file with `h5py` (no xarray) and a 512 KB block cache, so under 10% of each Full Disk file is transferred. Values are decoded exactly as xarray does (verified by a test). Files are processed in a process pool, because `h5py` holds a global lock that prevents threads from scaling.

### 3. Build merged model input

```bash
uv run python -m masters_project.pipeline.build_dataset
```

### 4. Training and tuning (optional)

```bash
uv run python -m masters_project.pipeline.run_training
uv run python -m masters_project.pipeline.run_tuning
```

### Recommended Execution Workflow

1. Set the period and `etl_config.stations` in `pipeline.json` (only the stations you need for that period).
2. Run the **SONDA ETL**, then the **GOES ETL**. A whole year can run at once: files are read with bounded memory and each channel and day is cached as soon as it finishes, so an interrupted run resumes where it stopped. List every channel you want in `goes.channels`.
3. Run `build_dataset`, then training and tuning.

**Several periods (e.g. 2022 with three stations, then 2023 with one).** Collect each period with its own `start_date`/`end_date` and its own `etl_config.stations`. The GOES CSVs of every period are stacked by file name in `build_dataset`, but the SONDA CSV is read **only for the configured span**, so rows of the other period would be silently dropped by the join. Before `build_dataset`, set `start_date`/`end_date` to the full span (e.g. `2022-01-01` to `2023-12-31`), run the SONDA ETL again (about a second per station-year) and then `build_dataset`. The GOES cache is reused for overlapping days, and a station with no GOES for a period simply contributes no rows for it.

## Outputs

Under `data/`:

- `data/raw/goes/` — GOES per-channel, per-station CSVs
- `data/cache/goes/` — GOES per-day extraction cache (safe to delete; it is rebuilt from S3)
- `data/raw/sonda/` — SONDA CSV per station
- `data/processed/` — Merged model input CSV per station (`model_input_*_{STATION}.csv`)
- `data/models/` — Saved joblib models (one per station)
- `data/results/` — Metrics and tuning JSON reports (one per station)
- `data/logs/` — Pipeline logs (rotated at 10 MB, 5 backups; INFO level unless `log_level` is `DEBUG`)

## Project Structure

```
masters_project/
├── config/
│   └── pipeline.json
├── src/masters_project/
│   ├── clients/          # GOES S3, SONDA HTTP
│   ├── processors/       # GOES (+ process-pool worker), SONDA, merger (drops repeated timestamps)
│   ├── loaders/          # CSV, Parquet, JSON exporters, GOES per-day cache
│   ├── models/           # ML base, factory, evaluate, tuning
│   ├── pipeline/         # ETL, build_dataset, training, tuning
│   ├── file_paths.py
│   ├── settings.py
│   └── utils.py
└── tests/
```

## Testing

```bash
uv run pytest tests/ -v
```

## License

See repository for license details.
