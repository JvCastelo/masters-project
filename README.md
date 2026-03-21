# Masters Project

A data pipeline for building ML-ready features by combining **GOES-R** satellite imagery (from AWS S3) with **SONDA** ground-based solar radiation measurements (from INPE's Brazilian network).

## Overview

The pipeline:

1. **GOES ETL** — Downloads GOES-R ABI NetCDF files from NOAA's Open Data Registry (S3), extracts pixel windows around a target station, and exports per-channel CSVs.
2. **SONDA ETL** — Downloads SONDA ZIP archives by year or year-month, extracts `.dat` files, and exports formatted CSVs with timestamps and radiation variables.
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

| Section | Description |
|---------|-------------|
| `execution` | `start_date`, `end_date` (YYYY-MM-DD), `selected_station` (key in `stations`), `selected_model` (key in `ml_config.models`), `max_workers`, `log_level` |
| `etl_config.goes` | `bucket_name`, `product_name`, `variable`, `selected_channel`, `pixel_radius` |
| `etl_config.sonda` | `data_type`, `target_variable` |
| `ml_config` | `test_size`, `evaluation_metrics`, `models` (per-model hyperparameters, e.g. `KNN`, `XGBoost`) |
| `tuning_config` | `n_iter`, `cv`, `scoring`, `n_jobs`, `verbose` |
| `stations` | Object mapping station name to `{ "latitude", "longitude" }` |

## Running the Pipeline

Use the same date range in `execution` for GOES, SONDA, and downstream steps.

### 1. GOES satellite data

```bash
uv run python -m masters_project.pipeline.etl_goes_s3
```

### 2. SONDA ground data

```bash
# Yearly ZIPs
uv run python -m masters_project.pipeline.etl_sonda_by_year

# Or monthly ZIPs
uv run python -m masters_project.pipeline.etl_sonda_by_year_month
```

### 3. Build merged model input

```bash
uv run python -m masters_project.pipeline.build_dataset
```

### 4. Training and tuning (optional)

```bash
uv run python -m masters_project.pipeline.run_training
uv run python -m masters_project.pipeline.run_tuning
```

## Outputs

Under `data/`:

- `data/raw/goes/` — GOES per-channel CSVs
- `data/raw/sonda/` — SONDA CSV
- `data/processed/` — Merged model input CSV (`model_input_*.csv`)
- `data/models/` — Saved joblib models
- `data/results/` — Metrics and tuning JSON reports
- `data/logs/` — Pipeline logs

## Project Structure

```
masters_project/
├── config/
│   └── pipeline.json
├── src/masters_project/
│   ├── clients/          # GOES S3, SONDA HTTP
│   ├── processors/       # GOES, SONDA, merger
│   ├── loaders/          # CSV, Parquet, JSON exporters
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
