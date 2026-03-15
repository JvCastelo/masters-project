# Masters Project

A data pipeline for building ML-ready features by combining **GOES-R** satellite imagery (from AWS S3) with **SONDA** ground-based solar radiation measurements (from INPE's Brazilian network).

## Overview

The pipeline:

1. **GOES ETL** — Downloads GOES-R ABI NetCDF files from NOAA's Open Data Registry (S3), extracts pixel windows around a target station, and exports per-channel CSVs.
2. **SONDA ETL** — Downloads SONDA ZIP archives by year or year-month, extracts `.dat` files, and exports formatted CSVs with timestamps and radiation variables.
3. **Build Features** — Merges GOES and SONDA datasets on timestamp, drops rows with NaNs, and exports the final feature table for modeling.

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

Edit `config/pipeline.json` before running the pipeline:

| Field | Description |
|-------|-------------|
| `general.start_date` | Start date (YYYY-MM-DD) |
| `general.end_date` | End date (YYYY-MM-DD) |
| `general.pixel_radius` | Half-width of the pixel window around the station |
| `general.sonda_data_type` | SONDA product type (e.g. `solarimetricos`) |
| `general.goes_product_name` | GOES product (e.g. `ABI-L1b-RadF`) |
| `general.goes_bucket_name` | S3 bucket (e.g. `noaa-goes16`) |
| `general.goes_variable` | Variable to extract (e.g. `Rad`) |
| `general.max_workers` | Thread pool size for GOES downloads |
| `stations` | List of `{name, latitude, longitude}` |
| `active_station` | Station name to use from the list |

## Running the Pipeline

To ensure the final feature set is consistent, follow this step-by-step execution sequence:

### 1. Collect GOES Satellite Data

Download and process satellite imagery for your desired spectral channels. Ensure the same date range is used for every channel to allow for proper temporal joining.

**Note:** Because this process is computationally intensive and time-consuming, it is recommended to run the collection in smaller chronological batches to eventually cover your full target period.

* **Action:** Set general.start_date and general.end_date in config/pipeline.json.
* **Execution:** 

```bash
# 1. GOES satellite data (by channel, date range)
uv run python -m masters_project.pipeline.etl_goes_s3
```

### 2. Collect SONDA Ground Data

Fetch the ground-truth radiation measurements for the target station. Ensure the date range matches the total period defined during the GOES data collection (e.g., from 2022-01-01 to 2022-03-31) to ensure the datasets can be synchronized.

* **Action:** Ensure the SONDA period in `config/pipeline.json` matches your GOES period.
* **Execution:**

```bash
# Yearly downloads
uv run python -m masters_project.pipeline.etl_sonda_by_year

# OR Monthly downloads
uv run python -m masters_project.pipeline.etl_sonda_by_year_month
```

### 3. Build Final Features

Once data is collected, trigger the multi-stage merge to align all channels and ground data into a single ML-ready table.

``` bash
uv run python -m masters_project.pipeline.build_features
```

Outputs are written under `data/`:

- `data/raw/goes/` — GOES per-channel CSVs
- `data/raw/sonda/` — SONDA CSVs
- `data/processed/` — Final merged feature CSV
- `data/logs/` — Pipeline logs

## Project Structure

```
masters_project/
├── config/
│   └── pipeline.json          # Pipeline configuration
├── src/masters_project/
│   ├── clients/               # Data sources
│   │   ├── goes_s3.py         # GOES S3 client
│   │   └── sonda.py           # SONDA HTTP client
│   ├── processors/            # Data processing
│   │   ├── goes.py            # GOES NetCDF → DataFrame
│   │   ├── merger.py          # GOES + SONDA merge
│   │   └── sonda.py           # SONDA ZIP → DataFrame
│   ├── loaders/               # Export (CSV, Parquet)
│   ├── pipeline/              # ETL scripts
│   ├── enums.py               # Station and channel enums
│   ├── settings.py            # Config loader
│   └── utils.py               # Date helpers, decorators
└── tests/
```

## Testing

```bash
uv run pytest tests/ -v
```

## License

See repository for license details.
