"""Monthly SONDA ETL. Kept as an entry point; the logic lives in :mod:`masters_project.pipeline.etl_sonda`."""

import sys

from masters_project.pipeline.etl_sonda import main

if __name__ == "__main__":
    sys.exit(main("month"))
