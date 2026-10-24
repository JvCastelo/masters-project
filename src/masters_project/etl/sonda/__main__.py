"""Entry point: ``python -m masters_project.etl.sonda [--by year|month]``."""

import argparse
import sys

from masters_project.etl.sonda.pipeline import main

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="SONDA ETL: download the ZIPs and export one CSV per station."
    )
    parser.add_argument(
        "--by",
        choices=["year", "month"],
        default="year",
        help="ZIP granularity to download (default: year, which needs fewer requests)",
    )
    sys.exit(main(parser.parse_args().by))
