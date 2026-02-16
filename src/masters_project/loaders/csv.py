import logging
from pathlib import Path

import pandas as pd

from masters_project.loaders.base import DataExporter

logger = logging.getLogger(__name__)


class CSVExporter(DataExporter):
    def export(self, df: pd.DataFrame, destination: Path) -> None:

        df.to_csv(destination, index=False)

        logger.info(f"Exported to CSV: {destination}")
