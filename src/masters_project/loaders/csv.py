import logging
from pathlib import Path

import pandas as pd

from masters_project.loaders.base import DataExporter
from masters_project.utils import measure_memory, time_track

logger = logging.getLogger(__name__)


class CSVExporter(DataExporter):
    @measure_memory
    @time_track
    def export(self, df: pd.DataFrame, destination: Path) -> None:

        df.to_csv(destination, index=False)

        logger.info(f"Exported to CSV: {destination}")
