from pathlib import Path

import pandas as pd

from masters_project.loaders.base import DataExporter


class ParquetExporter(DataExporter):
    def _save_to_disk(self, df: pd.DataFrame, destination: Path) -> None:
        df.to_parquet(destination, index=False)
