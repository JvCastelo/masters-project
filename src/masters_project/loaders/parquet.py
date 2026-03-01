from pathlib import Path

import pandas as pd

from masters_project.loaders.base import DataExporter


class ParquetExporter(DataExporter):
    """Exporter that writes DataFrames to Parquet files (no index)."""

    def _save_to_disk(self, df: pd.DataFrame, destination: Path) -> None:
        """Write the DataFrame to a Parquet file at destination.

        Args:
            df: DataFrame to serialize.
            destination: Output file path (e.g. .parquet).
        """
        df.to_parquet(destination, index=False)
