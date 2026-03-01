from pathlib import Path

import pandas as pd

from masters_project.loaders.base import DataExporter


class CSVExporter(DataExporter):
    """Exporter that writes DataFrames to CSV files (no index)."""

    def _save_to_disk(self, df: pd.DataFrame, destination: Path) -> None:
        """Write the DataFrame to a CSV file at destination.

        Args:
            df: DataFrame to serialize.
            destination: Output file path (e.g. .csv).
        """
        df.to_csv(destination, index=False)
