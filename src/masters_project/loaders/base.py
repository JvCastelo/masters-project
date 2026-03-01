import logging
from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd

from masters_project.utils import measure_memory, time_track

logger = logging.getLogger(__name__)


class DataExporter(ABC):
    """Abstract base for exporting DataFrames to disk (CSV, Parquet, etc.). Handles mkdir and logging."""

    @measure_memory
    @time_track
    def export(self, df: pd.DataFrame, destination: Path) -> None:
        """Ensure parent dir exists, delegate to _save_to_disk, and log result. Skips if df is empty.

        Args:
            df: DataFrame to export.
            destination: Full path for the output file.

        Raises:
            OSError: On filesystem errors when creating directories or writing.
            Exception: Re-raises any exception from _save_to_disk after logging.
        """
        if df.empty:
            logger.warning(
                f"Attempted to export an empty DataFrame to {destination.name}. Skipping."
            )
            return

        logger.debug(f"Preparing to export data to {destination}...")

        try:
            destination.parent.mkdir(parents=True, exist_ok=True)

            self._save_to_disk(df, destination)

            logger.info(
                f"Successfully exported {df.shape[0]} rows and {df.shape[1]} columns to {destination}"
            )

        except OSError as e:
            logger.error(f"OS Error while writing to {destination}: {e}")
            raise
        except Exception:
            logger.exception(f"Unexpected error while exporting data to {destination}")
            raise

    @abstractmethod
    def _save_to_disk(self, df: pd.DataFrame, destination: Path) -> None:
        """Write the DataFrame to the given path. Implementation is format-specific (e.g. CSV, Parquet).

        Args:
            df: DataFrame to write.
            destination: Full output file path.
        """
        pass
