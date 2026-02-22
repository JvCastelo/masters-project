import logging
from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd

from masters_project.utils import measure_memory, time_track

logger = logging.getLogger(__name__)


class DataExporter(ABC):
    @measure_memory
    @time_track
    def export(self, df: pd.DataFrame, destination: Path) -> None:

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
        """
        Must be implemented by subclasses.
        """
        pass
