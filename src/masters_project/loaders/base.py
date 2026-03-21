import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import pandas as pd

from masters_project.utils import measure_memory, time_track

logger = logging.getLogger(__name__)


class BaseExporter(ABC):
    """The absolute base class for exporting ANY type of data to disk."""

    @abstractmethod
    def export(self, data: Any, destination: Path) -> None:
        """All child classes MUST implement an export method."""
        pass


class DataFrameExporter(BaseExporter):
    """Base class strictly for tabular Pandas data (CSV, Parquet, etc.)."""

    @measure_memory
    @time_track
    def export(self, data: pd.DataFrame, destination: Path) -> None:
        if data.empty:
            logger.warning(
                f"Attempted to export an empty DataFrame to {destination.name}. Skipping."
            )
            return

        logger.debug(f"Preparing to export data to {destination}...")
        try:
            destination.parent.mkdir(parents=True, exist_ok=True)
            self._save_to_disk(data, destination)
            logger.info(f"Successfully exported {data.shape[0]} rows to {destination}")
        except Exception as e:
            logger.error(f"Error exporting DataFrame: {e}")
            raise

    @abstractmethod
    def _save_to_disk(self, df: pd.DataFrame, destination: Path) -> None:
        pass


class DictExporter(BaseExporter):
    """Base class strictly for saving Python dictionaries (JSON, YAML, etc.)."""

    @time_track
    def export(self, data: dict, destination: Path) -> None:
        if not data:
            logger.warning(
                f"Attempted to export an empty dictionary to {destination.name}. Skipping."
            )
            return

        logger.debug(f"Preparing to export dictionary to {destination}...")
        try:
            destination.parent.mkdir(parents=True, exist_ok=True)
            self._save_to_disk(data, destination)
            logger.info(f"Successfully exported dictionary to {destination}")
        except Exception as e:
            logger.error(f"Error exporting Dictionary: {e}")
            raise

    @abstractmethod
    def _save_to_disk(self, data: dict, destination: Path) -> None:
        pass
