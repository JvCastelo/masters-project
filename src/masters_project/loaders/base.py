import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import pandas as pd

from masters_project.utils import measure_memory, time_track

logger = logging.getLogger(__name__)


class BaseExporter(ABC):
    """Abstract base for exporting in-memory data to a file on disk.

    Subclasses narrow ``data`` to a concrete type (DataFrame, dict, etc.). ``export``
    uses ``Any`` here because each subclass accepts a different payload type.
    """

    @abstractmethod
    def export(self, data: Any, destination: Path) -> None:
        """Write ``data`` to ``destination`` using subclass-specific serialization."""
        pass


class DataFrameExporter(BaseExporter):
    """Base class for tabular exports (CSV, Parquet, etc.)."""

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
        """Serialize ``df`` to ``destination`` (format-specific)."""
        pass


class DictExporter(BaseExporter):
    """Base class for serializing mapping payloads (JSON, YAML, etc.)."""

    @time_track
    def export(self, data: dict[str, Any], destination: Path) -> None:
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
    def _save_to_disk(self, data: dict[str, Any], destination: Path) -> None:
        """Write ``data`` to ``destination`` (format-specific)."""
        pass
