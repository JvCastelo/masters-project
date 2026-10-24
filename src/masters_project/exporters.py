"""Write pipeline outputs to disk: DataFrames as CSV and dictionaries as JSON.

Every exporter skips an empty payload with a warning and creates the missing parent
directories, so callers never have to guard against either.
"""

import json
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import pandas as pd

from masters_project.utils import time_track

logger = logging.getLogger(__name__)


class BaseExporter(ABC):
    """Template for writing one in-memory payload (DataFrame, dict) to a file.

    Subclasses only implement ``_save_to_disk``; skipping empty payloads, creating the
    parent directory, logging and re-raising errors are done here once.
    """

    @time_track
    def export(self, data: Any, destination: Path) -> None:
        """Write ``data`` to ``destination``; an empty payload is skipped with a warning.

        Args:
            data: DataFrame or mapping to serialize.
            destination: Output file path. Missing parent directories are created.

        Raises:
            Exception: Anything raised while writing is logged and re-raised.
        """
        if len(data) == 0:
            logger.warning(
                f"Attempted to export an empty payload to {destination.name}. Skipping."
            )
            return

        try:
            destination.parent.mkdir(parents=True, exist_ok=True)
            self._save_to_disk(data, destination)
            logger.info(f"Successfully exported {len(data)} items to {destination}")
        except Exception as e:
            logger.error(f"Error exporting to {destination}: {e}")
            raise

    @abstractmethod
    def _save_to_disk(self, data: Any, destination: Path) -> None:
        """Serialize ``data`` to ``destination`` (format-specific)."""


class CSVExporter(BaseExporter):
    """Write a DataFrame to a CSV file (no index)."""

    def _save_to_disk(self, data: pd.DataFrame, destination: Path) -> None:
        data.to_csv(destination, index=False)


class JSONExporter(BaseExporter):
    """Write a dictionary to a UTF-8 JSON file (pretty-printed, 4-space indent)."""

    def _save_to_disk(self, data: dict[str, Any], destination: Path) -> None:
        with open(destination, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
