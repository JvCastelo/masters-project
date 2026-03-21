import json
from pathlib import Path
from typing import Any

from masters_project.loaders.base import DictExporter


class JSONExporter(DictExporter):
    """Serialize a dictionary to a UTF-8 JSON file (pretty-printed, 4-space indent)."""

    def _save_to_disk(self, data: dict[str, Any], destination: Path) -> None:
        """Write ``data`` as JSON to ``destination``.

        Args:
            data: Mapping to serialize (typically metrics or metadata).
            destination: Output file path (e.g. ``.json``).
        """
        with open(destination, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
