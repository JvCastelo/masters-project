import json
from pathlib import Path

from masters_project.loaders.base import DictExporter


class JSONExporter(DictExporter):
    def _save_to_disk(self, data: dict, destination: Path) -> None:
        with open(destination, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
