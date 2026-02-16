from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd


class DataExporter(ABC):
    @abstractmethod
    def export(self, df: pd.DataFrame, destination: Path):
        pass
