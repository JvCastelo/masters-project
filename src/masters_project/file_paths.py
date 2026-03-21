from pathlib import Path

from masters_project.settings import settings


class FilePathBuilder:
    """Centralized registry for generating standardized file paths across the project."""

    @staticmethod
    def raw_goes(channel: str) -> Path:
        filename = (
            f"goes_{channel}_"
            f"st_{settings.execution.start_date}_"
            f"et_{settings.execution.end_date}_"
            f"{settings.execution.selected_station}.csv"
        )
        return settings.RAW_PATH / "goes" / filename

    @staticmethod
    def raw_sonda() -> Path:
        filename = (
            f"sonda_{settings.etl.sonda.data_type}_"
            f"st_{settings.execution.start_date}_"
            f"et_{settings.execution.end_date}_"
            f"{settings.execution.selected_station}.csv"
        )
        return settings.RAW_PATH / "sonda" / filename

    @staticmethod
    def model_input() -> Path:
        filename = (
            f"model_input_"
            f"st_{settings.execution.start_date}_"
            f"et_{settings.execution.end_date}_"
            f"{settings.execution.selected_station}.csv"
        )
        return settings.PROCESSED_PATH / filename

    @staticmethod
    def model_save(model_name: str, version: str = "v1") -> Path:
        filename = (
            f"{model_name.lower()}_"
            f"st_{settings.execution.start_date}_"
            f"et_{settings.execution.end_date}_"
            f"{settings.execution.selected_station}_{version}.joblib"
        )

        return settings.MODELS_PATH / filename

    @staticmethod
    def model_metrics(model_name: str, version: str = "v1") -> Path:
        filename = (
            f"{model_name.lower()}_"
            f"st_{settings.execution.start_date}_"
            f"et_{settings.execution.end_date}_"
            f"{settings.execution.selected_station}_{version}_metrics.json"
        )

        return settings.RESULTS_PATH / filename

    @staticmethod
    def model_tuning(model_name: str, version: str = "v1") -> Path:
        filename = (
            f"{model_name.lower()}_"
            f"st_{settings.execution.start_date}_"
            f"et_{settings.execution.end_date}_"
            f"{settings.execution.selected_station}_{version}_tuning.json"
        )

        return settings.RESULTS_PATH / filename


file_paths = FilePathBuilder()
