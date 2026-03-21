from pathlib import Path

from masters_project.settings import settings


class FilePathBuilder:
    """Build canonical paths for raw ETL outputs, model inputs, and ML artifacts.

    Filenames embed execution dates and selected station from ``settings`` so runs are
    traceable and non-colliding across configurations.
    """

    @staticmethod
    def raw_goes(channel: str) -> Path:
        """Path to the GOES CSV for ``channel`` under ``data/raw/goes``.

        Args:
            channel: ABI channel id (e.g. ``\"C01\"``).

        Returns:
            Full path: ``goes_{channel}_st_{start}_et_{end}_{station}.csv``.
        """
        filename = (
            f"goes_{channel}_"
            f"st_{settings.execution.start_date}_"
            f"et_{settings.execution.end_date}_"
            f"{settings.execution.selected_station}.csv"
        )
        return settings.RAW_PATH / "goes" / filename

    @staticmethod
    def raw_sonda() -> Path:
        """Path to the SONDA CSV under ``data/raw/sonda``.

        Returns:
            Full path: ``sonda_{data_type}_st_{start}_et_{end}_{station}.csv``.
        """
        filename = (
            f"sonda_{settings.etl.sonda.data_type}_"
            f"st_{settings.execution.start_date}_"
            f"et_{settings.execution.end_date}_"
            f"{settings.execution.selected_station}.csv"
        )
        return settings.RAW_PATH / "sonda" / filename

    @staticmethod
    def model_input() -> Path:
        """Path to the merged feature table used as ML input under ``data/processed``.

        Returns:
            Full path: ``model_input_st_{start}_et_{end}_{station}.csv``.
        """
        filename = (
            f"model_input_"
            f"st_{settings.execution.start_date}_"
            f"et_{settings.execution.end_date}_"
            f"{settings.execution.selected_station}.csv"
        )
        return settings.PROCESSED_PATH / filename

    @staticmethod
    def model_save(model_name: str, version: str = "v1") -> Path:
        """Path for a trained model bundle (joblib) under ``data/models``.

        Args:
            model_name: Model identifier (e.g. ``\"KNN\"``); stored lowercased in filename.
            version: Version tag suffix (default ``\"v1\"``).

        Returns:
            Path ending in ``.joblib``.
        """
        filename = (
            f"{model_name.lower()}_"
            f"st_{settings.execution.start_date}_"
            f"et_{settings.execution.end_date}_"
            f"{settings.execution.selected_station}_{version}.joblib"
        )

        return settings.MODELS_PATH / filename

    @staticmethod
    def model_metrics(model_name: str, version: str = "v1") -> Path:
        """Path for evaluation metrics JSON under ``data/results``.

        Args:
            model_name: Model identifier; stored lowercased in filename.
            version: Version tag suffix (default ``\"v1\"``).

        Returns:
            Path ending in ``_metrics.json``.
        """
        filename = (
            f"{model_name.lower()}_"
            f"st_{settings.execution.start_date}_"
            f"et_{settings.execution.end_date}_"
            f"{settings.execution.selected_station}_{version}_metrics.json"
        )

        return settings.RESULTS_PATH / filename

    @staticmethod
    def model_tuning(model_name: str, version: str = "v1") -> Path:
        """Path for hyperparameter tuning results JSON under ``data/results``.

        Args:
            model_name: Model identifier; stored lowercased in filename.
            version: Version tag suffix (default ``\"v1\"``).

        Returns:
            Path ending in ``_tuning.json``.
        """
        filename = (
            f"{model_name.lower()}_"
            f"st_{settings.execution.start_date}_"
            f"et_{settings.execution.end_date}_"
            f"{settings.execution.selected_station}_{version}_tuning.json"
        )

        return settings.RESULTS_PATH / filename


file_paths = FilePathBuilder()
