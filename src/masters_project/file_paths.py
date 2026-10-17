from pathlib import Path

from masters_project.settings import settings


class FilePathBuilder:
    """Build canonical paths for raw ETL outputs, model inputs, and ML artifacts.

    Filenames embed the period from ``settings`` and the station passed in, so runs are
    traceable and non-colliding across configurations.
    """

    @staticmethod
    def raw_goes(channel: str, station: str) -> Path:
        """Path to the GOES CSV for ``channel`` under ``data/raw/goes``.

        Args:
            channel: ABI channel id (e.g. ``\"C01\"``).
            station: Station key (a key of ``etl_config.stations``).

        Returns:
            Full path: ``goes_{channel}_st_{start}_et_{end}_{station}.csv``.
        """
        filename = (
            f"goes_{channel}_"
            f"st_{settings.etl.start_date}_"
            f"et_{settings.etl.end_date}_"
            f"{station}.csv"
        )
        return settings.RAW_PATH / "goes" / filename

    @staticmethod
    def goes_cache_day(
        product: str,
        variable: str,
        radius: int,
        channel: str,
        year: int,
        day_of_year: int,
    ) -> Path:
        """Path to the per-day GOES extraction cache under ``data/cache/goes``.

        The cache is keyed by everything that changes the extracted values (product,
        variable, window radius, channel) and not by the execution dates or station,
        so overlapping runs and new stations reuse it.

        Returns:
            Full path: ``goes/{product}/{variable}_r{radius}/{channel}/{year}/{doy}.csv``.
        """
        return (
            settings.CACHE_PATH
            / "goes"
            / product
            / f"{variable}_r{radius}"
            / channel
            / str(year)
            / f"{day_of_year:03d}.csv"
        )

    @staticmethod
    def goes_listing_dir(bucket: str, product: str) -> Path:
        """Directory of the per-day S3 listing caches (``{year}/{doy}.json`` inside it)."""
        return settings.CACHE_PATH / "goes" / "listing" / bucket / product

    @staticmethod
    def goes_pixels(bucket: str, product: str, channel: str) -> Path:
        """Path of the cached target pixels (i, j) of every station for one channel's grid."""
        return (
            settings.CACHE_PATH
            / "goes"
            / "pixels"
            / bucket
            / product
            / f"{channel}.json"
        )

    @staticmethod
    def raw_sonda(station: str) -> Path:
        """Path to the SONDA CSV under ``data/raw/sonda``.

        Args:
            station: Station key (a key of ``etl_config.stations``).

        Returns:
            Full path: ``sonda_{data_type}_st_{start}_et_{end}_{station}.csv``.
        """
        filename = (
            f"sonda_{settings.etl.sonda.data_type}_"
            f"st_{settings.etl.start_date}_"
            f"et_{settings.etl.end_date}_"
            f"{station}.csv"
        )
        return settings.RAW_PATH / "sonda" / filename

    @staticmethod
    def model_input(station: str) -> Path:
        """Path to the merged feature table used as ML input under ``data/processed``.

        Args:
            station: Station key (a key of ``etl_config.stations``).

        Returns:
            Full path: ``model_input_st_{start}_et_{end}_{station}.csv``.
        """
        filename = (
            f"model_input_"
            f"st_{settings.etl.start_date}_"
            f"et_{settings.etl.end_date}_"
            f"{station}.csv"
        )
        return settings.PROCESSED_PATH / filename

    @staticmethod
    def model_save(station: str, model_name: str, version: str = "v1") -> Path:
        """Path for a trained model bundle (joblib) under ``data/models``.

        Args:
            station: Station key (a key of ``etl_config.stations``).
            model_name: Model identifier (e.g. ``\"KNN\"``); stored lowercased in filename.
            version: Version tag suffix (default ``\"v1\"``).

        Returns:
            Path ending in ``.joblib``.
        """
        filename = (
            f"{model_name.lower()}_"
            f"st_{settings.etl.start_date}_"
            f"et_{settings.etl.end_date}_"
            f"{station}_{version}.joblib"
        )

        return settings.MODELS_PATH / filename

    @staticmethod
    def model_metrics(station: str, model_name: str, version: str = "v1") -> Path:
        """Path for evaluation metrics JSON under ``data/results``.

        Args:
            station: Station key (a key of ``etl_config.stations``).
            model_name: Model identifier; stored lowercased in filename.
            version: Version tag suffix (default ``\"v1\"``).

        Returns:
            Path ending in ``_metrics.json``.
        """
        filename = (
            f"{model_name.lower()}_"
            f"st_{settings.etl.start_date}_"
            f"et_{settings.etl.end_date}_"
            f"{station}_{version}_metrics.json"
        )

        return settings.RESULTS_PATH / filename

    @staticmethod
    def model_tuning(station: str, model_name: str, version: str = "v1") -> Path:
        """Path for hyperparameter tuning results JSON under ``data/results``.

        Args:
            station: Station key (a key of ``etl_config.stations``).
            model_name: Model identifier; stored lowercased in filename.
            version: Version tag suffix (default ``\"v1\"``).

        Returns:
            Path ending in ``_tuning.json``.
        """
        filename = (
            f"{model_name.lower()}_"
            f"st_{settings.etl.start_date}_"
            f"et_{settings.etl.end_date}_"
            f"{station}_{version}_tuning.json"
        )

        return settings.RESULTS_PATH / filename


file_paths = FilePathBuilder()
