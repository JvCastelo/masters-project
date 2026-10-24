"""Canonical paths of every file the pipeline reads or writes: raw, cache, processed, models, results.

Everything that belongs to one station lives in a directory named after it, below the stage
directory: ``data/raw/goes/ourinhos/``, ``data/processed/ourinhos/``, ``data/models/ourinhos/``...
File names keep the station and the period, so a copied file still says what it is. The GOES
caches are per channel and day, not per station, and have their own tree under ``data/cache``.
"""

from pathlib import Path

from masters_project.settings import settings


class FilePathBuilder:
    """Build canonical paths for raw ETL outputs, model inputs, and ML artifacts.

    Filenames embed the period from ``settings`` and the station passed in, so runs are
    traceable and non-colliding across configurations.
    """

    @staticmethod
    def station_dir(station: str) -> str:
        """Directory name of a station: its key in lower case (``OURINHOS`` -> ``ourinhos``)."""
        return station.lower()

    @staticmethod
    def raw_goes_dir(station: str, name: str | None = None) -> Path:
        """Directory under ``data/raw/goes`` that holds a station's GOES CSVs.

        Args:
            station: Station key (a key of ``etl_config.stations``).
            name: Directory name to use instead of the station's own, relative to
                ``data/raw/goes`` (for example a directory with another set of CSVs).

        Returns:
            ``data/raw/goes/<station>`` (or ``data/raw/goes/<name>``).
        """
        return (
            settings.RAW_PATH / "goes" / (name or FilePathBuilder.station_dir(station))
        )

    @staticmethod
    def raw_goes(channel: str, station: str) -> Path:
        """Path to the GOES CSV for ``channel`` under the station's ``data/raw/goes`` directory.

        Args:
            channel: ABI channel id (e.g. ``\"C01\"``).
            station: Station key (a key of ``etl_config.stations``).

        Returns:
            Full path: ``raw/goes/<station>/goes_{channel}_st_{start}_et_{end}_{station}.csv``.
        """
        filename = (
            f"goes_{channel}_"
            f"st_{settings.etl.start_date}_"
            f"et_{settings.etl.end_date}_"
            f"{station}.csv"
        )
        return FilePathBuilder.raw_goes_dir(station) / filename

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
    def raw_sonda_dir(station: str) -> Path:
        """Directory under ``data/raw/sonda`` that holds a station's SONDA files."""
        return settings.RAW_PATH / "sonda" / FilePathBuilder.station_dir(station)

    @staticmethod
    def raw_sonda(station: str) -> Path:
        """Path to the SONDA CSV under the station's ``data/raw/sonda`` directory.

        Args:
            station: Station key (a key of ``etl_config.stations``).

        Returns:
            Full path: ``raw/sonda/<station>/sonda_{data_type}_st_{start}_et_{end}_{station}.csv``.
        """
        filename = (
            f"sonda_{settings.etl.sonda.data_type}_"
            f"st_{settings.etl.start_date}_"
            f"et_{settings.etl.end_date}_"
            f"{station}.csv"
        )
        return FilePathBuilder.raw_sonda_dir(station) / filename

    @staticmethod
    def model_input(station: str) -> Path:
        """Path to the merged feature table used as ML input, in the station's processed directory.

        Args:
            station: Station key (a key of ``etl_config.stations``).

        Returns:
            Full path: ``processed/<station>/model_input_st_{start}_et_{end}_{station}.csv``.
        """
        filename = (
            f"model_input_"
            f"st_{settings.etl.start_date}_"
            f"et_{settings.etl.end_date}_"
            f"{station}.csv"
        )
        return settings.PROCESSED_PATH / FilePathBuilder.station_dir(station) / filename

    @staticmethod
    def model_input_manifest(station: str) -> Path:
        """Path of the manifest written next to the model input (filters, hash, commit...)."""
        return FilePathBuilder.model_input(station).with_suffix(".meta.json")

    @staticmethod
    def _model_stem(
        station: str, model_name: str, start_date: str, end_date: str, version: str
    ) -> str:
        return f"{model_name.lower()}_st_{start_date}_et_{end_date}_{station}_{version}"

    @staticmethod
    def model_save(
        station: str,
        model_name: str,
        start_date: str,
        end_date: str,
        version: str = "v1",
    ) -> Path:
        """Path for a trained model (joblib), in the station's models directory.

        The name carries the period the model was **trained** on, which may be narrower than
        the ETL period. The model card is the sibling file with the ``.json`` suffix.

        Args:
            station: Station key (a key of ``etl_config.stations``).
            model_name: Model identifier (e.g. ``\"KNN\"``); stored lowercased in filename.
            start_date: First day of the training period.
            end_date: Last day of the training period.
            version: Version tag suffix (default ``\"v1\"``).

        Returns:
            Path ``models/<station>/{model}_st_{start}_et_{end}_{station}_{version}.joblib``.
        """
        stem = FilePathBuilder._model_stem(
            station, model_name, start_date, end_date, version
        )
        return (
            settings.MODELS_PATH
            / FilePathBuilder.station_dir(station)
            / f"{stem}.joblib"
        )

    @staticmethod
    def model_metrics(
        station: str,
        model_name: str,
        start_date: str,
        end_date: str,
        version: str = "v1",
    ) -> Path:
        """Path for the evaluation report of a trained model, in the station's results directory.

        Args:
            station: Station key (a key of ``etl_config.stations``).
            model_name: Model identifier; stored lowercased in filename.
            start_date: First day of the training period.
            end_date: Last day of the training period.
            version: Version tag suffix (default ``\"v1\"``).

        Returns:
            Path ``results/<station>/..._metrics.json``.
        """
        stem = FilePathBuilder._model_stem(
            station, model_name, start_date, end_date, version
        )
        return (
            settings.RESULTS_PATH
            / FilePathBuilder.station_dir(station)
            / f"{stem}_metrics.json"
        )

    @staticmethod
    def model_tuning(
        station: str,
        model_name: str,
        start_date: str,
        end_date: str,
        version: str = "v1",
    ) -> Path:
        """Path for hyperparameter tuning results JSON, in the station's results directory.

        Args:
            station: Station key (a key of ``etl_config.stations``).
            model_name: Model identifier; stored lowercased in filename.
            start_date: First day of the period the search ran on.
            end_date: Last day of that period.
            version: Version tag suffix (default ``\"v1\"``).

        Returns:
            Path ``results/<station>/..._tuning.json``.
        """
        stem = FilePathBuilder._model_stem(
            station, model_name, start_date, end_date, version
        )
        return (
            settings.RESULTS_PATH
            / FilePathBuilder.station_dir(station)
            / f"{stem}_tuning.json"
        )

    @staticmethod
    def _predict_stem(
        target: str, model_name: str, start_date: str, end_date: str
    ) -> str:
        return f"{model_name.lower()}_predict_st_{start_date}_et_{end_date}_{target}"

    @staticmethod
    def predictions(
        target: str, model_name: str, start_date: str, end_date: str
    ) -> Path:
        """CSV with the observed values and the predictions of every model and ensemble.

        Lives in the **target** station's results directory; the period is the one evaluated.
        """
        stem = FilePathBuilder._predict_stem(target, model_name, start_date, end_date)
        return (
            settings.RESULTS_PATH
            / FilePathBuilder.station_dir(target)
            / f"{stem}_predictions.csv"
        )

    @staticmethod
    def predictions_report(
        target: str, model_name: str, start_date: str, end_date: str
    ) -> Path:
        """JSON with the metrics of every model and ensemble and what produced them."""
        stem = FilePathBuilder._predict_stem(target, model_name, start_date, end_date)
        return (
            settings.RESULTS_PATH
            / FilePathBuilder.station_dir(target)
            / f"{stem}_metrics.json"
        )


file_paths = FilePathBuilder()
