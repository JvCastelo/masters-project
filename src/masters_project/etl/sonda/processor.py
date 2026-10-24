"""Turn a SONDA ZIP into a clean DataFrame (extract, parse the ``.dat``, keep timestamp and target)
and measure how many days have a valid target."""

import logging
import shutil
import zipfile
from collections.abc import Iterable
from datetime import date
from pathlib import Path

import pandas as pd

from masters_project.settings import settings
from masters_project.utils import time_track

logger = logging.getLogger(__name__)


class SondaProcessor:
    """Process SONDA ZIP archives: extract, load .dat into DataFrame, and format columns/timestamps."""

    @staticmethod
    @time_track
    def extract_zip(zip_path: Path, delete_zip: bool = True) -> Path:
        """Extract a SONDA ZIP to a directory with the same base name, optionally removing the ZIP.

        Args:
            zip_path: Path to the .zip file.
            delete_zip: If True, delete the ZIP after successful extraction. Defaults to True.

        Returns:
            Path to the extraction directory (zip_path with suffix removed).

        Raises:
            zipfile.BadZipFile: If the file is not a valid or readable ZIP.
        """
        extraction_dir = zip_path.with_suffix("")
        extraction_dir.mkdir(parents=True, exist_ok=True)

        logger.debug(
            f"Extracting {zip_path.name} into unique temp folder: {extraction_dir.name}"
        )

        try:
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(extraction_dir)

            if delete_zip:
                zip_path.unlink()
                logger.debug(f"Deleted original ZIP: {zip_path.name}")

            return extraction_dir

        except zipfile.BadZipFile:
            logger.error(
                f"The file {zip_path.name} is corrupted and cannot be unzipped."
            )
            shutil.rmtree(extraction_dir, ignore_errors=True)
            raise

    @staticmethod
    @time_track
    def create_dataframe(extraction_dir: Path) -> pd.DataFrame:
        """Load the first .dat file in the directory into a DataFrame and remove the extraction dir.

        Args:
            extraction_dir: Directory containing the extracted SONDA .dat file(s).

        Returns:
            DataFrame with parsed timestamps from the .dat file.

        Raises:
            FileNotFoundError: If no .dat file is found in extraction_dir.
            Exception: On read or cleanup failure (re-raised after logging).
        """
        path_dat_file = next(extraction_dir.glob("*.dat"), None)

        if not path_dat_file:
            logger.error(f"No DAT file found inside {extraction_dir.name}")
            raise FileNotFoundError(f"Missing .dat file in {extraction_dir.name}")

        try:
            df = pd.read_csv(
                path_dat_file, sep=",", skiprows=1, parse_dates=["timestamp"]
            )

            logger.debug(f"Loaded DataFrame from .dat: {path_dat_file.name}")

            shutil.rmtree(extraction_dir)

            logger.debug(
                f"Cleaned up temporary extraction folder: {extraction_dir.name}"
            )

            return df

        except Exception:
            logger.exception(f"Failed to read or delete {path_dat_file.name}")
            raise

    @staticmethod
    @time_track
    def format_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """Drop header row and unused columns, coerce glo_avg to numeric, localize timestamps to UTC.

        Args:
            df: Raw DataFrame from create_dataframe (SONDA .dat content).

        Returns:
            DataFrame with selected columns and UTC-localized timestamp.
        """
        df = df.iloc[1:].reset_index(drop=True)
        columns_to_maintain = [settings.etl.sonda.target_variable, "timestamp"]
        columns_to_drop = [col for col in df.columns if col not in columns_to_maintain]
        df.drop(
            columns=columns_to_drop,
            inplace=True,
        )
        df[settings.etl.sonda.target_variable] = pd.to_numeric(
            df[settings.etl.sonda.target_variable], errors="coerce"
        )

        logger.debug("Localizing timestamps to UTC...")
        df["timestamp"] = df["timestamp"].dt.tz_localize("UTC")

        return df

    @staticmethod
    def trim_to_period(
        df: pd.DataFrame, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """Keep rows from ``start_date`` 00:00 to ``end_date`` 23:59:59 (UTC), sorted by time.

        Args:
            df: DataFrame with a UTC-aware ``timestamp`` column.
            start_date: First day, 'YYYY-MM-DD'.
            end_date: Last day (inclusive), 'YYYY-MM-DD'.

        Returns:
            The trimmed, chronologically sorted DataFrame with a fresh index.
        """
        start_bound = pd.to_datetime(start_date).tz_localize("UTC")
        end_bound = pd.to_datetime(end_date).tz_localize("UTC") + pd.Timedelta(
            days=1, seconds=-1
        )
        trimmed = df[(df["timestamp"] >= start_bound) & (df["timestamp"] <= end_bound)]
        return trimmed.sort_values(by="timestamp").reset_index(drop=True)

    @staticmethod
    def coverage_days(df: pd.DataFrame, target_variable: str) -> tuple[int, int]:
        """Count the days that have at least one valid target value.

        Returns:
            (days with some valid value, days present in the frame).
        """
        days = df["timestamp"].dt.date
        return int(days[df[target_variable].notna()].nunique()), int(days.nunique())

    @staticmethod
    def days_with_target(csv_paths: Iterable[Path], target_variable: str) -> set[date]:
        """Days (UTC) on which at least one of the SONDA CSVs has a valid target value.

        Used to skip GOES days whose rows would be dropped by the join with the target.

        Args:
            csv_paths: SONDA CSVs written by the SONDA ETL (``timestamp`` + target column).
            target_variable: Name of the target column (e.g. 'glo_avg').

        Returns:
            Set of dates with a non-null target in any of the files.
        """
        days: set[date] = set()
        for path in csv_paths:
            df = pd.read_csv(path, usecols=["timestamp", target_variable])
            valid = pd.to_datetime(
                df.loc[df[target_variable].notna(), "timestamp"], utc=True
            )
            days.update(valid.dt.date)
        return days
