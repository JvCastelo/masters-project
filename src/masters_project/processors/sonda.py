import logging
import shutil
import zipfile
from pathlib import Path

import pandas as pd

from masters_project.utils import measure_memory, time_track

logger = logging.getLogger(__name__)


class SondaProcessor:
    @staticmethod
    @measure_memory
    @time_track
    def extract_zip(zip_path: Path, delete_zip: bool = True) -> Path:
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
    @measure_memory
    @time_track
    def create_dataframe(extraction_dir: Path) -> pd.DataFrame:

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
    @measure_memory
    @time_track
    def format_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        df = df.iloc[1:].reset_index(drop=True)
        columns_to_drop = [
            "acronym",
            "year",
            "day",
            "min",
            "dir_avg",
            "dif_avg",
            "lw_avg",
            "par_avg",
            "lux_avg",
        ]
        existing_cols_to_drop = [col for col in columns_to_drop if col in df.columns]
        df.drop(
            columns=existing_cols_to_drop,
            inplace=True,
        )
        df["glo_avg"] = pd.to_numeric(df["glo_avg"], errors="coerce")

        logger.debug("Localizing timestamps to UTC...")
        df["timestamp"] = df["timestamp"].dt.tz_localize("UTC")

        return df
