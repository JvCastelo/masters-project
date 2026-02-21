import logging
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
        extraction_dir = zip_path.parent
        extraction_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"Extracting {zip_path.name} to {extraction_dir}")

        try:
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(extraction_dir)

            logger.info(f"Extraction complete for {zip_path.name}")

            if delete_zip:
                zip_path.unlink()
                logger.debug(f"Deleted original ZIP: {zip_path.name}")
            return extraction_dir
        except zipfile.BadZipFile:
            logger.error(f"The file {zip_path} is corrupted and cannot be unzipped.")
            raise

    @staticmethod
    @measure_memory
    @time_track
    def create_dataframe(extraction_dir: Path) -> pd.DataFrame:

        path_dat_file = next(extraction_dir.glob("*.dat"), None)

        if not path_dat_file:
            logger.error(f"No DAT file found in {extraction_dir}")
            return pd.DataFrame()

        try:
            df = pd.read_csv(
                path_dat_file, sep=",", skiprows=1, parse_dates=["timestamp"]
            )

            logger.info(f"Loaded DataFrame from .dat: {path_dat_file.name}")

            path_dat_file.unlink()

            logger.debug(f"Deleted source file: {path_dat_file.name}")

            return df

        except Exception as e:
            logger.error(f"Failed to read or delete {path_dat_file.name}: {e}")

            return pd.DataFrame()

    @staticmethod
    @measure_memory
    @time_track
    def format_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        df = df.iloc[1:].reset_index(drop=True)
        df.drop(
            columns=[
                "acronym",
                "year",
                "day",
                "min",
                "dir_avg",
                "dif_avg",
                "lw_avg",
                "par_avg",
                "lux_avg",
            ],
            inplace=True,
        )
        df["glo_avg"] = pd.to_numeric(df["glo_avg"], errors="coerce")
        df["timestamp"] = df["timestamp"].dt.tz_localize("UTC")

        return df
