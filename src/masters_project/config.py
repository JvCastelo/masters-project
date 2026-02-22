import logging
import sys
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class GoesConfig:
    PRODUCT_NAME = "ABI-L1b-RadF"
    BUCKET_NAME = "noaa-goes16"
    GOES_VARIABLE = "Rad"
    RADIUS = 1
    BASE_PATH_FILE = Path("data") / "raw" / "goes"

    # FIRST STATION: PETROLINA
    TARGET_LATITUDE = -9.069
    TARGET_LONGITUDE = -40.320

    # SECOND STATION:
    # TARGET_LATITUDE =
    # TARGET_LONGITUDE =

    # THIRD STATION:
    # TARGET_LATITUDE =
    # TARGET_LONGITUDE =

    MAX_WORKERS = 10


class SondaConfig:
    DATA_TYPE = (
        "solarimetricos"  # Could be: solarimetricos, anemometricos, meteorologicos.
    )
    BASE_PATH_FILE = Path("data") / "raw" / "sonda"

    # FIRST STATION
    STATION = "PETROLINA"

    # SECOND STATION
    # STATION = ""

    # THIRD STATION
    # STATION = ""


class GeneralConfig:
    START_DATE = "2024-01-01"
    END_DATE = "2024-12-31"

    BASE_PATH_FILE_LOG = Path("data") / "logs"
    BASE_PATH_FILE_PROCESSED = Path("data") / "processed"
    BASE_PATH_FILE_OUPUT_MODEL = Path("data") / "output_model"


class Settings(BaseSettings):
    LOG_LEVEL: str = "INFO"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    def setup_logging(self, func_name):

        GeneralConfig.BASE_PATH_FILE_LOG.mkdir(parents=True, exist_ok=True)

        formatter = logging.Formatter(
            "%(asctime)s :: %(levelname)-8s :: %(name)s :: %(message)s"
        )

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(self.LOG_LEVEL)
        console_handler.setFormatter(formatter)

        file_handler = logging.FileHandler(
            Path(GeneralConfig.BASE_PATH_FILE_LOG) / f"{func_name}.log", mode="a"
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)

        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)

        if root_logger.hasHandlers():
            root_logger.handlers.clear()

        root_logger.addHandler(console_handler)
        root_logger.addHandler(file_handler)

        logging.getLogger("botocore").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("s3fs").setLevel(logging.WARNING)
        logging.getLogger("h5netcdf").setLevel(logging.WARNING)
        logging.getLogger("fsspec").setLevel(logging.WARNING)


settings = Settings()
