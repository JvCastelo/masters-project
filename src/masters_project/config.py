import logging
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
    YEAR = 2024
    BASE_PATH_FILE_LOG = Path("data") / "logs"
    BASE_PATH_FILE_INPUT_MODEL = Path("data") / "input_model"
    BASE_PATH_FILE_OUPUT_MODEL = Path("data") / "output_model"


class Settings(BaseSettings):
    LOG_LEVEL: str = "INFO"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    def setup_logging(self):
        logging.basicConfig(
            filename=f"{GeneralConfig.BASE_PATH_FILE_LOG}" / "application.log",
            filemode="w",
            level=self.LOG_LEVEL,
            format="%(asctime)s :: %(levelname)-8s :: %(name)s :: %(message)s",
            force=True,
        )


settings = Settings()
