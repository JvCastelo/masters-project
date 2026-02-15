import logging

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    BUCKET_NAME: str
    PRODUCT_NAME: str
    GOES_VARIABLE: str
    TARGET_LATITUDE: float
    TARGET_LONGITUDE: float
    RADIUS: int
    LOG_LEVEL: str = "INFO"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    def setup_logging(self):
        logging.basicConfig(
            level=self.LOG_LEVEL,
            format="%(asctime)s :: %(levelname)s :: %(message)s",
            force=True,
        )


settings = Settings()
