import logging
from pathlib import Path

import httpx

from masters_project.enums import SondaStationEnums
from masters_project.utils import measure_memory, time_track

logger = logging.getLogger(__name__)


class SondaClient:
    def __init__(self, station: str, data_type: str) -> None:
        try:
            station_enum = SondaStationEnums[station]
            self.station_code = station_enum.value
            self.station_name = station_enum.name
        except KeyError:
            valid_options = [s.name for s in SondaStationEnums]
            logger.error(
                f"Failed to initialize SondaClient: Invalid station '{station}'."
            )
            raise ValueError(
                f"Station '{station}' not recognized. Valid options: {valid_options}"
            )
        self.data_type = data_type
        self.timeout = httpx.Timeout(10, connect=60)

        logger.info(
            f"Initialized SondaClient for station {self.station_name} ({self.data_type})."
        )

    @measure_memory
    @time_track
    def _download(self, url: str, output_path: Path) -> Path:
        logger.debug(f"Ensuring output directory exists: {output_path.parent}")

        output_path.parent.mkdir(parents=True, exist_ok=True)

        logger.debug(f"Starting download from: {url}")

        try:
            with httpx.stream("GET", url, timeout=self.timeout) as response:
                response.raise_for_status()
                with open(output_path, "wb") as file:
                    for data in response.iter_bytes():
                        file.write(data)
            logger.debug(f"Successfully saved file to: {output_path}")
            return output_path

        except httpx.HTTPStatusError as e:
            logger.warning(
                f"HTTP {e.response.status_code} - File not found on server: {url}"
            )
            raise
        except httpx.RequestError as e:
            logger.warning(f"Network connection dropped while connecting to {url}: {e}")
            raise
        except Exception:
            logger.exception(f"Unexpected error during download from {url}")
            raise

    @measure_memory
    @time_track
    def download_file_by_year(self, year: int, output_dir: str) -> Path:
        url = f"https://sonda.ccst.inpe.br/dados/{self.data_type}/{self.station_code}/{year}/{self.station_code}_{year}_SD.zip"

        path = Path(output_dir) / f"{self.station_name}_{year}.zip"
        logger.debug(f"Constructed yearly URL for {year}: {url}")
        return self._download(url, path)

    @measure_memory
    @time_track
    def download_file_by_year_month(
        self, year: int, month: int, output_dir: str
    ) -> Path:
        url = f"https://sonda.ccst.inpe.br/dados/{self.data_type}/{self.station_code}/{year}/{self.station_code}_{year}_{month:02d}_SD.zip"

        path = Path(output_dir) / f"{self.station_name}_{year}_{month:02d}.zip"
        logger.debug(f"Constructed monthly URL for {year}-{month:02d}: {url}")
        return self._download(url, path)
