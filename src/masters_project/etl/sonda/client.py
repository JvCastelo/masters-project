import logging
from pathlib import Path

import httpx

from masters_project.enums import SondaStationEnums
from masters_project.utils import measure_memory, time_track

logger = logging.getLogger(__name__)


class SondaClient:
    """Client for downloading SONDA (Brazilian solar/radiation) data from INPE's HTTP API.

    Resolves station names to station codes via SondaStationEnums and downloads
    yearly or monthly ZIP archives to a local directory.
    """

    def __init__(self, station: str, data_type: str) -> None:
        """Initialize the SONDA client for a given station and data type.

        Args:
            station: Station name matching a key in SondaStationEnums (e.g. 'BRASILIA').
            data_type: Data product type used in the download URL path.

        Raises:
            ValueError: If station is not a valid SondaStationEnums member.
        """
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
        """Download a file from a URL to disk via streaming.

        Args:
            url: Full URL to the resource.
            output_path: Local path where the file will be saved.

        Returns:
            The path where the file was written (same as output_path).

        Raises:
            httpx.HTTPStatusError: On non-2xx response (e.g. 404).
            httpx.RequestError: On connection or request failures.
            Exception: Re-raises any other error during write or iteration.
        """
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
        """Download the SONDA yearly ZIP file for the given year.

        Args:
            year: Calendar year (e.g. 2023).
            output_dir: Directory path (string) where the ZIP will be saved.

        Returns:
            Path to the downloaded ZIP file.

        Raises:
            httpx.HTTPStatusError: If the URL returns a non-2xx status.
            httpx.RequestError: On network errors.
        """
        url = f"https://sonda.ccst.inpe.br/dados/{self.data_type}/{self.station_code}/{year}/{self.station_code}_{year}_SD.zip"

        path = Path(output_dir) / f"{self.station_name}_{year}.zip"
        logger.debug(f"Constructed yearly URL for {year}: {url}")
        return self._download(url, path)

    @measure_memory
    @time_track
    def download_file_by_year_month(
        self, year: int, month: int, output_dir: str
    ) -> Path:
        """Download the SONDA monthly ZIP file for the given year and month.

        Args:
            year: Calendar year (e.g. 2023).
            month: Month (1–12).
            output_dir: Directory path (string) where the ZIP will be saved.

        Returns:
            Path to the downloaded ZIP file.

        Raises:
            httpx.HTTPStatusError: If the URL returns a non-2xx status.
            httpx.RequestError: On network errors.
        """
        url = f"https://sonda.ccst.inpe.br/dados/{self.data_type}/{self.station_code}/{year}/{self.station_code}_{year}_{month:02d}_SD.zip"

        path = Path(output_dir) / f"{self.station_name}_{year}_{month:02d}.zip"
        logger.debug(f"Constructed monthly URL for {year}-{month:02d}: {url}")
        return self._download(url, path)
