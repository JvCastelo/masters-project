import logging
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)


class SondaClient:
    STATION_MAP = {
        "Brasília": "BRB",
        "Cachoeira Paulista": "CPA",
        "Caicó": "CAI",
        "Campo Grande Fazenda": "CGR",
        "Campo Grande UNIDERP": "CGU",
        "Campo Mourão": "CMS",
        "Cuiabá": "CBA",
        "Curitiba TECPAR": "CTB",
        "Curitiba UTFPR": "CTS",
        "Florianópolis BSRN": "FLN",
        "Florianópolis Sapiens Park": "SPK",
        "Joinville": "JOI",
        "Medianeira": "MDS",
        "Natal": "NAT",
        "Ourinhos": "ORN",
        "Palmas": "PMA",
        "Petrolina": "PTR",
        "Santarém": "STM",
        "São Luiz": "SLZ",
        "São Martinho da Serra": "SMS",
        "Sombrio": "SBR",
    }

    def __init__(self, station: str, data_type: str = "solarimetricos") -> None:
        if station not in self.STATION_MAP:
            raise ValueError(f"Station {station} not recognized.")

        self.station_name = station
        self.station_code = self.STATION_MAP[station]
        self.data_type = data_type
        self.timeout = httpx.Timeout(10, connect=60)

    def _download(self, url: str, output_path: Path) -> Path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with httpx.stream("GET", url, timeout=self.timeout) as response:
                response.raise_for_status()
                with open(output_path, "wb") as file:
                    for data in response.iter_bytes():
                        file.write(data)
            return output_path
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP {e.response.status_code} for URL: {url}")
            return None

    def download_file_by_year(self, year: int, output_dir: str = "data/sonda") -> Path:
        url = f"https://sonda.ccst.inpe.br/dados/{self.data_type}/{self.station_code}/{year}/{self.station_code}_{year}_SD.zip"

        path = Path(output_dir) / f"{self.station_name}_{year}.zip"
        return self._download(url, path)

    def download_file_by_year_month(
        self, year: int, month: int, output_dir: str = "data/sonda"
    ) -> Path:
        url = f"https://sonda.ccst.inpe.br/dados/{self.data_type}/{self.station_code}/{year}/{self.station_code}_{year}_{month:02d}_SD.zip"

        path = Path(output_dir) / f"{self.station_name}_{year}.zip"

        return self._download(url, path)
