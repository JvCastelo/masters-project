"""Tests for masters_project.etl.sonda.client: SondaClient URLs and streaming download."""

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock

import httpx
import pytest

from masters_project.etl.sonda.client import SondaClient


class FakeResponse:
    def __init__(
        self, chunks: list[bytes], status: int = 200, fail_on_read: bool = False
    ):
        self.chunks, self.status, self.fail_on_read = chunks, status, fail_on_read

    def raise_for_status(self) -> None:
        if self.status >= 400:
            raise httpx.HTTPStatusError(
                "error",
                request=httpx.Request("GET", "http://sonda"),
                response=httpx.Response(self.status),
            )

    def iter_bytes(self) -> Iterator[bytes]:
        yield from self.chunks
        if self.fail_on_read:
            raise OSError("disk full")


@pytest.fixture
def client() -> SondaClient:
    return SondaClient(station="OURINHOS", data_type="solarimetricos")


def _stream_returning(
    monkeypatch: pytest.MonkeyPatch, response: FakeResponse
) -> list[str]:
    """Replace httpx.stream; return the list of requested URLs."""
    urls: list[str] = []

    @contextmanager
    def fake_stream(method: str, url: str, timeout: httpx.Timeout):
        urls.append(url)
        yield response

    monkeypatch.setattr("masters_project.etl.sonda.client.httpx.stream", fake_stream)
    return urls


def test_sonda_client_invalid_station_raises_valueerror() -> None:
    """Critical failure: unknown station name must raise ValueError with valid options listed."""
    with pytest.raises(ValueError, match="not recognized"):
        SondaClient(station="INVALID_STATION", data_type="BSRN")


def test_client_resolves_the_station_code(client: SondaClient) -> None:
    assert (client.station_name, client.station_code) == ("OURINHOS", "ORN")


def test_download_by_year_builds_the_yearly_url_and_file_name(
    client: SondaClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    urls = _stream_returning(monkeypatch, FakeResponse([b"zip-", b"bytes"]))

    path = client.download_file_by_year(2022, tmp_path)

    assert urls == [
        "https://sonda.ccst.inpe.br/dados/solarimetricos/ORN/2022/ORN_2022_SD.zip"
    ]
    assert path == tmp_path / "OURINHOS_2022.zip"
    assert path.read_bytes() == b"zip-bytes"


def test_download_by_year_month_zero_pads_the_month(
    client: SondaClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    urls = _stream_returning(monkeypatch, FakeResponse([b"x"]))

    path = client.download_file_by_year_month(2023, 6, tmp_path)

    assert urls == [
        "https://sonda.ccst.inpe.br/dados/solarimetricos/ORN/2023/ORN_2023_06_SD.zip"
    ]
    assert path.name == "OURINHOS_2023_06.zip"


def test_download_creates_the_missing_output_directory(
    client: SondaClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _stream_returning(monkeypatch, FakeResponse([b"x"]))
    path = client.download_file_by_year(2022, tmp_path / "a" / "b")
    assert path.exists()


def test_download_http_404_is_raised_and_logged(
    client: SondaClient,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Failure mode: a month SONDA does not publish surfaces as HTTPStatusError(404)."""
    _stream_returning(monkeypatch, FakeResponse([], status=404))

    with pytest.raises(httpx.HTTPStatusError) as error:
        client.download_file_by_year(2022, tmp_path)

    assert error.value.response.status_code == 404
    assert "HTTP 404" in caplog.text


def test_download_connection_error_is_raised_and_logged(
    client: SondaClient,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    def broken(*args, **kwargs):
        raise httpx.ConnectError("network down")

    monkeypatch.setattr("masters_project.etl.sonda.client.httpx.stream", broken)

    with pytest.raises(httpx.ConnectError):
        client.download_file_by_year(2022, tmp_path)
    assert "Network connection dropped" in caplog.text


def test_download_unexpected_error_is_raised(
    client: SondaClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _stream_returning(monkeypatch, FakeResponse([b"x"], fail_on_read=True))
    with pytest.raises(OSError, match="disk full"):
        client.download_file_by_year(2022, tmp_path)


def test_download_file_by_year_delegates_url_and_destination_to_download(
    client: SondaClient, tmp_path: Path
) -> None:
    client._download = MagicMock(return_value=tmp_path / "f.zip")
    client.download_file_by_year(2021, tmp_path)
    url, destination = client._download.call_args.args
    assert url.endswith("/ORN/2021/ORN_2021_SD.zip")
    assert destination == tmp_path / "OURINHOS_2021.zip"
