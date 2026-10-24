"""Tests for masters_project.etl.sonda.pipeline: download, parse, trim, validate, export."""

import runpy
import sys
from pathlib import Path
from unittest.mock import MagicMock
from zipfile import BadZipFile, ZipFile

import httpx
import pandas as pd
import pytest

from masters_project.etl.sonda import pipeline
from masters_project.etl.sonda.pipeline import StationResult
from masters_project.file_paths import file_paths
from masters_project.settings import settings

STATION = "OURINHOS"

# SONDA .dat: station metadata, column names, then a units row (dropped), then the data.
DAT_HEADER = "metadata\ntimestamp,glo_avg,acronym\n"
UNITS_ROW = "2022-06-20 00:00:00,W/m2,x\n"


def _dat(rows: list[tuple[str, float | str]]) -> str:
    return DAT_HEADER + UNITS_ROW + "".join(f"{ts},{v},x\n" for ts, v in rows)


def _write_zip(destination: Path, dat: str) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(destination, "w") as zf:
        zf.writestr("station.dat", dat)
    return destination


def _http_error(status: int) -> httpx.HTTPStatusError:
    return httpx.HTTPStatusError(
        "error",
        request=httpx.Request("GET", "http://sonda"),
        response=httpx.Response(status),
    )


@pytest.fixture
def served(data_tree, one_station, monkeypatch: pytest.MonkeyPatch):
    """Replace the download with a dict {period: dat text | Exception}; returns that dict."""
    content: dict[tuple[int, int | None], str | Exception] = {}

    def fake_download(client, period):
        item = content[period]
        if isinstance(item, Exception):
            raise item
        return _write_zip(
            settings.RAW_PATH / "sonda" / f"{period[0]}_{period[1]}.zip", item
        )

    monkeypatch.setattr(pipeline, "download_period", fake_download)
    return content


def _csv() -> pd.DataFrame:
    return pd.read_csv(file_paths.raw_sonda(STATION), parse_dates=["timestamp"])


def test_download_period_picks_the_yearly_or_monthly_endpoint(tmp_path: Path) -> None:
    client = MagicMock()
    pipeline.download_period(client, (2022, None))
    pipeline.download_period(client, (2022, 6))
    assert client.download_file_by_year.call_args.args[0] == 2022
    assert client.download_file_by_year_month.call_args.args[:2] == (2022, 6)


def test_station_result_is_ok_only_without_missing_or_failed_periods() -> None:
    assert StationResult("X").ok
    assert not StationResult("X", missing=["2022-06"]).ok
    assert not StationResult("X", failed=["2022"]).ok


def test_process_station_exports_the_trimmed_target(served) -> None:
    served[(2022, None)] = _dat(
        [
            ("2022-06-20 23:59:00", 1.0),  # before the period: trimmed
            ("2022-06-21 12:00:00", 500.0),
            ("2022-06-21 12:01:00", 501.0),
            ("2022-06-22 00:00:00", 2.0),  # after the period: trimmed
        ]
    )

    result = pipeline.process_station(STATION, "year")

    assert result.ok
    out = _csv()
    assert out["glo_avg"].tolist() == [500.0, 501.0]
    assert set(out.columns) == {"glo_avg", "timestamp"}


def test_process_station_treats_unparseable_target_values_as_missing(served) -> None:
    served[(2022, None)] = _dat(
        [("2022-06-21 12:00:00", 500.0), ("2022-06-21 12:01:00", "n/a")]
    )
    pipeline.process_station(STATION, "year")
    assert _csv()["glo_avg"].isna().tolist() == [False, True]


def test_unpublished_month_is_reported_but_the_rest_is_exported(
    served, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Failure mode: SONDA answers 404 for a month; the CSV keeps what exists, with a gap."""
    monkeypatch.setattr(settings.etl, "end_date", "2022-07-02")
    served[(2022, 6)] = _dat([("2022-06-21 12:00:00", 500.0)])
    served[(2022, 7)] = _http_error(404)

    result = pipeline.process_station(STATION, "month")

    assert result.missing == ["2022-07"]
    assert not result.ok
    assert len(_csv()) == 1


def test_other_http_errors_and_bad_zips_are_recorded_as_failed(
    served, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings.etl, "end_date", "2022-08-02")
    served[(2022, 6)] = _dat([("2022-06-21 12:00:00", 500.0)])
    served[(2022, 7)] = _http_error(500)
    served[(2022, 8)] = BadZipFile("corrupted")

    result = pipeline.process_station(STATION, "month")

    assert result.failed == ["2022-07", "2022-08"]
    assert result.missing == []


def test_process_station_without_any_data_writes_nothing(served) -> None:
    served[(2022, None)] = _http_error(404)

    result = pipeline.process_station(STATION, "year")

    assert result.missing == ["2022"]
    assert not file_paths.raw_sonda(STATION).exists()


def test_main_returns_zero_when_every_period_is_obtained(served) -> None:
    served[(2022, None)] = _dat([("2022-06-21 12:00:00", 500.0)])
    assert pipeline.main("year") == 0


def test_main_returns_one_when_a_period_is_missing(served) -> None:
    served[(2022, None)] = _http_error(404)
    assert pipeline.main("year") == 1


def test_main_returns_one_when_the_data_breaks_the_contract(served) -> None:
    """Repeated timestamps fail pandera's structural check: the station is marked failed."""
    served[(2022, None)] = _dat(
        [("2022-06-21 12:00:00", 500.0), ("2022-06-21 12:00:00", 501.0)]
    )
    assert pipeline.main("year") == 1
    assert not file_paths.raw_sonda(STATION).exists()


def test_main_keeps_going_when_one_station_raises(
    data_tree, one_station, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = []

    def process(station: str, granularity: str) -> StationResult:
        calls.append(station)
        raise RuntimeError("boom")

    monkeypatch.setattr(pipeline, "process_station", process)
    assert pipeline.main("year") == 1
    assert calls == [STATION]


def test_module_entry_point_forwards_the_granularity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    received = []
    monkeypatch.setattr(pipeline, "main", lambda by: received.append(by) or 0)
    monkeypatch.setattr(sys, "argv", ["sonda", "--by", "month"])

    with pytest.raises(SystemExit) as exit_info:
        runpy.run_module("masters_project.etl.sonda", run_name="__main__")

    assert exit_info.value.code == 0
    assert received == ["month"]
