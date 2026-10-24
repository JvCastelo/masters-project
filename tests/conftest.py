"""Shared fixtures: every test works on a throw-away data tree, never on ``data/``."""

from collections.abc import Callable
from pathlib import Path

import pandas as pd
import pytest

from masters_project.settings import StationSettings, settings

# Ourinhos (SP). On 2022-06-21 solar noon is ~15:20 UTC.
STATION = "OURINHOS"
LAT, LON = -23.0, -49.844
DAY_START = "2022-06-21 15:00"


@pytest.fixture(autouse=True)
def isolated_logs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Run logs go to the test's temporary directory, never to ``data/logs``."""
    directory = tmp_path / "logs"
    monkeypatch.setattr(settings, "LOG_PATH", directory)
    return directory


@pytest.fixture
def data_tree(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point every ``settings`` path at ``tmp_path`` and return its root."""
    for name in (
        "RAW_PATH",
        "PROCESSED_PATH",
        "MODELS_PATH",
        "RESULTS_PATH",
        "CACHE_PATH",
    ):
        directory = tmp_path / name.removesuffix("_PATH").lower()
        directory.mkdir()
        monkeypatch.setattr(settings, name, directory)
    return tmp_path


@pytest.fixture
def one_station(monkeypatch: pytest.MonkeyPatch) -> str:
    """Run the pipelines for a single station over one day."""
    monkeypatch.setattr(
        settings.etl,
        "stations",
        {STATION: StationSettings(latitude=LAT, longitude=LON)},
    )
    monkeypatch.setattr(settings.etl, "start_date", "2022-06-21")
    monkeypatch.setattr(settings.etl, "end_date", "2022-06-21")
    return STATION


@pytest.fixture
def make_series() -> Callable[..., pd.DataFrame]:
    """Build ``timestamp`` + value columns sampled at a fixed frequency (UTC)."""

    def build(
        values: dict[str, list[float | None]],
        start: str = DAY_START,
        freq: str = "1min",
    ) -> pd.DataFrame:
        n = len(next(iter(values.values())))
        timestamps = pd.date_range(start, periods=n, freq=freq, tz="UTC")
        return pd.DataFrame({"timestamp": timestamps, **values})

    return build
