"""Tests for masters_project.etl.build_dataset: stack channels, filter, join SONDA, validate, write."""

import numpy as np
import pandas as pd
import pytest

from masters_project.etl import build_dataset
from masters_project.etl.goes.reader import GoesProcessor
from masters_project.file_paths import file_paths
from masters_project.settings import FiltersConfig, settings

STATION = "OURINHOS"
NIGHT = ["2022-06-21 03:00", "2022-06-21 03:10"]
DAY = [
    f"2022-06-21 15:{m:02d}" for m in range(0, 60, 10)
]  # six scans around solar noon


def _goes(channel: str, stamps: list[str], drop: str | None = None) -> pd.DataFrame:
    columns = GoesProcessor.window_columns(1, channel)
    values = np.arange(len(stamps) * 9, dtype=float).reshape(len(stamps), 9)
    df = pd.DataFrame(values, columns=columns)
    df["timestamp"] = pd.to_datetime(stamps, utc=True)
    return df.drop(columns=[drop]) if drop else df


def _sonda(
    night_value: float = -1.0, day_values: list[float] | None = None
) -> pd.DataFrame:
    """1-minute target: a night stretch and an hour around solar noon."""
    day_values = (
        day_values if day_values is not None else [400.0 + i for i in range(60)]
    )
    night_ts = pd.date_range(NIGHT[0], periods=11, freq="1min", tz="UTC")
    day_ts = pd.date_range(DAY[0], periods=len(day_values), freq="1min", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp": list(night_ts) + list(day_ts),
            "glo_avg": [night_value] * len(night_ts) + day_values,
        }
    )


@pytest.fixture
def raw_inputs(data_tree, one_station):
    """GOES C03 (night + day scans) and the SONDA series written where the ETL puts them."""
    goes_path = file_paths.raw_goes("C03", STATION)
    goes_path.parent.mkdir(parents=True, exist_ok=True)
    _goes("C03", NIGHT + DAY).to_csv(goes_path, index=False)

    sonda_path = file_paths.raw_sonda(STATION)
    sonda_path.parent.mkdir(parents=True, exist_ok=True)
    _sonda().to_csv(sonda_path, index=False)


def _filters(monkeypatch: pytest.MonkeyPatch, **kwargs) -> None:
    monkeypatch.setattr(settings.etl, "filters", FiltersConfig(**kwargs))


def _output() -> pd.DataFrame:
    return pd.read_csv(file_paths.model_input(STATION), parse_dates=["timestamp"])


def test_build_station_input_joins_goes_and_sonda_on_timestamp(
    raw_inputs, monkeypatch: pytest.MonkeyPatch
) -> None:
    _filters(monkeypatch)  # no filters
    assert build_dataset.build_station_input(STATION) is True

    out = _output()
    assert len(out) == 8  # every GOES scan has a SONDA minute
    assert out["glo_avg"].iloc[2] == 400.0  # 15:00 scan -> 15:00 minute
    assert out["glo_avg"].iloc[3] == 410.0  # 15:10 scan -> 15:10 minute
    assert "radius=1_C03_22" in out.columns


def test_solar_elevation_filter_drops_the_night_scans(
    raw_inputs, monkeypatch: pytest.MonkeyPatch
) -> None:
    _filters(monkeypatch, min_solar_elevation=5)
    build_dataset.build_station_input(STATION)
    assert len(_output()) == 6
    assert (_output()["glo_avg"] > 0).all()


def test_frozen_target_rows_are_dropped_before_the_join(
    raw_inputs, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A stuck sensor for 30 minutes removes the 15:00, 15:10 and 15:20 scans."""
    frozen = [500.0] * 30 + [400.0 + i for i in range(30)]
    _sonda(day_values=frozen).to_csv(file_paths.raw_sonda(STATION), index=False)
    _filters(monkeypatch, min_solar_elevation=5, frozen_min_run=10)

    build_dataset.build_station_input(STATION)

    out = _output()
    assert len(out) == 3
    assert out["timestamp"].dt.minute.tolist() == [30, 40, 50]


def test_goes_files_of_overlapping_periods_are_stacked_without_duplicates(
    raw_inputs, monkeypatch: pytest.MonkeyPatch
) -> None:
    _filters(monkeypatch)
    other = file_paths.raw_goes("C03", STATION).with_name(
        "goes_C03_st_2022-06-20_et_2022-06-21_OURINHOS.csv"
    )
    _goes("C03", DAY).to_csv(other, index=False)  # same scans again

    build_dataset.build_station_input(STATION)
    assert len(_output()) == 8


def test_channels_are_joined_side_by_side(
    raw_inputs, monkeypatch: pytest.MonkeyPatch
) -> None:
    _filters(monkeypatch)
    c13 = file_paths.raw_goes("C13", STATION)
    _goes("C13", NIGHT + DAY).to_csv(c13, index=False)

    build_dataset.build_station_input(STATION)

    out = _output()
    assert len([c for c in out.columns if c.startswith("radius=")]) == 18


def test_missing_goes_files_return_false(data_tree, one_station) -> None:
    assert build_dataset.build_station_input(STATION) is False
    assert not file_paths.model_input(STATION).exists()


def test_missing_sonda_file_returns_false(raw_inputs) -> None:
    file_paths.raw_sonda(STATION).unlink()
    assert build_dataset.build_station_input(STATION) is False
    assert not file_paths.model_input(STATION).exists()


def test_invalid_model_input_is_not_written(
    raw_inputs, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Failure mode: a GOES window missing a pixel must stop the build, not reach the ML stage."""
    _filters(monkeypatch)
    _goes("C03", NIGHT + DAY, drop="radius=1_C03_11").to_csv(
        file_paths.raw_goes("C03", STATION), index=False
    )

    assert build_dataset.build_station_input(STATION) is False
    assert not file_paths.model_input(STATION).exists()


def test_build_model_input_exit_code_reflects_every_station(
    raw_inputs, monkeypatch: pytest.MonkeyPatch
) -> None:
    _filters(monkeypatch)
    assert build_dataset.build_model_input() == 0

    file_paths.raw_sonda(STATION).unlink()
    assert build_dataset.build_model_input() == 1
