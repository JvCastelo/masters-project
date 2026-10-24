"""Tests for masters_project.etl.build_dataset: stack channels, filter, join SONDA, validate, write."""

from pathlib import Path

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


def test_only_the_stations_own_directory_is_read(
    raw_inputs, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A CSV of the same station left in another directory must not leak into the dataset."""
    _filters(monkeypatch)
    stray = (
        file_paths.raw_goes_dir(STATION, "other") / "goes_C03_st_x_et_y_OURINHOS.csv"
    )
    stray.parent.mkdir(parents=True)
    _goes("C03", ["2022-06-21 16:00"]).to_csv(stray, index=False)

    build_dataset.build_station_input(STATION)

    assert len(_output()) == 8  # the stray 16:00 scan is not there


def test_files_of_another_station_in_the_directory_are_ignored(
    raw_inputs, monkeypatch: pytest.MonkeyPatch
) -> None:
    _filters(monkeypatch)
    other = (
        file_paths.raw_goes_dir(STATION) / "goes_C03_st_x_et_y_CACHOEIRA_PAULISTA.csv"
    )
    _goes("C03", ["2022-06-21 16:00"]).to_csv(other, index=False)

    build_dataset.build_station_input(STATION)

    assert len(_output()) == 8


def test_goes_dir_reads_another_directory_inside_raw_goes(
    raw_inputs, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``goes_dir`` points the build at a different set of CSVs without moving any file."""
    _filters(monkeypatch)
    experiment = file_paths.raw_goes_dir(STATION, "ourinhos_experiment")
    experiment.mkdir(parents=True)
    _goes("C03", DAY[:3]).to_csv(
        experiment / file_paths.raw_goes("C03", STATION).name, index=False
    )

    assert build_dataset.build_station_input(STATION, goes_dir="ourinhos_experiment")

    assert len(_output()) == 3


def test_missing_goes_directory_names_the_expected_path(
    data_tree, one_station, caplog: pytest.LogCaptureFixture
) -> None:
    """Failure mode: the layout moved and the build used to find nothing without saying where."""
    assert build_dataset.build_station_input(STATION) is False
    assert "GOES directory not found" in caplog.text
    assert str(file_paths.raw_goes_dir(STATION)) in caplog.text


def test_empty_goes_directory_is_reported_as_no_files(
    data_tree, one_station, caplog: pytest.LogCaptureFixture
) -> None:
    file_paths.raw_goes_dir(STATION).mkdir(parents=True)

    assert build_dataset.build_station_input(STATION) is False
    assert "No GOES files found" in caplog.text


# =============================================================================
# command line
# =============================================================================


@pytest.fixture
def two_stations(raw_inputs, monkeypatch: pytest.MonkeyPatch) -> None:
    """OURINHOS has inputs; CACHOEIRA_PAULISTA is configured but has none."""
    from masters_project.settings import StationSettings

    monkeypatch.setattr(
        settings.etl,
        "stations",
        {
            **settings.etl.stations,
            "CACHOEIRA_PAULISTA": StationSettings(latitude=-22.69, longitude=-45.006),
        },
    )
    _filters(monkeypatch)


def test_build_model_input_can_be_limited_to_some_stations(two_stations) -> None:
    assert build_dataset.build_model_input([STATION]) == 0
    assert file_paths.model_input(STATION).exists()
    assert not file_paths.model_input("CACHOEIRA_PAULISTA").exists()


def test_build_model_input_without_a_selection_builds_every_station(
    two_stations,
) -> None:
    """The station without inputs fails the run, but OURINHOS is still built."""
    assert build_dataset.build_model_input() == 1
    assert file_paths.model_input(STATION).exists()


def test_goes_dir_is_rejected_for_more_than_one_station(two_stations) -> None:
    with pytest.raises(ValueError, match="single station"):
        build_dataset.build_model_input(goes_dir="somewhere")


def test_parse_args_defaults_to_every_station(two_stations) -> None:
    args = build_dataset.parse_args([])
    assert args.station is None and args.goes_dir is None


def test_parse_args_accepts_a_station_and_a_goes_dir(two_stations) -> None:
    args = build_dataset.parse_args(["--station", STATION, "--goes-dir", "experiment"])
    assert (args.station, args.goes_dir) == (STATION, "experiment")


def test_parse_args_goes_dir_requires_a_station(
    two_stations, capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit) as exit_info:
        build_dataset.parse_args(["--goes-dir", "experiment"])
    assert exit_info.value.code == 2
    assert "--goes-dir requires --station" in capsys.readouterr().err


def test_parse_args_rejects_a_station_that_is_not_configured(
    two_stations, capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit) as exit_info:
        build_dataset.parse_args(["--station", "ATLANTIS"])
    assert exit_info.value.code == 2
    assert "invalid choice" in capsys.readouterr().err


def test_main_builds_the_selected_station_and_writes_a_run_log(
    two_stations, isolated_logs
) -> None:
    assert build_dataset.main(["--station", STATION]) == 0

    assert file_paths.model_input(STATION).exists()
    log = (isolated_logs / "etl_build_dataset" / "latest.log").read_text()
    assert "stations: ['OURINHOS']" in log
    assert "status=ok" in log and "exit_code=0" in log


def test_main_reads_the_goes_dir_given_on_the_command_line(
    two_stations, isolated_logs
) -> None:
    experiment = file_paths.raw_goes_dir(STATION, "ourinhos_experiment")
    experiment.mkdir(parents=True)
    _goes("C03", DAY[:3]).to_csv(
        experiment / file_paths.raw_goes("C03", STATION).name, index=False
    )

    code = build_dataset.main(
        ["--station", STATION, "--goes-dir", "ourinhos_experiment"]
    )

    assert code == 0 and len(_output()) == 3


def test_main_exit_code_is_one_when_a_station_cannot_be_built(two_stations) -> None:
    assert build_dataset.main([]) == 1


# =============================================================================
# determinism: the result must not depend on the file system's listing order
# =============================================================================


@pytest.fixture
def adversarial_glob_order(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make ``Path.glob`` return files in reverse name order, as an unlucky file system might."""
    real_glob = Path.glob
    monkeypatch.setattr(
        Path,
        "glob",
        lambda self, pattern: iter(sorted(real_glob(self, pattern), reverse=True)),
    )


def _channel_blocks(columns: list[str]) -> list[str]:
    seen: list[str] = []
    for column in columns:
        if column.startswith("radius=") and column.split("_")[1] not in seen:
            seen.append(column.split("_")[1])
    return seen


def test_feature_columns_follow_channel_order_whatever_the_listing_order(
    raw_inputs, adversarial_glob_order, monkeypatch: pytest.MonkeyPatch
) -> None:
    _filters(monkeypatch)
    for channel in ("C13", "C01"):
        _goes(channel, NIGHT + DAY).to_csv(
            file_paths.raw_goes(channel, STATION), index=False
        )

    build_dataset.build_station_input(STATION)

    assert _channel_blocks(list(_output().columns)) == ["C01", "C03", "C13"]


def test_the_later_period_wins_a_timestamp_present_in_two_files(
    raw_inputs, adversarial_glob_order, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two scans can round to the same timestamp; which one is kept must not be luck."""
    _filters(monkeypatch)
    later = _goes("C03", DAY[:1])
    later[GoesProcessor.window_columns(1, "C03")] = (
        999.0  # same timestamp, other values
    )
    later.to_csv(
        file_paths.raw_goes_dir(STATION)
        / "goes_C03_st_2022-06-21_et_2022-06-22_OURINHOS.csv",
        index=False,
    )

    build_dataset.build_station_input(STATION)

    row = _output().loc[lambda df: df["timestamp"] == pd.Timestamp(DAY[0], tz="UTC")]
    assert (row["radius=1_C03_11"] == 999.0).all()
