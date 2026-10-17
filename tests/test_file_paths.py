"""Tests for masters_project.file_paths: FilePathBuilder."""

from masters_project.file_paths import file_paths
from masters_project.settings import settings

STATION = "OURINHOS"


def test_raw_goes_filename_matches_period_channel_and_station() -> None:
    """raw_goes builds path under raw/goes with goes_{channel}_st_..._et_..._{station}.csv."""
    channel = "C01"
    path = file_paths.raw_goes(channel, STATION)
    assert path.parent == settings.RAW_PATH / "goes"
    assert path.name.startswith(f"goes_{channel}_st_{settings.etl.start_date}_")
    assert path.name.endswith(f"_{STATION}.csv")


def test_raw_goes_different_stations_do_not_collide() -> None:
    """Each station gets its own file for the same channel and period."""
    paths = {file_paths.raw_goes("C03", name) for name in settings.stations}
    assert len(paths) == len(settings.stations)


def test_raw_sonda_filename_matches_period_etl_and_station() -> None:
    """raw_sonda builds path under raw/sonda with sonda_{data_type}_st_..._{station}.csv."""
    path = file_paths.raw_sonda(STATION)
    assert path.parent == settings.RAW_PATH / "sonda"
    assert settings.etl.sonda.data_type in path.name
    assert path.name.endswith(f"_{STATION}.csv")


def test_model_input_under_processed_with_expected_prefix() -> None:
    """model_input points to processed dir with model_input_st_... prefix and the station."""
    path = file_paths.model_input(STATION)
    assert path.parent == settings.PROCESSED_PATH
    assert path.name.startswith("model_input_st_")
    assert path.name.endswith(f"_{STATION}.csv")


def test_model_save_and_model_metrics_suffixes() -> None:
    """model_save uses .joblib under models; model_metrics uses _metrics.json under results."""
    save_path = file_paths.model_save(STATION, "KNN", version="v1")
    assert save_path.parent == settings.MODELS_PATH
    assert save_path.suffix == ".joblib"
    assert "knn_" in save_path.name.lower()
    assert STATION in save_path.name

    metrics_path = file_paths.model_metrics(STATION, "KNN", version="v1")
    assert metrics_path.parent == settings.RESULTS_PATH
    assert metrics_path.name.endswith("_metrics.json")
    assert STATION in metrics_path.name


def test_goes_cache_day_is_independent_of_period_and_station() -> None:
    """The cache path depends on product/variable/radius/channel/day only."""
    path = file_paths.goes_cache_day("ABI-L1b-RadF", "Rad", 1, "C03", 2022, 32)
    assert path.parent.parent == (
        settings.CACHE_PATH / "goes" / "ABI-L1b-RadF" / "Rad_r1" / "C03"
    )
    assert path.name == "032.csv"
