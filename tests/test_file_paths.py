"""Tests for masters_project.file_paths: FilePathBuilder."""

import pytest

from masters_project.file_paths import file_paths
from masters_project.settings import settings

STATION = "OURINHOS"


def test_raw_goes_filename_matches_period_channel_and_station() -> None:
    """raw_goes builds goes_{channel}_st_..._et_..._{station}.csv in the station's raw/goes directory."""
    channel = "C01"
    path = file_paths.raw_goes(channel, STATION)
    assert path.parent == settings.RAW_PATH / "goes" / "ourinhos"
    assert path.name.startswith(f"goes_{channel}_st_{settings.etl.start_date}_")
    assert path.name.endswith(f"_{STATION}.csv")


def test_raw_goes_different_stations_do_not_collide() -> None:
    """Each station gets its own file for the same channel and period."""
    paths = {file_paths.raw_goes("C03", name) for name in settings.stations}
    assert len(paths) == len(settings.stations)


def test_raw_sonda_filename_matches_period_etl_and_station() -> None:
    """raw_sonda builds sonda_{data_type}_st_..._{station}.csv in the station's raw/sonda directory."""
    path = file_paths.raw_sonda(STATION)
    assert path.parent == settings.RAW_PATH / "sonda" / "ourinhos"
    assert settings.etl.sonda.data_type in path.name
    assert path.name.endswith(f"_{STATION}.csv")


def test_model_input_under_processed_with_expected_prefix() -> None:
    """model_input points to the station's processed dir, with the model_input_st_... prefix."""
    path = file_paths.model_input(STATION)
    assert path.parent == settings.PROCESSED_PATH / "ourinhos"
    assert path.name.startswith("model_input_st_")
    assert path.name.endswith(f"_{STATION}.csv")


def test_model_save_and_model_metrics_suffixes() -> None:
    """model_save uses .joblib under models; model_metrics uses _metrics.json under results."""
    save_path = file_paths.model_save(STATION, "KNN", version="v1")
    assert save_path.parent == settings.MODELS_PATH / "ourinhos"
    assert save_path.suffix == ".joblib"
    assert "knn_" in save_path.name.lower()
    assert STATION in save_path.name

    metrics_path = file_paths.model_metrics(STATION, "KNN", version="v1")
    assert metrics_path.parent == settings.RESULTS_PATH / "ourinhos"
    assert metrics_path.name.endswith("_metrics.json")
    assert STATION in metrics_path.name


def test_goes_cache_day_is_independent_of_period_and_station() -> None:
    """The cache path depends on product/variable/radius/channel/day only."""
    path = file_paths.goes_cache_day("ABI-L1b-RadF", "Rad", 1, "C03", 2022, 32)
    assert path.parent.parent == (
        settings.CACHE_PATH / "goes" / "ABI-L1b-RadF" / "Rad_r1" / "C03"
    )
    assert path.name == "032.csv"


def test_goes_listing_and_pixel_caches_live_under_cache_goes() -> None:
    listing = file_paths.goes_listing_dir("noaa-goes16", "ABI-L1b-RadF")
    pixels = file_paths.goes_pixels("noaa-goes16", "ABI-L1b-RadF", "C03")

    assert (
        listing
        == settings.CACHE_PATH / "goes" / "listing" / "noaa-goes16" / "ABI-L1b-RadF"
    )
    assert pixels.name == "C03.json"
    assert (
        pixels.parent
        == settings.CACHE_PATH / "goes" / "pixels" / "noaa-goes16" / "ABI-L1b-RadF"
    )


def test_model_tuning_path_has_the_tuning_suffix_under_results() -> None:
    path = file_paths.model_tuning("OURINHOS", "KNN", "v2")

    assert path.parent == settings.RESULTS_PATH / "ourinhos"
    assert path.name.startswith("knn_st_")
    assert path.name.endswith("_OURINHOS_v2_tuning.json")


@pytest.mark.parametrize(
    ("station", "directory"),
    [
        ("OURINHOS", "ourinhos"),
        ("CACHOEIRA_PAULISTA", "cachoeira_paulista"),
        ("SAO_MARTINHO_DA_SERRA", "sao_martinho_da_serra"),
    ],
)
def test_station_dir_is_the_lowercase_station_key(station: str, directory: str) -> None:
    assert file_paths.station_dir(station) == directory


def test_every_per_station_path_lives_under_the_same_station_directory() -> None:
    paths = [
        file_paths.raw_goes("C03", STATION),
        file_paths.raw_sonda(STATION),
        file_paths.model_input(STATION),
        file_paths.model_save(STATION, "KNN"),
        file_paths.model_metrics(STATION, "KNN"),
        file_paths.model_tuning(STATION, "KNN"),
    ]
    assert all(path.parent.name == "ourinhos" for path in paths)


def test_raw_goes_dir_can_be_overridden_with_another_directory_name() -> None:
    assert file_paths.raw_goes_dir(STATION) == settings.RAW_PATH / "goes" / "ourinhos"
    assert (
        file_paths.raw_goes_dir(STATION, "ourinhos_experiment")
        == settings.RAW_PATH / "goes" / "ourinhos_experiment"
    )


def test_raw_sonda_dir_is_the_station_directory_under_raw_sonda() -> None:
    assert file_paths.raw_sonda_dir(STATION) == settings.RAW_PATH / "sonda" / "ourinhos"
