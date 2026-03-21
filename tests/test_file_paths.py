"""Tests for masters_project.file_paths: FilePathBuilder."""

from masters_project.file_paths import file_paths
from masters_project.settings import settings


def test_raw_goes_filename_matches_execution_and_channel() -> None:
    """raw_goes builds path under raw/goes with goes_{channel}_st_..._et_..._{station}.csv."""
    channel = "C01"
    path = file_paths.raw_goes(channel)
    assert path.parent == settings.RAW_PATH / "goes"
    assert path.name.startswith(f"goes_{channel}_st_{settings.execution.start_date}_")
    assert settings.execution.selected_station in path.name
    assert path.suffix == ".csv"


def test_raw_sonda_filename_matches_execution_and_etl() -> None:
    """raw_sonda builds path under raw/sonda with sonda_{data_type}_st_..."""
    path = file_paths.raw_sonda()
    assert path.parent == settings.RAW_PATH / "sonda"
    assert settings.etl.sonda.data_type in path.name
    assert path.suffix == ".csv"


def test_model_input_under_processed_with_expected_prefix() -> None:
    """model_input points to processed dir with model_input_st_... prefix."""
    path = file_paths.model_input()
    assert path.parent == settings.PROCESSED_PATH
    assert path.name.startswith("model_input_st_")
    assert path.suffix == ".csv"


def test_model_save_and_model_metrics_suffixes() -> None:
    """model_save uses .joblib under models; model_metrics uses _metrics.json under results."""
    save_path = file_paths.model_save("KNN", version="v1")
    assert save_path.parent == settings.MODELS_PATH
    assert save_path.suffix == ".joblib"
    assert "knn_" in save_path.name.lower()

    metrics_path = file_paths.model_metrics("KNN", version="v1")
    assert metrics_path.parent == settings.RESULTS_PATH
    assert metrics_path.name.endswith("_metrics.json")
