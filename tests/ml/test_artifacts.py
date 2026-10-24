"""Tests for masters_project.ml.artifacts: a model saved with its card, and the metadata helpers."""

import json
import logging

import numpy as np
import pandas as pd
import pytest

from masters_project.file_paths import file_paths
from masters_project.ml.artifacts import (
    card_path,
    dataset_fingerprint,
    display_path,
    estimator_parameters,
    jsonable,
    load_model,
    read_manifest,
    run_metadata,
    save_model,
)
from masters_project.ml.models import build_model
from masters_project.settings import settings
from masters_project.utils import file_sha256

STATION = "OURINHOS"


def _fitted():
    X = pd.DataFrame({"a": [1.0, 2.0, 3.0, 4.0], "b": [4.0, 3.0, 2.0, 1.0]})
    return build_model("KNN", n_neighbors=2).fit(X, [1.0, 2.0, 3.0, 4.0]), X


def _write_input(station: str = STATION) -> None:
    path = file_paths.model_input(station)
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"timestamp": ["2022-06-21 12:00:00+00:00"], "glo_avg": [1.0]}).to_csv(
        path, index=False
    )


def test_jsonable_converts_numpy_and_unknown_values() -> None:
    value = {
        "n": np.int64(3),
        "x": np.float32(1.5),
        "list": (1, np.bool_(True)),
        "obj": object,
    }

    result = jsonable(value)

    assert result["n"] == 3 and type(result["n"]) is int
    assert result["list"] == [1, True]
    assert isinstance(result["obj"], str)
    json.dumps(result)


def test_run_metadata_says_which_code_and_libraries_produced_a_result() -> None:
    metadata = run_metadata()

    assert set(metadata) == {"git_revision", "created_at", "versions"}
    assert {"python", "pandas", "numpy", "scikit-learn", "xgboost"} <= set(
        metadata["versions"]
    )
    assert metadata["created_at"].endswith("+00:00")


def test_display_path_is_relative_inside_the_project_and_absolute_outside(
    tmp_path,
) -> None:
    assert display_path(settings.PROJECT_ROOT / "data" / "x.csv") == "data/x.csv"
    assert display_path(tmp_path / "x.csv") == str(tmp_path / "x.csv")


def test_estimator_parameters_are_json_safe() -> None:
    model, _ = _fitted()
    parameters = estimator_parameters(model)

    assert parameters["n_neighbors"] == 2
    json.dumps(parameters)


def test_save_model_writes_the_pipeline_and_a_card_beside_it(tmp_path) -> None:
    model, X = _fitted()
    path = tmp_path / "models" / "ourinhos" / "knn.joblib"

    save_model(model, path, {"station": STATION, "rows": {"train": np.int64(4)}})

    assert card_path(path) == tmp_path / "models" / "ourinhos" / "knn.json"
    loaded, card = load_model(path)
    np.testing.assert_allclose(loaded.predict(X), model.predict(X))
    assert list(loaded.feature_names_in_) == ["a", "b"]
    assert card == {"station": STATION, "rows": {"train": 4}}


def test_load_model_without_a_card_returns_none_for_it(tmp_path) -> None:
    model, _ = _fitted()
    path = tmp_path / "knn.joblib"
    save_model(model, path, {"station": STATION})
    card_path(path).unlink()

    assert load_model(path)[1] is None


def test_load_model_missing_file_says_how_to_train_it(tmp_path) -> None:
    with pytest.raises(FileNotFoundError, match=r"No trained model at .*ml\.train"):
        load_model(tmp_path / "absent.joblib")


def test_read_manifest_returns_what_build_dataset_wrote(data_tree, one_station) -> None:
    path = file_paths.model_input_manifest(STATION)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"filters": {"min_solar_elevation": 5.0}}))

    assert read_manifest(STATION) == {"filters": {"min_solar_elevation": 5.0}}


def test_read_manifest_warns_when_the_dataset_has_none(
    data_tree, one_station, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.WARNING):
        assert read_manifest(STATION) is None

    assert "filters it was built with are unknown" in caplog.text


def test_dataset_fingerprint_hashes_the_file_and_attaches_the_manifest(
    data_tree, one_station
) -> None:
    _write_input()
    manifest = file_paths.model_input_manifest(STATION)
    manifest.write_text(json.dumps({"rows": 1}))

    fingerprint = dataset_fingerprint(STATION)

    assert fingerprint["sha256"] == file_sha256(file_paths.model_input(STATION))
    assert fingerprint["manifest"] == {"rows": 1}
    assert fingerprint["path"].endswith(file_paths.model_input(STATION).name)
