"""Tests for masters_project.ml.models: every model is a scaler followed by an estimator."""

import joblib
import numpy as np
import pandas as pd
import pytest
from sklearn.exceptions import NotFittedError
from sklearn.model_selection import RandomizedSearchCV
from sklearn.pipeline import Pipeline

from masters_project.ml.models import MODELS, build_model, search_space
from masters_project.settings import settings


def _data(n: int = 60) -> tuple[pd.DataFrame, pd.Series]:
    """Two features on very different scales, and a target that depends on both."""
    rng = np.random.default_rng(0)
    X = pd.DataFrame({"small": rng.normal(size=n), "large": rng.normal(size=n) * 1000})
    y = pd.Series(X["small"] * 3 + X["large"] / 500, name="glo_avg")
    return X, y


@pytest.mark.parametrize("model_name", list(settings.ml.models))
def test_every_configured_model_builds_from_its_config_hyperparameters(
    model_name: str,
) -> None:
    """Regression: the config sent n_jobs to XGBoost, which did not accept it."""
    model = build_model(model_name, **settings.ml.models[model_name])

    assert isinstance(model, Pipeline)
    assert [name for name, _ in model.steps] == ["scaler", "model"]


@pytest.mark.parametrize("name", ["KNN", "knn", "XGBoost", "xgboost"])
def test_model_names_are_case_insensitive(name: str) -> None:
    assert isinstance(build_model(name), Pipeline)
    assert search_space(name)


def test_unknown_model_raises_with_the_available_names() -> None:
    with pytest.raises(ValueError, match="ATLANTIS.*Available models: KNN, XGBOOST"):
        build_model("ATLANTIS")
    with pytest.raises(ValueError, match="ATLANTIS"):
        search_space("ATLANTIS")


def test_the_scaler_is_fitted_on_the_training_data_and_applied_before_the_estimator() -> (
    None
):
    X, y = _data()
    model = build_model("KNN", n_neighbors=3).fit(X, y)

    scaler = model.named_steps["scaler"]
    np.testing.assert_allclose(scaler.mean_, X.mean().to_numpy())
    by_hand = model.named_steps["model"].predict(scaler.transform(X))
    np.testing.assert_allclose(model.predict(X), by_hand)


def test_a_fitted_pipeline_remembers_the_training_column_names() -> None:
    """The prediction step relies on this to select columns by name, whatever their order."""
    X, y = _data()
    model = build_model("KNN").fit(X, y)

    assert list(model.feature_names_in_) == ["small", "large"]


def test_predicting_before_fitting_fails_loudly() -> None:
    with pytest.raises(NotFittedError):
        build_model("KNN").predict(np.zeros((2, 2)))


def test_xgboost_gives_the_same_model_on_every_run() -> None:
    X, y = _data()
    first = build_model("XGBOOST", n_estimators=20).fit(X, y).predict(X)
    second = build_model("XGBOOST", n_estimators=20).fit(X, y).predict(X)

    assert build_model("XGBOOST").named_steps["model"].random_state == 42
    np.testing.assert_allclose(first, second)


def test_a_fitted_pipeline_survives_a_joblib_round_trip(tmp_path) -> None:
    X, y = _data()
    model = build_model("KNN", n_neighbors=3).fit(X, y)

    joblib.dump(model, tmp_path / "m.joblib")
    loaded = joblib.load(tmp_path / "m.joblib")

    np.testing.assert_allclose(loaded.predict(X), model.predict(X))
    assert list(loaded.feature_names_in_) == ["small", "large"]


@pytest.mark.parametrize("name", list(MODELS))
def test_every_search_key_is_a_parameter_of_the_pipeline(name: str) -> None:
    """A typo here would only fail deep inside RandomizedSearchCV."""
    assert set(search_space(name)) <= set(build_model(name).get_params())


def test_search_space_is_a_copy_of_the_registry() -> None:
    space = search_space("KNN")
    space["model__n_neighbors"].append(999)

    assert 999 not in MODELS["KNN"].search_space["n_neighbors"]


def test_the_search_runs_on_the_pipeline_so_the_scaler_is_part_of_it() -> None:
    """Regression: tuning used the bare estimator, while training scaled the features."""
    X, y = _data()
    search = RandomizedSearchCV(
        build_model("KNN"),
        search_space("KNN"),
        n_iter=3,
        cv=2,
        scoring="neg_root_mean_squared_error",
        random_state=0,
    ).fit(X, y)

    best = search.best_estimator_
    assert isinstance(best, Pipeline)
    np.testing.assert_allclose(best.named_steps["scaler"].mean_, X.mean().to_numpy())
    assert set(search.best_params_) <= set(search_space("KNN"))
