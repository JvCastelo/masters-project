"""Tests for masters_project.ml.tuning: estimator and search grid per model."""

import pytest
from sklearn.base import BaseEstimator

from masters_project.ml.tuning import TUNING_REGISTRY, get_tuning_config


@pytest.mark.parametrize("name", ["KNN", "knn", "XGBoost", "xgboost"])
def test_get_tuning_config_is_case_insensitive(name: str) -> None:
    estimator, grid = get_tuning_config(name)
    assert isinstance(estimator, BaseEstimator)
    assert grid and all(isinstance(values, list) and values for values in grid.values())


def test_every_grid_key_is_a_parameter_of_its_estimator() -> None:
    """A typo in the grid would only fail deep inside RandomizedSearchCV."""
    for name in TUNING_REGISTRY:
        estimator, grid = get_tuning_config(name)
        assert set(grid) <= set(estimator.get_params())


def test_get_tuning_config_unknown_model_raises_with_the_available_names() -> None:
    with pytest.raises(ValueError, match="ATLANTIS.*Available: XGBOOST, KNN"):
        get_tuning_config("ATLANTIS")
