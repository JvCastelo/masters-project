"""Tests for masters_project.models.factory: build_model."""

import pytest

from masters_project.models.factory import build_model
from masters_project.models.model import KNNModel, XGBoostModel


def test_build_model_knn_returns_knn_model() -> None:
    """Happy path: KNN registry key returns a KNNModel instance."""
    model = build_model("KNN", n_neighbors=5, n_jobs=-1)
    assert isinstance(model, KNNModel)


def test_build_model_xgboost_registry_key_returns_xgboost_model() -> None:
    """Happy path: XGBOOST registry key (case-insensitive) returns XGBoostModel."""
    model = build_model("xgboost", n_estimators=50, learning_rate=0.1)
    assert isinstance(model, XGBoostModel)


def test_build_model_unknown_name_raises_valueerror() -> None:
    """Failure mode: unknown model name raises ValueError listing available models."""
    with pytest.raises(ValueError, match="not found"):
        build_model("UNKNOWN_MODEL")
