"""The radiation models: one scikit-learn ``Pipeline`` (scaler, then estimator) per entry of ``MODELS``.

A model is one entry of :data:`MODELS`: its estimator class and the search space used to tune it.
Training and tuning build the *same* ``Pipeline``, so the scaler that is fitted on the training
data is also part of every hyperparameter search. To add a model, add an entry here and its
hyperparameters under ``ml_config.models`` in the config.
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass
from functools import partial
from typing import Any

from sklearn.base import BaseEstimator
from sklearn.neighbors import KNeighborsRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from xgboost import XGBRegressor

logger = logging.getLogger(__name__)

#: Name of the estimator step inside every pipeline; search-space keys are prefixed with it.
ESTIMATOR_STEP = "model"


@dataclass(frozen=True)
class ModelSpec:
    """What it takes to train and to tune one model."""

    estimator: Callable[..., BaseEstimator]
    #: Hyperparameter name -> candidate values, as understood by the estimator itself.
    search_space: dict[str, list[Any]]


MODELS: dict[str, ModelSpec] = {
    "KNN": ModelSpec(
        estimator=KNeighborsRegressor,
        search_space={
            "n_neighbors": [3, 5, 7, 9, 11, 15],
            "weights": ["uniform", "distance"],
            "p": [1, 2],
        },
    ),
    # random_state is fixed so that two runs with the same data give the same model.
    "XGBOOST": ModelSpec(
        estimator=partial(XGBRegressor, random_state=42),
        search_space={
            "n_estimators": [50, 100, 150, 200, 300],
            "learning_rate": [0.01, 0.05, 0.1, 0.2],
            "max_depth": [3, 5, 7, 9],
            "subsample": [0.8, 0.9, 1.0],
        },
    ),
}


def _spec(model_name: str) -> ModelSpec:
    key = model_name.upper()
    if key not in MODELS:
        raise ValueError(
            f"Model '{model_name}' not found. Available models: {', '.join(MODELS)}"
        )
    return MODELS[key]


def build_model(model_name: str, **hyperparameters: Any) -> Pipeline:
    """Build an unfitted pipeline: ``StandardScaler`` followed by the model's estimator.

    The pipeline remembers the training column names (``feature_names_in_``) once fitted on a
    DataFrame, so predictions can select the same columns by name.

    Args:
        model_name: Key of :data:`MODELS`, case-insensitive (``KNN``, ``XGBoost``...).
        **hyperparameters: Constructor arguments of the estimator (``n_neighbors=5``...).

    Returns:
        An unfitted ``Pipeline`` with the steps ``scaler`` and ``model``.

    Raises:
        ValueError: If ``model_name`` is not in :data:`MODELS`.
    """
    spec = _spec(model_name)
    logger.info(f"Building {model_name.upper()} with parameters: {hyperparameters}")
    return Pipeline(
        [
            ("scaler", StandardScaler()),
            (ESTIMATOR_STEP, spec.estimator(**hyperparameters)),
        ]
    )


def search_space(model_name: str) -> dict[str, list[Any]]:
    """Hyperparameter candidates for ``RandomizedSearchCV``, named for the *pipeline*.

    Args:
        model_name: Key of :data:`MODELS`, case-insensitive.

    Returns:
        ``{"model__n_neighbors": [...], ...}``: each key is prefixed with the estimator step,
        which is how a pipeline addresses its estimator's parameters.

    Raises:
        ValueError: If ``model_name`` is not in :data:`MODELS`.
    """
    return {
        f"{ESTIMATOR_STEP}__{name}": list(values)
        for name, values in _spec(model_name).search_space.items()
    }
