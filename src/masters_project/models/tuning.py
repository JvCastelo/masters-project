import logging
from typing import Any

from sklearn.base import BaseEstimator
from sklearn.neighbors import KNeighborsRegressor
from xgboost import XGBRegressor

logger = logging.getLogger(__name__)

TUNING_REGISTRY: dict[str, dict[str, Any]] = {
    "XGBOOST": {
        "estimator": XGBRegressor(random_state=42, n_jobs=-1),
        "param_grid": {
            "n_estimators": [50, 100, 150, 200, 300],
            "learning_rate": [0.01, 0.05, 0.1, 0.2],
            "max_depth": [3, 5, 7, 9],
            "subsample": [0.8, 0.9, 1.0],
        },
    },
    "KNN": {
        "estimator": KNeighborsRegressor(n_jobs=-1),
        "param_grid": {
            "n_neighbors": [3, 5, 7, 9, 11, 15],
            "weights": ["uniform", "distance"],
            "p": [1, 2],
        },
    },
}


def get_tuning_config(
    model_name: str,
) -> tuple[BaseEstimator, dict[str, list[Any]]]:
    """Return a sklearn estimator and its hyperparameter grid for RandomizedSearchCV.

    Model names are matched case-insensitively against ``TUNING_REGISTRY`` keys.

    Args:
        model_name: Registry key (e.g. ``\"KNN\"``, ``\"XGBOOST\"``).

    Returns:
        Tuple of ``(estimator, param_distributions)`` for use with
        :class:`sklearn.model_selection.RandomizedSearchCV`.

    Raises:
        ValueError: If ``model_name`` is not in the registry.
    """
    name_upper = model_name.upper()

    if name_upper not in TUNING_REGISTRY:
        available = ", ".join(TUNING_REGISTRY.keys())
        raise ValueError(
            f"No tuning grid found for '{model_name}'. Available: {available}"
        )

    logger.info(f"Loaded tuning configuration for {name_upper}")
    config = TUNING_REGISTRY[name_upper]

    return config["estimator"], config["param_grid"]
