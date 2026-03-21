import logging

from masters_project.models.base import BaseRadiationModel
from masters_project.models.model import KNNModel, XGBoostModel

logger = logging.getLogger(__name__)


MODEL_REGISTRY = {"KNN": KNNModel, "XGBOOST": XGBoostModel}


def build_model(model_name: str, **kwargs: object) -> BaseRadiationModel:
    """Instantiate a radiation model class by registry name.

    Names are matched case-insensitively against ``MODEL_REGISTRY`` keys.

    Args:
        model_name: Registry key (e.g. ``KNN``, ``XGBOOST``).
        **kwargs: Constructor arguments passed to the model class.

    Returns:
        A concrete ``BaseRadiationModel`` instance.

    Raises:
        ValueError: If ``model_name`` does not match any registry entry.
    """
    name_upper = model_name.upper()

    if name_upper not in MODEL_REGISTRY:
        available = ", ".join(MODEL_REGISTRY.keys())
        raise ValueError(
            f"Model '{model_name}' not found. Available models: {available}"
        )

    ModelClass = MODEL_REGISTRY[name_upper]

    logger.info(f"Building {name_upper} with parameters: {kwargs}")
    return ModelClass(**kwargs)
