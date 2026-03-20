import logging

from masters_project.models.base import BaseRadiationModel
from masters_project.models.model import KNNModel, XGBoostModel

logger = logging.getLogger(__name__)


MODEL_REGISTRY = {"KNN": KNNModel, "XGBOOST": XGBoostModel}


def build_model(model_name: str, **kwargs) -> BaseRadiationModel:
    """
    Dynamically instantiates a model based on its name and parameters.
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
