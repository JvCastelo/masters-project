import logging
from abc import ABC
from pathlib import Path

import joblib
import numpy as np
from numpy.typing import ArrayLike, NDArray
from sklearn.preprocessing import StandardScaler

logger = logging.getLogger(__name__)


class BaseRadiationModel(ABC):
    """Abstract base for scikit-learn regressors wrapped with StandardScaler.

    Subclasses set ``self.model`` to a fitted estimator. Training scales features;
    prediction applies the same scaler.
    """

    def __init__(self) -> None:
        self.model = None
        self.scaler = StandardScaler()
        self.is_trained = False

    def train(self, X_train: ArrayLike, y_train: ArrayLike) -> None:
        """Fit the scaler and underlying estimator on training data.

        Args:
            X_train: Feature matrix (2D array-like).
            y_train: Target vector (1D array-like).
        """
        logger.info(f"Training {self.__class__.__name__}...")
        X_train_scaled = self.scaler.fit_transform(X_train)
        self.model.fit(X_train_scaled, y_train)
        self.is_trained = True

    def predict(self, X_new: ArrayLike) -> NDArray[np.float64]:
        """Predict targets for new features using the fitted scaler and model.

        Args:
            X_new: Feature matrix (2D array-like).

        Returns:
            Predicted values as a 1D float array.

        Raises:
            ValueError: If the model has not been trained or loaded.
        """
        if not self.is_trained:
            raise ValueError("Model must be trained or loaded before predicting!")
        X_new_scaled = self.scaler.transform(X_new)
        return self.model.predict(X_new_scaled)

    def save(self, filepath: Path) -> None:
        """Persist model, scaler, and training flag with joblib.

        Args:
            filepath: Destination path for the serialized bundle.
        """
        joblib.dump(
            {"model": self.model, "scaler": self.scaler, "is_trained": self.is_trained},
            filepath,
        )
        logger.info(f"Model saved to {filepath}")

    def load(self, filepath: str | Path) -> None:
        """Load model state from a joblib file produced by :meth:`save`.

        Args:
            filepath: Path to the saved bundle (string or Path).
        """
        package = joblib.load(filepath)
        self.model = package["model"]
        self.scaler = package["scaler"]
        self.is_trained = package["is_trained"]
        logger.info(f"Model loaded from {filepath}")
