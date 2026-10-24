"""Radiation regressors: a base class that scales features before an estimator, and the KNN and
XGBoost models built on it."""

import logging
from abc import ABC
from pathlib import Path

import joblib
import numpy as np
from numpy.typing import ArrayLike, NDArray
from sklearn.neighbors import KNeighborsRegressor
from sklearn.preprocessing import StandardScaler
from xgboost import XGBRegressor

logger = logging.getLogger(__name__)


class BaseRadiationModel(ABC):
    """Abstract base for scikit-learn regressors wrapped with StandardScaler.

    Subclasses set ``self.model`` to a fitted estimator. Training scales features;
    prediction applies the same scaler.
    """

    def __init__(self) -> None:
        """Create an unfitted model: no estimator yet, a fresh scaler, ``is_trained`` False."""
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
            filepath: Destination path for the serialized bundle. Missing parent
                directories are created.
        """
        filepath.parent.mkdir(parents=True, exist_ok=True)
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


class KNNModel(BaseRadiationModel):
    """K-nearest neighbors regressor for radiation prediction."""

    def __init__(self, n_neighbors: int = 5, n_jobs: int = -1) -> None:
        """Build the regressor.

        Args:
            n_neighbors: Number of neighbours that vote on each prediction.
            n_jobs: Parallel jobs for the neighbour search (-1 uses every core).
        """
        super().__init__()

        self.model = KNeighborsRegressor(n_neighbors=n_neighbors, n_jobs=n_jobs)


class XGBoostModel(BaseRadiationModel):
    """Gradient boosted trees (XGBoost) regressor for radiation prediction."""

    def __init__(
        self, n_estimators: int = 100, learning_rate: float = 0.1, n_jobs: int = -1
    ) -> None:
        """Build the regressor (``random_state`` is fixed at 42 for reproducible runs).

        Args:
            n_estimators: Number of boosted trees.
            learning_rate: Shrinkage applied to each tree.
            n_jobs: Parallel threads (-1 uses every core).
        """
        super().__init__()
        self.model = XGBRegressor(
            n_estimators=n_estimators,
            learning_rate=learning_rate,
            random_state=42,
            n_jobs=n_jobs,
        )
