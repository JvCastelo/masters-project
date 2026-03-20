import logging
from abc import ABC

import joblib
from sklearn.preprocessing import StandardScaler

logger = logging.getLogger(__name__)


class BaseRadiationModel(ABC):
    def __init__(self):
        self.model = None
        self.scaler = StandardScaler()
        self.is_trained = False

    def train(self, X_train, y_train):
        logger.info(f"Training {self.__class__.__name__}...")
        X_train_scaled = self.scaler.fit_transform(X_train)
        self.model.fit(X_train_scaled, y_train)
        self.is_trained = True

    def predict(self, X_new):
        if not self.is_trained:
            raise ValueError("Model must be trained or loaded before predicting!")
        X_new_scaled = self.scaler.transform(X_new)
        return self.model.predict(X_new_scaled)

    def save(self, filepath: str):
        joblib.dump(
            {"model": self.model, "scaler": self.scaler, "is_trained": self.is_trained},
            filepath,
        )
        logger.info(f"Model saved to {filepath}")

    def load(self, filepath: str):
        package = joblib.load(filepath)
        self.model = package["model"]
        self.scaler = package["scaler"]
        self.is_trained = package["is_trained"]
        logger.info(f"Model loaded from {filepath}")
