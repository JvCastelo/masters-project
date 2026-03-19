from sklearn.neighbors import KNeighborsRegressor
from xgboost import XGBRegressor

from masters_project.models.base import BaseRadiationModel


class KNNModel(BaseRadiationModel):
    def __init__(self, n_neighbors: int = 5):
        super().__init__()

        self.model = KNeighborsRegressor(n_neighbors=n_neighbors, n_jobs=-1)


class XGBoostModel(BaseRadiationModel):
    def __init__(self, n_estimators: int = 100, learning_rate: float = 0.1):
        super().__init__()
        self.model = XGBRegressor(
            n_estimators=n_estimators,
            learning_rate=learning_rate,
            random_state=42,
            n_jobs=-1,
        )
