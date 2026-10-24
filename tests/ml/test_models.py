"""Models built from the project's own config: train, save (creating folders), load, predict."""

from pathlib import Path

import numpy as np
import pytest

from masters_project.ml.factory import build_model
from masters_project.settings import settings


@pytest.mark.parametrize("model_name", list(settings.ml.models))
def test_every_configured_model_builds_from_its_config_hyperparameters(
    model_name: str,
) -> None:
    """Regression: the config sent n_jobs to XGBoost, which did not accept it."""
    hyperparameters = settings.ml.models[model_name].model_dump(exclude_none=True)
    assert build_model(model_name, **hyperparameters) is not None


@pytest.mark.parametrize("model_name", list(settings.ml.models))
def test_model_save_creates_missing_directories_and_roundtrips(
    model_name: str, tmp_path: Path
) -> None:
    """Regression: save() failed with FileNotFoundError when data/models did not exist."""
    rng = np.random.default_rng(0)
    X, y = rng.normal(size=(40, 4)), rng.normal(size=40)
    hyperparameters = settings.ml.models[model_name].model_dump(exclude_none=True)
    model = build_model(model_name, **hyperparameters)
    model.train(X, y)

    path = tmp_path / "not" / "created" / "yet" / "m.joblib"
    model.save(path)

    loaded = build_model(model_name, **hyperparameters)
    loaded.load(path)
    np.testing.assert_allclose(loaded.predict(X), model.predict(X))


@pytest.mark.parametrize("model_name", list(settings.ml.models))
def test_predict_before_training_raises(model_name: str) -> None:
    """Failure mode: predicting with an unfitted scaler must fail loudly."""
    model = build_model(model_name)
    with pytest.raises(ValueError, match="must be trained or loaded"):
        model.predict(np.zeros((2, 4)))
