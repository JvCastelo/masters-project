"""A trained model is saved together with a card that records how it was made.

``save_model`` writes ``<name>.joblib`` (the fitted pipeline) and ``<name>.json`` next to it
(the card). The card and the reports built from :func:`run_metadata` and
:func:`dataset_fingerprint` answer, months later, which data, which hyperparameters, which
code revision and which library versions produced a number.
"""

import json
import logging
import sys
from importlib.metadata import version
from pathlib import Path
from typing import Any

import joblib
import numpy as np
from sklearn.pipeline import Pipeline

from masters_project.exporters import JSONExporter
from masters_project.file_paths import file_paths
from masters_project.logging_setup import git_revision
from masters_project.ml.models import ESTIMATOR_STEP
from masters_project.settings import settings
from masters_project.utils import file_sha256, utc_now_iso

logger = logging.getLogger(__name__)


def display_path(path: Path) -> str:
    """Path relative to the project root when it lies inside it, else as given."""
    try:
        return str(path.relative_to(settings.PROJECT_ROOT))
    except ValueError:
        return str(path)


def jsonable(value: Any) -> Any:
    """Convert numpy scalars and other non-JSON values to plain Python ones."""
    if isinstance(value, dict):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(item) for item in value]
    if isinstance(value, np.generic):
        return value.item()
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def run_metadata() -> dict[str, Any]:
    """Code revision, library versions and the time, to stamp a model or a report."""
    return {
        "git_revision": git_revision(),
        "created_at": utc_now_iso(),
        "versions": {
            "python": sys.version.split()[0],
            **{
                name: version(name)
                for name in ("pandas", "numpy", "scikit-learn", "xgboost")
            },
        },
    }


def read_manifest(station: str) -> dict[str, Any] | None:
    """The manifest ``build_dataset`` wrote next to the station's model input, if there is one."""
    path = file_paths.model_input_manifest(station)
    if not path.exists():
        logger.warning(
            f"[{station}] No dataset manifest at {path}: the filters it was built with "
            "are unknown. Rebuild it with `python -m masters_project.etl.build_dataset`."
        )
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def dataset_fingerprint(station: str) -> dict[str, Any]:
    """Which file, which content and which manifest a station's model input is."""
    path = file_paths.model_input(station)
    return {
        "path": display_path(path),
        "sha256": file_sha256(path),
        "manifest": read_manifest(station),
    }


def estimator_parameters(pipeline: Pipeline) -> dict[str, Any]:
    """All parameters of the pipeline's estimator, made JSON-safe."""
    return jsonable(pipeline.named_steps[ESTIMATOR_STEP].get_params())


def card_path(model_path: Path) -> Path:
    """The card of a model is the sibling file with the ``.json`` suffix."""
    return model_path.with_suffix(".json")


def save_model(pipeline: Pipeline, path: Path, card: dict[str, Any]) -> None:
    """Write the fitted pipeline and its card; missing directories are created."""
    path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipeline, path)
    JSONExporter().export(jsonable(card), card_path(path))
    logger.info(f"Model saved to {path}")


def load_model(path: Path) -> tuple[Pipeline, dict[str, Any] | None]:
    """Read a model and its card (``None`` if the card is missing).

    Raises:
        FileNotFoundError: If there is no model at ``path``; the message says how to train it.
    """
    if not path.exists():
        raise FileNotFoundError(
            f"No trained model at {path}. Train it with "
            "`python -m masters_project.ml.train`."
        )
    card = card_path(path)
    return joblib.load(path), (
        json.loads(card.read_text(encoding="utf-8")) if card.exists() else None
    )
