"""Combine the predictions of several stations' models into one, weighted by distance or equally."""

import math
from collections.abc import Mapping

import numpy as np
from numpy.typing import NDArray

from masters_project.settings import StationSettings

EARTH_RADIUS_KM = 6371.0088


def haversine_km(a: StationSettings, b: StationSettings) -> float:
    """Great-circle distance between two stations, in kilometres."""
    lat_a, lat_b = math.radians(a.latitude), math.radians(b.latitude)
    d_lat = lat_b - lat_a
    d_lon = math.radians(b.longitude - a.longitude)
    h = (
        math.sin(d_lat / 2) ** 2
        + math.cos(lat_a) * math.cos(lat_b) * math.sin(d_lon / 2) ** 2
    )
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(h))


def idw_weights(distances_km: Mapping[str, float]) -> dict[str, float]:
    """Inverse-distance weights: proportional to ``1 / distance``, normalised to sum to 1.

    With two members at distances d1 and d2 this is ``d2 / (d1 + d2)`` for the first, so the
    closer station weighs more.

    Raises:
        ValueError: If a distance is zero (the member is the target station itself).
    """
    zero = [name for name, distance in distances_km.items() if distance <= 0]
    if zero:
        raise ValueError(
            f"Inverse-distance weights are undefined for {zero}: it is the target station. "
            "Leave it out of the ensemble or use equal weights."
        )
    inverse = {name: 1.0 / distance for name, distance in distances_km.items()}
    total = sum(inverse.values())
    return {name: value / total for name, value in inverse.items()}


def ensemble_weights(
    scheme: str,
    target: str,
    members: list[str],
    stations: Mapping[str, StationSettings],
) -> dict[str, float]:
    """Weights of an ensemble's members when predicting at ``target``.

    Args:
        scheme: ``"idw"`` or ``"equal"``.
        target: Station being predicted.
        members: Stations whose models are combined.
        stations: Coordinates of every station (``etl_config.stations``).

    Raises:
        ValueError: For an unknown scheme, a station without coordinates, or IDW with the
            target itself as a member.
    """
    if scheme == "equal":
        return {name: 1.0 / len(members) for name in members}
    if scheme != "idw":
        raise ValueError(f"Unknown weighting '{scheme}'. Use 'idw' or 'equal'.")

    missing = [name for name in [target, *members] if name not in stations]
    if missing:
        raise ValueError(
            f"No coordinates for {missing} in etl_config.stations; they are needed for IDW."
        )
    return idw_weights(
        {name: haversine_km(stations[target], stations[name]) for name in members}
    )


def combine(
    predictions: Mapping[str, NDArray[np.float64]], weights: Mapping[str, float]
) -> NDArray[np.float64]:
    """Weighted sum of the members' predictions (the weights already sum to 1)."""
    return sum(weights[name] * predictions[name] for name in weights)
