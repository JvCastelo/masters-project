"""Tests for masters_project.ml.ensemble: distances, inverse-distance weights, combination."""

import numpy as np
import pytest

from masters_project.ml.ensemble import (
    combine,
    ensemble_weights,
    haversine_km,
    idw_weights,
)
from masters_project.settings import StationSettings

ORIGIN = StationSettings(latitude=0.0, longitude=0.0)
STATIONS = {
    "OURINHOS": StationSettings(latitude=-23.0, longitude=-49.844),
    "CACHOEIRA_PAULISTA": StationSettings(latitude=-22.69, longitude=-45.006),
    "SAO_MARTINHO_DA_SERRA": StationSettings(latitude=-29.443, longitude=-53.823),
}


def test_one_degree_of_latitude_is_about_111_km() -> None:
    assert haversine_km(
        ORIGIN, StationSettings(latitude=1.0, longitude=0.0)
    ) == pytest.approx(111.19, abs=0.05)


def test_distance_is_symmetric_and_zero_to_itself() -> None:
    a, b = STATIONS["OURINHOS"], STATIONS["CACHOEIRA_PAULISTA"]
    assert haversine_km(a, b) == pytest.approx(haversine_km(b, a))
    assert haversine_km(a, a) == 0.0


def test_the_stations_of_the_project_are_hundreds_of_kilometres_apart() -> None:
    assert haversine_km(
        STATIONS["OURINHOS"], STATIONS["CACHOEIRA_PAULISTA"]
    ) == pytest.approx(497, abs=2)
    assert haversine_km(
        STATIONS["OURINHOS"], STATIONS["SAO_MARTINHO_DA_SERRA"]
    ) == pytest.approx(819, abs=2)


def test_idw_weights_sum_to_one_and_favour_the_closest_member() -> None:
    weights = idw_weights({"near": 100.0, "far": 300.0})

    assert sum(weights.values()) == pytest.approx(1.0)
    assert weights["near"] == pytest.approx(0.75)
    assert weights["near"] > weights["far"]


def test_idw_with_two_members_is_the_formula_of_the_original_notebook() -> None:
    """w1 = d2 / (d1 + d2): the weight of a member is the OTHER member's distance share."""
    d1, d2 = 497.0, 819.0
    weights = idw_weights({"a": d1, "b": d2})

    assert weights["a"] == pytest.approx(d2 / (d1 + d2))
    assert weights["b"] == pytest.approx(d1 / (d1 + d2))


def test_idw_rejects_a_member_at_distance_zero() -> None:
    with pytest.raises(
        ValueError, match=r"undefined for \['OURINHOS'\].*target station"
    ):
        idw_weights({"OURINHOS": 0.0, "OTHER": 100.0})


def test_ensemble_weights_idw_uses_the_distances_from_the_target() -> None:
    weights = ensemble_weights(
        "idw", "OURINHOS", ["CACHOEIRA_PAULISTA", "SAO_MARTINHO_DA_SERRA"], STATIONS
    )

    assert sum(weights.values()) == pytest.approx(1.0)
    assert weights["CACHOEIRA_PAULISTA"] == pytest.approx(0.6223, abs=1e-3)


def test_ensemble_weights_equal_is_a_plain_mean() -> None:
    weights = ensemble_weights("equal", "OURINHOS", ["A", "B", "C", "D"], {})
    assert weights == {"A": 0.25, "B": 0.25, "C": 0.25, "D": 0.25}


def test_idw_with_the_target_itself_as_a_member_fails() -> None:
    with pytest.raises(ValueError, match="target station"):
        ensemble_weights(
            "idw", "OURINHOS", ["OURINHOS", "CACHOEIRA_PAULISTA"], STATIONS
        )


def test_idw_needs_coordinates_for_every_station() -> None:
    with pytest.raises(ValueError, match=r"No coordinates for \['GHOST'\]"):
        ensemble_weights("idw", "OURINHOS", ["GHOST", "CACHOEIRA_PAULISTA"], STATIONS)


def test_an_unknown_weighting_scheme_is_rejected() -> None:
    with pytest.raises(ValueError, match="Unknown weighting 'median'"):
        ensemble_weights("median", "OURINHOS", ["A", "B"], STATIONS)


def test_combine_is_the_weighted_sum_of_the_members() -> None:
    predictions = {
        "a": np.array([10.0, 20.0]),
        "b": np.array([30.0, 40.0]),
        "unused": np.array([0.0, 0.0]),
    }

    result = combine(predictions, {"a": 0.25, "b": 0.75})

    np.testing.assert_allclose(result, [25.0, 35.0])
