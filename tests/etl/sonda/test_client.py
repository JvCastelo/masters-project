"""Tests for masters_project.etl.sonda.client: SondaClient."""

import pytest

from masters_project.etl.sonda.client import SondaClient


def test_sonda_client_invalid_station_raises_valueerror() -> None:
    """Critical failure: unknown station name must raise ValueError with valid options listed."""
    with pytest.raises(ValueError, match="not recognized"):
        SondaClient(station="INVALID_STATION", data_type="BSRN")
