"""Tests for masters_project.etl.schemas: structure raises, quality only warns."""

import logging

import numpy as np
import pandas as pd
import pytest

from masters_project.etl.schemas import (
    DataValidationError,
    validate_model_input,
    validate_sonda,
)
from masters_project.etl.goes.reader import GoesProcessor

TARGET = "glo_avg"
PIXELS = GoesProcessor.window_columns(1, "C03") + GoesProcessor.window_columns(1, "C13")


def _sonda(n: int = 5) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.date_range(
                "2022-06-21 15:00", periods=n, freq="1min", tz="UTC"
            ),
            TARGET: np.linspace(400.0, 410.0, n),
        }
    )


def _model_input(n: int = 5) -> pd.DataFrame:
    df = _sonda(n)
    features = pd.DataFrame(
        np.arange(n * len(PIXELS), dtype=float).reshape(n, len(PIXELS)), columns=PIXELS
    )
    return pd.concat([df, features], axis=1)


# =============================================================================
# SONDA
# =============================================================================


def test_validate_sonda_accepts_a_well_formed_frame() -> None:
    validate_sonda(_sonda(), TARGET)


def test_validate_sonda_accepts_missing_target_values() -> None:
    """SONDA has real gaps: a NaN target is not a structural error."""
    df = _sonda()
    df.loc[2, TARGET] = np.nan
    validate_sonda(df, TARGET)


def test_validate_sonda_rejects_naive_timestamps() -> None:
    df = _sonda()
    df["timestamp"] = df["timestamp"].dt.tz_localize(None)
    with pytest.raises(DataValidationError, match="timezone-aware UTC"):
        validate_sonda(df, TARGET)


def test_validate_sonda_rejects_repeated_timestamps() -> None:
    df = pd.concat([_sonda(), _sonda().iloc[[0]]], ignore_index=True)
    with pytest.raises(DataValidationError, match="timestamp"):
        validate_sonda(df, TARGET)


def test_validate_sonda_rejects_unsorted_timestamps() -> None:
    df = _sonda().iloc[::-1].reset_index(drop=True)
    with pytest.raises(DataValidationError, match="sorted ascending"):
        validate_sonda(df, TARGET)


def test_validate_sonda_rejects_unexpected_or_missing_columns() -> None:
    with pytest.raises(DataValidationError, match="extra"):
        validate_sonda(_sonda().assign(extra=1), TARGET)
    with pytest.raises(DataValidationError, match=TARGET):
        validate_sonda(_sonda().drop(columns=[TARGET]), TARGET)


def test_validate_sonda_out_of_range_target_only_warns(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Quality rule: a defective sensor is logged with its extent, the frame is kept."""
    df = _sonda()
    df.loc[1, TARGET] = -765.0
    with caplog.at_level(logging.WARNING):
        validate_sonda(df, TARGET, label="[CPA]")
    assert "[CPA]: 1 'glo_avg' values outside [-100, 2000]" in caplog.text
    assert "min -765" in caplog.text


def test_validate_sonda_in_range_target_does_not_warn(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Midday cloud-enhancement peaks above the solar constant are real, not defects."""
    df = _sonda()
    df.loc[1, TARGET] = 1657.0
    with caplog.at_level(logging.WARNING):
        validate_sonda(df, TARGET)
    assert caplog.text == ""


# =============================================================================
# model input
# =============================================================================


def test_validate_model_input_accepts_a_well_formed_frame() -> None:
    validate_model_input(_model_input(), TARGET)


def test_validate_model_input_rejects_a_missing_value() -> None:
    df = _model_input()
    df.loc[2, "radius=1_C03_22"] = np.nan
    with pytest.raises(DataValidationError, match="radius=1_C03_22"):
        validate_model_input(df, TARGET)


def test_validate_model_input_rejects_a_missing_target() -> None:
    df = _model_input()
    df.loc[0, TARGET] = np.nan
    with pytest.raises(DataValidationError, match=TARGET):
        validate_model_input(df, TARGET)


def test_validate_model_input_rejects_a_frame_without_goes_features() -> None:
    with pytest.raises(DataValidationError):
        validate_model_input(_sonda(), TARGET)


def test_validate_model_input_rejects_an_incomplete_window() -> None:
    """Dropping one pixel of a 3x3 window must be caught, not silently accepted."""
    df = _model_input().drop(columns=["radius=1_C03_11"])
    with pytest.raises(DataValidationError, match="all the pixels of its window"):
        validate_model_input(df, TARGET)


def test_validate_model_input_rejects_a_stray_column() -> None:
    with pytest.raises(DataValidationError, match="stray"):
        validate_model_input(_model_input().assign(stray=1.0), TARGET)


def test_validation_error_lists_the_failed_checks_with_examples() -> None:
    df = _model_input()
    df.loc[0, "radius=1_C13_33"] = np.nan
    with pytest.raises(DataValidationError) as error:
        validate_model_input(df, TARGET, label="[OURINHOS]")
    message = str(error.value)
    assert message.startswith("[OURINHOS] failed validation:")
    assert "radius=1_C13_33" in message and "1 failures" in message
