"""Tests for masters_project.utils: the time_track decorator."""

import logging

import pytest

from masters_project.utils import time_track


@time_track
def add(a: int, b: int = 0) -> int:
    """Add two numbers."""
    return a + b


def test_time_track_preserves_the_result_and_the_function_metadata() -> None:
    assert add(2, b=3) == 5
    assert add.__name__ == "add"
    assert add.__doc__ == "Add two numbers."


def test_time_track_logs_the_elapsed_time_at_debug(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.DEBUG, logger="masters_project.utils"):
        add(1)
    assert "Function 'add' took" in caplog.text


def test_time_track_does_not_swallow_exceptions() -> None:
    @time_track
    def fail() -> None:
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        fail()
