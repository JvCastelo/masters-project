"""Tests for masters_project.utils: the time_track decorator."""

import hashlib
import logging
import re

import pytest

from masters_project.utils import file_sha256, time_track, utc_now_iso


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


def test_file_sha256_matches_hashlib_for_a_file_larger_than_one_block(tmp_path) -> None:
    content = b"abc" * 1_000_000  # 3 MB: several read blocks
    path = tmp_path / "data.bin"
    path.write_bytes(content)

    assert file_sha256(path) == hashlib.sha256(content).hexdigest()


def test_utc_now_iso_is_a_second_resolution_utc_timestamp() -> None:
    assert re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\+00:00", utc_now_iso())
