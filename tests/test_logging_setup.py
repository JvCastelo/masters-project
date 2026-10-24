"""Tests for masters_project.logging_setup: one log per run, header, footer, clean teardown."""

import logging
import re
import subprocess
from datetime import datetime
from pathlib import Path

import pytest

from masters_project import logging_setup
from masters_project.logging_setup import (
    git_revision,
    new_log_path,
    run_logging,
)
from masters_project.settings import settings

logger = logging.getLogger("tests.logging_setup")


class FrozenDatetime:
    """Stand-in for ``datetime`` in the module under test: the clock is a class attribute."""

    current = datetime(2026, 10, 24, 13, 5, 9)

    @classmethod
    def now(cls) -> datetime:
        return cls.current


@pytest.fixture
def frozen_clock(monkeypatch: pytest.MonkeyPatch) -> type[FrozenDatetime]:
    monkeypatch.setattr(logging_setup, "datetime", FrozenDatetime)
    return FrozenDatetime


def _text(run) -> str:
    return run.path.read_text(encoding="utf-8")


# =============================================================================
# file naming
# =============================================================================


def test_new_log_path_is_a_timestamped_file_in_the_pipeline_directory(
    isolated_logs: Path, frozen_clock
) -> None:
    path = new_log_path("etl_goes")

    assert path == isolated_logs / "etl_goes" / "2026-10-24_130509.log"
    assert path.parent.is_dir()


def test_new_log_path_numbers_a_second_run_started_in_the_same_second(
    frozen_clock,
) -> None:
    first = new_log_path("demo")
    first.touch()
    second = new_log_path("demo")
    second.touch()

    assert second.name == "2026-10-24_130509-2.log"
    assert new_log_path("demo").name == "2026-10-24_130509-3.log"


def test_each_run_gets_its_own_file_and_latest_points_to_the_newest(
    isolated_logs: Path, frozen_clock
) -> None:
    with run_logging("demo") as first:
        logger.info("first")
    frozen_clock.current = datetime(2026, 10, 24, 13, 5, 10)
    with run_logging("demo") as second:
        logger.info("second")

    assert first.path != second.path
    assert "first" in _text(first) and "second" not in _text(first)
    latest = isolated_logs / "demo" / "latest.log"
    assert latest.is_symlink()
    assert latest.resolve() == second.path.resolve()


def test_run_still_works_where_symlinks_are_not_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def no_symlink(self, target, *args, **kwargs):
        raise OSError("symlinks not supported")

    monkeypatch.setattr(Path, "symlink_to", no_symlink)

    with run_logging("demo") as run:
        logger.info("still logged")

    assert "still logged" in _text(run)


# =============================================================================
# header and footer
# =============================================================================


def test_header_says_what_produced_the_log(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(logging_setup, "git_revision", lambda: "abc1234-dirty")

    with run_logging(
        "demo", period="2022-01-01..2022-12-31", stations=["A", "B"]
    ) as run:
        pass

    text = _text(run)
    assert "Run started: pipeline=demo" in text
    assert "Command: " in text
    assert "Revision: abc1234-dirty | Python 3." in text
    assert f"Config: {settings.PIPELINE_CONFIG_FILE}" in text
    assert f"Log: {run.path}" in text
    assert "  period: 2022-01-01..2022-12-31" in text
    assert "  stations: ['A', 'B']" in text


def test_footer_reports_status_duration_and_exit_code() -> None:
    with run_logging("demo") as run:
        run.exit_code = 0

    assert re.search(
        r"Run finished: status=ok duration=00:00:0\d exit_code=0", _text(run)
    )


def test_a_nonzero_exit_code_is_a_failed_run_logged_as_an_error() -> None:
    with run_logging("demo") as run:
        run.exit_code = 1

    lines = [line for line in _text(run).splitlines() if "Run finished" in line]
    assert "status=failed" in lines[0] and "| ERROR" in lines[0]


def test_lines_follow_the_documented_format() -> None:
    with run_logging("demo") as run:
        logger.warning("a recoverable problem")

    pattern = (
        r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{4} \| WARNING  \| "
        r"tests\.logging_setup \| a recoverable problem$"
    )
    assert any(re.match(pattern, line) for line in _text(run).splitlines())


def test_the_console_gets_the_same_records(capsys: pytest.CaptureFixture[str]) -> None:
    with run_logging("demo"):
        logger.info("visible on the console")

    assert "visible on the console" in capsys.readouterr().out


# =============================================================================
# failures
# =============================================================================


def test_unhandled_exception_is_logged_with_its_traceback_and_propagates() -> None:
    holder = {}
    with pytest.raises(ZeroDivisionError):
        with run_logging("demo") as run:
            holder["run"] = run
            1 / 0

    text = _text(holder["run"])
    assert "| CRITICAL | masters_project.logging_setup | Run aborted" in text
    assert "ZeroDivisionError: division by zero" in text
    assert "status=failed" in text


def test_keyboard_interrupt_is_an_interrupted_run() -> None:
    holder = {}
    with pytest.raises(KeyboardInterrupt):
        with run_logging("demo") as run:
            holder["run"] = run
            raise KeyboardInterrupt

    assert "status=interrupted" in _text(holder["run"])


# =============================================================================
# levels and teardown
# =============================================================================


@pytest.mark.parametrize(
    ("configured", "debug_in_file"), [("INFO", False), ("DEBUG", True)]
)
def test_debug_records_reach_the_file_only_when_log_level_is_debug(
    monkeypatch: pytest.MonkeyPatch, configured: str, debug_in_file: bool
) -> None:
    monkeypatch.setattr(settings.etl, "log_level", configured)

    with run_logging("demo") as run:
        logger.debug("per-file detail")
        logger.info("milestone")

    assert ("per-file detail" in _text(run)) is debug_in_file
    assert "milestone" in _text(run)


def test_noisy_libraries_are_quiet_inside_the_run_and_restored_after() -> None:
    fsspec = logging.getLogger("fsspec")
    before = fsspec.level

    with run_logging("demo") as run:
        fsspec.info("chatter")
        assert fsspec.getEffectiveLevel() == logging.WARNING

    assert "chatter" not in _text(run)
    assert fsspec.level == before


def test_handlers_and_level_of_the_root_logger_are_restored() -> None:
    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level

    with run_logging("demo"):
        assert len(root.handlers) == len(handlers) + 2  # console + file

    assert root.handlers == handlers
    assert root.level == level


def test_other_handlers_keep_working_during_the_run(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The root logger is not reset, so pytest's own handler still sees the records."""
    with caplog.at_level(logging.INFO):
        with run_logging("demo"):
            logger.info("seen by caplog too")

    assert "seen by caplog too" in caplog.text
    assert "Run started: pipeline=demo" in caplog.text


# =============================================================================
# git revision
# =============================================================================


def test_git_revision_of_this_checkout_is_a_short_hash() -> None:
    assert re.fullmatch(r"[0-9a-f]{7,}(-dirty)?", git_revision())


def test_git_revision_marks_uncommitted_changes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outputs = iter(["abc1234\n", " M src/file.py\n"])
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(args, 0, next(outputs), ""),
    )
    assert git_revision() == "abc1234-dirty"


def test_git_revision_is_unknown_without_git(monkeypatch: pytest.MonkeyPatch) -> None:
    def missing(*args, **kwargs):
        raise FileNotFoundError("git")

    monkeypatch.setattr(subprocess, "run", missing)
    assert git_revision() == "unknown"


def test_format_duration_is_hours_minutes_seconds() -> None:
    assert logging_setup._format_duration(3725.9) == "01:02:05"
    assert logging_setup._format_duration(0) == "00:00:00"
