"""Tests for the Makefile: what each target would run, and the safety checks around it.

Nothing here runs a pipeline: targets are only printed (``make -n``), or run with ``UV`` replaced
by ``echo``/``false`` so that no real command can start, even if a guard were removed.
"""

import importlib.util
import re
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
MAKEFILE = (ROOT / "Makefile").read_text()
EXPERIMENT = ROOT / "config" / "experiments" / "ourinhos-2023.json"

pytestmark = pytest.mark.skipif(
    shutil.which("make") is None, reason="make is not installed"
)


def make(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["make", *args], cwd=ROOT, capture_output=True, text=True, check=False
    )


def dry_run(*args: str) -> str:
    result = make("-n", *args)
    assert result.returncode == 0, result.stderr
    return result.stdout


def documented_targets() -> list[str]:
    return re.findall(r"^([a-zA-Z_-]+):.*## ", MAKEFILE, flags=re.MULTILINE)


def test_help_lists_every_documented_target() -> None:
    output = make("help").stdout
    for target in documented_targets():
        assert re.search(rf"^\s+{re.escape(target)}\s", output, flags=re.MULTILINE), (
            target
        )


def test_help_is_the_default_target() -> None:
    assert make().stdout == make("help").stdout


@pytest.mark.parametrize(
    "module", sorted(set(re.findall(r"\$\(PY\)\.([a-z_.]+)", MAKEFILE)))
)
def test_every_module_the_makefile_runs_exists(module: str) -> None:
    assert importlib.util.find_spec(f"masters_project.{module}") is not None


# =============================================================================
# which config a target uses
# =============================================================================


@pytest.mark.parametrize("target", ["sonda", "goes"])
def test_collection_targets_use_the_default_config(target: str) -> None:
    assert "MASTERS_PIPELINE_CONFIG" not in dry_run(target)


@pytest.mark.parametrize(
    ("target", "module"),
    [
        ("dataset", "etl.build_dataset"),
        ("train", "ml.train"),
        ("tune", "ml.tune"),
        ("predict", "ml.predict"),
        ("plots", "ml.plots"),
    ],
)
def test_model_targets_use_the_experiment_config_by_absolute_path(
    target: str, module: str
) -> None:
    output = dry_run(target)

    assert f"MASTERS_PIPELINE_CONFIG={EXPERIMENT} " in output
    assert f"python -m masters_project.{module}" in output


def test_config_overrides_the_default_of_every_target() -> None:
    for target in ("sonda", "train"):
        output = dry_run(target, "CONFIG=config/pipeline.json")
        assert f"MASTERS_PIPELINE_CONFIG={ROOT / 'config' / 'pipeline.json'} " in output


def test_experiment_replaces_only_the_default_of_the_model_targets() -> None:
    assert f"MASTERS_PIPELINE_CONFIG={ROOT / 'config' / 'pipeline.json'} " in dry_run(
        "train", "EXPERIMENT=config/pipeline.json"
    )
    assert "MASTERS_PIPELINE_CONFIG" not in dry_run(
        "sonda", "EXPERIMENT=config/pipeline.json"
    )


def test_a_config_that_does_not_exist_is_an_error_before_anything_runs() -> None:
    result = make("-n", "sonda", "CONFIG=no/such/file.json")

    assert result.returncode != 0
    assert "CONFIG=no/such/file.json does not exist" in result.stderr


# =============================================================================
# arguments
# =============================================================================


def test_sonda_downloads_by_year_unless_told_otherwise() -> None:
    assert "etl.sonda --by year" in dry_run("sonda")
    assert "etl.sonda --by month" in dry_run("sonda", "BY=month")


def test_dataset_passes_the_station_and_the_goes_directory() -> None:
    assert "--station" not in dry_run("dataset")
    assert "build_dataset --station OURINHOS" in dry_run("dataset", "STATION=OURINHOS")
    assert "--station OURINHOS --goes-dir ourinhos_experiment" in dry_run(
        "dataset", "STATION=OURINHOS", "GOES_DIR=ourinhos_experiment"
    )


# =============================================================================
# chains
# =============================================================================


@pytest.mark.parametrize("flags", [[], ["-j4"]])
def test_the_experiment_runs_its_steps_in_order_even_with_parallel_make(
    flags: list[str],
) -> None:
    output = dry_run(*flags, "experiment")

    positions = [
        output.index(f"masters_project.{m}")
        for m in ("etl.build_dataset", "ml.train", "ml.predict", "ml.plots")
    ]
    assert positions == sorted(positions)


def test_etl_runs_sonda_then_goes() -> None:
    output = dry_run("etl")
    assert output.index("etl.sonda") < output.index("etl.goes")


def test_etl_goes_on_to_goes_when_sonda_fails_and_says_so() -> None:
    """UV=false makes every command fail without running anything."""
    result = make("etl", "UV=false")

    assert "WARNING: SONDA exited with an error" in result.stdout
    assert "-> etl.goes" in result.stdout  # the GOES step was still attempted
    assert result.returncode != 0  # and its own failure is not hidden


# =============================================================================
# safety
# =============================================================================


def test_goes_refuses_an_experiment_config() -> None:
    """UV=echo: even without the guard, nothing real could start."""
    result = make("goes", f"CONFIG={EXPERIMENT.relative_to(ROOT)}", "UV=echo")

    assert result.returncode != 0
    assert "Refusing to run the GOES ETL with an experiment config" in result.stderr
    assert "masters_project.etl.goes" not in result.stdout


def test_goes_accepts_the_regular_config() -> None:
    result = make("goes", "CONFIG=config/pipeline.json", "UV=echo")

    assert result.returncode == 0
    assert "masters_project.etl.goes" in result.stdout


def test_logs_needs_to_know_which_pipeline() -> None:
    result = make("logs")

    assert result.returncode != 0
    assert "usage: make logs PIPELINE=" in result.stderr


def test_clean_only_removes_caches_never_data() -> None:
    output = dry_run("clean")

    assert "data" not in output.replace("find src tests notebooks", "")
    assert "__pycache__" in output and ".pytest_cache" in output


def test_check_runs_lint_formatting_and_coverage() -> None:
    output = dry_run("check")

    assert (
        "ruff check" in output
        and "ruff format --check" in output
        and "pytest --cov" in output
    )


def test_show_config_reports_the_periods_of_the_experiment() -> None:
    output = make("show-config").stdout

    assert (
        "2022-01-01..2023-12-31" in output
        and "OURINHOS 2023-01-01..2023-12-31" in output
    )
