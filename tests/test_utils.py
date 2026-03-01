"""Tests for masters_project.utils: date parsing and date-range helpers."""

from datetime import date

import pytest

from masters_project.utils import get_target_years, get_target_year_months


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def single_year_range() -> tuple[str, str]:
    """Date range confined to one calendar year (edge case: minimal span)."""
    return ("2023-06-15", "2023-08-20")


@pytest.fixture
def multi_year_range() -> tuple[str, str]:
    """Date range spanning multiple years (main happy path)."""
    return ("2023-11-01", "2024-02-15")


@pytest.fixture
def year_boundary_range() -> tuple[str, str]:
    """Date range crossing December–January (critical edge case for month rollover)."""
    return ("2023-12-10", "2024-01-05")


# =============================================================================
# Tests
# =============================================================================


def test_get_target_years_multi_year_range_returns_ordered_list(
    multi_year_range: tuple[str, str],
) -> None:
    """Happy path: span across years produces a sorted list of all years in range."""
    start, end = multi_year_range
    result = get_target_years(start, end)
    assert result == [2023, 2024]


def test_get_target_years_single_year_returns_single_element(
    single_year_range: tuple[str, str],
) -> None:
    """Edge case: range within one year returns only that year."""
    start, end = single_year_range
    result = get_target_years(start, end)
    assert result == [2023]


@pytest.mark.parametrize(
    ("start_date", "end_date", "expected"),
    [
        ("2023-01-01", "2023-01-01", [(2023, 1)]),
        ("2023-11-15", "2024-02-10", [(2023, 11), (2023, 12), (2024, 1), (2024, 2)]),
        (date(2024, 6, 1), date(2024, 6, 30), [(2024, 6)]),
    ],
    ids=["single_month", "spanning_year_boundary", "date_objects_accepted"],
)
def test_get_target_year_months_various_inputs_returns_chronological_tuples(
    start_date: str | date, end_date: str | date, expected: list[tuple[int, int]]
) -> None:
    """Parametrized: single month, year rollover, and date-object inputs all produce correct (year, month) lists."""
    result = get_target_year_months(start_date, end_date)
    assert result == expected


def test_get_target_year_months_december_to_january_handles_year_rollover(
    year_boundary_range: tuple[str, str],
) -> None:
    """Critical edge case: December–January transition must increment year and reset month correctly."""
    start, end = year_boundary_range
    result = get_target_year_months(start, end)
    assert result == [(2023, 12), (2024, 1)]


def test_get_target_years_invalid_date_string_raises_valueerror() -> None:
    """Failure mode: malformed date string causes datetime.strptime to raise ValueError."""
    with pytest.raises(ValueError):
        get_target_years("2023-13-01", "2024-01-01")  # month 13 invalid
