"""Periods (calendar years or months) covering the ETL date range, one SONDA ZIP each."""

from datetime import date, datetime


def _parse_date(date_input: str | date) -> date:
    """Convert a string or date to a date object.

    Args:
        date_input: Either a date object (returned as-is) or an ISO date string
            in 'YYYY-MM-DD' format.

    Returns:
        A datetime.date instance.
    """
    if isinstance(date_input, str):
        return datetime.strptime(date_input, "%Y-%m-%d").date()
    return date_input


def get_target_years(start_date: str | date, end_date: str | date) -> list[int]:
    """Build a sorted list of calendar years spanning the given date range.

    Args:
        start_date: Start of the range (date or 'YYYY-MM-DD' string).
        end_date: End of the range (date or 'YYYY-MM-DD' string).

    Returns:
        List of distinct years from start_date.year through end_date.year inclusive.
        Example: '2023-11-01' to '2024-02-01' -> [2023, 2024].
    """
    start = _parse_date(start_date)
    end = _parse_date(end_date)

    return list(range(start.year, end.year + 1))


def get_target_year_months(
    start_date: str | date, end_date: str | date
) -> list[tuple[int, int]]:
    """Build a sequential list of (year, month) tuples covering the date range.

    Args:
        start_date: Start of the range (date or 'YYYY-MM-DD' string).
        end_date: End of the range (date or 'YYYY-MM-DD' string).

    Returns:
        List of (year, month) tuples in chronological order.
        Example: '2023-11-15' to '2024-02-10' -> [(2023, 11), (2023, 12), (2024, 1), (2024, 2)].
    """
    start = _parse_date(start_date)
    end = _parse_date(end_date)

    year_months = []
    current_year = start.year
    current_month = start.month

    while (current_year < end.year) or (
        current_year == end.year and current_month <= end.month
    ):
        year_months.append((current_year, current_month))

        current_month += 1
        if current_month > 12:
            current_month = 1
            current_year += 1

    return year_months


def get_target_periods(
    start_date: str | date, end_date: str | date, granularity: str
) -> list[tuple[int, int | None]]:
    """Build the (year, month) or (year, None) periods covering the date range.

    Args:
        start_date: Start of the range (date or 'YYYY-MM-DD' string).
        end_date: End of the range (date or 'YYYY-MM-DD' string).
        granularity: 'year' for one period per calendar year, 'month' for one per month.

    Returns:
        List of (year, None) for 'year' or (year, month) for 'month', chronological.

    Raises:
        ValueError: If granularity is neither 'year' nor 'month'.
    """
    if granularity == "year":
        return [(year, None) for year in get_target_years(start_date, end_date)]
    if granularity == "month":
        return list(get_target_year_months(start_date, end_date))
    raise ValueError(f"granularity must be 'year' or 'month', got '{granularity}'.")


def format_period(period: tuple[int, int | None]) -> str:
    """Format a period from :func:`get_target_periods` as 'YYYY' or 'YYYY-MM'."""
    year, month = period
    return f"{year}" if month is None else f"{year}-{month:02d}"
