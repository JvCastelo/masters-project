"""Data contracts at the two boundaries other stages depend on: the SONDA CSV and the model input.

Two kinds of rule, with different consequences:

* **Structure** (columns, types, UTC timestamps, unique and sorted, no missing values in
  the model input): a violation means the pipeline built something the next stage cannot
  use, so :class:`DataValidationError` is raised.
* **Quality** (target inside a physical range): a violation is a property of the data, for
  example a defective sensor, not a bug. It is logged with the offending rows and the
  pipeline carries on; deciding what to do about it is a modelling choice.

Validate once per CSV, never per file in the GOES hot path.
"""

import logging
import re
from collections import Counter

import pandas as pd
import pandera.pandas as pa

logger = logging.getLogger(__name__)

# Physical plausibility of the global horizontal irradiance in W/m2. Measured on the SONDA
# series of 2022-2023: the only values below -100 are a defective Cachoeira Paulista sensor
# (2023-10-03 to 12, down to -765). Above 1500 there are 39 isolated midday peaks (up to 1657)
# that are real cloud-enhancement events, so the ceiling sits well above them, past the solar
# constant (1361) plus a generous enhancement margin.
TARGET_MIN = -100.0
TARGET_MAX = 2000.0

# One GOES window pixel: radius=<r>_<channel>_<row><col>, e.g. radius=1_C03_22.
FEATURE_PATTERN = r"^radius=\d+_C\d{2}_\d+$"
_FEATURE_NAME = re.compile(r"^radius=(\d+)_(C\d{2})_\d+$")

# How many failure rows are quoted in a log message or an exception.
MAX_REPORTED = 5


class DataValidationError(ValueError):
    """A frame breaks the structural contract of its stage."""


def _is_utc(series: pd.Series) -> bool:
    return (
        isinstance(series.dtype, pd.DatetimeTZDtype) and str(series.dtype.tz) == "UTC"
    )


def _windows_complete(df: pd.DataFrame) -> bool:
    """Each (radius, channel) must have all (2*radius+1)**2 pixels of its window."""
    counts: Counter[tuple[int, str]] = Counter()
    for name in df.columns:
        match = _FEATURE_NAME.match(str(name))
        if match:
            counts[(int(match.group(1)), match.group(2))] += 1
    return all(n == (2 * radius + 1) ** 2 for (radius, _), n in counts.items())


def _timestamp_column() -> pa.Column:
    # dtype=None because pandas 3 parses to microseconds and older code paths to nanoseconds.
    return pa.Column(
        dtype=None,
        nullable=False,
        unique=True,
        checks=[
            pa.Check(_is_utc, error="timestamp must be timezone-aware UTC"),
            pa.Check(
                lambda s: s.is_monotonic_increasing,
                error="timestamp must be sorted ascending",
            ),
        ],
    )


def _summarize(failure_cases: pd.DataFrame) -> str:
    """One line per failed (column, check), with how many rows and a few examples."""
    lines = []
    grouped = failure_cases.groupby(["column", "check"], dropna=False)
    for (column, check), cases in list(grouped)[:MAX_REPORTED]:
        examples = cases["failure_case"].head(MAX_REPORTED).tolist()
        where = "(whole frame)" if pd.isna(column) else column
        lines.append(f"  {where}: {check} ({len(cases)} failures, e.g. {examples})")
    if grouped.ngroups > MAX_REPORTED:
        lines.append(f"  ... and {grouped.ngroups - MAX_REPORTED} more failed checks")
    return "\n".join(lines)


def _enforce(schema: pa.DataFrameSchema, df: pd.DataFrame, label: str) -> None:
    try:
        schema.validate(df, lazy=True)
    except pa.errors.SchemaErrors as error:
        raise DataValidationError(
            f"{label} failed validation:\n{_summarize(error.failure_cases)}"
        ) from error


def _warn_target_out_of_range(df: pd.DataFrame, target: str, label: str) -> None:
    outside = df[(df[target] < TARGET_MIN) | (df[target] > TARGET_MAX)]
    if outside.empty:
        return
    first, last = outside["timestamp"].min(), outside["timestamp"].max()
    logger.warning(
        f"{label}: {len(outside)} '{target}' values outside "
        f"[{TARGET_MIN:g}, {TARGET_MAX:g}] W/m2 (min {outside[target].min():g}, "
        f"max {outside[target].max():g}), between {first} and {last}. "
        "Likely a defective sensor; the rows are kept."
    )


def validate_sonda(df: pd.DataFrame, target: str, label: str = "SONDA") -> None:
    """Check the frame the SONDA ETL is about to export.

    Args:
        df: Frame with ``timestamp`` (UTC) and the target column.
        target: Name of the target column (e.g. ``glo_avg``).
        label: Prefix for messages, usually the station.

    Raises:
        DataValidationError: If the structure is wrong (see module docstring).
    """
    schema = pa.DataFrameSchema(
        {"timestamp": _timestamp_column(), target: pa.Column(float, nullable=True)},
        strict=True,
    )
    _enforce(schema, df, label)
    _warn_target_out_of_range(df, target, label)


def validate_model_input(
    df: pd.DataFrame, target: str, label: str = "model input"
) -> None:
    """Check the table the ML stage will read: UTC timestamp, target and GOES window pixels.

    Args:
        df: Frame built by :mod:`masters_project.etl.build_dataset`.
        target: Name of the target column (e.g. ``glo_avg``).
        label: Prefix for messages, usually the station.

    Raises:
        DataValidationError: If the structure is wrong, or any value is missing.
    """
    schema = pa.DataFrameSchema(
        {
            "timestamp": _timestamp_column(),
            target: pa.Column(float, nullable=False),
            FEATURE_PATTERN: pa.Column(float, nullable=False, regex=True),
        },
        checks=pa.Check(
            _windows_complete,
            error="every channel needs all the pixels of its window",
        ),
        strict=True,
    )
    _enforce(schema, df, label)
    _warn_target_out_of_range(df, target, label)
