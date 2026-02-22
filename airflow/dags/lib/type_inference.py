"""
Type inference engine for CSV auto-ingest.

Analyses CSV column data to infer the most appropriate Iceberg/Trino column
type.  Supports: BOOLEAN, INTEGER, BIGINT, DOUBLE, DATE, TIMESTAMP(6),
VARCHAR (fallback).

Algorithm:
  1. Sample the first ``MAX_SAMPLE_ROWS`` non-empty values per column.
  2. Try each type parser from most-specific to least-specific.
  3. If >= ``TYPE_THRESHOLD`` (90 %) of values parse as a type, assign it.
  4. Fall back to VARCHAR.

Author: data-engineering
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAX_SAMPLE_ROWS: int = 1000
TYPE_THRESHOLD: float = 0.90

INT32_MIN: int = -(2**31)
INT32_MAX: int = 2**31 - 1

# Date formats to try (order matters — most common first)
_DATE_FORMATS: list[str] = [
    "%Y-%m-%d",     # 2024-01-15
    "%d/%m/%Y",     # 15/01/2024
    "%m/%d/%Y",     # 01/15/2024
    "%Y/%m/%d",     # 2024/01/15
    "%d-%m-%Y",     # 15-01-2024
    "%m-%d-%Y",     # 01-15-2024
]

_TIMESTAMP_FORMATS: list[str] = [
    "%Y-%m-%dT%H:%M:%S",       # ISO 8601 without tz
    "%Y-%m-%dT%H:%M:%SZ",      # ISO 8601 with Z
    "%Y-%m-%d %H:%M:%S",       # space-separated
    "%Y-%m-%dT%H:%M:%S.%f",    # with microseconds
    "%Y-%m-%dT%H:%M:%S.%fZ",   # with microseconds + Z
    "%Y-%m-%d %H:%M:%S.%f",    # space + microseconds
]

_BOOLEAN_TRUE: set[str] = {"true", "yes", "1"}
_BOOLEAN_FALSE: set[str] = {"false", "no", "0"}
_BOOLEAN_ALL: set[str] = _BOOLEAN_TRUE | _BOOLEAN_FALSE

_INTEGER_RE = re.compile(r"^-?\d+$")
_DOUBLE_RE = re.compile(r"^-?\d+\.?\d*([eE][+-]?\d+)?$")

# ---------------------------------------------------------------------------
# Individual type checkers
# ---------------------------------------------------------------------------


def _is_boolean(value: str) -> bool:
    return value.strip().lower() in _BOOLEAN_ALL


def _is_integer(value: str) -> bool:
    if not _INTEGER_RE.match(value.strip()):
        return False
    try:
        n = int(value.strip())
        return INT32_MIN <= n <= INT32_MAX
    except (ValueError, OverflowError):
        return False


def _is_bigint(value: str) -> bool:
    if not _INTEGER_RE.match(value.strip()):
        return False
    try:
        int(value.strip())
        return True
    except (ValueError, OverflowError):
        return False


def _is_double(value: str) -> bool:
    v = value.strip()
    if not v:
        return False
    # Must contain a dot or exponent to distinguish from integer
    if "." not in v and "e" not in v.lower():
        return False
    return bool(_DOUBLE_RE.match(v))


def _is_date(value: str) -> bool:
    v = value.strip()
    for fmt in _DATE_FORMATS:
        try:
            datetime.strptime(v, fmt)
            return True
        except ValueError:
            continue
    return False


def _is_timestamp(value: str) -> bool:
    v = value.strip()
    for fmt in _TIMESTAMP_FORMATS:
        try:
            datetime.strptime(v, fmt)
            return True
        except ValueError:
            continue
    return False


# Ordered from most specific to least specific.  BOOLEAN is checked first
# because "1" and "0" would otherwise match INTEGER.
_TYPE_CHECKERS: list[tuple[str, Any]] = [
    ("BOOLEAN", _is_boolean),
    ("INTEGER", _is_integer),
    ("BIGINT", _is_bigint),
    ("DOUBLE", _is_double),
    ("DATE", _is_date),
    ("TIMESTAMP(6)", _is_timestamp),
]

# ---------------------------------------------------------------------------
# Column name sanitisation
# ---------------------------------------------------------------------------


def sanitise_column_name(raw: str) -> str:
    """Sanitise a CSV header into a valid Iceberg column name.

    Rules:
      - Lowercase all characters.
      - Replace spaces and non-alphanumeric characters with underscores.
      - Collapse consecutive underscores.
      - Strip leading/trailing underscores.
      - Prefix with ``col_`` if the result is empty or starts with a digit.

    >>> sanitise_column_name("Order ID  #")
    'order_id'
    >>> sanitise_column_name("123abc")
    'col_123abc'
    >>> sanitise_column_name("")
    'col_'
    """
    name = raw.lower()
    name = re.sub(r"[^a-z0-9]", "_", name)
    name = re.sub(r"_+", "_", name)
    name = name.strip("_")
    if not name or name[0].isdigit():
        name = f"col_{name}"
    return name


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def infer_column_type(values: list[str]) -> str:
    """Infer the Iceberg/Trino type for a single column from sampled values.

    Args:
        values: Non-empty string values sampled from the column
                (up to ``MAX_SAMPLE_ROWS``).

    Returns:
        An Iceberg-compatible SQL type string (e.g. ``"INTEGER"``,
        ``"DATE"``, ``"VARCHAR"``).
    """
    if not values:
        return "VARCHAR"

    sample = [v for v in values[:MAX_SAMPLE_ROWS] if v.strip()]
    if not sample:
        return "VARCHAR"

    total = len(sample)

    for type_name, checker in _TYPE_CHECKERS:
        matches = sum(1 for v in sample if checker(v))
        if matches / total >= TYPE_THRESHOLD:
            return type_name

    return "VARCHAR"


def infer_schema(
    rows: list[list[str]],
    headers: list[str],
) -> list[dict[str, str]]:
    """Infer the Iceberg schema for a CSV dataset.

    Args:
        rows: Data rows (list of lists).  Only the first
              ``MAX_SAMPLE_ROWS`` are used.
        headers: Raw CSV header names.

    Returns:
        A list of dicts ``[{"name": "<sanitised>", "type": "<iceberg_type>"}]``
        preserving column order.
    """
    sample_rows = rows[:MAX_SAMPLE_ROWS]

    schema: list[dict[str, str]] = []
    for col_idx, raw_header in enumerate(headers):
        col_values = [
            row[col_idx] for row in sample_rows
            if col_idx < len(row)
        ]
        col_type = infer_column_type(col_values)
        schema.append({
            "name": sanitise_column_name(raw_header),
            "type": col_type,
        })

    return schema
