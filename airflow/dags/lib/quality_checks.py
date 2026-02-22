"""
Data quality validation framework for the lakehouse.

Provides a declarative check system that runs SQL-based validations
against Iceberg tables via Trino.  Each check returns a standardised
``CheckResult`` and results are persisted to an Iceberg metadata table
for historical analysis.

Supported check types:
  - **uniqueness**: Column has no duplicate non-null values.
  - **not_null**: Column contains no NULL values.
  - **accepted_values**: Column values belong to an allowed set.
  - **positive**: Numeric column has only positive values.
  - **range**: Numeric column falls within [min, max].
  - **row_count_range**: Table row count is within [min, max].
  - **no_future_dates**: Date/timestamp column has no future values.
  - **custom_sql**: Arbitrary SQL returns zero failing rows.

Author: data-engineering
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ICEBERG_CATALOG: str = "iceberg"
ICEBERG_SCHEMA: str = "lakehouse"


def _fqn(table_name: str) -> str:
    """Return fully-qualified Iceberg table name."""
    return f"{ICEBERG_CATALOG}.{ICEBERG_SCHEMA}.{table_name}"


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


class CheckStatus(str, Enum):
    """Outcome of a quality check."""

    PASSED = "passed"
    FAILED = "failed"
    ERROR = "error"


@dataclass
class CheckResult:
    """Result of a single quality check execution."""

    check_name: str
    check_type: str
    table_name: str
    column_name: str | None
    status: CheckStatus
    failing_rows: int
    total_rows: int
    details: str
    executed_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        )
    )

    def to_dict(self) -> dict[str, Any]:
        """Serialise to a dict suitable for XCom / JSON."""
        return {
            "check_name": self.check_name,
            "check_type": self.check_type,
            "table_name": self.table_name,
            "column_name": self.column_name or "",
            "status": self.status.value,
            "failing_rows": self.failing_rows,
            "total_rows": self.total_rows,
            "details": self.details,
            "executed_at": self.executed_at,
        }


@dataclass
class QualityCheck:
    """Declarative specification of a quality check to execute."""

    name: str
    check_type: str
    table_name: str
    column_name: str | None = None
    params: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# DDL for results table
# ---------------------------------------------------------------------------


def build_quality_results_ddl() -> str:
    """Return CREATE TABLE DDL for the ``_quality_results`` metadata table."""
    return (
        f"CREATE TABLE IF NOT EXISTS {_fqn('_quality_results')} (\n"
        f"    check_name    VARCHAR,\n"
        f"    check_type    VARCHAR,\n"
        f"    table_name    VARCHAR,\n"
        f"    column_name   VARCHAR,\n"
        f"    status        VARCHAR,\n"
        f"    failing_rows  INTEGER,\n"
        f"    total_rows    INTEGER,\n"
        f"    details       VARCHAR,\n"
        f"    dag_run_id    VARCHAR,\n"
        f"    executed_at   TIMESTAMP(6)\n"
        f")\n"
        f"WITH (\n"
        f"    format = 'PARQUET'\n"
        f")"
    )


def build_quality_result_insert(
    result: CheckResult,
    dag_run_id: str,
) -> tuple[str, tuple]:
    """Build an INSERT for a single check result."""
    sql = (
        f"INSERT INTO {_fqn('_quality_results')} "
        f"(check_name, check_type, table_name, column_name, status, "
        f"failing_rows, total_rows, details, dag_run_id, executed_at) "
        f"VALUES (?, ?, ?, ?, ?, CAST(? AS INTEGER), CAST(? AS INTEGER), "
        f"?, ?, CAST(? AS TIMESTAMP))"
    )
    params = (
        result.check_name,
        result.check_type,
        result.table_name,
        result.column_name or "",
        result.status.value,
        str(result.failing_rows),
        str(result.total_rows),
        result.details[:1000],  # Truncate long details
        dag_run_id,
        result.executed_at,
    )
    return sql, params


# ---------------------------------------------------------------------------
# Check executors
# ---------------------------------------------------------------------------


def _run_count_query(cursor: Any, sql: str) -> int:
    """Execute a COUNT query and return the scalar result."""
    cursor.execute(sql)
    row = cursor.fetchone()
    return int(row[0]) if row else 0


def check_uniqueness(
    cursor: Any,
    table_name: str,
    column_name: str,
    check_name: str,
) -> CheckResult:
    """Check that a column has no duplicate non-null values."""
    fqn = _fqn(table_name)
    total = _run_count_query(cursor, f"SELECT COUNT(*) FROM {fqn}")
    dupes = _run_count_query(
        cursor,
        f"SELECT COUNT(*) FROM ("
        f"  SELECT {column_name}, COUNT(*) AS cnt "
        f"  FROM {fqn} "
        f"  WHERE {column_name} IS NOT NULL "
        f"  GROUP BY {column_name} "
        f"  HAVING COUNT(*) > 1"
        f")",
    )
    status = CheckStatus.PASSED if dupes == 0 else CheckStatus.FAILED
    return CheckResult(
        check_name=check_name,
        check_type="uniqueness",
        table_name=table_name,
        column_name=column_name,
        status=status,
        failing_rows=dupes,
        total_rows=total,
        details=f"{dupes} duplicate value(s) found" if dupes else "All values unique",
    )


def check_not_null(
    cursor: Any,
    table_name: str,
    column_name: str,
    check_name: str,
) -> CheckResult:
    """Check that a column contains no NULL values."""
    fqn = _fqn(table_name)
    total = _run_count_query(cursor, f"SELECT COUNT(*) FROM {fqn}")
    nulls = _run_count_query(
        cursor,
        f"SELECT COUNT(*) FROM {fqn} WHERE {column_name} IS NULL",
    )
    status = CheckStatus.PASSED if nulls == 0 else CheckStatus.FAILED
    return CheckResult(
        check_name=check_name,
        check_type="not_null",
        table_name=table_name,
        column_name=column_name,
        status=status,
        failing_rows=nulls,
        total_rows=total,
        details=f"{nulls} NULL value(s)" if nulls else "No NULLs",
    )


def check_accepted_values(
    cursor: Any,
    table_name: str,
    column_name: str,
    accepted: list[str],
    check_name: str,
) -> CheckResult:
    """Check that all column values belong to an accepted set."""
    fqn = _fqn(table_name)
    total = _run_count_query(cursor, f"SELECT COUNT(*) FROM {fqn}")
    values_list = ", ".join(f"'{v}'" for v in accepted)
    bad = _run_count_query(
        cursor,
        f"SELECT COUNT(*) FROM {fqn} "
        f"WHERE {column_name} IS NOT NULL "
        f"AND {column_name} NOT IN ({values_list})",
    )
    status = CheckStatus.PASSED if bad == 0 else CheckStatus.FAILED
    return CheckResult(
        check_name=check_name,
        check_type="accepted_values",
        table_name=table_name,
        column_name=column_name,
        status=status,
        failing_rows=bad,
        total_rows=total,
        details=(
            f"{bad} row(s) with values outside accepted set"
            if bad
            else "All values in accepted set"
        ),
    )


def check_positive(
    cursor: Any,
    table_name: str,
    column_name: str,
    check_name: str,
) -> CheckResult:
    """Check that a numeric column has only positive (> 0) values."""
    fqn = _fqn(table_name)
    total = _run_count_query(cursor, f"SELECT COUNT(*) FROM {fqn}")
    bad = _run_count_query(
        cursor,
        f"SELECT COUNT(*) FROM {fqn} "
        f"WHERE {column_name} IS NOT NULL AND {column_name} <= 0",
    )
    status = CheckStatus.PASSED if bad == 0 else CheckStatus.FAILED
    return CheckResult(
        check_name=check_name,
        check_type="positive",
        table_name=table_name,
        column_name=column_name,
        status=status,
        failing_rows=bad,
        total_rows=total,
        details=(
            f"{bad} row(s) with non-positive values"
            if bad
            else "All values positive"
        ),
    )


def check_range(
    cursor: Any,
    table_name: str,
    column_name: str,
    min_value: float,
    max_value: float,
    check_name: str,
) -> CheckResult:
    """Check that a numeric column falls within [min, max]."""
    fqn = _fqn(table_name)
    total = _run_count_query(cursor, f"SELECT COUNT(*) FROM {fqn}")
    bad = _run_count_query(
        cursor,
        f"SELECT COUNT(*) FROM {fqn} "
        f"WHERE {column_name} IS NOT NULL "
        f"AND ({column_name} < {min_value} OR {column_name} > {max_value})",
    )
    status = CheckStatus.PASSED if bad == 0 else CheckStatus.FAILED
    return CheckResult(
        check_name=check_name,
        check_type="range",
        table_name=table_name,
        column_name=column_name,
        status=status,
        failing_rows=bad,
        total_rows=total,
        details=(
            f"{bad} row(s) outside range [{min_value}, {max_value}]"
            if bad
            else f"All values within [{min_value}, {max_value}]"
        ),
    )


def check_row_count_range(
    cursor: Any,
    table_name: str,
    min_count: int,
    max_count: int,
    check_name: str,
) -> CheckResult:
    """Check that table row count is within [min, max]."""
    fqn = _fqn(table_name)
    total = _run_count_query(cursor, f"SELECT COUNT(*) FROM {fqn}")
    passed = min_count <= total <= max_count
    status = CheckStatus.PASSED if passed else CheckStatus.FAILED
    return CheckResult(
        check_name=check_name,
        check_type="row_count_range",
        table_name=table_name,
        column_name=None,
        status=status,
        failing_rows=0 if passed else 1,
        total_rows=total,
        details=(
            f"Row count {total} within [{min_count}, {max_count}]"
            if passed
            else f"Row count {total} outside [{min_count}, {max_count}]"
        ),
    )


def check_no_future_dates(
    cursor: Any,
    table_name: str,
    column_name: str,
    check_name: str,
) -> CheckResult:
    """Check that a date column has no values in the future."""
    fqn = _fqn(table_name)
    total = _run_count_query(cursor, f"SELECT COUNT(*) FROM {fqn}")
    bad = _run_count_query(
        cursor,
        f"SELECT COUNT(*) FROM {fqn} "
        f"WHERE {column_name} IS NOT NULL "
        f"AND {column_name} > CURRENT_DATE",
    )
    status = CheckStatus.PASSED if bad == 0 else CheckStatus.FAILED
    return CheckResult(
        check_name=check_name,
        check_type="no_future_dates",
        table_name=table_name,
        column_name=column_name,
        status=status,
        failing_rows=bad,
        total_rows=total,
        details=(
            f"{bad} row(s) with future dates"
            if bad
            else "No future dates"
        ),
    )


def check_custom_sql(
    cursor: Any,
    table_name: str,
    sql: str,
    check_name: str,
) -> CheckResult:
    """Run a custom SQL query; expect zero rows returned for pass.

    The query should return failing rows. If the count is zero, the
    check passes.
    """
    fqn = _fqn(table_name)
    total = _run_count_query(cursor, f"SELECT COUNT(*) FROM {fqn}")
    # Wrap the custom SQL in a COUNT
    bad = _run_count_query(
        cursor,
        f"SELECT COUNT(*) FROM ({sql})",
    )
    status = CheckStatus.PASSED if bad == 0 else CheckStatus.FAILED
    return CheckResult(
        check_name=check_name,
        check_type="custom_sql",
        table_name=table_name,
        column_name=None,
        status=status,
        failing_rows=bad,
        total_rows=total,
        details=(
            f"{bad} failing row(s) from custom query"
            if bad
            else "Custom SQL check passed"
        ),
    )


# ---------------------------------------------------------------------------
# Check dispatcher
# ---------------------------------------------------------------------------

_CHECK_DISPATCH: dict[str, Any] = {
    "uniqueness": check_uniqueness,
    "not_null": check_not_null,
    "accepted_values": check_accepted_values,
    "positive": check_positive,
    "range": check_range,
    "row_count_range": check_row_count_range,
    "no_future_dates": check_no_future_dates,
    "custom_sql": check_custom_sql,
}


def run_check(cursor: Any, check: QualityCheck) -> CheckResult:
    """Execute a single quality check via the dispatcher.

    Args:
        cursor: An open Trino DBAPI cursor.
        check: The check specification.

    Returns:
        A ``CheckResult`` with the outcome.
    """
    check_fn = _CHECK_DISPATCH.get(check.check_type)
    if check_fn is None:
        return CheckResult(
            check_name=check.name,
            check_type=check.check_type,
            table_name=check.table_name,
            column_name=check.column_name,
            status=CheckStatus.ERROR,
            failing_rows=0,
            total_rows=0,
            details=f"Unknown check type: {check.check_type}",
        )

    try:
        # Build kwargs from check spec
        kwargs: dict[str, Any] = {
            "cursor": cursor,
            "table_name": check.table_name,
            "check_name": check.name,
        }
        if check.column_name:
            kwargs["column_name"] = check.column_name

        # Add extra params
        kwargs.update(check.params)

        return check_fn(**kwargs)
    except Exception as exc:
        log.exception("Check %s failed with error: %s", check.name, exc)
        return CheckResult(
            check_name=check.name,
            check_type=check.check_type,
            table_name=check.table_name,
            column_name=check.column_name,
            status=CheckStatus.ERROR,
            failing_rows=0,
            total_rows=0,
            details=f"Error: {str(exc)[:500]}",
        )


# ---------------------------------------------------------------------------
# Sales table check suite
# ---------------------------------------------------------------------------

SALES_CHECKS: list[QualityCheck] = [
    QualityCheck(
        name="sales_order_id_unique",
        check_type="uniqueness",
        table_name="sales",
        column_name="order_id",
    ),
    QualityCheck(
        name="sales_order_id_not_null",
        check_type="not_null",
        table_name="sales",
        column_name="order_id",
    ),
    QualityCheck(
        name="sales_amount_positive",
        check_type="positive",
        table_name="sales",
        column_name="amount",
    ),
    QualityCheck(
        name="sales_amount_reasonable_range",
        check_type="range",
        table_name="sales",
        column_name="amount",
        params={"min_value": 0.01, "max_value": 100000.0},
    ),
    QualityCheck(
        name="sales_country_accepted",
        check_type="accepted_values",
        table_name="sales",
        column_name="country",
        params={
            "accepted": [
                "Italy",
                "Germany",
                "France",
                "United States",
                "Spain",
                "United Kingdom",
                "Netherlands",
                "Japan",
                "Canada",
                "Brazil",
            ]
        },
    ),
    QualityCheck(
        name="sales_ingestion_date_not_future",
        check_type="no_future_dates",
        table_name="sales",
        column_name="ingestion_date",
    ),
    QualityCheck(
        name="sales_row_count_reasonable",
        check_type="row_count_range",
        table_name="sales",
        params={"min_count": 1, "max_count": 100000},
    ),
    QualityCheck(
        name="sales_customer_id_not_null",
        check_type="not_null",
        table_name="sales",
        column_name="customer_id",
    ),
]
