"""
Data quality validation pipeline for the lakehouse.

Runs a suite of declarative quality checks against the ``sales`` Iceberg
table, persists individual results to the ``_quality_results`` metadata
table, and acts as a gate: the DAG fails if any check does not pass,
allowing downstream pipelines to depend on it via ``ExternalTaskSensor``.

Designed for CeleryExecutor: every task is a standalone PythonOperator that
serialises cleanly across Celery workers.

Author: data-engineering
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from typing import Any

import trino
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

from lib.quality_checks import (
    SALES_CHECKS,
    CheckResult,
    CheckStatus,
    QualityCheck,
    build_quality_result_insert,
    build_quality_results_ddl,
    run_check,
)

# =============================================================================
# Constants
# =============================================================================

TRINO_USER: str = "airflow"
ICEBERG_CATALOG: str = "iceberg"
ICEBERG_SCHEMA: str = "lakehouse"

log = logging.getLogger(__name__)

# =============================================================================
# DAG documentation
# =============================================================================

DAG_DOC_MD: str = """
### Data Quality Checks

| Property       | Value                                         |
|----------------|-----------------------------------------------|
| **Schedule**   | `@daily`                                      |
| **Owner**      | `data-engineering`                            |
| **Executor**   | `CeleryExecutor`                              |
| **Gate**       | Yes — fails if any check is not passed        |

#### Task Graph

```
run_quality_checks
       |
   +---+---+
   |       |
persist  evaluate_gate
results
```

#### Description

End-to-end data quality validation pipeline:

1. **Run checks** -- executes every check in ``SALES_CHECKS`` against the
   ``sales`` Iceberg table via Trino and collects results.
2. **Persist results** -- writes each ``CheckResult`` to the
   ``_quality_results`` metadata table for historical analysis.
3. **Evaluate gate** -- inspects results and raises
   ``AirflowFailException`` if any check failed or errored.  Downstream
   DAGs can use ``ExternalTaskSensor`` on this task to gate their
   execution on data quality.

#### Environment Variables

| Variable           | Purpose                           |
|--------------------|-----------------------------------|
| `TRINO_HOST`       | Trino coordinator hostname        |
| `TRINO_PORT`       | Trino coordinator port            |
"""


# =============================================================================
# Helper functions
# =============================================================================


def _get_trino_connection() -> trino.dbapi.Connection:
    """Open a DBAPI connection to the Trino coordinator."""
    return trino.dbapi.connect(
        host=os.environ["TRINO_HOST"],
        port=int(os.environ["TRINO_PORT"]),
        user=TRINO_USER,
        catalog=ICEBERG_CATALOG,
        schema=ICEBERG_SCHEMA,
    )


# =============================================================================
# DAG definition
# =============================================================================


@dag(
    dag_id="data_quality_checks",
    description=(
        "Run declarative quality checks against Iceberg tables and gate "
        "downstream pipelines on the results"
    ),
    doc_md=DAG_DOC_MD,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "iceberg", "quality"],
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    },
)
def data_quality_checks() -> None:
    """Top-level DAG factory for the data quality checks pipeline."""

    # --------------------------------------------------------------------- #
    # Task 1 -- Run quality checks
    # --------------------------------------------------------------------- #

    @task()
    def run_quality_checks() -> list[dict[str, Any]]:
        """Execute every check in ``SALES_CHECKS`` and return the results.

        Opens a single Trino connection, ensures the ``_quality_results``
        table exists, then iterates through each declared check.

        Returns:
            A list of serialised ``CheckResult`` dicts (via ``.to_dict()``).
        """
        conn = _get_trino_connection()
        try:
            cursor = conn.cursor()

            # Ensure the results metadata table exists
            ddl = build_quality_results_ddl()
            log.info("Ensuring _quality_results table exists")
            cursor.execute(ddl)
            cursor.fetchone()  # Trino requires fetching to materialise DDL

            # Execute each check
            results: list[dict[str, Any]] = []
            for check in SALES_CHECKS:
                log.info("Running check: %s (%s)", check.name, check.check_type)
                result = run_check(cursor, check)
                log.info(
                    "Check %s: %s — %s",
                    result.check_name,
                    result.status.value,
                    result.details,
                )
                results.append(result.to_dict())

            log.info(
                "Completed %d quality check(s)",
                len(results),
            )
            return results
        finally:
            conn.close()

    # --------------------------------------------------------------------- #
    # Task 2 -- Persist results to Iceberg
    # --------------------------------------------------------------------- #

    @task()
    def persist_results(
        results: list[dict[str, Any]], **context
    ) -> None:
        """Write each check result to the ``_quality_results`` table.

        Reconstructs ``CheckResult`` objects from the serialised dicts
        and inserts them with the current DAG run ID for traceability.
        """
        dag_run_id: str = context.get("run_id", "unknown")

        conn = _get_trino_connection()
        try:
            cursor = conn.cursor()
            persisted = 0

            for row in results:
                result = CheckResult(
                    check_name=row["check_name"],
                    check_type=row["check_type"],
                    table_name=row["table_name"],
                    column_name=row["column_name"] or None,
                    status=CheckStatus(row["status"]),
                    failing_rows=int(row["failing_rows"]),
                    total_rows=int(row["total_rows"]),
                    details=row["details"],
                    executed_at=row["executed_at"],
                )
                sql, params = build_quality_result_insert(result, dag_run_id)
                cursor.execute(sql, params)
                cursor.fetchone()  # Trino requires fetching to materialise DML
                persisted += 1

            log.info(
                "Persisted %d quality result(s) for dag_run_id=%s",
                persisted,
                dag_run_id,
            )
        finally:
            conn.close()

    # --------------------------------------------------------------------- #
    # Task 3 -- Evaluate quality gate
    # --------------------------------------------------------------------- #

    @task()
    def evaluate_gate(results: list[dict[str, Any]]) -> None:
        """Inspect check results and fail the DAG if any check did not pass.

        Logs a summary table and raises ``AirflowFailException`` when
        one or more checks have status ``failed`` or ``error``.  This
        task serves as the quality gate for downstream pipelines.
        """
        passed = 0
        failed = 0
        error = 0
        problem_checks: list[str] = []

        log.info("=" * 60)
        log.info("DATA QUALITY GATE — RESULTS SUMMARY")
        log.info("=" * 60)
        log.info("%-40s %-10s %s", "CHECK", "STATUS", "DETAILS")
        log.info("-" * 60)

        for row in results:
            status = row["status"]
            log.info(
                "%-40s %-10s %s",
                row["check_name"],
                status,
                row["details"],
            )

            if status == CheckStatus.PASSED.value:
                passed += 1
            elif status == CheckStatus.FAILED.value:
                failed += 1
                problem_checks.append(row["check_name"])
            elif status == CheckStatus.ERROR.value:
                error += 1
                problem_checks.append(row["check_name"])

        log.info("-" * 60)
        log.info(
            "TOTAL: %d passed, %d failed, %d error (out of %d)",
            passed,
            failed,
            error,
            len(results),
        )
        log.info("=" * 60)

        if problem_checks:
            raise AirflowFailException(
                f"Quality gate FAILED — {len(problem_checks)} check(s) "
                f"did not pass: {', '.join(problem_checks)}"
            )

        log.info("Quality gate PASSED — all %d check(s) succeeded.", passed)

    # --------------------------------------------------------------------- #
    # Task dependencies
    # --------------------------------------------------------------------- #

    results = run_quality_checks()
    persist = persist_results(results)
    gate = evaluate_gate(results)

    results >> [persist, gate]


# Instantiate the DAG
data_quality_checks()
