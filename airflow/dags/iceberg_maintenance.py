"""
Automated Iceberg table maintenance pipeline.

Discovers all user tables in the lakehouse and performs routine maintenance
operations: file compaction (``OPTIMIZE``), snapshot expiration, and orphan
file removal.  Results are logged to the ``_maintenance_log`` metadata table
for observability and historical trending.

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

from lib.iceberg_maintenance import (
    build_maintenance_log_ddl,
    get_files_count,
    get_snapshots_count,
    insert_maintenance_log,
    list_tables,
    run_expire_snapshots,
    run_optimize,
    run_remove_orphan_files,
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
### Iceberg Table Maintenance

| Property       | Value                                         |
|----------------|-----------------------------------------------|
| **Schedule**   | `0 3 * * *` (daily at 3 AM)                  |
| **Owner**      | `data-engineering`                            |
| **Executor**   | `CeleryExecutor`                              |

#### Task Graph

```
discover_tables
       |
  run_maintenance  (dynamic — one per table)
```

#### Description

Automated maintenance pipeline for all Iceberg tables in the lakehouse:

1. **Discover tables** -- scans the lakehouse schema for all user tables
   (excluding metadata tables prefixed with ``_``) and ensures the
   ``_maintenance_log`` metadata table exists.
2. **Run maintenance** -- for each discovered table, executes:
   - **OPTIMIZE** -- compacts small files to improve read performance.
   - **Expire snapshots** -- removes snapshots older than 7 days.
   - **Remove orphan files** -- cleans up unreferenced data files
     older than 7 days.
   Each operation is independently wrapped in error handling so a single
   failure does not block the remaining operations.  A summary record is
   written to ``_maintenance_log`` for every table processed.

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
    dag_id="iceberg_maintenance",
    description=(
        "Automated Iceberg table maintenance: file compaction, snapshot "
        "expiration, and orphan file removal"
    ),
    doc_md=DAG_DOC_MD,
    schedule="0 3 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "iceberg", "maintenance"],
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    },
)
def iceberg_maintenance() -> None:
    """Top-level DAG factory for the Iceberg maintenance pipeline."""

    # --------------------------------------------------------------------- #
    # Task 1 -- Discover tables
    # --------------------------------------------------------------------- #

    @task()
    def discover_tables() -> list[str]:
        """Scan the lakehouse schema for all user tables.

        Ensures the ``_maintenance_log`` metadata table exists, then
        queries the schema for all user-facing tables.

        Returns:
            A list of table name strings to process.
        """
        conn = _get_trino_connection()
        try:
            cursor = conn.cursor()

            # Ensure the maintenance log table exists
            ddl = build_maintenance_log_ddl()
            log.info("Ensuring _maintenance_log table exists")
            cursor.execute(ddl)
            cursor.fetchone()  # Trino requires fetching to materialise DDL

            # Discover all user tables
            tables = list_tables(cursor)
            log.info("Discovered %d table(s) for maintenance", len(tables))
            return tables
        finally:
            conn.close()

    # --------------------------------------------------------------------- #
    # Task 2 -- Run maintenance (dynamically mapped)
    # --------------------------------------------------------------------- #

    @task()
    def run_maintenance(table_name: str, **context) -> dict[str, Any]:
        """Run all maintenance operations on a single Iceberg table.

        For each table, executes file compaction, snapshot expiration, and
        orphan file removal.  Each operation is wrapped in ``try/except``
        so that a failure in one does not prevent the others from running.

        A summary record is inserted into ``_maintenance_log`` for
        historical tracking.

        Returns:
            A dict with the table name and results of each operation.
        """
        dag_run_id: str = context.get("run_id", "unknown")

        log.info("Starting maintenance for table: %s", table_name)

        conn = _get_trino_connection()
        try:
            cursor = conn.cursor()

            # Capture before-state
            snapshots_before = get_snapshots_count(cursor, table_name)
            files_before = get_files_count(cursor, table_name)
            log.info(
                "Table %s — before: %d snapshot(s), %d file(s)",
                table_name,
                snapshots_before,
                files_before,
            )

            # --- Operation 1: Optimize (compact small files) ---
            optimize_status = "success"
            optimize_error: str | None = None
            try:
                run_optimize(cursor, table_name)
                log.info("Table %s — OPTIMIZE completed", table_name)
            except Exception as exc:
                optimize_status = "error"
                optimize_error = str(exc)[:500]
                log.exception(
                    "Table %s — OPTIMIZE failed: %s", table_name, exc
                )

            # --- Operation 2: Expire snapshots ---
            expire_status = "success"
            expire_error: str | None = None
            try:
                run_expire_snapshots(cursor, table_name, retention_days=7)
                log.info(
                    "Table %s — expire_snapshots completed", table_name
                )
            except Exception as exc:
                expire_status = "error"
                expire_error = str(exc)[:500]
                log.exception(
                    "Table %s — expire_snapshots failed: %s",
                    table_name,
                    exc,
                )

            # --- Operation 3: Remove orphan files ---
            orphan_status = "success"
            orphan_error: str | None = None
            try:
                run_remove_orphan_files(cursor, table_name, retention_days=7)
                log.info(
                    "Table %s — remove_orphan_files completed", table_name
                )
            except Exception as exc:
                orphan_status = "error"
                orphan_error = str(exc)[:500]
                log.exception(
                    "Table %s — remove_orphan_files failed: %s",
                    table_name,
                    exc,
                )

            # Capture after-state
            snapshots_after = get_snapshots_count(cursor, table_name)
            files_after = get_files_count(cursor, table_name)
            log.info(
                "Table %s — after: %d snapshot(s), %d file(s)",
                table_name,
                snapshots_after,
                files_after,
            )

            # Log summary
            log.info(
                "Table %s — maintenance summary: "
                "optimize=%s, expire_snapshots=%s, remove_orphan_files=%s, "
                "snapshots %d→%d, files %d→%d",
                table_name,
                optimize_status,
                expire_status,
                orphan_status,
                snapshots_before,
                snapshots_after,
                files_before,
                files_after,
            )

            # Persist summary to _maintenance_log
            overall_status = "success"
            error_messages: list[str] = []
            if optimize_error:
                error_messages.append(f"optimize: {optimize_error}")
            if expire_error:
                error_messages.append(f"expire_snapshots: {expire_error}")
            if orphan_error:
                error_messages.append(f"remove_orphan_files: {orphan_error}")

            if error_messages:
                overall_status = "partial_failure"

            insert_maintenance_log(
                cursor,
                table_name=table_name,
                dag_run_id=dag_run_id,
                status=overall_status,
                snapshots_before=snapshots_before,
                snapshots_after=snapshots_after,
                files_before=files_before,
                files_after=files_after,
                optimize_status=optimize_status,
                expire_status=expire_status,
                orphan_status=orphan_status,
                error_message="; ".join(error_messages) if error_messages else None,
            )
            log.info(
                "Persisted maintenance log for table %s (status=%s)",
                table_name,
                overall_status,
            )

            return {
                "table_name": table_name,
                "status": overall_status,
                "snapshots_before": snapshots_before,
                "snapshots_after": snapshots_after,
                "files_before": files_before,
                "files_after": files_after,
                "optimize": optimize_status,
                "expire_snapshots": expire_status,
                "remove_orphan_files": orphan_status,
            }
        finally:
            conn.close()

    # --------------------------------------------------------------------- #
    # Task dependencies
    # --------------------------------------------------------------------- #

    tables = discover_tables()
    run_maintenance.expand(table_name=tables)


# Instantiate the DAG
iceberg_maintenance()
