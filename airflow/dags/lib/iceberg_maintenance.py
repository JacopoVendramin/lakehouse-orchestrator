"""
Iceberg table maintenance operations via Trino SQL.

Provides helpers for routine Iceberg housekeeping tasks:
  - **optimize**: Compact small data files into larger ones.
  - **expire_snapshots**: Remove snapshots beyond a retention period.
  - **remove_orphan_files**: Delete files not referenced by any snapshot.
  - **maintenance log**: DDL and INSERT builders for a ``_maintenance_log``
    metadata table that tracks every maintenance operation.

Author: data-engineering
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ICEBERG_CATALOG: str = "iceberg"
ICEBERG_SCHEMA: str = "lakehouse"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _fqn(table_name: str) -> str:
    """Return fully-qualified table name."""
    return f"{ICEBERG_CATALOG}.{ICEBERG_SCHEMA}.{table_name}"


# ---------------------------------------------------------------------------
# Table discovery
# ---------------------------------------------------------------------------


def list_tables(cursor: Any) -> list[str]:
    """List all user tables in the lakehouse schema.

    Tables prefixed with ``_`` are considered internal metadata tables
    and are excluded from the result.

    Args:
        cursor: An open Trino DBAPI cursor.

    Returns:
        A sorted list of user table names.
    """
    cursor.execute(f"SHOW TABLES FROM {ICEBERG_CATALOG}.{ICEBERG_SCHEMA}")
    tables = [row[0] for row in cursor.fetchall()]
    return [t for t in tables if not t.startswith("_")]


# ---------------------------------------------------------------------------
# Table metrics
# ---------------------------------------------------------------------------


def get_snapshot_count(cursor: Any, table_name: str) -> int:
    """Get the number of snapshots for a table.

    Uses the Iceberg ``$snapshots`` metadata table exposed by Trino.

    Args:
        cursor: An open Trino DBAPI cursor.
        table_name: Table name without catalog/schema prefix.

    Returns:
        The snapshot count, or ``0`` if the metadata table is empty.
    """
    cursor.execute(f'SELECT COUNT(*) FROM "{_fqn(table_name)}$snapshots"')
    row = cursor.fetchone()
    return int(row[0]) if row else 0


def get_file_count(cursor: Any, table_name: str) -> int:
    """Get the number of data files for a table.

    Uses the Iceberg ``$files`` metadata table exposed by Trino.

    Args:
        cursor: An open Trino DBAPI cursor.
        table_name: Table name without catalog/schema prefix.

    Returns:
        The data file count, or ``0`` if the metadata table is empty.
    """
    cursor.execute(f'SELECT COUNT(*) FROM "{_fqn(table_name)}$files"')
    row = cursor.fetchone()
    return int(row[0]) if row else 0


# ---------------------------------------------------------------------------
# Maintenance operations
# ---------------------------------------------------------------------------


def run_optimize(cursor: Any, table_name: str) -> dict[str, str]:
    """Run ``ALTER TABLE EXECUTE optimize`` to compact small files.

    Uses Trino's ``optimize`` procedure which rewrites small files into
    larger ones.  This is the Trino equivalent of Iceberg's
    ``rewrite_data_files``.

    The ``file_size_threshold`` parameter targets files smaller than 10 MB.

    Args:
        cursor: An open Trino DBAPI cursor.
        table_name: Table name without catalog/schema prefix.

    Returns:
        A dict with ``table``, ``action``, and ``status`` keys.
    """
    fqn = _fqn(table_name)
    sql = f"ALTER TABLE {fqn} EXECUTE optimize(file_size_threshold => '10MB')"
    log.info("Running optimize on %s", fqn)
    cursor.execute(sql)
    cursor.fetchone()
    return {"table": table_name, "action": "optimize", "status": "completed"}


def run_expire_snapshots(
    cursor: Any,
    table_name: str,
    retention_days: int = 7,
) -> dict[str, Any]:
    """Expire old snapshots beyond the retention period.

    Uses Trino's ``expire_snapshots`` procedure.

    Args:
        cursor: An open Trino DBAPI cursor.
        table_name: Table name without catalog/schema prefix.
        retention_days: Number of days to retain snapshots (default 7).

    Returns:
        A dict with ``table``, ``action``, ``retention_days``, and
        ``status`` keys.
    """
    fqn = _fqn(table_name)
    sql = (
        f"ALTER TABLE {fqn} EXECUTE expire_snapshots("
        f"retention_threshold => '{retention_days}d')"
    )
    log.info("Expiring snapshots on %s (retention=%dd)", fqn, retention_days)
    cursor.execute(sql)
    cursor.fetchone()
    return {
        "table": table_name,
        "action": "expire_snapshots",
        "retention_days": retention_days,
        "status": "completed",
    }


def run_remove_orphan_files(
    cursor: Any,
    table_name: str,
    retention_days: int = 7,
) -> dict[str, Any]:
    """Remove orphan files not referenced by any snapshot.

    Uses Trino's ``remove_orphan_files`` procedure.

    Args:
        cursor: An open Trino DBAPI cursor.
        table_name: Table name without catalog/schema prefix.
        retention_days: Number of days to retain orphan files (default 7).

    Returns:
        A dict with ``table``, ``action``, ``retention_days``, and
        ``status`` keys.
    """
    fqn = _fqn(table_name)
    sql = (
        f"ALTER TABLE {fqn} EXECUTE remove_orphan_files("
        f"retention_threshold => '{retention_days}d')"
    )
    log.info("Removing orphan files on %s (retention=%dd)", fqn, retention_days)
    cursor.execute(sql)
    cursor.fetchone()
    return {
        "table": table_name,
        "action": "remove_orphan_files",
        "retention_days": retention_days,
        "status": "completed",
    }


# ---------------------------------------------------------------------------
# Maintenance log DDL
# ---------------------------------------------------------------------------


def build_maintenance_log_ddl() -> str:
    """Return CREATE TABLE DDL for the ``_maintenance_log`` metadata table.

    This table tracks every maintenance operation executed against
    user tables, recording before/after snapshot and file counts for
    observability.
    """
    return (
        f"CREATE TABLE IF NOT EXISTS {_fqn('_maintenance_log')} (\n"
        f"    table_name       VARCHAR,\n"
        f"    operation        VARCHAR,\n"
        f"    status           VARCHAR,\n"
        f"    details          VARCHAR,\n"
        f"    snapshots_before INTEGER,\n"
        f"    snapshots_after  INTEGER,\n"
        f"    files_before     INTEGER,\n"
        f"    files_after      INTEGER,\n"
        f"    dag_run_id       VARCHAR,\n"
        f"    executed_at      TIMESTAMP(6)\n"
        f")\n"
        f"WITH (\n"
        f"    format = 'PARQUET'\n"
        f")"
    )


def build_maintenance_log_insert(
    table_name: str,
    operation: str,
    status: str,
    details: str,
    snapshots_before: int,
    snapshots_after: int,
    files_before: int,
    files_after: int,
    dag_run_id: str,
) -> tuple[str, tuple]:
    """Build an INSERT statement for the ``_maintenance_log`` table.

    Args:
        table_name: The table that was maintained.
        operation: Maintenance operation name (e.g. ``optimize``).
        status: Outcome (``completed`` or ``error``).
        details: Human-readable detail string (truncated to 1000 chars).
        snapshots_before: Snapshot count before the operation.
        snapshots_after: Snapshot count after the operation.
        files_before: File count before the operation.
        files_after: File count after the operation.
        dag_run_id: Airflow DAG run identifier.

    Returns:
        A ``(sql, params)`` tuple suitable for ``cursor.execute(sql, params)``.
    """
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
    sql = (
        f"INSERT INTO {_fqn('_maintenance_log')} "
        f"(table_name, operation, status, details, snapshots_before, "
        f"snapshots_after, files_before, files_after, dag_run_id, executed_at) "
        f"VALUES (?, ?, ?, ?, CAST(? AS INTEGER), CAST(? AS INTEGER), "
        f"CAST(? AS INTEGER), CAST(? AS INTEGER), ?, CAST(? AS TIMESTAMP))"
    )
    params = (
        table_name,
        operation,
        status,
        details[:1000],
        str(snapshots_before),
        str(snapshots_after),
        str(files_before),
        str(files_after),
        dag_run_id,
        now_str,
    )
    return sql, params
