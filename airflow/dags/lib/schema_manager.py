"""
Schema manager for CSV auto-ingest.

Handles all Iceberg/Trino DDL operations:
  - CREATE SCHEMA IF NOT EXISTS
  - CREATE TABLE IF NOT EXISTS (from inferred schema)
  - DESCRIBE table (parse existing columns)
  - ALTER TABLE ADD COLUMN (additive schema evolution)
  - Batch INSERT builder

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
SCHEMA_LOCATION: str = "s3a://lakehouse-warehouse/lakehouse/"
BATCH_SIZE: int = 500

# Audit columns appended to every table
AUDIT_COLUMNS: list[dict[str, str]] = [
    {"name": "_source_file", "type": "VARCHAR"},
    {"name": "_ingested_at", "type": "TIMESTAMP(6)"},
]


# ---------------------------------------------------------------------------
# DDL helpers
# ---------------------------------------------------------------------------


def full_table_name(table_name: str) -> str:
    """Return the fully-qualified Iceberg table name."""
    return f"{ICEBERG_CATALOG}.{ICEBERG_SCHEMA}.{table_name}"


def build_create_schema_ddl() -> str:
    """Return the CREATE SCHEMA IF NOT EXISTS statement."""
    return (
        f"CREATE SCHEMA IF NOT EXISTS {ICEBERG_CATALOG}.{ICEBERG_SCHEMA} "
        f"WITH (location = '{SCHEMA_LOCATION}')"
    )


def build_create_table_ddl(
    table_name: str,
    columns: list[dict[str, str]],
) -> str:
    """Build a CREATE TABLE IF NOT EXISTS statement.

    Args:
        table_name: Sanitised table name (no catalog/schema prefix).
        columns: List of ``{"name": ..., "type": ...}`` dicts from type
                 inference.  Audit columns are appended automatically.

    Returns:
        A complete DDL string.
    """
    all_columns = columns + AUDIT_COLUMNS
    col_defs = ",\n    ".join(
        f"{col['name']} {col['type']}" for col in all_columns
    )
    return (
        f"CREATE TABLE IF NOT EXISTS {full_table_name(table_name)} (\n"
        f"    {col_defs}\n"
        f")\n"
        f"WITH (\n"
        f"    format = 'PARQUET'\n"
        f")"
    )


def build_create_registry_ddl() -> str:
    """Return the DDL for the ``_ingest_registry`` tracking table."""
    return (
        f"CREATE TABLE IF NOT EXISTS {full_table_name('_ingest_registry')} (\n"
        f"    file_key      VARCHAR,\n"
        f"    etag          VARCHAR,\n"
        f"    table_name    VARCHAR,\n"
        f"    row_count     INTEGER,\n"
        f"    status        VARCHAR,\n"
        f"    error_message VARCHAR,\n"
        f"    ingested_at   TIMESTAMP(6),\n"
        f"    dag_run_id    VARCHAR\n"
        f")\n"
        f"WITH (\n"
        f"    format = 'PARQUET'\n"
        f")"
    )


# ---------------------------------------------------------------------------
# Schema introspection
# ---------------------------------------------------------------------------


def parse_describe_output(rows: list[tuple]) -> dict[str, str]:
    """Parse the output of ``DESCRIBE iceberg.lakehouse.<table>``.

    Trino DESCRIBE returns rows of ``(column_name, data_type, extra, comment)``.

    Returns:
        A dict mapping column name → data type (uppercased).
    """
    result: dict[str, str] = {}
    for row in rows:
        col_name = row[0].strip().lower()
        col_type = row[1].strip().upper()
        result[col_name] = col_type
    return result


def find_missing_columns(
    inferred: list[dict[str, str]],
    existing: dict[str, str],
) -> list[dict[str, str]]:
    """Find columns present in the inferred schema but missing from the table.

    Args:
        inferred: Inferred column list (from type inference).
        existing: Existing columns from ``parse_describe_output``.

    Returns:
        A list of ``{"name": ..., "type": ...}`` dicts for missing columns.
    """
    return [
        col for col in inferred
        if col["name"] not in existing
    ]


def build_add_column_ddl(table_name: str, column: dict[str, str]) -> str:
    """Build an ALTER TABLE ADD COLUMN statement."""
    return (
        f"ALTER TABLE {full_table_name(table_name)} "
        f"ADD COLUMN {column['name']} {column['type']}"
    )


# ---------------------------------------------------------------------------
# Data ingestion helpers
# ---------------------------------------------------------------------------


def build_delete_by_source(table_name: str, source_file: str) -> str:
    """Build a DELETE statement to remove rows from a specific source file."""
    return (
        f"DELETE FROM {full_table_name(table_name)} "
        f"WHERE _source_file = '{source_file}'"
    )


def build_batch_insert(
    table_name: str,
    columns: list[dict[str, str]],
    rows: list[list[Any]],
    source_file: str,
) -> list[tuple[str, list[tuple]]]:
    """Build batched INSERT statements with parameterised values.

    Each batch contains up to ``BATCH_SIZE`` rows.  Audit columns
    (``_source_file``, ``_ingested_at``) are appended to every row.

    Args:
        table_name: Target table name (without catalog/schema).
        columns: Inferred column definitions.
        rows: Data rows (list of lists, matching column order).
        source_file: The S3 key used as ``_source_file`` value.

    Returns:
        A list of ``(sql, params)`` tuples, where ``sql`` is an INSERT
        statement with ``?`` placeholders and ``params`` is a list of
        value tuples.
    """
    col_names = [c["name"] for c in columns] + ["_source_file", "_ingested_at"]
    col_types = [c["type"] for c in columns] + ["VARCHAR", "TIMESTAMP(6)"]

    placeholders = []
    for ct in col_types:
        if ct == "DATE":
            placeholders.append("CAST(? AS DATE)")
        elif ct.startswith("TIMESTAMP"):
            placeholders.append("CAST(? AS TIMESTAMP)")
        elif ct == "INTEGER":
            placeholders.append("CAST(? AS INTEGER)")
        elif ct == "BIGINT":
            placeholders.append("CAST(? AS BIGINT)")
        elif ct == "DOUBLE":
            placeholders.append("CAST(? AS DOUBLE)")
        elif ct == "BOOLEAN":
            placeholders.append("CAST(? AS BOOLEAN)")
        else:
            placeholders.append("?")

    placeholder_str = ", ".join(placeholders)
    col_list = ", ".join(col_names)

    fqn = full_table_name(table_name)
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")

    batches: list[tuple[str, list[tuple]]] = []

    for i in range(0, len(rows), BATCH_SIZE):
        batch_rows = rows[i : i + BATCH_SIZE]
        sql = (
            f"INSERT INTO {fqn} ({col_list}) "
            f"VALUES ({placeholder_str})"
        )

        params_list: list[tuple] = []
        for row in batch_rows:
            # Coerce each value to string (Trino parameterised queries
            # receive all values as strings)
            values: list[str | None] = []
            for idx, col in enumerate(columns):
                raw = row[idx] if idx < len(row) else None
                if raw is None or (isinstance(raw, str) and raw.strip() == ""):
                    values.append(None)
                else:
                    values.append(str(raw))
            # Append audit columns
            values.append(source_file)
            values.append(now_str)
            params_list.append(tuple(values))

        batches.append((sql, params_list))

    return batches


def build_registry_insert(
    file_key: str,
    etag: str,
    table_name: str,
    row_count: int,
    status: str,
    error_message: str | None,
    dag_run_id: str,
) -> tuple[str, tuple]:
    """Build an INSERT statement for the ``_ingest_registry`` table."""
    fqn = full_table_name("_ingest_registry")
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")

    sql = (
        f"INSERT INTO {fqn} "
        f"(file_key, etag, table_name, row_count, status, error_message, "
        f"ingested_at, dag_run_id) "
        f"VALUES (?, ?, ?, ?, ?, ?, CAST(? AS TIMESTAMP), ?)"
    )
    params = (
        file_key,
        etag,
        table_name,
        row_count,
        status,
        error_message or "",
        now_str,
        dag_run_id,
    )
    return sql, params
