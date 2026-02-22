"""
CSV Auto-Ingest Pipeline.

Polls the ``csv-uploads`` S3 bucket every 5 minutes for new or modified CSV
files, infers their schema, creates/evolves Iceberg tables, and loads the
data — all without manual configuration.

Bucket layout::

    s3://csv-uploads/
    ├── customers/
    │   ├── q1.csv   → iceberg.lakehouse.customers
    │   └── q2.csv   → iceberg.lakehouse.customers
    └── products/
        └── catalog.csv → iceberg.lakehouse.products

Uses Airflow's dynamic task mapping (``expand()``) to process multiple
files in parallel across CeleryExecutor workers.

Author: data-engineering
"""

from __future__ import annotations

import csv
import io
import logging
import os
import shutil
import tempfile
from datetime import datetime, timedelta
from typing import Any

import boto3
import trino
from airflow.decorators import dag, task

from lib.schema_manager import (
    build_add_column_ddl,
    build_batch_insert,
    build_create_registry_ddl,
    build_create_schema_ddl,
    build_create_table_ddl,
    build_delete_by_source,
    build_registry_insert,
    find_missing_columns,
    full_table_name,
    parse_describe_output,
)
from lib.superset_client import register_dataset
from lib.type_inference import infer_schema, sanitise_column_name

# =============================================================================
# Constants
# =============================================================================

TRINO_USER: str = "airflow"
ICEBERG_CATALOG: str = "iceberg"
ICEBERG_SCHEMA: str = "lakehouse"
LOCAL_TMP_DIR: str = "/tmp/auto_ingest"
SIZE_WARNING_BYTES: int = 100 * 1024 * 1024  # 100 MB

log = logging.getLogger(__name__)

# =============================================================================
# Helper functions
# =============================================================================


def _get_s3_client() -> boto3.client:
    """Build a boto3 S3 client from environment variables."""
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT_URL"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name=os.environ.get("S3_REGION", "us-east-1"),
    )


def _get_trino_connection() -> trino.dbapi.Connection:
    """Open a DBAPI connection to the Trino coordinator."""
    return trino.dbapi.connect(
        host=os.environ["TRINO_HOST"],
        port=int(os.environ["TRINO_PORT"]),
        user=TRINO_USER,
        catalog=ICEBERG_CATALOG,
        schema=ICEBERG_SCHEMA,
    )


def _table_name_from_key(s3_key: str) -> str:
    """Extract and sanitise a table name from an S3 key.

    ``customers/q1.csv`` → ``customers``
    """
    first_dir = s3_key.split("/")[0]
    return sanitise_column_name(first_dir)


# =============================================================================
# DAG documentation
# =============================================================================

DAG_DOC_MD: str = """
### CSV Auto-Ingest Pipeline

| Property       | Value                                        |
|----------------|----------------------------------------------|
| **Schedule**   | `*/5 * * * *` (every 5 minutes)              |
| **Owner**      | `data-engineering`                            |
| **Executor**   | `CeleryExecutor`                              |
| **Idempotent** | Yes (ETag-based dedup + delete/insert)        |

#### Description

Automatically detects CSV files in the `s3://csv-uploads` bucket, infers
their schema, creates Iceberg tables, and loads the data.  Supports schema
evolution (additive) and idempotent re-processing via ETag tracking.
"""


# =============================================================================
# DAG definition
# =============================================================================


@dag(
    dag_id="csv_auto_ingest",
    description="Auto-detect and ingest CSV files from S3 into Iceberg tables",
    doc_md=DAG_DOC_MD,
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "iceberg", "auto-ingest"],
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    },
)
def csv_auto_ingest() -> None:
    """Top-level DAG factory for the CSV auto-ingest pipeline."""

    # ------------------------------------------------------------------ #
    # Task 1 — Scan bucket
    # ------------------------------------------------------------------ #

    @task()
    def scan_bucket() -> list[dict[str, Any]]:
        """List all ``.csv`` files in the ingest bucket.

        Returns:
            A list of dicts with ``key``, ``etag``, ``size``,
            ``last_modified``, and ``table_name``.
        """
        bucket = os.environ.get("S3_INGEST_BUCKET", "csv-uploads")
        s3 = _get_s3_client()

        files: list[dict[str, Any]] = []
        continuation_token: str | None = None

        while True:
            kwargs: dict[str, Any] = {"Bucket": bucket}
            if continuation_token:
                kwargs["ContinuationToken"] = continuation_token

            resp = s3.list_objects_v2(**kwargs)

            for obj in resp.get("Contents", []):
                key: str = obj["Key"]
                if not key.lower().endswith(".csv"):
                    continue
                # Must have a directory prefix (table name)
                if "/" not in key:
                    continue

                size = obj.get("Size", 0)
                if size > SIZE_WARNING_BYTES:
                    log.warning(
                        "File %s is %.1f MB — processing may be slow.",
                        key,
                        size / (1024 * 1024),
                    )

                files.append({
                    "key": key,
                    "etag": obj["ETag"].strip('"'),
                    "size": size,
                    "last_modified": obj["LastModified"].isoformat(),
                    "table_name": _table_name_from_key(key),
                })

            if resp.get("IsTruncated"):
                continuation_token = resp["NextContinuationToken"]
            else:
                break

        log.info("Found %d CSV file(s) in s3://%s", len(files), bucket)
        return files

    # ------------------------------------------------------------------ #
    # Task 2 — Filter new files
    # ------------------------------------------------------------------ #

    @task()
    def filter_new_files(all_files: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Filter out files that have already been processed (same ETag).

        Queries the ``_ingest_registry`` table for known ETags.
        """
        if not all_files:
            log.info("No files found — nothing to filter.")
            return []

        conn = _get_trino_connection()
        try:
            cursor = conn.cursor()

            # Ensure the registry table exists
            cursor.execute(build_create_schema_ddl())
            cursor.fetchone()
            cursor.execute(build_create_registry_ddl())
            cursor.fetchone()

            # Load known etags
            cursor.execute(
                f"SELECT file_key, etag FROM {full_table_name('_ingest_registry')} "
                f"WHERE status = 'success'"
            )
            known = {(row[0], row[1]) for row in cursor.fetchall()}
        finally:
            conn.close()

        new_files = [
            f for f in all_files
            if (f["key"], f["etag"]) not in known
        ]

        log.info(
            "Filtered: %d new/changed file(s) out of %d total.",
            len(new_files),
            len(all_files),
        )
        return new_files

    # ------------------------------------------------------------------ #
    # Task 3 — Process a single file (all-in-one for dynamic mapping)
    # ------------------------------------------------------------------ #

    @task()
    def process_file(file_meta: dict[str, Any], **context) -> dict[str, Any]:
        """Process a single CSV file end-to-end.

        Steps:
          1. Download CSV from S3
          2. Detect format (delimiter)
          3. Infer schema
          4. Ensure table exists (CREATE TABLE IF NOT EXISTS)
          5. Evolve schema (ALTER TABLE ADD COLUMN)
          6. Ingest data (DELETE + INSERT)
          7. Register Superset dataset (if new table)
          8. Update registry

        This is implemented as a single task to simplify dynamic mapping
        with ``.expand()``.  Each file is still processed independently
        and in parallel via CeleryExecutor.
        """
        dag_run_id = context.get("run_id", "unknown")
        s3_key = file_meta["key"]
        etag = file_meta["etag"]
        table_name = file_meta["table_name"]

        log.info("Processing file: s3://%s/%s → %s", "csv-uploads", s3_key, table_name)

        local_path = None
        try:
            # ----- Step 1: Download CSV -----
            bucket = os.environ.get("S3_INGEST_BUCKET", "csv-uploads")
            s3 = _get_s3_client()

            os.makedirs(LOCAL_TMP_DIR, exist_ok=True)
            local_path = os.path.join(
                LOCAL_TMP_DIR,
                s3_key.replace("/", "_"),
            )
            s3.download_file(bucket, s3_key, local_path)
            log.info("Downloaded %s to %s", s3_key, local_path)

            # ----- Step 2: Detect format -----
            with open(local_path, "r", encoding="utf-8") as f:
                sample = f.read(8192)

            sniffer = csv.Sniffer()
            try:
                dialect = sniffer.sniff(sample)
                delimiter = dialect.delimiter
            except csv.Error:
                delimiter = ","
                log.warning(
                    "csv.Sniffer failed for %s — defaulting to comma.",
                    s3_key,
                )

            log.info("Detected delimiter: %r", delimiter)

            # ----- Step 3: Read CSV and infer schema -----
            with open(local_path, "r", encoding="utf-8") as f:
                reader = csv.reader(f, delimiter=delimiter)
                raw_headers = next(reader)
                rows: list[list[str]] = []
                for row in reader:
                    rows.append(row)

            if not raw_headers:
                raise ValueError(f"No header row found in {s3_key}")

            headers = [h.strip() for h in raw_headers]
            schema = infer_schema(rows, headers)
            log.info(
                "Inferred schema (%d columns): %s",
                len(schema),
                [(c["name"], c["type"]) for c in schema],
            )

            # ----- Step 4: Ensure table exists -----
            conn = _get_trino_connection()
            table_created = False
            try:
                cursor = conn.cursor()

                # Ensure schema exists
                cursor.execute(build_create_schema_ddl())
                cursor.fetchone()

                # Check if table exists by trying DESCRIBE
                try:
                    cursor.execute(
                        f"DESCRIBE {full_table_name(table_name)}"
                    )
                    existing_cols = parse_describe_output(cursor.fetchall())
                    log.info("Table %s already exists with %d columns.", table_name, len(existing_cols))
                except Exception:
                    existing_cols = {}
                    # Table doesn't exist — create it
                    ddl = build_create_table_ddl(table_name, schema)
                    log.info("Creating table: %s", ddl)
                    cursor.execute(ddl)
                    cursor.fetchone()
                    table_created = True
                    log.info("Created table %s", table_name)

                # ----- Step 5: Schema evolution -----
                columns_added: list[str] = []
                if not table_created and existing_cols:
                    missing = find_missing_columns(schema, existing_cols)
                    for col in missing:
                        alter_ddl = build_add_column_ddl(table_name, col)
                        log.info("Adding column: %s", alter_ddl)
                        cursor.execute(alter_ddl)
                        cursor.fetchone()
                        columns_added.append(col["name"])

                    if columns_added:
                        log.info(
                            "Added %d column(s): %s",
                            len(columns_added),
                            columns_added,
                        )

                # ----- Step 6: Ingest data -----
                # Delete existing rows for this source file
                delete_sql = build_delete_by_source(table_name, s3_key)
                log.info("Deleting old rows: %s", delete_sql)
                cursor.execute(delete_sql)
                cursor.fetchone()

                # Batch insert
                batches = build_batch_insert(
                    table_name, schema, rows, s3_key
                )
                total_inserted = 0
                for batch_idx, (sql, params_list) in enumerate(batches):
                    for params in params_list:
                        cursor.execute(sql, params)
                        cursor.fetchone()
                        total_inserted += 1

                    log.info(
                        "Batch %d/%d: inserted %d rows",
                        batch_idx + 1,
                        len(batches),
                        len(params_list),
                    )

                log.info(
                    "Ingested %d rows into %s",
                    total_inserted,
                    table_name,
                )

            finally:
                conn.close()

            # ----- Step 7: Register Superset dataset -----
            if table_created:
                superset_result = register_dataset(table_name)
                log.info("Superset registration: %s", superset_result)
            else:
                superset_result = {"dataset_id": None, "skipped": True}

            # ----- Step 8: Update registry -----
            conn = _get_trino_connection()
            try:
                cursor = conn.cursor()
                reg_sql, reg_params = build_registry_insert(
                    file_key=s3_key,
                    etag=etag,
                    table_name=table_name,
                    row_count=total_inserted,
                    status="success",
                    error_message=None,
                    dag_run_id=dag_run_id,
                )
                cursor.execute(reg_sql, reg_params)
                cursor.fetchone()
                log.info("Updated registry for %s", s3_key)
            finally:
                conn.close()

            return {
                "key": s3_key,
                "table_name": table_name,
                "rows_inserted": total_inserted,
                "table_created": table_created,
                "columns_added": columns_added,
                "superset": superset_result,
                "status": "success",
            }

        except Exception as exc:
            log.exception("Failed to process %s: %s", s3_key, exc)

            # Record failure in registry
            try:
                conn = _get_trino_connection()
                try:
                    cursor = conn.cursor()
                    # Ensure registry table exists
                    cursor.execute(build_create_registry_ddl())
                    cursor.fetchone()

                    reg_sql, reg_params = build_registry_insert(
                        file_key=s3_key,
                        etag=etag,
                        table_name=table_name,
                        row_count=0,
                        status="failed",
                        error_message=str(exc)[:500],
                        dag_run_id=dag_run_id,
                    )
                    cursor.execute(reg_sql, reg_params)
                    cursor.fetchone()
                finally:
                    conn.close()
            except Exception:
                log.exception("Failed to record failure in registry")

            raise

        finally:
            # Cleanup temp file
            if local_path and os.path.exists(local_path):
                os.remove(local_path)

    # ------------------------------------------------------------------ #
    # Wire the DAG
    # ------------------------------------------------------------------ #

    all_files = scan_bucket()
    new_files = filter_new_files(all_files)
    process_file.expand(file_meta=new_files)


# Instantiate the DAG
csv_auto_ingest()
