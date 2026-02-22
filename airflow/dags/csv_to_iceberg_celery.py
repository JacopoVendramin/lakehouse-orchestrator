"""
CSV to Apache Iceberg ingestion pipeline with incremental upsert.

Reads a local CSV file, validates its schema, uploads it to S3-compatible
object storage (SeaweedFS), then uses MERGE INTO for upsert semantics:
new records are inserted, existing records (by ``order_id``) are updated.

Supports watermark-based incremental extraction via Airflow Variables.
Only rows with ``ingestion_date`` greater than the last watermark are
processed, drastically reducing Trino work on re-runs.

Designed for CeleryExecutor: every task is a standalone PythonOperator that
serialises cleanly across Celery workers.

Author: data-engineering
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import boto3
import pandas as pd
import trino
from airflow.decorators import dag, task
from airflow.models import Variable

# =============================================================================
# Constants
# =============================================================================

# -- File & schema -----------------------------------------------------------
CSV_FILE_PATH: str = "/opt/airflow/data/raw/sales_sample.csv"
EXPECTED_COLUMNS: list[str] = [
    "order_id",
    "customer_id",
    "amount",
    "country",
    "ingestion_date",
]

# -- S3 / SeaweedFS ----------------------------------------------------------
S3_UPLOAD_KEY: str = "raw/sales/sales_sample.csv"

# -- Iceberg / Trino ---------------------------------------------------------
ICEBERG_CATALOG: str = "iceberg"
ICEBERG_SCHEMA: str = "lakehouse"
ICEBERG_TABLE: str = "sales"
ICEBERG_FULL_TABLE: str = f"{ICEBERG_CATALOG}.{ICEBERG_SCHEMA}.{ICEBERG_TABLE}"
SCHEMA_LOCATION: str = "s3a://lakehouse-warehouse/lakehouse/"
TRINO_USER: str = "airflow"

# -- Watermark ---------------------------------------------------------------
WATERMARK_VAR_KEY: str = "sales_pipeline_watermark"

# -- Batch size for MERGE INTO -----------------------------------------------
MERGE_BATCH_SIZE: int = 50

log = logging.getLogger(__name__)

# =============================================================================
# DAG documentation
# =============================================================================

DAG_DOC_MD: str = """
### CSV to Iceberg Pipeline (Incremental Upsert)

| Property       | Value                                         |
|----------------|-----------------------------------------------|
| **Schedule**   | `@daily`                                      |
| **Owner**      | `data-engineering`                            |
| **Executor**   | `CeleryExecutor`                              |
| **Idempotent** | Yes (MERGE INTO by `order_id`)                |
| **Incremental**| Yes (watermark on `ingestion_date`)           |

#### Task Graph

```
validate_csv_schema
        |
  upload_csv_to_s3
        |
create_iceberg_namespace
        |
 create_iceberg_table
        |
 upsert_data_into_iceberg
        |
 update_watermark
```

#### Description

End-to-end batch ingestion pipeline with incremental upsert:

1. **Schema validation** -- confirms the CSV matches the expected column
   contract before any downstream work begins.
2. **S3 upload** -- persists the raw file to SeaweedFS for durability and
   lineage.  Overwrites on re-run (idempotent).
3. **Namespace creation** -- ensures the Iceberg schema exists in Trino.
4. **Table creation** -- creates the partitioned Iceberg table with audit
   columns if it does not already exist.
5. **Upsert** -- uses MERGE INTO with `order_id` as the merge key.
   Only rows newer than the stored watermark are processed.
   Existing rows are updated, new rows are inserted.
6. **Watermark update** -- persists the highest `ingestion_date` processed
   to an Airflow Variable for the next run.

#### Environment Variables

| Variable           | Purpose                           |
|--------------------|-----------------------------------|
| `S3_ENDPOINT_URL`  | SeaweedFS / S3 endpoint           |
| `S3_ACCESS_KEY`    | S3 access key                     |
| `S3_SECRET_KEY`    | S3 secret key                     |
| `S3_BUCKET_NAME`   | Target S3 bucket                  |
| `S3_REGION`        | S3 region                         |
| `TRINO_HOST`       | Trino coordinator hostname        |
| `TRINO_PORT`       | Trino coordinator port            |
"""


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


def _read_csv(path: str) -> pd.DataFrame:
    """Read a CSV file into a DataFrame with basic sanity checks."""
    if not Path(path).is_file():
        raise FileNotFoundError(f"CSV file not found: {path}")

    df = pd.read_csv(path)

    if df.empty:
        raise ValueError(f"CSV file is empty: {path}")

    return df


def _get_watermark() -> str | None:
    """Retrieve the last watermark from Airflow Variables.

    Returns:
        The watermark date string (``YYYY-MM-DD``) or ``None`` if no
        watermark has been set yet (first run).
    """
    try:
        return Variable.get(WATERMARK_VAR_KEY, default_var=None)
    except Exception:
        return None


def _set_watermark(date_str: str) -> None:
    """Persist a new watermark to Airflow Variables."""
    Variable.set(WATERMARK_VAR_KEY, date_str)
    log.info("Watermark updated to %s", date_str)


# =============================================================================
# DAG definition
# =============================================================================


@dag(
    dag_id="csv_to_iceberg_pipeline",
    description=(
        "Ingest a CSV file into an Apache Iceberg table via Trino "
        "with MERGE INTO upsert and watermark-based incremental extraction"
    ),
    doc_md=DAG_DOC_MD,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["lakehouse", "iceberg", "ingestion", "incremental"],
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    },
)
def csv_to_iceberg_pipeline() -> None:
    """Top-level DAG factory for the CSV-to-Iceberg pipeline."""

    # --------------------------------------------------------------------- #
    # Task 1 -- Validate CSV schema
    # --------------------------------------------------------------------- #

    @task()
    def validate_csv_schema() -> dict[str, Any]:
        """Read the source CSV and verify its columns match the expected schema.

        Returns:
            A summary dict with ``row_count`` and ``columns``.
        """
        log.info("Reading CSV from %s", CSV_FILE_PATH)
        df = _read_csv(CSV_FILE_PATH)

        actual_columns = list(df.columns)

        if actual_columns != EXPECTED_COLUMNS:
            raise ValueError(
                f"Schema mismatch. "
                f"Expected columns: {EXPECTED_COLUMNS}  |  "
                f"Actual columns: {actual_columns}"
            )

        log.info(
            "Schema validation passed -- %d rows, columns: %s",
            len(df),
            actual_columns,
        )
        return {"row_count": len(df), "columns": actual_columns}

    # --------------------------------------------------------------------- #
    # Task 2 -- Upload CSV to S3
    # --------------------------------------------------------------------- #

    @task()
    def upload_csv_to_s3() -> dict[str, str]:
        """Upload the CSV file to S3-compatible storage (SeaweedFS)."""
        bucket = os.environ["S3_BUCKET_NAME"]

        log.info(
            "Uploading %s -> s3://%s/%s",
            CSV_FILE_PATH,
            bucket,
            S3_UPLOAD_KEY,
        )

        s3 = _get_s3_client()
        s3.upload_file(CSV_FILE_PATH, bucket, S3_UPLOAD_KEY)

        log.info("Upload complete")
        return {"bucket": bucket, "key": S3_UPLOAD_KEY}

    # --------------------------------------------------------------------- #
    # Task 3 -- Create Iceberg namespace
    # --------------------------------------------------------------------- #

    @task()
    def create_iceberg_namespace() -> None:
        """Create the Iceberg schema (namespace) in Trino if it does not exist."""
        ddl = (
            f"CREATE SCHEMA IF NOT EXISTS {ICEBERG_CATALOG}.{ICEBERG_SCHEMA} "
            f"WITH (location = '{SCHEMA_LOCATION}')"
        )

        log.info("Executing: %s", ddl)

        conn = _get_trino_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(ddl)
            cursor.fetchone()  # Trino requires fetching to materialise DDL
            log.info("Namespace %s.%s is ready", ICEBERG_CATALOG, ICEBERG_SCHEMA)
        finally:
            conn.close()

    # --------------------------------------------------------------------- #
    # Task 4 -- Create Iceberg table (with audit columns)
    # --------------------------------------------------------------------- #

    @task()
    def create_iceberg_table() -> None:
        """Create the partitioned Iceberg table with audit columns."""
        ddl = f"""
            CREATE TABLE IF NOT EXISTS {ICEBERG_FULL_TABLE} (
                order_id       VARCHAR,
                customer_id    VARCHAR,
                amount         DOUBLE,
                country        VARCHAR,
                ingestion_date DATE,
                _source_file   VARCHAR,
                _ingested_at   TIMESTAMP(6)
            )
            WITH (
                format       = 'PARQUET',
                partitioning = ARRAY['ingestion_date']
            )
        """

        log.info("Creating table %s (if not exists)", ICEBERG_FULL_TABLE)

        conn = _get_trino_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(ddl)
            cursor.fetchone()
            log.info("Table %s is ready", ICEBERG_FULL_TABLE)
        finally:
            conn.close()

    # --------------------------------------------------------------------- #
    # Task 5 -- Upsert data via MERGE INTO
    # --------------------------------------------------------------------- #

    @task()
    def upsert_data_into_iceberg() -> dict[str, Any]:
        """Load CSV rows into the Iceberg table using MERGE INTO.

        Incremental strategy:
            1. Read the watermark from Airflow Variables.
            2. Filter the CSV to only rows with ``ingestion_date`` >
               watermark (or all rows on first run).
            3. For each batch, execute a MERGE INTO statement that:
               - Updates existing rows (matched by ``order_id``)
               - Inserts new rows (not matched)

        This approach guarantees:
            - No duplicate records (dedup by ``order_id``)
            - Changed records are updated in-place
            - ``_ingested_at`` reflects the most recent ingestion time
            - Re-runs are safe (MERGE INTO is idempotent)

        Returns:
            A dict with processing stats.
        """
        df = _read_csv(CSV_FILE_PATH)

        # Coerce ingestion_date to proper date strings for Trino
        df["ingestion_date"] = pd.to_datetime(df["ingestion_date"]).dt.date

        # --- Watermark filtering ---
        watermark = _get_watermark()
        total_rows = len(df)

        if watermark:
            watermark_date = datetime.strptime(watermark, "%Y-%m-%d").date()
            df = df[df["ingestion_date"] > watermark_date]
            log.info(
                "Watermark filter: %s → %d/%d rows selected (after %s)",
                WATERMARK_VAR_KEY,
                len(df),
                total_rows,
                watermark,
            )
        else:
            log.info(
                "No watermark found — processing all %d rows (first run).",
                total_rows,
            )

        if df.empty:
            log.info("No new rows to process after watermark filtering.")
            return {
                "rows_merged": 0,
                "watermark_before": watermark,
                "watermark_after": watermark,
                "skipped": True,
            }

        # Track the max date for watermark update
        max_date = str(df["ingestion_date"].max())
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
        source_file = S3_UPLOAD_KEY

        log.info(
            "Preparing MERGE INTO for %d rows (dates: %s to %s)",
            len(df),
            df["ingestion_date"].min(),
            max_date,
        )

        conn = _get_trino_connection()
        rows_merged = 0
        try:
            cursor = conn.cursor()

            # Process in batches to avoid oversized queries
            records = df.to_dict("records")
            for batch_start in range(0, len(records), MERGE_BATCH_SIZE):
                batch = records[batch_start : batch_start + MERGE_BATCH_SIZE]

                # Build VALUES clause for the source CTE
                value_rows: list[str] = []
                for rec in batch:
                    # Escape single quotes in string values
                    order_id = str(rec["order_id"]).replace("'", "''")
                    customer_id = str(rec["customer_id"]).replace("'", "''")
                    country = str(rec["country"]).replace("'", "''")
                    amount = float(rec["amount"])
                    ing_date = str(rec["ingestion_date"])

                    value_rows.append(
                        f"('{order_id}', '{customer_id}', {amount}, "
                        f"'{country}', DATE '{ing_date}', "
                        f"'{source_file}', TIMESTAMP '{now_str}')"
                    )

                values_clause = ",\n        ".join(value_rows)

                merge_sql = f"""
                    MERGE INTO {ICEBERG_FULL_TABLE} AS target
                    USING (
                        VALUES
                        {values_clause}
                    ) AS source (
                        order_id, customer_id, amount, country,
                        ingestion_date, _source_file, _ingested_at
                    )
                    ON target.order_id = source.order_id
                    WHEN MATCHED THEN UPDATE SET
                        customer_id    = source.customer_id,
                        amount         = source.amount,
                        country        = source.country,
                        ingestion_date = source.ingestion_date,
                        _source_file   = source._source_file,
                        _ingested_at   = source._ingested_at
                    WHEN NOT MATCHED THEN INSERT (
                        order_id, customer_id, amount, country,
                        ingestion_date, _source_file, _ingested_at
                    ) VALUES (
                        source.order_id, source.customer_id, source.amount,
                        source.country, source.ingestion_date,
                        source._source_file, source._ingested_at
                    )
                """

                cursor.execute(merge_sql)
                cursor.fetchone()
                rows_merged += len(batch)

                log.info(
                    "MERGE batch %d: %d rows processed (%d/%d total)",
                    batch_start // MERGE_BATCH_SIZE + 1,
                    len(batch),
                    rows_merged,
                    len(records),
                )

            log.info(
                "MERGE INTO complete: %d rows processed",
                rows_merged,
            )
        finally:
            conn.close()

        return {
            "rows_merged": rows_merged,
            "watermark_before": watermark,
            "watermark_after": max_date,
            "source_file": source_file,
        }

    # --------------------------------------------------------------------- #
    # Task 6 -- Update watermark
    # --------------------------------------------------------------------- #

    @task()
    def update_watermark(upsert_result: dict[str, Any]) -> None:
        """Persist the new watermark after a successful upsert.

        Only updates the watermark if the upsert actually processed
        rows and the new watermark is higher than the current one.
        """
        if upsert_result.get("skipped"):
            log.info("Upsert was skipped — watermark unchanged.")
            return

        new_watermark = upsert_result.get("watermark_after")
        if not new_watermark:
            log.warning("No watermark_after in upsert result — skipping.")
            return

        current = _get_watermark()
        if current and current >= new_watermark:
            log.info(
                "Current watermark %s >= new %s — not updating.",
                current,
                new_watermark,
            )
            return

        _set_watermark(new_watermark)

    # --------------------------------------------------------------------- #
    # Task dependencies
    # --------------------------------------------------------------------- #

    t1 = validate_csv_schema()
    t2 = upload_csv_to_s3()
    t3 = create_iceberg_namespace()
    t4 = create_iceberg_table()
    t5 = upsert_data_into_iceberg()
    t6 = update_watermark(t5)

    t1 >> t2 >> t3 >> t4 >> t5 >> t6


# Instantiate the DAG
csv_to_iceberg_pipeline()
