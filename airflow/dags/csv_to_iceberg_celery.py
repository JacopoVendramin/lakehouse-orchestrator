"""
CSV to Apache Iceberg ingestion pipeline.

Reads a local CSV file, validates its schema, uploads it to S3-compatible
object storage (SeaweedFS), then creates the target Iceberg namespace and
table via Trino before inserting the data with full idempotency.

Designed for CeleryExecutor: every task is a standalone PythonOperator that
serialises cleanly across Celery workers.

Author: data-engineering
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import boto3
import pandas as pd
import trino
from airflow.decorators import dag, task

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

log = logging.getLogger(__name__)

# =============================================================================
# DAG documentation
# =============================================================================

DAG_DOC_MD: str = """
### CSV to Iceberg Pipeline

| Property       | Value                                |
|----------------|--------------------------------------|
| **Schedule**   | `@daily`                             |
| **Owner**      | `data-engineering`                   |
| **Executor**   | `CeleryExecutor`                     |
| **Idempotent** | Yes (delete + re-insert per date)    |

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
insert_data_into_iceberg
```

#### Description

End-to-end batch ingestion pipeline that moves a local CSV file through
the lakehouse stack:

1. **Schema validation** -- confirms the CSV matches the expected column
   contract before any downstream work begins.
2. **S3 upload** -- persists the raw file to SeaweedFS for durability and
   lineage.  Overwrites on re-run (idempotent).
3. **Namespace creation** -- ensures the Iceberg schema exists in Trino.
4. **Table creation** -- creates the partitioned Iceberg table if it does
   not already exist.
5. **Data insertion** -- loads rows into the Iceberg table using a
   *delete-then-insert* pattern keyed on `ingestion_date` so that
   re-runs never produce duplicates.

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
    """Build a boto3 S3 client from environment variables.

    Returns:
        A configured ``boto3.client('s3')`` pointed at the SeaweedFS
        gateway (or any S3-compatible endpoint).

    Raises:
        KeyError: If a required environment variable is missing.
    """
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT_URL"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name=os.environ.get("S3_REGION", "us-east-1"),
    )


def _get_trino_connection() -> trino.dbapi.Connection:
    """Open a DBAPI connection to the Trino coordinator.

    Returns:
        A ``trino.dbapi.Connection`` ready for cursor operations.

    Raises:
        KeyError: If ``TRINO_HOST`` or ``TRINO_PORT`` is not set.
    """
    return trino.dbapi.connect(
        host=os.environ["TRINO_HOST"],
        port=int(os.environ["TRINO_PORT"]),
        user=TRINO_USER,
        catalog=ICEBERG_CATALOG,
        schema=ICEBERG_SCHEMA,
    )


def _read_csv(path: str) -> pd.DataFrame:
    """Read a CSV file into a DataFrame with basic sanity checks.

    Args:
        path: Absolute filesystem path to the CSV file.

    Returns:
        A ``pandas.DataFrame`` with the file contents.

    Raises:
        FileNotFoundError: If *path* does not exist.
        ValueError: If the file is empty.
    """
    if not Path(path).is_file():
        raise FileNotFoundError(f"CSV file not found: {path}")

    df = pd.read_csv(path)

    if df.empty:
        raise ValueError(f"CSV file is empty: {path}")

    return df


# =============================================================================
# DAG definition
# =============================================================================


@dag(
    dag_id="csv_to_iceberg_pipeline",
    description="Ingest a CSV file into an Apache Iceberg table via Trino",
    doc_md=DAG_DOC_MD,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["lakehouse", "iceberg", "ingestion"],
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

        Ensures the file exists, is not empty, and contains exactly the
        columns defined in ``EXPECTED_COLUMNS`` (order-sensitive).

        Returns:
            A summary dict with ``row_count`` and ``columns`` for
            downstream XCom consumers.

        Raises:
            FileNotFoundError: If the CSV file is missing.
            ValueError: If the file is empty or the schema does not match.
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
        """Upload the CSV file to S3-compatible storage (SeaweedFS).

        The upload is idempotent: if the object already exists it will be
        silently overwritten.

        Returns:
            A dict with ``bucket`` and ``key`` confirming the upload
            location.

        Raises:
            FileNotFoundError: If the local CSV file is missing.
            botocore.exceptions.ClientError: On S3 communication failures.
        """
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
        """Create the Iceberg schema (namespace) in Trino if it does not exist.

        Executes:
            ``CREATE SCHEMA IF NOT EXISTS iceberg.lakehouse``

        The schema is backed by the S3 location defined in
        ``SCHEMA_LOCATION``.

        Raises:
            trino.exceptions.TrinoExternalError: On Trino query failures.
        """
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
    # Task 4 -- Create Iceberg table
    # --------------------------------------------------------------------- #

    @task()
    def create_iceberg_table() -> None:
        """Create the partitioned Iceberg table in Trino if it does not exist.

        The table is partitioned by ``ingestion_date`` and stored in
        Parquet format to balance query performance with storage
        efficiency.

        Raises:
            trino.exceptions.TrinoExternalError: On Trino query failures.
        """
        ddl = f"""
            CREATE TABLE IF NOT EXISTS {ICEBERG_FULL_TABLE} (
                order_id    VARCHAR,
                customer_id VARCHAR,
                amount      DOUBLE,
                country     VARCHAR,
                ingestion_date DATE
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
    # Task 5 -- Insert data into Iceberg
    # --------------------------------------------------------------------- #

    @task()
    def insert_data_into_iceberg() -> dict[str, Any]:
        """Load CSV rows into the Iceberg table with idempotent semantics.

        Idempotency strategy (per ``ingestion_date``):
            1. Identify every distinct ``ingestion_date`` present in the
               source CSV.
            2. ``DELETE`` existing rows for those dates from the target
               table.
            3. ``INSERT`` all rows from the CSV.

        This delete-then-insert pattern guarantees that re-running the
        task for the same file produces the same result without
        duplicating data.

        Returns:
            A dict with ``rows_inserted`` and ``dates_processed`` for
            observability.

        Raises:
            trino.exceptions.TrinoExternalError: On Trino query failures.
            ValueError: If the CSV is empty.
        """
        df = _read_csv(CSV_FILE_PATH)

        # Coerce ingestion_date to proper date strings for Trino
        df["ingestion_date"] = pd.to_datetime(df["ingestion_date"]).dt.date

        distinct_dates: list[str] = sorted(
            df["ingestion_date"].unique().astype(str).tolist()
        )

        log.info(
            "Preparing to insert %d rows for %d distinct ingestion date(s): %s",
            len(df),
            len(distinct_dates),
            distinct_dates,
        )

        conn = _get_trino_connection()
        try:
            cursor = conn.cursor()

            # -- Step 1: Delete existing data for affected dates ---------- #
            for date_str in distinct_dates:
                delete_sql = (
                    f"DELETE FROM {ICEBERG_FULL_TABLE} "
                    f"WHERE ingestion_date = DATE '{date_str}'"
                )
                log.info("Deleting existing rows: %s", delete_sql)
                cursor.execute(delete_sql)
                cursor.fetchone()

            # -- Step 2: Insert fresh rows -------------------------------- #
            insert_sql = (
                f"INSERT INTO {ICEBERG_FULL_TABLE} "
                f"(order_id, customer_id, amount, country, ingestion_date) "
                f"VALUES (?, ?, ?, ?, CAST(? AS DATE))"
            )

            for row in df.itertuples(index=False):
                cursor.execute(
                    insert_sql,
                    (
                        str(row.order_id),
                        str(row.customer_id),
                        float(row.amount),
                        str(row.country),
                        str(row.ingestion_date),
                    ),
                )
                cursor.fetchone()

            log.info(
                "Successfully inserted %d rows across dates %s",
                len(df),
                distinct_dates,
            )
        finally:
            conn.close()

        return {
            "rows_inserted": len(df),
            "dates_processed": distinct_dates,
        }

    # --------------------------------------------------------------------- #
    # Task dependencies
    # --------------------------------------------------------------------- #

    (
        validate_csv_schema()
        >> upload_csv_to_s3()
        >> create_iceberg_namespace()
        >> create_iceberg_table()
        >> insert_data_into_iceberg()
    )


# Instantiate the DAG
csv_to_iceberg_pipeline()
