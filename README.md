# Lakehouse Orchestrator

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Docker Compose](https://img.shields.io/badge/Docker_Compose-ready-2496ED.svg)](docker-compose.yml)
[![Airflow 2.10](https://img.shields.io/badge/Airflow-2.10-017CEE.svg)](https://airflow.apache.org/)
[![Trino](https://img.shields.io/badge/Trino-latest-DD00A1.svg)](https://trino.io/)
[![Apache Iceberg](https://img.shields.io/badge/Iceberg-REST_Catalog-blue.svg)](https://iceberg.apache.org/)

An enterprise-grade, container-native lakehouse platform that ingests raw CSV data into Apache Iceberg tables and serves interactive dashboards -- deployed with a single command.

---

## Architecture Overview

The platform runs seven core services on a single Docker bridge network. Apache Airflow orchestrates the data pipeline, writing raw files to SeaweedFS (S3-compatible storage) and registering Iceberg tables through Trino. Superset connects to Trino for business intelligence and visualization. PostgreSQL provides metadata storage for Airflow, Celery, and Superset. Valkey serves as the Celery message broker.

```
                            Data Flow
  ┌─────────┐    ┌──────────────────────────────┐    ┌──────────────┐
  │   CSV   │───>│        Apache Airflow        │───>│  SeaweedFS   │
  │  Files  │    │  Scheduler | Worker | Flower │    │  (S3 API)    │
  └─────────┘    └──────────────┬───────────────┘    └──────┬───────┘
                                │                           │
                     ┌──────────┴──────────┐                │
                     │                     │                │
              ┌──────▼──────┐  ┌───────────▼──┐   ┌────────▼───────┐
              │ PostgreSQL  │  │    Valkey     │   │  Iceberg REST  │
              │ (metadata)  │  │   (broker)    │   │   Catalog      │
              └─────────────┘  └──────────────┘   └────────┬───────┘
                                                           │
                                                    ┌──────▼───────┐
                                                    │    Trino     │
                                                    │ (query engine)│
                                                    └──────┬───────┘
                                                           │
                                                    ┌──────▼───────┐
                                                    │   Superset   │
                                                    │ (dashboards) │
                                                    └──────────────┘
```

**Pipeline stages:** CSV → Airflow (validate + upload) → SeaweedFS (S3 object store) → Iceberg REST Catalog (table metadata) → Trino (distributed SQL) → Superset (visualization)

---

## Why This Stack

### Why Iceberg over Hive

| Criterion | Hive Table Format | Apache Iceberg |
|-----------|-------------------|----------------|
| Schema evolution | Limited `ALTER TABLE`; not always backward-compatible | Full add, drop, rename, reorder with metadata versioning |
| Partition evolution | Requires rewriting all data | New partition specs apply to new data only |
| Time travel | Not supported natively | Built-in snapshot isolation; query any historical state |
| Hidden partitioning | Queries must reference partition columns explicitly | Partition transforms applied automatically |
| File-level metadata | Partition-level tracking only | Per-file column min/max statistics for predicate pushdown |
| Engine independence | Coupled to Hive metastore | Works with Trino, Spark, Flink without a Hive metastore |

Iceberg provides stronger guarantees for schema evolution, partition management, and time travel -- critical for a lakehouse where schemas evolve as new sources are onboarded.

### Why CeleryExecutor + Valkey

Airflow's CeleryExecutor enables distributed task execution across horizontally scalable workers. Tasks are serialized as messages, sent to a broker, and consumed by any available worker -- allowing the platform to scale ingestion throughput by adding workers.

Valkey 8 is a BSD 3-Clause licensed fork of Redis, maintained by the Linux Foundation. It provides 100% Redis wire-protocol compatibility, meaning Airflow's Celery integration connects via the standard `redis://` URI with no code changes. Choosing Valkey over Redis avoids the licensing ambiguity introduced by Redis Ltd.'s SSPL/RSALv2 relicensing.

### Why SeaweedFS over MinIO

| Criterion | MinIO | SeaweedFS |
|-----------|-------|-----------|
| License | AGPL-3.0 (copyleft) | Apache 2.0 (permissive) |
| Architecture | Monolithic server | Separated master/volume/filer/S3 gateway |
| Resource footprint | Heavier; designed for large-scale deployments | Lightweight; runs well on a single machine |
| S3 compatibility | Full S3 API | Sufficient coverage for Iceberg, Trino, and Airflow |
| Startup time | Moderate | Fast; volume servers register in seconds |

SeaweedFS provides the S3 compatibility required by Trino's Iceberg connector and Airflow's boto3 client under a permissive Apache 2.0 license. Its separated architecture allows independent scaling of metadata and storage.

---

## Quick Start

### Prerequisites

- Docker Engine 20.10+ and Docker Compose v2
- At least 8 GB of RAM allocated to Docker

### Steps

```bash
git clone https://github.com/jvendramin/lakehouse-orchestrator.git
cd lakehouse-orchestrator
cp .env.example .env
docker compose up -d
```

Initial startup takes approximately 2-3 minutes. Airflow runs database migrations and creates the admin user on first boot. SeaweedFS buckets (`lakehouse`, `lakehouse-warehouse`, `csv-uploads`) are provisioned automatically by the `s3-init` container.

Verify all services are healthy:

```bash
docker compose ps
```

Once all services report `healthy` or `running`, open the interfaces listed in the next section.

---

## Service Endpoints

| Service | URL | Port | Credentials |
|---------|-----|------|-------------|
| Airflow Webserver | [http://localhost:8081](http://localhost:8081) | 8081 | `admin` / `admin` |
| Flower (Celery monitor) | [http://localhost:5555](http://localhost:5555) | 5555 | -- |
| Trino | [http://localhost:8083](http://localhost:8083) | 8083 | -- |
| Superset | [http://localhost:8088](http://localhost:8088) | 8088 | `admin` / `admin` |
| SeaweedFS S3 Gateway | [http://localhost:8333](http://localhost:8333) | 8333 | configured in `.env` |
| SeaweedFS Master | [http://localhost:9333](http://localhost:9333) | 9333 | -- |
| SeaweedFS Filer | [http://localhost:8888](http://localhost:8888) | 8888 | -- |
| Iceberg REST Catalog | [http://localhost:8181](http://localhost:8181) | 8181 | -- |
| PostgreSQL | `localhost:5432` | 5432 | `airflow` / `airflow` |

All ports are configurable via environment variables in `.env`.

---

## How to Trigger the DAG

### Sales Pipeline (Manual)

The `csv_to_iceberg_pipeline` DAG is paused by default. Unpause and trigger it from the Airflow UI or the CLI:

```bash
# Unpause the DAG
docker compose exec airflow-webserver airflow dags unpause csv_to_iceberg_pipeline

# Trigger a DAG run
docker compose exec airflow-webserver airflow dags trigger csv_to_iceberg_pipeline
```

The pipeline executes five sequential tasks:

1. **validate_csv_schema** -- confirms the CSV matches the expected column contract
2. **upload_csv_to_s3** -- uploads the raw file to SeaweedFS for durability
3. **create_iceberg_namespace** -- creates the `iceberg.lakehouse` schema in Trino
4. **create_iceberg_table** -- creates the partitioned Iceberg table (Parquet format, partitioned by `ingestion_date`)
5. **insert_data_into_iceberg** -- loads rows using a delete-then-insert pattern for idempotency

Monitor progress in the Airflow UI at [http://localhost:8081](http://localhost:8081) or watch Celery worker activity in Flower at [http://localhost:5555](http://localhost:5555).

### CSV Auto-Ingest Pipeline (Automatic)

The `csv_auto_ingest` DAG polls the `s3://csv-uploads` bucket every 5 minutes for new CSV files and automatically creates Iceberg tables from them. No schema definition or DAG modification is needed.

**Usage:**

1. Unpause the DAG:

   ```bash
   docker compose exec airflow-webserver airflow dags unpause csv_auto_ingest
   ```

2. Upload a CSV to the ingest bucket. The directory name becomes the table name:

   ```bash
   # Upload via aws CLI
   aws --endpoint-url http://localhost:8333 s3 cp my_data.csv s3://csv-uploads/my_table/my_data.csv

   # Or via curl (SeaweedFS S3 gateway)
   curl -X PUT "http://localhost:8333/csv-uploads/my_table/my_data.csv" \
     --data-binary @my_data.csv
   ```

3. Wait for the next polling cycle (up to 5 minutes) or trigger manually:

   ```bash
   docker compose exec airflow-webserver airflow dags trigger csv_auto_ingest
   ```

4. Query the new table in Trino:

   ```sql
   SELECT * FROM iceberg.lakehouse.my_table;
   ```

**Features:**

| Feature | Details |
|---------|---------|
| Schema inference | Detects INTEGER, BIGINT, DOUBLE, BOOLEAN, DATE, TIMESTAMP, VARCHAR |
| Delimiter detection | Auto-detects comma, semicolon, tab, pipe via `csv.Sniffer` |
| Schema evolution | New columns in re-uploaded CSVs are added via `ALTER TABLE ADD COLUMN` |
| Idempotent | ETag-based tracking prevents re-processing unchanged files |
| Replace strategy | Re-uploading a file replaces only its rows (keyed on `_source_file`) |
| Audit columns | `_source_file` and `_ingested_at` added to every row |
| Superset auto-registration | New tables are automatically registered as Superset datasets |
| Parallel processing | Multiple files processed concurrently via CeleryExecutor |

---

## How to Query via Trino CLI

Open an interactive Trino session:

```bash
docker compose exec trino trino
```

Run queries against the Iceberg lakehouse:

```sql
-- List available schemas
SHOW SCHEMAS FROM iceberg;

-- List tables in the lakehouse schema
SHOW TABLES FROM iceberg.lakehouse;

-- Preview all data
SELECT * FROM iceberg.lakehouse.sales;

-- Revenue by country
SELECT
    country,
    SUM(amount) AS total_revenue,
    COUNT(*)    AS orders
FROM iceberg.lakehouse.sales
GROUP BY country
ORDER BY total_revenue DESC;

-- Top customers by spend
SELECT
    customer_id,
    SUM(amount)  AS total_spend,
    COUNT(*)     AS order_count
FROM iceberg.lakehouse.sales
GROUP BY customer_id
ORDER BY total_spend DESC
LIMIT 10;
```

---

## How to Connect Superset

Superset is auto-configured on first startup. The bootstrap script registers a Trino database connection named **"Trino Lakehouse"** and starts the web server. A background provisioner (`provision_dashboard.py`) automatically creates a **"Sales Lakehouse Dashboard"** with 8 charts -- no manual setup required.

### Auto-Provisioned Dashboard

After the DAG has been run at least once, the dashboard displays:

| Chart | Type | Description |
|-------|------|-------------|
| Total Revenue | Big Number | Sum of all order amounts |
| Total Orders | Big Number | Total order count |
| Average Order Value | Big Number | Average revenue per order |
| Revenue by Country | Bar Chart | Countries ranked by total revenue |
| Orders by Country | Pie Chart | Order distribution across countries |
| Daily Revenue Trend | Line Chart | Revenue over time |
| Daily Orders Trend | Line Chart | Order volume over time |
| Top 10 Customers | Table | Customers ranked by total spend |

> **Note:** The dashboard is provisioned at Superset startup, but charts will
> only display data after the Airflow DAG has been triggered and completed
> successfully.

### Manual Configuration

If you need to add the connection manually (or reconfigure it):

1. Open Superset at [http://localhost:8088](http://localhost:8088) and log in (`admin` / `admin`).
2. Navigate to **Settings > Database Connections > + Database**.
3. Select **Trino** as the database type.
4. Enter the SQLAlchemy URI:

   ```
   trino://trino@trino:8080/iceberg/lakehouse
   ```

5. Test the connection and save.

### Creating Datasets and Dashboards

1. Go to **Datasets > + Dataset**.
2. Select the **Trino Lakehouse** database, `lakehouse` schema, and `sales` table.
3. Create charts using SQL Lab or the chart builder. Example queries:

```sql
-- Revenue by country (bar chart)
SELECT country, SUM(amount) AS revenue
FROM sales
GROUP BY country
ORDER BY revenue DESC;

-- Daily order trends (line chart)
SELECT ingestion_date, COUNT(*) AS orders, SUM(amount) AS revenue
FROM sales
GROUP BY ingestion_date
ORDER BY ingestion_date;

-- Top 5 customers (table)
SELECT customer_id, SUM(amount) AS total_spend, COUNT(*) AS orders
FROM sales
GROUP BY customer_id
ORDER BY total_spend DESC
LIMIT 5;
```

---

## Project Structure

```
lakehouse-orchestrator/
├── airflow/
│   ├── dags/
│   │   ├── csv_to_iceberg_celery.py   # Ingestion DAG: CSV → S3 → Iceberg
│   │   ├── csv_auto_ingest.py         # Auto-ingest DAG: S3 polling → schema inference → Iceberg
│   │   └── lib/
│   │       ├── __init__.py
│   │       ├── type_inference.py      # Column type detection engine
│   │       ├── schema_manager.py      # DDL generation and schema evolution
│   │       └── superset_client.py     # Superset dataset auto-registration
│   ├── plugins/                        # Custom Airflow plugins (extensible)
│   ├── Dockerfile                      # Custom Airflow image (boto3, trino, celery)
│   └── requirements.txt               # Python dependencies (unpinned; managed by Airflow constraints)
├── data/
│   └── raw/
│       └── sales_sample.csv           # Sample dataset (~200 records, 10 countries, 30 days)
├── openspec/
│   ├── architecture.md                # Architecture specification
│   ├── roadmap.md                     # Phased project roadmap
│   └── specs/
│       ├── celery_execution.md        # CeleryExecutor design spec
│       ├── dashboard.md               # Superset dashboard spec
│       ├── iceberg_tables.md          # Iceberg table design spec
│       ├── ingestion.md               # Ingestion pipeline spec
│       └── storage_layer.md           # Storage layer spec
├── postgres/
│   └── init-superset-db.sh            # Creates Superset database on first boot (env-driven)
├── seaweedfs/
│   └── s3-config.json                 # S3 gateway IAM and bucket configuration
├── superset/
│   ├── Dockerfile                     # Custom Superset image (sqlalchemy-trino, psycopg2-binary, requests)
│   ├── bootstrap.sh                   # Auto-provisioning entrypoint script
│   ├── provision_dashboard.py         # Dashboard auto-provisioner (REST API, 8 charts)
│   └── superset_config.py             # Superset application configuration
├── trino/
│   ├── catalog/
│   │   └── iceberg.properties         # Iceberg connector: REST catalog + S3 backend
│   └── config.properties              # Trino coordinator configuration
├── .env.example                       # Environment variable template
├── .gitignore
├── docker-compose.yml                 # Full stack deployment (14 containers)
└── README.md
```

---

## Scaling Workers

Scale Celery workers horizontally to increase ingestion throughput:

```bash
docker compose up --scale airflow-worker=3 -d
```

This starts three independent worker containers, each consuming tasks from the Valkey broker. Monitor worker status and task distribution in Flower at [http://localhost:5555](http://localhost:5555).

To scale back down:

```bash
docker compose up --scale airflow-worker=1 -d
```

---

## OpenSpec Documentation

This project follows a spec-driven development methodology. Each major component was designed against a written specification before implementation. The full specification suite is available in the [`openspec/`](openspec/) directory:

| Document | Description |
|----------|-------------|
| [`architecture.md`](openspec/architecture.md) | System architecture, networking, port mappings, and technology decisions |
| [`roadmap.md`](openspec/roadmap.md) | Phased delivery plan with acceptance criteria |
| [`specs/ingestion.md`](openspec/specs/ingestion.md) | Ingestion pipeline design |
| [`specs/storage_layer.md`](openspec/specs/storage_layer.md) | SeaweedFS storage layer design |
| [`specs/iceberg_tables.md`](openspec/specs/iceberg_tables.md) | Iceberg table schema and partitioning design |
| [`specs/celery_execution.md`](openspec/specs/celery_execution.md) | CeleryExecutor configuration and worker design |
| [`specs/dashboard.md`](openspec/specs/dashboard.md) | Superset dashboard and visualization design |
| [`specs/csv-auto-ingest.md`](openspec/specs/csv-auto-ingest.md) | CSV auto-ingest pipeline specification |

---

## Known Limitations

### Ingestion Pipeline

| Limitation | Description |
|-----------|-------------|
| **Single-file ingestion** | The DAG reads only `data/raw/sales_sample.csv`. Additional CSV files in the directory are ignored. |
| **No orphan date cleanup** | If all rows for a specific `ingestion_date` are removed from the CSV and the DAG is re-run, the old data for that date remains in the Iceberg table. The delete-then-insert strategy only processes dates *present* in the current CSV. |
| **Row-by-row INSERT** | Data is inserted one row at a time via parameterized Trino queries. Acceptable for the sample dataset but would be a bottleneck at scale. |
| **No Airflow Connections** | S3 and Trino connections are built directly from environment variables, bypassing Airflow's connection management UI. |

### Re-run Behaviour

| Scenario | Result |
|----------|--------|
| Re-run with identical CSV | Idempotent. Same rows are deleted and re-inserted. |
| Remove some rows for a date (other rows for that date remain) | Correct. The date's rows are replaced with the CSV's current content. |
| Remove all rows for a specific date | **Stale data remains.** The date is no longer in the CSV, so no DELETE is issued. |
| Add rows with new dates | Correct. New dates are inserted, existing dates are refreshed. |
| Add a new CSV file to `data/raw/` | **Ignored.** Only `sales_sample.csv` is processed. |

---

## Roadmap

- **Incremental ingestion + CDC** -- watermark-based extraction and MERGE INTO for upsert semantics
- **CSV auto-ingest** -- automatic schema inference and table creation from uploaded CSVs (in progress)
- **Data quality checks** -- automated validation gates using Great Expectations or Soda
- **CI/CD pipeline** -- GitHub Actions for linting, DAG validation, and integration testing
- **Monitoring and observability** -- Prometheus metrics collection and Grafana dashboards
- **Iceberg compaction + maintenance** -- automated snapshot expiry, orphan cleanup, and file compaction
- **Multi-tenant support** -- schema-level isolation, tenant provisioning, and row-level security

See [`openspec/roadmap.md`](openspec/roadmap.md) for detailed phase descriptions and acceptance criteria.

---

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
