# Architecture Specification

> Lakehouse Orchestrator -- an open-source, container-native data lakehouse
> platform that transforms raw CSV data into queryable Iceberg tables and
> interactive Superset dashboards.

---

## System Overview

The platform is composed of seven core services deployed as Docker containers on a
single bridge network. Each service communicates over internal DNS hostnames
defined by Docker Compose. No service is exposed to the public internet by
default; all host-port mappings are intended for local development and can be
removed in production.

---

## Architecture Diagram

```
 ┌──────────────────────────────────────────────────────────────────────────────┐
 │                        lakehouse-network (bridge)                           │
 │                                                                             │
 │  ┌─────────────┐        ┌─────────────────────────────────────────────────┐ │
 │  │  CSV Files   │        │              Apache Airflow                    │ │
 │  │  (./data/)   │───────▶│  ┌───────────┐ ┌───────────┐ ┌─────────────┐  │ │
 │  └─────────────┘        │  │ Scheduler  │ │ Webserver │ │   Worker    │  │ │
 │                          │  │            │ │  :8081    │ │  (Celery)   │  │ │
 │                          │  └─────┬──────┘ └───────────┘ └──────┬──────┘  │ │
 │                          │        │                             │         │ │
 │                          │        └──────────┬──────────────────┘         │ │
 │                          │                   │                            │ │
 │                          │              ┌────▼────┐                       │ │
 │                          │              │ Flower  │                       │ │
 │                          │              │  :5555  │                       │ │
 │                          │              └─────────┘                       │ │
 │                          └───────────────────┬───────────────────────────┘ │
 │                                              │                             │
 │               ┌──────────────────────────────┼──────────────────┐          │
 │               │                              │                  │          │
 │               ▼                              ▼                  ▼          │
 │  ┌────────────────────┐        ┌──────────────────┐   ┌────────────────┐  │
 │  │     PostgreSQL      │        │      Valkey       │   │   SeaweedFS    │  │
 │  │    (metadata DB)    │        │    (broker)       │   │  (S3 storage)  │  │
 │  │      :5432          │        │      :6379        │   │                │  │
 │  │                     │        └──────────────────┘   │ ┌────────────┐ │  │
 │  │ ┌───────┐ ┌───────┐│                               │ │   Master   │ │  │
 │  │ │airflow│ │superse││                               │ │   :9333    │ │  │
 │  │ │  db   │ │ t db  ││                               │ ├────────────┤ │  │
 │  │ └───────┘ └───────┘│                               │ │   Volume   │ │  │
 │  └────────────────────┘                               │ │   :8082    │ │  │
 │                                                        │ ├────────────┤ │  │
 │                                                        │ │   Filer    │ │  │
 │                                                        │ │   :8888    │ │  │
 │                                                        │ ├────────────┤ │  │
 │                                                        │ │ S3 Gateway │ │  │
 │                                                        │ │   :8333    │ │  │
 │                                                        │ └────────────┘ │  │
 │                                                        └───────┬────────┘  │
 │                                                                │           │
 │                                                                │ S3 API    │
 │                                                                │           │
 │                                                        ┌───────▼────────┐  │
 │                                                        │     Trino      │  │
 │                                                        │ (query engine) │  │
 │                                                        │     :8083      │  │
 │                                                        └───────┬────────┘  │
 │                                                                │           │
 │                                                                │ JDBC/SQL  │
 │                                                                │           │
 │                                                        ┌───────▼────────┐  │
 │                                                        │    Superset    │  │
 │                                                        │  (dashboards)  │  │
 │                                                        │     :8088      │  │
 │                                                        └────────────────┘  │
 └──────────────────────────────────────────────────────────────────────────────┘
```

---

## Data Flow

The platform implements a linear data pipeline with clear stage boundaries:

```
CSV  ──▶  Airflow  ──▶  SeaweedFS (S3)  ──▶  Iceberg Table  ──▶  Trino  ──▶  Superset
(raw)    (ingest)      (object store)       (table format)     (query)     (visualise)
```

### Stage Breakdown

| Stage | Action | Input | Output |
|-------|--------|-------|--------|
| **1. Ingest** | Airflow DAG reads CSV from the local `./data/raw/` volume mount | `sales_sample.csv` | Validated DataFrame (pandas) |
| **2. Upload** | Worker converts the DataFrame to Parquet and uploads via the S3 API | DataFrame | `s3://lakehouse/raw/sales/sales_sample.parquet` |
| **3. Register** | Worker issues DDL through Trino to create or update the Iceberg table | Parquet object | Iceberg table in `iceberg.lakehouse` schema |
| **4. Query** | Trino reads Iceberg metadata and Parquet data files from SeaweedFS | Iceberg table | SQL result sets |
| **5. Visualise** | Superset connects to Trino via `sqlalchemy-trino` and renders dashboards | SQL result sets | Charts, filters, dashboards |

---

## Service Responsibilities

| Service | Container Name | Role |
|---------|---------------|------|
| **PostgreSQL 16** | `lakehouse-postgres` | Metadata store for Airflow (scheduler state, task history, connections) and Superset (dashboards, datasets, users). Also serves as the Celery result backend. |
| **Valkey 8** | `lakehouse-valkey` | Message broker for the Celery executor. Receives task messages from the scheduler and delivers them to workers. Redis-wire-protocol compatible. |
| **SeaweedFS** | `lakehouse-seaweedfs-master`, `-volume`, `-filer`, `-s3` | S3-compatible distributed object store. Stores raw Parquet data files and Iceberg metadata. Four containers implement the master/volume/filer/gateway architecture. |
| **Airflow 2.10** | `lakehouse-airflow-webserver`, `-scheduler`, `-worker`, `-flower` | Workflow orchestrator. The scheduler triggers DAG runs, the worker executes tasks via Celery, the webserver exposes the management UI, and Flower provides Celery monitoring. |
| **Iceberg REST Catalog** | `lakehouse-iceberg-rest` | Metadata service for Iceberg tables. Manages table schemas, snapshots, and partition specs. Backed by SeaweedFS for warehouse storage. Apache 2.0 licensed (`tabulario/iceberg-rest`). |
| **Trino** | `lakehouse-trino` | Distributed SQL query engine. Connects to Iceberg tables via the REST catalog and reads data from SeaweedFS using the native Iceberg connector. |
| **Superset** | `lakehouse-superset` | Business intelligence platform. Connects to Trino via `sqlalchemy-trino` to build datasets, charts, and interactive dashboards. |
| **S3 Init** | `lakehouse-s3-init` | Ephemeral init container. Creates the `lakehouse` and `lakehouse-warehouse` buckets on SeaweedFS at startup using the AWS CLI, then exits. |

---

## Networking

### Docker Bridge Network

All services are attached to a single user-defined bridge network named
`lakehouse-network` (driver: `bridge`). This provides:

- **Automatic DNS resolution**: each container is reachable by its Compose
  service name (e.g., `postgres`, `valkey`, `trino`). No hardcoded IP addresses.
- **Container isolation**: only containers on `lakehouse-network` can
  communicate with each other. The host machine reaches services only through
  explicitly mapped ports.
- **Deterministic hostnames**: configuration files reference internal hostnames
  (e.g., `seaweedfs-s3:8333`, `trino:8080`) that remain stable across container
  restarts.

### Internal DNS Names

| Hostname | Resolved By |
|----------|-------------|
| `postgres` | PostgreSQL container |
| `valkey` | Valkey container |
| `seaweedfs-master` | SeaweedFS master node |
| `seaweedfs-s3` | SeaweedFS S3 gateway |
| `seaweedfs-filer` | SeaweedFS filer |
| `iceberg-rest` | Iceberg REST catalog |
| `trino` | Trino coordinator |
| `superset` | Superset (not typically called by other services) |

---

## Port Mapping

All ports are configurable via environment variables in `.env`. The defaults
below are set for local development and avoid conflicts with common services.

| Service | Container Port | Host Port | Protocol | Purpose |
|---------|---------------|-----------|----------|---------|
| PostgreSQL | 5432 | `5432` | TCP | Database client access |
| Valkey | 6379 | `6379` | TCP | Redis-compatible broker |
| SeaweedFS Master | 9333 | `9333` | HTTP | Cluster management UI |
| SeaweedFS Volume | 8080 | `8082` | HTTP | Volume server |
| SeaweedFS Filer | 8888 | `8888` | HTTP | POSIX-like file interface |
| SeaweedFS S3 | 8333 | `8333` | HTTP | S3-compatible API endpoint |
| Airflow Webserver | 8080 | `8081` | HTTP | DAG management UI |
| Airflow Flower | 5555 | `5555` | HTTP | Celery worker monitoring |
| Iceberg REST Catalog | 8181 | `8181` | HTTP | Iceberg catalog REST API |
| Trino | 8080 | `8083` | HTTP | SQL query endpoint and UI |
| Superset | 8088 | `8088` | HTTP | Dashboard UI |

---

## Technology Decisions

### Apache Iceberg over Apache Hive

| Criterion | Hive Table Format | Iceberg |
|-----------|-------------------|---------|
| **Schema evolution** | Limited; adding columns requires `ALTER TABLE` and is not always backward-compatible | Full schema evolution (add, drop, rename, reorder columns) with metadata versioning |
| **Partition evolution** | Changing partition scheme requires rewriting all data | Partition spec changes apply to new data only; historical data is unaffected |
| **Time travel** | Not supported natively | Built-in snapshot isolation; query any historical snapshot by ID or timestamp |
| **Hidden partitioning** | Users must know the partition layout and include partition columns in queries | Iceberg applies partition transforms automatically; queries do not need to reference partitions |
| **File-level metadata** | Partition-level tracking only; no per-file statistics | Manifest files track per-file column-level min/max statistics for aggressive predicate pushdown |
| **Engine independence** | Tightly coupled to the Hive metastore | Engine-agnostic; works with Trino, Spark, Flink, and others without a Hive metastore dependency |

**Decision**: Iceberg provides stronger guarantees for schema evolution,
partition management, and time travel -- all critical for a lakehouse where the
schema may change as new data sources are onboarded.

### Valkey over Redis

| Criterion | Redis (post-2024) | Valkey |
|-----------|-------------------|-------|
| **License** | SSPL / RSALv2 (not OSI-approved) | BSD 3-Clause (fully open source) |
| **Wire compatibility** | Native Redis protocol | 100% Redis wire-protocol compatible; drop-in replacement |
| **Community** | Controlled by Redis Ltd. | Linux Foundation project with broad vendor support |
| **Functional parity** | Reference implementation | Forked from Redis 7.2; feature-equivalent for broker workloads |

**Decision**: Valkey is a drop-in replacement for Redis with an unambiguously
open-source license. For a public portfolio project, using a permissively
licensed component avoids any licensing ambiguity. Airflow's Celery integration
connects to Valkey using the standard `redis://` URI without modification.

### SeaweedFS over MinIO

| Criterion | MinIO | SeaweedFS |
|-----------|-------|-----------|
| **License** | AGPL-3.0 (copyleft; may require disclosing source of linked services) | Apache 2.0 (permissive) |
| **Resource footprint** | Heavier; designed for production-scale distributed deployments | Lightweight; master + volume architecture runs well on a single machine |
| **S3 compatibility** | Full S3 API coverage | Sufficient S3 API coverage for Iceberg, Trino, and Airflow workloads |
| **Architecture** | Monolithic server with erasure coding | Separated master/volume/filer/S3-gateway; each concern is independently scalable |
| **Startup time** | Moderate | Fast; volume servers register with master in seconds |

**Decision**: SeaweedFS provides the S3 compatibility required by Trino's
Iceberg connector and Airflow's boto3 client, under a permissive Apache 2.0
license. Its low resource footprint makes it suitable for local development
while its separated architecture allows independent scaling of metadata and
storage in production.

---

## Volume Strategy

Persistent data is stored in named Docker volumes to survive container restarts:

| Volume | Service | Contents |
|--------|---------|----------|
| `postgres-data` | PostgreSQL | Airflow metadata, Celery results, Superset metadata |
| `valkey-data` | Valkey | Broker message persistence (AOF/RDB) |
| `seaweedfs-master-data` | SeaweedFS Master | Volume ID assignments and topology |
| `seaweedfs-volume-data` | SeaweedFS Volume | Actual data blobs (Parquet files, Iceberg metadata) |
| `seaweedfs-filer-data` | SeaweedFS Filer | File-to-chunk mapping metadata |
| `airflow-logs` | Airflow | Task execution logs |

The `./data/` directory is bind-mounted into Airflow containers at
`/opt/airflow/data` for direct access to raw CSV source files.

---

## Health Checks

Every stateful service defines a Docker health check to enforce startup ordering
via `depends_on` conditions:

| Service | Health Check | Interval | Retries |
|---------|-------------|----------|---------|
| PostgreSQL | `pg_isready` | 10s | 5 |
| Valkey | `valkey-cli ping` | 10s | 5 |
| Airflow Webserver | `curl http://localhost:8080/health` | 30s | 5 |
| Airflow Scheduler | `airflow jobs check --job-type SchedulerJob` | 30s | 5 |
| Airflow Worker | `celery inspect ping` | 30s | 5 |
| Airflow Flower | `curl http://localhost:5555/` | 30s | 5 |
