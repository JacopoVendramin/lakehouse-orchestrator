# Project Roadmap

> A phased plan for evolving the lakehouse-orchestrator from a functional
> reference architecture into a production-hardened, observable, multi-tenant
> data platform.

---

## Phase 1: Foundation (Current)

### Description

Establish the core lakehouse architecture with all essential services running as
Docker containers on a single machine. This phase delivers the end-to-end data
path: CSV ingestion through Airflow, Parquet storage on SeaweedFS, Iceberg table
registration via Trino, and interactive dashboards in Superset.

### Deliverables

- Docker Compose deployment with all services (PostgreSQL, Valkey, SeaweedFS,
  Airflow with CeleryExecutor, Trino, Superset).
- Custom Airflow Docker image with `boto3`, `pyarrow`, `pandas`, `trino`, and
  Celery provider dependencies.
- Custom Superset Docker image with `sqlalchemy-trino` and auto-provisioning
  bootstrap script.
- SeaweedFS S3 configuration with bucket auto-creation (`lakehouse`,
  `lakehouse-warehouse`).
- Trino Iceberg catalog configuration with REST catalog and S3 storage backend.
- PostgreSQL initialisation script for Superset metadata database.
- Sample sales dataset (`data/raw/sales_sample.csv`) with 20 records across 7
  countries.
- Environment configuration template (`.env.example`) with all service
  credentials.
- OpenSpec documentation suite (architecture, specs, roadmap).

### Acceptance Criteria

- [ ] `docker compose up -d` starts all services without errors.
- [ ] All health checks pass within 120 seconds.
- [ ] Airflow webserver is accessible at `http://localhost:8081`.
- [ ] Flower is accessible at `http://localhost:5555` and shows one registered
      worker.
- [ ] Trino is accessible at `http://localhost:8083` and the `iceberg` catalog
      is listed.
- [ ] Superset is accessible at `http://localhost:8088` with the Trino
      datasource pre-registered.
- [ ] `aws s3 ls --endpoint-url http://localhost:8333` returns the `lakehouse`
      and `lakehouse-warehouse` buckets.

---

## Phase 2: Incremental Ingestion and Change Data Capture

### Description

Extend the ingestion pipeline to support incremental loads and change data
capture (CDC). Rather than re-ingesting entire files, the pipeline detects new
or modified records and applies only the delta to the Iceberg table. This
reduces processing time, storage waste, and Trino query overhead.

### Deliverables

- Airflow DAG for incremental ingestion using watermark-based extraction (track
  the last processed `ingestion_date` or row offset).
- Iceberg MERGE INTO support for upsert semantics (insert new records, update
  changed records).
- Deduplication logic based on `order_id` as the primary key.
- Audit columns (`_ingested_at`, `_source_file`) added to the Iceberg table
  for lineage tracking.
- Optional: Debezium CDC connector for capturing changes from an upstream
  transactional database.

### Acceptance Criteria

- [ ] Running the incremental DAG twice with the same file produces no
      duplicate records.
- [ ] Running the incremental DAG with an updated file (changed `amount` for an
      existing `order_id`) updates the record in-place via MERGE INTO.
- [ ] The `_ingested_at` timestamp reflects the time of the most recent
      ingestion, not the original load time.
- [ ] Watermark state persists across DAG runs (stored in Airflow Variables or
      an external state store).

---

## Phase 3: Data Quality Checks

### Description

Integrate automated data quality validation into the ingestion pipeline. Quality
checks run after ingestion and before the data is promoted to the "trusted"
layer. Failed checks halt downstream processing and generate alerts.

### Deliverables

- Data quality framework integration (Great Expectations, Soda, or a custom
  validation layer using PyArrow).
- Quality check suite for the `sales` table:
  - `order_id` uniqueness.
  - `amount` is positive and within a reasonable range.
  - `country` values belong to a known reference set.
  - `ingestion_date` is not in the future.
  - Row count is within expected bounds (no catastrophic data loss or
    explosion).
- Airflow task group that runs quality checks as a gate between ingestion and
  table promotion.
- Quality check results persisted to a metadata table for historical analysis.
- Alerting on quality check failures (Airflow email/Slack callback).

### Acceptance Criteria

- [ ] A CSV with a negative `amount` triggers a quality check failure.
- [ ] A CSV with a duplicate `order_id` triggers a quality check failure.
- [ ] Quality check results are queryable through Trino (stored in an Iceberg
      metadata table).
- [ ] Failed quality checks prevent the data from being promoted to the trusted
      layer.
- [ ] Quality check history is visible in the Airflow UI as a separate task
      group.

---

## Phase 4: CI/CD Pipeline

### Description

Automate testing, linting, and deployment validation with a GitHub Actions
pipeline. Every pull request runs automated checks to prevent regressions. The
main branch is always deployable.

### Deliverables

- GitHub Actions workflow for pull requests:
  - Python linting (ruff or flake8) for DAG source code.
  - DAG import validation (`python -c "import dags.<dag_name>"`).
  - Docker Compose build validation (`docker compose build`).
  - Integration test: spin up the full stack, run the ingestion DAG, verify
    data appears in Trino.
- GitHub Actions workflow for main branch:
  - Full integration test suite.
  - Docker image build and push to GitHub Container Registry (GHCR).
- Pre-commit hooks for local development (ruff, trailing whitespace, YAML
  validation).

### Acceptance Criteria

- [ ] A pull request with a syntax error in a DAG file fails the CI check.
- [ ] A pull request that breaks `docker compose build` fails the CI check.
- [ ] The integration test validates end-to-end data flow (CSV to Trino query
      result).
- [ ] Docker images are tagged with the git SHA and pushed to GHCR on merge to
      main.
- [ ] CI runs complete within 10 minutes.

---

## Phase 5: Monitoring and Observability

### Description

Add comprehensive monitoring and observability to the platform using Prometheus
for metrics collection and Grafana for dashboards and alerting. Every service
must expose health and performance metrics.

### Deliverables

- Prometheus deployment with scrape targets for all services.
- Grafana deployment with pre-built dashboards:
  - **Airflow**: DAG run duration, task success/failure rates, worker
    utilisation, queue depth.
  - **Trino**: Query latency, active queries, memory utilisation, failed
    queries.
  - **SeaweedFS**: Storage capacity, object count, S3 request rates, error
    rates.
  - **Valkey**: Memory usage, connected clients, command throughput.
  - **PostgreSQL**: Connection count, query latency, cache hit ratio.
- Alerting rules:
  - Airflow task failure rate exceeds threshold.
  - Trino query latency exceeds SLA.
  - SeaweedFS disk usage exceeds 80%.
  - Valkey memory usage exceeds 80%.
- Structured logging (JSON format) for all custom components.
- Airflow StatsD exporter for Prometheus.

### Acceptance Criteria

- [ ] Prometheus scrapes metrics from all services without errors.
- [ ] Grafana dashboards display real-time metrics for Airflow, Trino,
      SeaweedFS, Valkey, and PostgreSQL.
- [ ] An alert fires when an Airflow task fails.
- [ ] An alert fires when SeaweedFS disk usage exceeds 80%.
- [ ] All metrics are retained for at least 7 days.
- [ ] Grafana is accessible at `http://localhost:3000`.

---

## Phase 6: Iceberg Maintenance

### Description

Implement automated Iceberg table maintenance procedures to manage storage
growth, optimise query performance, and ensure metadata hygiene. Without
maintenance, Iceberg tables accumulate small files, expired snapshots, and
orphaned data files over time.

### Deliverables

- Airflow DAG for scheduled Iceberg maintenance with the following tasks:
  - **Compaction**: merge small Parquet files into larger files to reduce
    metadata overhead and improve scan performance.
  - **Snapshot expiry**: remove snapshots older than a configurable retention
    period (e.g., 7 days) to free storage.
  - **Orphan file cleanup**: identify and delete data files that are not
    referenced by any current snapshot.
  - **Manifest rewriting**: rewrite manifests to reduce manifest list size and
    improve planning performance.
- Configuration parameters for retention periods and compaction thresholds.
- Maintenance run history tracked in an Iceberg metadata table.

### Acceptance Criteria

- [ ] Compaction reduces the number of Parquet files per partition when the
      file count exceeds the configured threshold.
- [ ] Snapshot expiry removes snapshots older than the retention period.
- [ ] Orphan file cleanup reclaims storage for unreferenced files.
- [ ] Maintenance DAG runs on a schedule (e.g., daily) without manual
      intervention.
- [ ] Maintenance operations do not block concurrent read queries.
- [ ] Maintenance run history is queryable through Trino.

---

## Phase 7: Multi-Tenant Support

### Description

Evolve the platform to support multiple tenants, each with isolated data,
schemas, and access controls. This enables the platform to serve multiple teams
or business units from a single deployment.

### Deliverables

- Tenant isolation model:
  - Each tenant gets a dedicated Iceberg schema (e.g., `iceberg.tenant_a`,
    `iceberg.tenant_b`).
  - Each tenant gets a dedicated S3 prefix or bucket.
  - Trino access controls enforce schema-level isolation.
- Tenant provisioning automation:
  - Airflow DAG or script to create a new tenant (schema, bucket, credentials,
    Superset workspace).
- Superset row-level security (RLS) or workspace isolation per tenant.
- SeaweedFS IAM policies scoped per tenant.
- Trino system access control plugin for fine-grained authorization.
- Tenant usage metering (storage consumed, queries executed, DAG runs).

### Acceptance Criteria

- [ ] A new tenant can be provisioned by running a single command or DAG.
- [ ] Tenant A cannot query or access Tenant B's data through Trino.
- [ ] Tenant A cannot access Tenant B's S3 objects.
- [ ] Superset dashboards are scoped to the authenticated tenant's data.
- [ ] Tenant usage metrics are queryable for billing or capacity planning.
- [ ] Deleting a tenant removes all associated schemas, data, and credentials.

---

## Timeline Summary

| Phase | Status | Dependencies |
|-------|--------|-------------|
| Phase 1: Foundation | **Current** | None |
| Phase 2: Incremental Ingestion + CDC | Planned | Phase 1 |
| Phase 3: Data Quality Checks | Planned | Phase 2 |
| Phase 4: CI/CD Pipeline | Planned | Phase 1 |
| Phase 5: Monitoring & Observability | Planned | Phase 1 |
| Phase 6: Iceberg Maintenance | Planned | Phase 2 |
| Phase 7: Multi-Tenant Support | Planned | Phases 3, 4, 5 |

Phases 4 and 5 can be pursued in parallel with Phases 2 and 3, as they address
orthogonal concerns (developer workflow and operational visibility).
