# Spec: Superset Dashboard

---

## Goal

Provide an interactive business intelligence dashboard using Apache Superset
that connects to Trino, queries Iceberg tables, and visualises sales data with
filterable charts covering revenue breakdowns, customer rankings, and temporal
trends.

---

## Context

```mermaid
sequenceDiagram
    participant User as User (Browser)
    participant SS as Superset :8088
    participant T as Trino :8080
    participant IC as Iceberg REST Catalog
    participant S3 as SeaweedFS S3

    User->>SS: Open dashboard
    SS->>T: SQL query via sqlalchemy-trino
    T->>IC: Resolve table metadata
    IC->>S3: Read metadata files
    S3-->>IC: Metadata response
    IC-->>T: Table schema & manifest list
    T->>S3: Read Parquet data files
    S3-->>T: Data response
    T-->>SS: Query result set
    SS-->>User: Rendered charts & filters
```

Apache Superset is the visualisation layer of the lakehouse platform. It
connects to Trino via the `sqlalchemy-trino` driver, which is installed in a
custom Docker image built from `superset/Dockerfile`. On first startup, a
bootstrap script (`superset/bootstrap.sh`) automatically:

1. Runs database migrations against the Superset metadata database (PostgreSQL).
2. Creates an admin user.
3. Initialises default roles and permissions.
4. Registers the Trino datasource.
5. Starts Gunicorn on port 8088.

### Bootstrap Sequence

```mermaid
flowchart TD
    Start["Container starts"] --> Wait["Wait for PostgreSQL<br/>(30 retries, 2s interval)"]
    Wait --> Migrate["superset db upgrade<br/>(schema migrations)"]
    Migrate --> Admin["superset fab create-admin<br/>(idempotent)"]
    Admin --> Init["superset init<br/>(roles & permissions)"]
    Init --> DS["superset set-database-uri<br/>(register Trino datasource)"]
    DS --> Gunicorn["Start Gunicorn<br/>(4 workers, port 8088)"]
```

This self-provisioning design means Superset is fully operational after
`docker compose up` without manual configuration.

---

## Requirements

### Functional Requirements

| ID | Requirement |
|----|-------------|
| FR-1 | Superset must connect to Trino using the `sqlalchemy-trino` driver. |
| FR-2 | The Trino datasource must be auto-registered at startup with the connection URI `trino://trino@trino:8080/iceberg/lakehouse`. |
| FR-3 | Users must be able to create datasets from Iceberg tables in the `iceberg.lakehouse` schema. |
| FR-4 | The dashboard must include at minimum: revenue by country, top customers by revenue, and daily revenue trends. |
| FR-5 | Charts must support interactive filtering by date range and country. |
| FR-6 | The admin user must be created automatically at startup. |

### Non-Functional Requirements

| ID | Requirement |
|----|-------------|
| NFR-1 | Superset must be accessible at `http://localhost:8088`. |
| NFR-2 | Superset must start within 120 seconds, including database migrations and datasource registration. |
| NFR-3 | Superset metadata must be stored in a dedicated PostgreSQL database (`superset`), separate from the Airflow metadata database. |
| NFR-4 | The Superset container must run as a non-root user (`superset`) after initial setup. |

---

## Trino Connection Configuration

### Connection URI

```
trino://trino@trino:8080/iceberg/lakehouse
```

| Component | Value | Description |
|-----------|-------|-------------|
| Driver | `trino` | SQLAlchemy dialect provided by `sqlalchemy-trino` |
| User | `trino` | Trino user (no authentication in development mode) |
| Host | `trino` | Docker DNS hostname of the Trino coordinator |
| Port | `8080` | Trino internal HTTP port |
| Catalog | `iceberg` | Trino catalog name (maps to `trino/catalog/iceberg.properties`) |
| Schema | `lakehouse` | Default schema containing the sales tables |

### Registration Method

The bootstrap script registers the datasource using the Superset CLI:

```bash
superset set-database-uri \
  --database_name "Trino Lakehouse" \
  --uri "trino://trino@trino:8080/iceberg/lakehouse"
```

This command is idempotent. If the datasource already exists, it updates the
URI. If it does not exist, it creates a new database entry.

### Driver Installation

The `superset/Dockerfile` installs the Trino driver and PostgreSQL adapter into
the application's `uv`-managed virtual environment:

```dockerfile
RUN uv pip install --python /app/.venv/bin/python --no-cache sqlalchemy-trino psycopg2-binary
```

`psycopg2-binary` is required because Superset uses a `postgresql+psycopg2://`
URI to connect to its metadata database. The `--python` flag ensures packages
are installed into the correct venv (`/app/.venv/`), which the base image
creates with `include-system-site-packages = false`.

---

## Dataset Registration

After the Trino datasource is registered, datasets can be created in Superset
from any table in the `iceberg.lakehouse` schema.

### Primary Dataset: `sales`

| Field | Configuration |
|-------|--------------|
| Database | Trino Lakehouse |
| Schema | `lakehouse` |
| Table | `sales` |
| Columns | `order_id`, `customer_id`, `amount`, `country`, `ingestion_date` |

### Calculated Columns (optional)

| Column Name | Expression | Purpose |
|------------|------------|---------|
| `revenue` | `CAST(amount AS DOUBLE)` | Superset aggregation-friendly numeric type |
| `order_month` | `DATE_TRUNC('month', ingestion_date)` | Monthly aggregation dimension |

---

## Example Queries

The following queries represent the analytical patterns the dashboard must
support. Each query is written in Trino SQL and targets the
`iceberg.lakehouse.sales` table.

### Revenue by Country

```sql
SELECT
    country,
    SUM(amount)   AS total_revenue,
    COUNT(*)      AS order_count
FROM iceberg.lakehouse.sales
GROUP BY country
ORDER BY total_revenue DESC;
```

**Visualisation**: horizontal bar chart or choropleth map. Each bar represents a
country, sized by total revenue.

### Top Customers by Revenue

```sql
SELECT
    customer_id,
    SUM(amount)   AS total_revenue,
    COUNT(*)      AS order_count
FROM iceberg.lakehouse.sales
GROUP BY customer_id
ORDER BY total_revenue DESC
LIMIT 10;
```

**Visualisation**: table or horizontal bar chart showing the top 10 customers
ranked by cumulative spend.

### Daily Revenue Trends

```sql
SELECT
    ingestion_date,
    SUM(amount)   AS daily_revenue,
    COUNT(*)      AS daily_orders
FROM iceberg.lakehouse.sales
GROUP BY ingestion_date
ORDER BY ingestion_date;
```

**Visualisation**: line chart with `ingestion_date` on the x-axis and
`daily_revenue` on the y-axis. Optionally overlay `daily_orders` as a secondary
axis.

### Revenue Distribution by Country and Date

```sql
SELECT
    ingestion_date,
    country,
    SUM(amount) AS revenue
FROM iceberg.lakehouse.sales
GROUP BY ingestion_date, country
ORDER BY ingestion_date, country;
```

**Visualisation**: stacked area chart or heatmap showing revenue contribution
by country over time.

---

## Dashboard Layout

The recommended dashboard layout organises charts for an executive-level
overview:

```mermaid
block-beta
    columns 2
    block:filters:2
        F1["Filter: Date Range"]
        F2["Filter: Country"]
    end
    block:topL
        C1["Revenue by Country<br/>(bar chart)"]
    end
    block:topR
        C2["Daily Revenue Trend<br/>(line chart)"]
    end
    block:botL
        C3["Top 10 Customers<br/>(table)"]
    end
    block:botR
        C4["Revenue by Country & Date<br/>(stacked area chart)"]
    end
```

### Filter Bar

- **Date Range**: filters all charts to a specific `ingestion_date` range.
- **Country**: filters all charts to one or more selected countries.

Filters are cross-linked: selecting a country in the bar chart applies the
same filter to all other charts on the dashboard.

---

## Superset Metadata Database

Superset stores its own configuration (dashboards, datasets, users, permissions)
in a separate PostgreSQL database:

| Setting | Value |
|---------|-------|
| Database | `superset` |
| User | `superset` |
| Host | `postgres` (shared PostgreSQL instance) |
| Port | `5432` |
| SQLAlchemy URI | `postgresql+psycopg2://superset:superset@postgres:5432/superset` |

The database and user are created by `postgres/init-superset-db.sh`, which
runs automatically on the first PostgreSQL startup via
`docker-entrypoint-initdb.d`. The script reads credentials from environment
variables (`SUPERSET_DB_USER`, `SUPERSET_DB_PASSWORD`, `SUPERSET_DB_NAME`),
ensuring the password stays in sync with `.env` and `docker-compose.yml`.

---

## Acceptance Criteria

- [ ] Superset is accessible at `http://localhost:8088` after startup.
- [ ] The admin user can log in with the configured credentials.
- [ ] The "Trino Lakehouse" datasource is visible in the database list.
- [ ] A dataset can be created from the `iceberg.lakehouse.sales` table.
- [ ] The "Revenue by Country" query returns results grouped by country.
- [ ] The "Top Customers" query returns the top 10 customers by revenue.
- [ ] The "Daily Revenue Trends" query returns one row per ingestion date.
- [ ] Dashboard filters apply cross-chart filtering by date range and country.
- [ ] Superset metadata persists across container restarts (stored in
      PostgreSQL).

---

## Failure Scenarios

| Scenario | Expected Behaviour |
|----------|-------------------|
| Trino is unreachable | Superset displays a connection error when attempting to query datasets. Dashboard charts show "Query failed" messages. Superset itself remains operational. |
| Trino returns no data (empty table) | Charts render with empty state indicators. No errors are raised. |
| Superset metadata database is down | Superset fails to start. The bootstrap script retries database connectivity for up to 60 seconds before exiting with an error. |
| `sqlalchemy-trino` is not installed | Superset cannot register the Trino datasource. The `set-database-uri` command fails with an "unknown dialect" error. |
| Admin user already exists | The `superset fab create-admin` command logs "Admin user already exists" and continues. The bootstrap process is idempotent. |
| Trino datasource already registered | The `superset set-database-uri` command updates the existing entry. No duplicate datasources are created. |
| Concurrent dashboard users | Gunicorn runs 4 workers, supporting multiple concurrent sessions. Under heavy load, consider increasing the worker count via the `--workers` flag in `bootstrap.sh`. |
