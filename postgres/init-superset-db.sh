#!/bin/bash
# =============================================================================
# PostgreSQL Initialization Script
# =============================================================================
# Creates the Superset database and user on first startup.
# This runs automatically via docker-entrypoint-initdb.d.
#
# Uses environment variables so passwords are never hardcoded:
#   SUPERSET_DB_USER     (default: superset)
#   SUPERSET_DB_PASSWORD (default: superset)
#   SUPERSET_DB_NAME     (default: superset)
# =============================================================================

set -e

SUPERSET_DB_USER="${SUPERSET_DB_USER:-superset}"
SUPERSET_DB_PASSWORD="${SUPERSET_DB_PASSWORD:-superset}"
SUPERSET_DB_NAME="${SUPERSET_DB_NAME:-superset}"

echo "Creating Superset database role '${SUPERSET_DB_USER}' and database '${SUPERSET_DB_NAME}'..."

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '${SUPERSET_DB_USER}') THEN
            CREATE ROLE ${SUPERSET_DB_USER} WITH LOGIN PASSWORD '${SUPERSET_DB_PASSWORD}';
        END IF;
    END
    \$\$;

    SELECT 'CREATE DATABASE ${SUPERSET_DB_NAME} OWNER ${SUPERSET_DB_USER}'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${SUPERSET_DB_NAME}')\gexec

    GRANT ALL PRIVILEGES ON DATABASE ${SUPERSET_DB_NAME} TO ${SUPERSET_DB_USER};
EOSQL

echo "Superset database initialization complete."
