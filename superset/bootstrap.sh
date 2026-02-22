#!/usr/bin/env bash
# =============================================================================
# Lakehouse Orchestrator - Superset Bootstrap
# =============================================================================
# First-run initialisation for Superset:
#   1. Wait for the metadata database to accept connections
#   2. Run schema migrations (superset db upgrade)
#   3. Create the admin user (idempotent - skips if user exists)
#   4. Initialise roles & permissions (superset init)
#   5. Register the Trino datasource
#   6. Provision dashboard with charts (background, non-blocking)
#   7. Start Gunicorn
#
# Required environment variables (with defaults set in docker-compose):
#   SUPERSET_DB_URI              - SQLAlchemy URI for the metadata DB
#   SUPERSET_SECRET_KEY          - Flask SECRET_KEY
#   SUPERSET_ADMIN_USERNAME      - Admin login name
#   SUPERSET_ADMIN_PASSWORD      - Admin password
#   SUPERSET_ADMIN_FIRSTNAME     - Admin first name
#   SUPERSET_ADMIN_LASTNAME      - Admin last name
#   SUPERSET_ADMIN_EMAIL         - Admin email address
#   TRINO_HOST                   - Trino coordinator hostname
#   TRINO_PORT                   - Trino coordinator port
# =============================================================================
set -e

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
log() { echo "[superset-bootstrap] $(date '+%Y-%m-%d %H:%M:%S') $*"; }

# ---------------------------------------------------------------------------
# 1. Wait for the metadata database
# ---------------------------------------------------------------------------
log "Waiting for Superset metadata database to be ready ..."

MAX_RETRIES=30
RETRY_INTERVAL=2

for i in $(seq 1 "$MAX_RETRIES"); do
  if superset db upgrade 2>/dev/null; then
    log "Database is ready and migrations applied."
    break
  fi

  if [ "$i" -eq "$MAX_RETRIES" ]; then
    log "ERROR: metadata database not reachable after $((MAX_RETRIES * RETRY_INTERVAL))s. Exiting."
    exit 1
  fi

  log "Database not ready yet (attempt ${i}/${MAX_RETRIES}). Retrying in ${RETRY_INTERVAL}s ..."
  sleep "$RETRY_INTERVAL"
done

# ---------------------------------------------------------------------------
# 2. Create admin user (idempotent)
# ---------------------------------------------------------------------------
log "Creating admin user '${SUPERSET_ADMIN_USERNAME}' ..."

superset fab create-admin \
  --username "${SUPERSET_ADMIN_USERNAME}" \
  --password "${SUPERSET_ADMIN_PASSWORD}" \
  --firstname "${SUPERSET_ADMIN_FIRSTNAME}" \
  --lastname "${SUPERSET_ADMIN_LASTNAME}" \
  --email "${SUPERSET_ADMIN_EMAIL}" \
  || log "Admin user already exists - skipping."

# ---------------------------------------------------------------------------
# 3. Initialise roles & permissions
# ---------------------------------------------------------------------------
log "Initialising default roles and permissions ..."
superset init

# ---------------------------------------------------------------------------
# 4. Register Trino datasource
# ---------------------------------------------------------------------------
TRINO_URI="trino://trino@${TRINO_HOST}:${TRINO_PORT}/iceberg/lakehouse"

log "Registering Trino database connection (${TRINO_URI}) ..."
superset set-database-uri \
  --database_name "Trino Lakehouse" \
  --uri "${TRINO_URI}" \
  || log "Trino datasource may already be registered - skipping."

# ---------------------------------------------------------------------------
# 5. Provision dashboard (background, non-blocking)
# ---------------------------------------------------------------------------
log "Launching dashboard provisioner in background ..."
python3 /app/provision_dashboard.py &

# ---------------------------------------------------------------------------
# 6. Start Gunicorn
# ---------------------------------------------------------------------------
log "Starting Superset via Gunicorn on 0.0.0.0:8088 ..."
exec gunicorn \
  --bind 0.0.0.0:8088 \
  --workers 4 \
  --timeout 120 \
  --access-logfile - \
  --error-logfile - \
  "superset.app:create_app()"
