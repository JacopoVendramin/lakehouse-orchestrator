# =============================================================================
# Lakehouse Orchestrator - Apache Superset
# =============================================================================
# Custom Superset image with the Trino SQLAlchemy driver for querying the
# Iceberg lakehouse.  A bootstrap script handles first-run initialisation
# (DB migration, admin user, Trino datasource) so the container is fully
# self-provisioning.
#
# Build:
#   docker build -t lakehouse-superset ./superset
# =============================================================================

FROM apache/superset:latest

# ---- Metadata ---------------------------------------------------------------
LABEL maintainer="lakehouse-orchestrator"
LABEL org.opencontainers.image.title="lakehouse-orchestrator-superset"
LABEL org.opencontainers.image.description="Apache Superset with Trino connectivity for lakehouse BI & dashboards"
LABEL org.opencontainers.image.source="https://github.com/jvendramin/lakehouse-orchestrator"

# ---- Install Trino driver ---------------------------------------------------
# Switch to root to install Python packages, then drop back to superset.
USER root

RUN uv pip install --python /app/.venv/bin/python --no-cache sqlalchemy-trino psycopg2-binary requests

# ---- Copy configuration, bootstrap, and dashboard provisioner ----------------
COPY superset_config.py /app/superset_config.py
COPY bootstrap.sh /app/bootstrap.sh
COPY provision_dashboard.py /app/provision_dashboard.py
RUN chmod +x /app/bootstrap.sh

# ---- Set Superset config path ------------------------------------------------
ENV SUPERSET_CONFIG_PATH=/app/superset_config.py

# ---- Runtime configuration ---------------------------------------------------
# Drop privileges back to the non-root superset user shipped with the base
# image.  This satisfies container security scanners and follows the principle
# of least privilege.
USER superset

EXPOSE 8088

ENTRYPOINT ["/app/bootstrap.sh"]
