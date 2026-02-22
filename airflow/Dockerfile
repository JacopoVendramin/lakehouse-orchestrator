# =============================================================================
# Lakehouse Orchestrator - Apache Airflow
# =============================================================================
# Custom Airflow image with Trino, S3, and Celery dependencies for
# orchestrating lakehouse data pipelines.
#
# Build:
#   docker build -t lakehouse-airflow ./airflow
# =============================================================================

ARG AIRFLOW_VERSION=2.10.4
ARG PYTHON_VERSION=3.11

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

# ---- Metadata ---------------------------------------------------------------
LABEL maintainer="lakehouse-orchestrator"
LABEL org.opencontainers.image.title="lakehouse-orchestrator-airflow"
LABEL org.opencontainers.image.description="Apache Airflow with Trino, S3, and Celery for lakehouse orchestration"
LABEL org.opencontainers.image.source="https://github.com/jvendramin/lakehouse-orchestrator"
LABEL org.opencontainers.image.version="${AIRFLOW_VERSION}"

# ---- Install Python dependencies --------------------------------------------
# Use Airflow's published constraints to prevent dependency conflicts.
# The constraints file pins every transitive dependency to versions tested
# against this exact Airflow release.
ARG AIRFLOW_VERSION
ARG PYTHON_VERSION
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

COPY requirements.txt /opt/airflow/requirements.txt

RUN pip install --no-cache-dir \
    --constraint "${CONSTRAINT_URL}" \
    -r /opt/airflow/requirements.txt

# ---- Runtime configuration ---------------------------------------------------
# Airflow base image already sets USER=airflow (uid=50000).
# Explicitly declare it here for clarity and security scanners.
USER airflow

WORKDIR /opt/airflow
