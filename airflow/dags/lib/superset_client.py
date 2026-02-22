"""
Superset REST API client for auto-ingest dataset registration.

Handles authentication (JWT + CSRF), database discovery, and dataset
creation.  Follows the same patterns as ``provision_dashboard.py``.

Usage inside an Airflow task::

    from lib.superset_client import register_dataset
    result = register_dataset("my_table")
    # result = {"dataset_id": 42, "skipped": False}

Author: data-engineering
"""

from __future__ import annotations

import json
import logging
import os

import requests

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# When called from an Airflow worker, Superset is reachable via the Docker
# network name.  The provisioner uses localhost because it runs *inside* the
# Superset container; we run in the Airflow worker.
SUPERSET_BASE_URL: str = "http://superset:8088"
SUPERSET_USERNAME: str = os.environ.get("SUPERSET_ADMIN_USERNAME", "admin")
SUPERSET_PASSWORD: str = os.environ.get("SUPERSET_ADMIN_PASSWORD", "admin")

ICEBERG_CATALOG: str = "iceberg"
ICEBERG_SCHEMA: str = "lakehouse"
DATABASE_NAME: str = "Trino Lakehouse"


# ---------------------------------------------------------------------------
# Session management
# ---------------------------------------------------------------------------


def _get_session() -> requests.Session:
    """Create an authenticated Superset session with JWT + CSRF tokens."""
    session = requests.Session()

    # Login
    resp = session.post(
        f"{SUPERSET_BASE_URL}/api/v1/security/login",
        json={
            "username": SUPERSET_USERNAME,
            "password": SUPERSET_PASSWORD,
            "provider": "db",
            "refresh": True,
        },
        timeout=30,
    )
    resp.raise_for_status()
    token = resp.json()["access_token"]

    session.headers.update({
        "Authorization": f"Bearer {token}",
        "Referer": SUPERSET_BASE_URL,
    })

    # Fetch CSRF token
    csrf_resp = session.get(
        f"{SUPERSET_BASE_URL}/api/v1/security/csrf_token/",
        timeout=10,
    )
    csrf_resp.raise_for_status()
    csrf_token = csrf_resp.json()["result"]

    session.headers.update({
        "X-CSRFToken": csrf_token,
        "Content-Type": "application/json",
    })

    return session


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------


def _find_database_id(session: requests.Session) -> int | None:
    """Find the Trino Lakehouse database ID in Superset."""
    resp = session.get(
        f"{SUPERSET_BASE_URL}/api/v1/database/",
        timeout=10,
    )
    resp.raise_for_status()
    for db in resp.json().get("result", []):
        if db["database_name"] == DATABASE_NAME:
            return db["id"]
    return None


def _dataset_exists(session: requests.Session, table_name: str) -> bool:
    """Check if a dataset for this table already exists."""
    resp = session.get(
        f"{SUPERSET_BASE_URL}/api/v1/dataset/",
        params={
            "q": json.dumps({
                "filters": [
                    {"col": "table_name", "opr": "eq", "value": table_name},
                ]
            })
        },
        timeout=10,
    )
    resp.raise_for_status()
    return len(resp.json().get("result", [])) > 0


def _create_dataset(
    session: requests.Session,
    database_id: int,
    table_name: str,
) -> int:
    """Create a Superset dataset and return its ID."""
    payload = {
        "database": database_id,
        "table_name": table_name,
        "schema": ICEBERG_SCHEMA,
        "catalog": ICEBERG_CATALOG,
    }
    resp = session.post(
        f"{SUPERSET_BASE_URL}/api/v1/dataset/",
        json=payload,
        timeout=15,
    )
    resp.raise_for_status()
    dataset_id = resp.json()["id"]
    log.info("Created Superset dataset '%s' (id=%d)", table_name, dataset_id)
    return dataset_id


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def register_dataset(table_name: str) -> dict:
    """Register an Iceberg table as a Superset dataset.

    Idempotent: if the dataset already exists, it is skipped.

    Args:
        table_name: The Iceberg table name (without catalog/schema).

    Returns:
        ``{"dataset_id": int | None, "skipped": bool}``
    """
    try:
        session = _get_session()

        # Check if already registered
        if _dataset_exists(session, table_name):
            log.info(
                "Dataset '%s' already exists in Superset — skipping.",
                table_name,
            )
            return {"dataset_id": None, "skipped": True}

        # Find the Trino database
        db_id = _find_database_id(session)
        if db_id is None:
            log.warning(
                "Trino Lakehouse database not found in Superset. "
                "Cannot register dataset '%s'.",
                table_name,
            )
            return {"dataset_id": None, "skipped": True}

        # Create dataset
        dataset_id = _create_dataset(session, db_id, table_name)
        return {"dataset_id": dataset_id, "skipped": False}

    except Exception:
        log.exception(
            "Failed to register dataset '%s' in Superset (non-fatal).",
            table_name,
        )
        return {"dataset_id": None, "skipped": True}
