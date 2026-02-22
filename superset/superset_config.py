"""
Superset configuration module.

Injected into the container at /app/superset_config.py and referenced via
the SUPERSET_CONFIG_PATH environment variable.  Values are read from
environment variables so that no secrets are committed to source control.
"""

import os

# ---------------------------------------------------------------------------
# Core
# ---------------------------------------------------------------------------
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "change-me-to-a-secure-key")
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SUPERSET_DB_URI",
    "postgresql+psycopg2://superset:superset@postgres:5432/superset",
)

# ---------------------------------------------------------------------------
# Feature flags
# ---------------------------------------------------------------------------
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}

# ---------------------------------------------------------------------------
# Cache (use simple in-memory cache for local dev)
# ---------------------------------------------------------------------------
CACHE_CONFIG = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
}

# ---------------------------------------------------------------------------
# Web server
# ---------------------------------------------------------------------------
ENABLE_PROXY_FIX = True
