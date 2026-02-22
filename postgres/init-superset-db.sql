-- =============================================================================
-- PostgreSQL Initialization Script
-- =============================================================================
-- Creates the Superset database and user on first startup.
-- This runs automatically via docker-entrypoint-initdb.d.
-- =============================================================================

-- Create Superset user (ignore if exists)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'superset') THEN
        CREATE ROLE superset WITH LOGIN PASSWORD 'superset';
    END IF;
END
$$;

-- Create Superset database
SELECT 'CREATE DATABASE superset OWNER superset'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'superset')\gexec

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE superset TO superset;
