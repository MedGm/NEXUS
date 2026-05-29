#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    -- ── Airflow ──────────────────────────────────────────────
    CREATE USER airflow WITH PASSWORD 'airflow';
    CREATE DATABASE airflow OWNER airflow;

    -- ── MLflow ───────────────────────────────────────────────
    CREATE DATABASE mlflow OWNER postgres;

    -- ── NEXUS: Meta ──────────────────────────────────────────
    CREATE DATABASE nexus_meta OWNER postgres;

    -- ── NEXUS: Metrics ───────────────────────────────────────
    CREATE DATABASE nexus_metrics OWNER postgres;

    -- ── NEXUS: Incidents ─────────────────────────────────────
    CREATE DATABASE nexus_incidents OWNER postgres;
EOSQL

# nexus_meta schemas
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname nexus_meta <<-EOSQL
    CREATE SCHEMA IF NOT EXISTS pipelines;   -- pipeline run history, DAG metadata
    CREATE SCHEMA IF NOT EXISTS models;      -- registered models, versions, lineage
EOSQL

# nexus_metrics schemas
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname nexus_metrics <<-EOSQL
    CREATE SCHEMA IF NOT EXISTS trust;       -- per-model/per-batch trust scores
    CREATE SCHEMA IF NOT EXISTS drift;       -- feature drift stats, thresholds, alerts
EOSQL

# nexus_incidents schemas
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname nexus_incidents <<-EOSQL
    CREATE SCHEMA IF NOT EXISTS incidents;   -- incident records, severity, status
    CREATE SCHEMA IF NOT EXISTS audit;       -- action log, resolution history
EOSQL
