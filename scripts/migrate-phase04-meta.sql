-- Inference log (nexus_meta.models schema, created Phase 01)
CREATE TABLE IF NOT EXISTS models.inference_log (
    id                       SERIAL PRIMARY KEY,
    dataset_id               VARCHAR(64) NOT NULL,
    event_id                 VARCHAR(64) NOT NULL,
    model_name               VARCHAR(128) NOT NULL,
    model_version            INTEGER,
    anomaly_score            FLOAT NOT NULL,
    is_anomaly               BOOLEAN NOT NULL,
    tier                     VARCHAR(16) NOT NULL,
    trust_score_at_inference FLOAT NOT NULL,
    consumer_lag_ms          BIGINT,
    timestamp                TIMESTAMPTZ NOT NULL,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_il_dataset_ts
    ON models.inference_log (dataset_id, timestamp DESC);
