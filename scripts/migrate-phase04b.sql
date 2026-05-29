-- Phase 04b: drift flags and shadow evaluation log
CREATE TABLE IF NOT EXISTS models.model_drift_flags (
    id               SERIAL PRIMARY KEY,
    model_name       VARCHAR(128) NOT NULL,
    model_version    INTEGER NOT NULL,
    event_type       VARCHAR(64) NOT NULL,
    ks_stat          FLOAT NOT NULL,
    ks_pvalue        FLOAT NOT NULL,
    reference_n      INTEGER NOT NULL,
    recent_n         INTEGER NOT NULL,
    status           VARCHAR(32) NOT NULL DEFAULT 'flagged',
    detected_at      TIMESTAMPTZ NOT NULL,
    resolved_at      TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_mdf_status CHECK (status IN ('flagged','shadow','promoted','dismissed'))
);
CREATE INDEX IF NOT EXISTS idx_mdf_event_status
    ON models.model_drift_flags (event_type, status, detected_at DESC);

CREATE TABLE IF NOT EXISTS models.shadow_log (
    id                    SERIAL PRIMARY KEY,
    drift_flag_id         INTEGER NOT NULL REFERENCES models.model_drift_flags(id),
    event_id              VARCHAR(64) NOT NULL,
    dataset_id            VARCHAR(64) NOT NULL,
    production_score      FLOAT NOT NULL,
    shadow_score          FLOAT NOT NULL,
    shadow_model_version  INTEGER NOT NULL,
    shadow_latency_ms     BIGINT,
    timestamp             TIMESTAMPTZ NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_sl_flag_ts
    ON models.shadow_log (drift_flag_id, timestamp DESC);
