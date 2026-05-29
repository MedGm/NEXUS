CREATE TABLE IF NOT EXISTS trust.validation_results (
    id               SERIAL PRIMARY KEY,
    dataset_id       VARCHAR(64) NOT NULL,
    batch_timestamp  TIMESTAMPTZ NOT NULL,
    batch_size       INTEGER NOT NULL,
    success          BOOLEAN NOT NULL,
    null_rate        FLOAT NOT NULL,
    results_json     JSONB NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_null_rate CHECK (null_rate BETWEEN 0.0 AND 1.0)
);
COMMENT ON COLUMN trust.validation_results.null_rate
    IS 'total_null_fields / (total_fields * batch_size) — per-field sensitivity';
COMMENT ON COLUMN trust.validation_results.results_json
    IS 'GE ValidationResult + raw observed values for psi_columns under key psi_values';
COMMENT ON COLUMN trust.validation_results.dataset_id
    IS 'Exact Avro record name: OrderPlaced — NOT com.nexus.events.OrderPlaced';

CREATE INDEX IF NOT EXISTS idx_vr_dataset_ts
    ON trust.validation_results (dataset_id, batch_timestamp DESC);

CREATE TABLE IF NOT EXISTS trust.dataset_trust_scores (
    id               SERIAL PRIMARY KEY,
    dataset_id       VARCHAR(64) NOT NULL,
    timestamp        TIMESTAMPTZ NOT NULL,
    freshness        FLOAT NOT NULL,
    completeness     FLOAT NOT NULL,
    drift_psi        FLOAT,
    lineage_depth    INTEGER NOT NULL CHECK (lineage_depth >= 1),
    composite_score  FLOAT NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_freshness CHECK (freshness BETWEEN 0.0 AND 1.0),
    CONSTRAINT chk_completeness CHECK (completeness BETWEEN 0.0 AND 1.0),
    CONSTRAINT chk_composite CHECK (composite_score BETWEEN 0.0 AND 1.0)
);
COMMENT ON COLUMN trust.dataset_trust_scores.drift_psi
    IS 'NULL until min_reference_samples reached (first ~10 batches)';

CREATE INDEX IF NOT EXISTS idx_dts_dataset_ts
    ON trust.dataset_trust_scores (dataset_id, timestamp DESC);
