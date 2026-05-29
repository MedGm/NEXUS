-- Phase 05: incident intelligence storage
-- Runs against nexus_incidents DB (incidents schema already exists from Phase 01)

CREATE TABLE IF NOT EXISTS incidents.incidents (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    started_at          TIMESTAMPTZ NOT NULL,
    resolved_at         TIMESTAMPTZ,
    root_cause_node_id  VARCHAR(64),
    causal_graph_json   JSONB NOT NULL,
    llm_summary         TEXT,
    severity            VARCHAR(8)  NOT NULL,
    node_count          INTEGER     NOT NULL,
    status              VARCHAR(16) NOT NULL DEFAULT 'open',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_inc_severity CHECK (severity  IN ('LOW','MEDIUM','HIGH')),
    CONSTRAINT chk_inc_status   CHECK (status    IN ('open','resolved'))
);
CREATE INDEX IF NOT EXISTS idx_inc_started ON incidents.incidents (started_at DESC);
CREATE INDEX IF NOT EXISTS idx_inc_status  ON incidents.incidents (status, started_at DESC);
