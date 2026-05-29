-- Rename drift_psi → drift_psi_score (Phase 03 debt)
ALTER TABLE trust.dataset_trust_scores
    RENAME COLUMN drift_psi TO drift_psi_score;
