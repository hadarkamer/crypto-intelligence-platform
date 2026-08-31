-- Formula relevance hysteresis v1
--
-- Relevance is a separate, append-only research axis.  These rows do not
-- mutate research_formulas.active/current_stage, approve LIVE or authorize a
-- delivery.  A retained legacy formula remains explicitly read-only.

CREATE TABLE IF NOT EXISTS research_formula_relevance_assessments (
    relevance_assessment_id BIGSERIAL PRIMARY KEY,
    formula_id BIGINT NOT NULL REFERENCES research_formulas(formula_id),
    formula_version INTEGER NOT NULL CHECK (formula_version > 0),
    relevance_policy_version TEXT NOT NULL CHECK (
        BTRIM(relevance_policy_version) <> ''
    ),
    observation_fingerprint CHAR(64) NOT NULL CHECK (
        BTRIM(observation_fingerprint) ~ '^[0-9a-f]{64}$'
    ),
    evidence_fingerprint CHAR(64) NOT NULL CHECK (
        BTRIM(evidence_fingerprint) ~ '^[0-9a-f]{64}$'
    ),
    snapshot_id CHAR(64) NOT NULL REFERENCES research_formula_evidence_snapshots(snapshot_id),
    observed_at_utc TIMESTAMPTZ NOT NULL,
    observation_utc_date DATE NOT NULL,
    compatibility TEXT NOT NULL CHECK (
        compatibility IN ('CURRENT_V7', 'LEGACY_SHADOW_READ_ONLY')
    ),
    observation TEXT NOT NULL CHECK (
        observation IN ('STRONG', 'EARLY', 'INSUFFICIENT', 'WEAK', 'STALE', 'LEGACY')
    ),
    previous_state TEXT CHECK (
        previous_state IS NULL OR previous_state IN (
            'OBSERVING', 'RELEVANT', 'WEAKENING', 'SUSPENDED',
            'RECOVERING', 'LEGACY_READ_ONLY'
        )
    ),
    relevance_state TEXT NOT NULL CHECK (
        relevance_state IN (
            'OBSERVING', 'RELEVANT', 'WEAKENING', 'SUSPENDED',
            'RECOVERING', 'LEGACY_READ_ONLY'
        )
    ),
    transition TEXT NOT NULL CHECK (
        transition IN (
            'NONE', 'INITIALIZED', 'BECAME_RELEVANT', 'WEAKENING_STARTED',
            'WEAKNESS_CLEARED', 'SUSPENDED', 'RECOVERY_STARTED',
            'RECOVERY_FAILED', 'REACTIVATED'
        )
    ),
    weak_observation_streak INTEGER NOT NULL CHECK (weak_observation_streak >= 0),
    recovery_evidence_streak INTEGER NOT NULL CHECK (recovery_evidence_streak >= 0),
    experimental_relevance_eligible BOOLEAN NOT NULL,
    decision_payload JSONB NOT NULL CHECK (
        JSONB_TYPEOF(decision_payload) = 'object'
        AND decision_payload ->> 'policy_version' = relevance_policy_version
        AND decision_payload ->> 'observation_fingerprint' = observation_fingerprint
        AND decision_payload ->> 'evidence_fingerprint' = evidence_fingerprint
        AND decision_payload ->> 'snapshot_id' = snapshot_id
        AND decision_payload ->> 'state' = relevance_state
        AND decision_payload ->> 'delivery_channel' = 'NONE'
        AND decision_payload ->> 'live_effect' = 'NONE'
    ),
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (
        formula_id, formula_version, relevance_policy_version,
        observation_fingerprint
    )
);

CREATE INDEX IF NOT EXISTS idx_formula_relevance_latest
    ON research_formula_relevance_assessments (
        formula_id, formula_version, relevance_policy_version,
        relevance_assessment_id DESC
    );

CREATE INDEX IF NOT EXISTS idx_formula_relevance_state_time
    ON research_formula_relevance_assessments (
        relevance_state, observed_at_utc DESC, formula_id
    );

CREATE OR REPLACE FUNCTION prevent_formula_relevance_assessment_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'research_formula_relevance_assessments is append-only';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_formula_relevance_assessments_append_only
    ON research_formula_relevance_assessments;

CREATE TRIGGER trg_formula_relevance_assessments_append_only
BEFORE UPDATE OR DELETE ON research_formula_relevance_assessments
FOR EACH ROW EXECUTE FUNCTION prevent_formula_relevance_assessment_mutation();

DROP TRIGGER IF EXISTS trg_formula_relevance_assessments_no_truncate
    ON research_formula_relevance_assessments;

CREATE TRIGGER trg_formula_relevance_assessments_no_truncate
BEFORE TRUNCATE ON research_formula_relevance_assessments
FOR EACH STATEMENT EXECUTE FUNCTION prevent_formula_relevance_assessment_mutation();

COMMENT ON TABLE research_formula_relevance_assessments IS
    'Append-only versioned relevance decisions; separate from research maturity, formula lifecycle and delivery authority.';
