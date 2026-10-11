-- Formula Evidence snapshots v1
--
-- Additive, append-only infrastructure.  This table does not promote formulas,
-- enable Telegram, or authorize LIVE.  Writers arrive in later stages; the
-- current migration only provides a content-addressed audit boundary shared by
-- Discovery, Shadow and future experimental renderers.  An explicit
-- idempotent store API exists, but no production worker calls it in stage 2.

CREATE TABLE IF NOT EXISTS research_formula_evidence_snapshots (
    snapshot_id CHAR(64) PRIMARY KEY CHECK (
        BTRIM(snapshot_id) ~ '^[0-9a-f]{64}$'
    ),
    snapshot_schema_version TEXT NOT NULL CHECK (
        BTRIM(snapshot_schema_version) <> ''
    ),
    assessment_schema_version TEXT NOT NULL CHECK (
        BTRIM(assessment_schema_version) <> ''
    ),
    contract_version TEXT NOT NULL CHECK (BTRIM(contract_version) <> ''),
    compatibility TEXT NOT NULL CHECK (
        compatibility IN ('CURRENT_V7', 'LEGACY_SHADOW_READ_ONLY')
    ),
    formula_id BIGINT NOT NULL REFERENCES research_formulas(formula_id),
    source_run_id BIGINT REFERENCES research_formula_runs(run_id),
    formula_key CHAR(64) NOT NULL CHECK (
        BTRIM(formula_key) ~ '^[0-9a-f]{64}$'
    ),
    formula_version INTEGER NOT NULL CHECK (formula_version > 0),
    formula_schema_version TEXT NOT NULL CHECK (
        BTRIM(formula_schema_version) <> ''
    ),
    phase TEXT NOT NULL CHECK (phase IN ('HISTORICAL', 'PROSPECTIVE')),
    assessed_at_utc TIMESTAMPTZ NOT NULL,
    formula_family_id CHAR(64) NOT NULL CHECK (
        BTRIM(formula_family_id) ~ '^[0-9a-f]{64}$'
    ),
    matched_market_episode_ids JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (
        JSONB_TYPEOF(matched_market_episode_ids) = 'array'
    ),
    control_market_episode_ids JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (
        JSONB_TYPEOF(control_market_episode_ids) = 'array'
    ),
    matched_parent_market_episode_ids JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (
        JSONB_TYPEOF(matched_parent_market_episode_ids) = 'array'
    ),
    control_parent_market_episode_ids JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (
        JSONB_TYPEOF(control_parent_market_episode_ids) = 'array'
    ),
    raw_match_count INTEGER NOT NULL CHECK (raw_match_count >= 0),
    raw_control_count INTEGER NOT NULL CHECK (raw_control_count >= 0),
    matched_n_eff DOUBLE PRECISION NOT NULL CHECK (
        matched_n_eff >= 0.0
        AND matched_n_eff NOT IN (
            'Infinity'::DOUBLE PRECISION,
            '-Infinity'::DOUBLE PRECISION,
            'NaN'::DOUBLE PRECISION
        )
    ),
    control_n_eff DOUBLE PRECISION NOT NULL CHECK (
        control_n_eff >= 0.0
        AND control_n_eff NOT IN (
            'Infinity'::DOUBLE PRECISION,
            '-Infinity'::DOUBLE PRECISION,
            'NaN'::DOUBLE PRECISION
        )
    ),
    snapshot_payload JSONB NOT NULL CHECK (
        JSONB_TYPEOF(snapshot_payload) = 'object'
        AND snapshot_payload ->> 'snapshot_id' = snapshot_id
        AND snapshot_payload ->> 'snapshot_schema_version' = snapshot_schema_version
        AND snapshot_payload ->> 'assessment_schema_version' = assessment_schema_version
        AND snapshot_payload ->> 'contract_version' = contract_version
        AND snapshot_payload ->> 'compatibility' = compatibility
        AND snapshot_payload ->> 'formula_family_id' = formula_family_id
        AND snapshot_payload ->> 'phase' = phase
        AND snapshot_payload -> 'live_eligible' = 'false'::jsonb
        AND snapshot_payload ->> 'delivery_channel' = 'NONE'
    ),
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_formula_evidence_snapshots_formula_time
    ON research_formula_evidence_snapshots (
        formula_id, phase, assessed_at_utc DESC, snapshot_id
    );

CREATE INDEX IF NOT EXISTS idx_formula_evidence_snapshots_family_time
    ON research_formula_evidence_snapshots (
        formula_family_id, assessed_at_utc DESC, snapshot_id
    );

CREATE OR REPLACE FUNCTION prevent_formula_evidence_snapshot_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'research_formula_evidence_snapshots is append-only';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_formula_evidence_snapshots_append_only
    ON research_formula_evidence_snapshots;

CREATE TRIGGER trg_formula_evidence_snapshots_append_only
BEFORE UPDATE OR DELETE ON research_formula_evidence_snapshots
FOR EACH ROW EXECUTE FUNCTION prevent_formula_evidence_snapshot_mutation();

DROP TRIGGER IF EXISTS trg_formula_evidence_snapshots_no_truncate
    ON research_formula_evidence_snapshots;

CREATE TRIGGER trg_formula_evidence_snapshots_no_truncate
BEFORE TRUNCATE ON research_formula_evidence_snapshots
FOR EACH STATEMENT EXECUTE FUNCTION prevent_formula_evidence_snapshot_mutation();

COMMENT ON TABLE research_formula_evidence_snapshots IS
    'Append-only, content-addressed FormulaAssessment/EvidenceSnapshot envelopes; infrastructure only, with no LIVE or delivery authority.';
