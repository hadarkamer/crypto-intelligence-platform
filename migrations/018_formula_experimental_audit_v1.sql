-- Disabled Experimental audit storage v1
--
-- Additive, append-only schema preparation only.  This file is deliberately
-- absent from research_formula_schema_admin.MIGRATION_PATHS while Stage 5 is
-- WAITING_DATA.  It creates no subscription, queue, Telegram route, formula
-- approval, research evidence or LIVE authority.

CREATE TABLE IF NOT EXISTS research_experimental_audit_batches (
    audit_batch_id CHAR(64) PRIMARY KEY CHECK (
        BTRIM(audit_batch_id) ~ '^[0-9a-f]{64}$'
    ),
    audit_contract_version TEXT NOT NULL CHECK (
        BTRIM(audit_contract_version) <> ''
    ),
    gate_policy_version TEXT NOT NULL CHECK (
        BTRIM(gate_policy_version) <> ''
    ),
    evaluated_at_utc TIMESTAMPTZ NOT NULL,
    stage5_status TEXT NOT NULL CHECK (
        stage5_status IN ('READY', 'WAITING_DATA')
    ),
    chat_id BIGINT NOT NULL CHECK (chat_id <> 0),
    route TEXT NOT NULL CHECK (
        route IN ('TEST_ALLOWLIST', 'OPT_IN', 'NONE')
    ),
    batch_payload JSONB NOT NULL CHECK (
        JSONB_TYPEOF(batch_payload) = 'object'
        AND batch_payload ->> 'audit_batch_id' IS NULL
        AND batch_payload ->> 'audit_contract_version' = audit_contract_version
        AND batch_payload ->> 'gate_policy_version' = gate_policy_version
        AND batch_payload ->> 'stage5_status' = stage5_status
        AND (batch_payload ->> 'chat_id')::BIGINT = chat_id
        AND batch_payload ->> 'route' = route
    ),
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS research_experimental_audit_decisions (
    audit_decision_id CHAR(64) PRIMARY KEY CHECK (
        BTRIM(audit_decision_id) ~ '^[0-9a-f]{64}$'
    ),
    audit_batch_id CHAR(64) NOT NULL REFERENCES
        research_experimental_audit_batches(audit_batch_id),
    delivery_key CHAR(64) NOT NULL CHECK (
        BTRIM(delivery_key) ~ '^[0-9a-f]{64}$'
    ),
    status TEXT NOT NULL CHECK (
        status IN ('SIMULATED_ELIGIBLE', 'SUPPRESSED')
    ),
    chat_id BIGINT NOT NULL CHECK (chat_id <> 0),
    route TEXT NOT NULL CHECK (
        route IN ('TEST_ALLOWLIST', 'OPT_IN', 'NONE')
    ),
    formula_family_id CHAR(64) NOT NULL CHECK (
        BTRIM(formula_family_id) ~ '^[0-9a-f]{64}$'
    ),
    representative_snapshot_id CHAR(64) NOT NULL REFERENCES
        research_formula_evidence_snapshots(snapshot_id),
    relevance_decision_sha256 CHAR(64) CHECK (
        relevance_decision_sha256 IS NULL
        OR BTRIM(relevance_decision_sha256) ~ '^[0-9a-f]{64}$'
    ),
    rendered_message_sha256 CHAR(64) NOT NULL CHECK (
        BTRIM(rendered_message_sha256) ~ '^[0-9a-f]{64}$'
    ),
    evaluated_at_utc TIMESTAMPTZ NOT NULL,
    decision_payload JSONB NOT NULL CHECK (
        JSONB_TYPEOF(decision_payload) = 'object'
        AND decision_payload ->> 'audit_decision_id' = audit_decision_id
        AND decision_payload ->> 'formula_family_id' = formula_family_id
        AND decision_payload ->> 'representative_snapshot_id' =
            representative_snapshot_id
        AND decision_payload ->> 'delivery_key' = delivery_key
        AND decision_payload ->> 'status' = status
        AND decision_payload ->> 'research_evidence_effect' = 'NONE'
    ),
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS research_experimental_preview_audit_batches (
    preview_batch_id CHAR(64) PRIMARY KEY CHECK (
        BTRIM(preview_batch_id) ~ '^[0-9a-f]{64}$'
    ),
    preview_policy_version TEXT NOT NULL CHECK (
        BTRIM(preview_policy_version) <> ''
    ),
    source_audit_batch_id CHAR(64) NOT NULL REFERENCES
        research_experimental_audit_batches(audit_batch_id),
    stage5_status TEXT NOT NULL CHECK (
        stage5_status IN ('READY', 'WAITING_DATA')
    ),
    chat_id BIGINT NOT NULL CHECK (chat_id <> 0),
    route TEXT NOT NULL CHECK (route IN ('TEST_ALLOWLIST', 'NONE')),
    public_opt_in BOOLEAN NOT NULL DEFAULT FALSE CHECK (public_opt_in = FALSE),
    stage6_activated BOOLEAN NOT NULL DEFAULT FALSE CHECK (
        stage6_activated = FALSE
    ),
    batch_payload JSONB NOT NULL CHECK (
        JSONB_TYPEOF(batch_payload) = 'object'
        AND batch_payload ->> 'preview_batch_id' IS NULL
        AND batch_payload ->> 'policy_version' = preview_policy_version
        AND batch_payload ->> 'source_audit_batch_id' = source_audit_batch_id
        AND batch_payload ->> 'stage5_status' = stage5_status
        AND (batch_payload ->> 'chat_id')::BIGINT = chat_id
        AND batch_payload ->> 'route' = route
        AND batch_payload -> 'public_opt_in' = 'false'::jsonb
        AND batch_payload -> 'stage6_activated' = 'false'::jsonb
    ),
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS research_experimental_preview_audit_decisions (
    preview_decision_id CHAR(64) PRIMARY KEY CHECK (
        BTRIM(preview_decision_id) ~ '^[0-9a-f]{64}$'
    ),
    preview_batch_id CHAR(64) NOT NULL REFERENCES
        research_experimental_preview_audit_batches(preview_batch_id),
    source_audit_decision_id CHAR(64) NOT NULL REFERENCES
        research_experimental_audit_decisions(audit_decision_id),
    preview_key CHAR(64) NOT NULL CHECK (
        BTRIM(preview_key) ~ '^[0-9a-f]{64}$'
    ),
    status TEXT NOT NULL CHECK (
        status IN ('PREVIEW_SIMULATED_ELIGIBLE', 'PREVIEW_SUPPRESSED')
    ),
    chat_id BIGINT NOT NULL CHECK (chat_id <> 0),
    route TEXT NOT NULL CHECK (route IN ('TEST_ALLOWLIST', 'NONE')),
    formula_family_id CHAR(64) NOT NULL CHECK (
        BTRIM(formula_family_id) ~ '^[0-9a-f]{64}$'
    ),
    representative_snapshot_id CHAR(64) NOT NULL REFERENCES
        research_formula_evidence_snapshots(snapshot_id),
    preview_message_sha256 CHAR(64) NOT NULL CHECK (
        BTRIM(preview_message_sha256) ~ '^[0-9a-f]{64}$'
    ),
    decision_payload JSONB NOT NULL CHECK (
        JSONB_TYPEOF(decision_payload) = 'object'
        AND decision_payload ->> 'preview_decision_id' = preview_decision_id
        AND decision_payload ->> 'source_audit_decision_id' =
            source_audit_decision_id
        AND decision_payload ->> 'preview_key' = preview_key
        AND decision_payload ->> 'status' = status
        AND decision_payload ->> 'route' = route
        AND decision_payload ->> 'formula_family_id' = formula_family_id
        AND decision_payload ->> 'representative_snapshot_id' =
            representative_snapshot_id
        AND decision_payload ->> 'research_evidence_effect' = 'NONE'
    ),
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_experimental_audit_batch_time
    ON research_experimental_audit_batches (
        evaluated_at_utc DESC, audit_batch_id
    );

CREATE INDEX IF NOT EXISTS idx_experimental_audit_decision_delivery
    ON research_experimental_audit_decisions (
        delivery_key, evaluated_at_utc DESC, audit_decision_id
    );

CREATE INDEX IF NOT EXISTS idx_experimental_preview_batch_time
    ON research_experimental_preview_audit_batches (
        created_at_utc DESC, preview_batch_id
    );

CREATE INDEX IF NOT EXISTS idx_experimental_preview_decision_key
    ON research_experimental_preview_audit_decisions (
        preview_key, created_at_utc DESC, preview_decision_id
    );

CREATE OR REPLACE FUNCTION prevent_experimental_audit_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'Experimental audit storage is append-only';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_experimental_audit_batches_append_only
    ON research_experimental_audit_batches;

CREATE TRIGGER trg_experimental_audit_batches_append_only
BEFORE UPDATE OR DELETE ON research_experimental_audit_batches
FOR EACH ROW EXECUTE FUNCTION prevent_experimental_audit_mutation();

DROP TRIGGER IF EXISTS trg_experimental_audit_batches_no_truncate
    ON research_experimental_audit_batches;

CREATE TRIGGER trg_experimental_audit_batches_no_truncate
BEFORE TRUNCATE ON research_experimental_audit_batches
FOR EACH STATEMENT EXECUTE FUNCTION prevent_experimental_audit_mutation();

DROP TRIGGER IF EXISTS trg_experimental_audit_decisions_append_only
    ON research_experimental_audit_decisions;

CREATE TRIGGER trg_experimental_audit_decisions_append_only
BEFORE UPDATE OR DELETE ON research_experimental_audit_decisions
FOR EACH ROW EXECUTE FUNCTION prevent_experimental_audit_mutation();

DROP TRIGGER IF EXISTS trg_experimental_audit_decisions_no_truncate
    ON research_experimental_audit_decisions;

CREATE TRIGGER trg_experimental_audit_decisions_no_truncate
BEFORE TRUNCATE ON research_experimental_audit_decisions
FOR EACH STATEMENT EXECUTE FUNCTION prevent_experimental_audit_mutation();

DROP TRIGGER IF EXISTS trg_experimental_preview_batches_append_only
    ON research_experimental_preview_audit_batches;

CREATE TRIGGER trg_experimental_preview_batches_append_only
BEFORE UPDATE OR DELETE ON research_experimental_preview_audit_batches
FOR EACH ROW EXECUTE FUNCTION prevent_experimental_audit_mutation();

DROP TRIGGER IF EXISTS trg_experimental_preview_batches_no_truncate
    ON research_experimental_preview_audit_batches;

CREATE TRIGGER trg_experimental_preview_batches_no_truncate
BEFORE TRUNCATE ON research_experimental_preview_audit_batches
FOR EACH STATEMENT EXECUTE FUNCTION prevent_experimental_audit_mutation();

DROP TRIGGER IF EXISTS trg_experimental_preview_decisions_append_only
    ON research_experimental_preview_audit_decisions;

CREATE TRIGGER trg_experimental_preview_decisions_append_only
BEFORE UPDATE OR DELETE ON research_experimental_preview_audit_decisions
FOR EACH ROW EXECUTE FUNCTION prevent_experimental_audit_mutation();

DROP TRIGGER IF EXISTS trg_experimental_preview_decisions_no_truncate
    ON research_experimental_preview_audit_decisions;

CREATE TRIGGER trg_experimental_preview_decisions_no_truncate
BEFORE TRUNCATE ON research_experimental_preview_audit_decisions
FOR EACH STATEMENT EXECUTE FUNCTION prevent_experimental_audit_mutation();

COMMENT ON TABLE research_experimental_audit_batches IS
    'Unapplied append-only preparation for disabled Experimental gate audits; no delivery, evidence or LIVE authority.';

COMMENT ON TABLE research_experimental_audit_decisions IS
    'Unapplied content-addressed Experimental decisions; simulated output only and never research evidence.';

COMMENT ON TABLE research_experimental_preview_audit_batches IS
    'Unapplied PREVIEW_ONLY audit batches; test-chat simulation only, with no public opt-in or Stage-6 activation.';

COMMENT ON TABLE research_experimental_preview_audit_decisions IS
    'Unapplied content-addressed PREVIEW_ONLY decisions; never delivery authority or research evidence.';
