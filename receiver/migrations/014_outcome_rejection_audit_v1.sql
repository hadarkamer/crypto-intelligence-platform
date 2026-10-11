-- Research outcome rejection audit v1
--
-- Additive and fail-closed.  A legacy Alert whose immutable decision-price
-- provenance is not canonical is recorded once for the current validation
-- policy.  The original research_event remains unchanged and readable, while
-- outcome workers can exclude the audited rejection from later polling.

CREATE TABLE IF NOT EXISTS research_outcome_event_rejections (
    event_id BIGINT NOT NULL REFERENCES research_events(event_id),
    rejection_policy_version TEXT NOT NULL CHECK (
        BTRIM(rejection_policy_version) <> ''
    ),
    reason_code TEXT NOT NULL CHECK (BTRIM(reason_code) <> ''),
    reason_text TEXT NOT NULL CHECK (BTRIM(reason_text) <> ''),
    event_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (
        JSONB_TYPEOF(event_snapshot) = 'object'
    ),
    rejected_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, rejection_policy_version)
);

CREATE INDEX IF NOT EXISTS idx_outcome_event_rejections_policy_time
    ON research_outcome_event_rejections (
        rejection_policy_version, rejected_at_utc DESC, event_id
    );

CREATE OR REPLACE FUNCTION prevent_outcome_event_rejection_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'research_outcome_event_rejections is append-only';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_outcome_event_rejections_append_only
    ON research_outcome_event_rejections;

CREATE TRIGGER trg_outcome_event_rejections_append_only
BEFORE UPDATE OR DELETE ON research_outcome_event_rejections
FOR EACH ROW EXECUTE FUNCTION prevent_outcome_event_rejection_mutation();

DROP TRIGGER IF EXISTS trg_outcome_event_rejections_no_truncate
    ON research_outcome_event_rejections;

CREATE TRIGGER trg_outcome_event_rejections_no_truncate
BEFORE TRUNCATE ON research_outcome_event_rejections
FOR EACH STATEMENT EXECUTE FUNCTION prevent_outcome_event_rejection_mutation();

COMMENT ON TABLE research_outcome_event_rejections IS
    'Append-only audit of immutable events excluded from outcome enrichment under a versioned validation policy; source events are never rewritten.';

