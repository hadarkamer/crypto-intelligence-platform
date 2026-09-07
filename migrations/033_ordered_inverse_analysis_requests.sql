-- Explicit research-only inverse requests. Original delivered alerts remain
-- immutable; derived DECISION_SAMPLE rows are not Telegram deliveries or
-- prospective no-signal anchors. Existing v7 direction FKs stay unchanged.
CREATE TABLE IF NOT EXISTS research_ordered_inverse_requests (
    linked_source_event_id BIGINT NOT NULL REFERENCES research_events(event_id) ON DELETE CASCADE,
    inverse_version TEXT NOT NULL CHECK(inverse_version='ordered-inverse-analysis-v1'),
    source_fingerprint CHAR(64) NOT NULL,
    source_contract_sha256 CHAR(64) NOT NULL,
    source_contract JSONB NOT NULL CHECK(jsonb_typeof(source_contract)='object'),
    outcome_event_id BIGINT REFERENCES research_events(event_id),
    queue_status TEXT NOT NULL DEFAULT 'PENDING'
        CHECK(queue_status IN ('PENDING','IN_FLIGHT','RETRY','COMPLETE','REJECTED')),
    claim_token UUID,
    lease_expires_at_utc TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts>=0),
    requested_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    next_attempt_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_checked_at_utc TIMESTAMPTZ,
    last_error TEXT,
    result JSONB NOT NULL DEFAULT '{}'::jsonb CHECK(jsonb_typeof(result)='object'),
    PRIMARY KEY(linked_source_event_id,inverse_version),
    UNIQUE(outcome_event_id),
    CHECK(outcome_event_id IS NULL OR outcome_event_id<>linked_source_event_id),
    CHECK((queue_status='IN_FLIGHT')=(claim_token IS NOT NULL)),
    CHECK((queue_status='IN_FLIGHT')=(lease_expires_at_utc IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS idx_ordered_inverse_requests_due
    ON research_ordered_inverse_requests(next_attempt_at_utc,linked_source_event_id)
    WHERE queue_status IN ('PENDING','RETRY','IN_FLIGHT');
COMMENT ON TABLE research_ordered_inverse_requests IS
    'On-demand inverse v7 computation over original immutable entry time/price. Matches and BTC wave identity always use linked_source_event_id, never the derived outcome_event_id. No Telegram delivery or neutral-anchor authority is implied.';
