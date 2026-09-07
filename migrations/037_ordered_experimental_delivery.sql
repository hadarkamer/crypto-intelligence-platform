-- Separate, authorized research notifications; legacy v6 remains quarantined.
CREATE TABLE IF NOT EXISTS research_ordered_experimental_eligibility (
    freeze_id TEXT PRIMARY KEY REFERENCES research_ordered_validation_freezes(freeze_id),
    scope_key TEXT NOT NULL UNIQUE REFERENCES research_ordered_formula_scopes(scope_key),
    evidence_sha256 TEXT NOT NULL,
    evaluated_at_utc TIMESTAMPTZ NOT NULL,
    published_at_utc TIMESTAMPTZ NOT NULL,
    eligible_until_utc TIMESTAMPTZ,
    ready BOOLEAN NOT NULL,
    last_scanned_at_utc TIMESTAMPTZ,
    FOREIGN KEY(freeze_id,evidence_sha256) REFERENCES research_ordered_validation_evaluations(freeze_id,evidence_sha256),
    CHECK (ready = (eligible_until_utc IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS idx_ordered_experimental_eligible
    ON research_ordered_experimental_eligibility(last_scanned_at_utc ASC NULLS FIRST,freeze_id) WHERE ready;
CREATE TABLE IF NOT EXISTS research_ordered_experimental_deliveries (
    delivery_id BIGSERIAL PRIMARY KEY,
    freeze_id TEXT NOT NULL REFERENCES research_ordered_validation_freezes(freeze_id),
    evidence_sha256 TEXT NOT NULL,
    candidate_key TEXT NOT NULL,
    event_id BIGINT NOT NULL REFERENCES research_events(event_id),
    btc_parent_movement_id TEXT NOT NULL,
    direction TEXT NOT NULL CHECK(direction IN ('LONG','SHORT')),
    chat_id BIGINT NOT NULL REFERENCES research_formula_alert_subscriptions(chat_id),
    payload JSONB NOT NULL CHECK(jsonb_typeof(payload)='object'),
    status TEXT NOT NULL DEFAULT 'PENDING' CHECK(status IN ('PENDING','CLAIMED','SENDING','SENT','UNKNOWN','CANCELLED')),
    claim_token UUID,
    lease_expires_at_utc TIMESTAMPTZ,
    expires_at_utc TIMESTAMPTZ NOT NULL,
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    sent_at_utc TIMESTAMPTZ,
    telegram_message_id BIGINT,
    last_error TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY(freeze_id,evidence_sha256) REFERENCES research_ordered_validation_evaluations(freeze_id,evidence_sha256),
    CHECK((status IN ('CLAIMED','SENDING'))=(claim_token IS NOT NULL)),
    CHECK((status='SENT')=(sent_at_utc IS NOT NULL))
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_ordered_experimental_wave_dedup
    ON research_ordered_experimental_deliveries(chat_id,candidate_key,btc_parent_movement_id,direction) WHERE status<>'CANCELLED';
CREATE UNIQUE INDEX IF NOT EXISTS idx_ordered_experimental_event_dedup
    ON research_ordered_experimental_deliveries(chat_id,event_id,direction) WHERE status<>'CANCELLED';
CREATE INDEX IF NOT EXISTS idx_ordered_experimental_pending ON research_ordered_experimental_deliveries(created_at_utc,delivery_id) WHERE status='PENDING';
CREATE INDEX IF NOT EXISTS idx_ordered_experimental_lease ON research_ordered_experimental_deliveries(lease_expires_at_utc) WHERE status IN ('CLAIMED','SENDING');
COMMENT ON TABLE research_ordered_experimental_deliveries IS
 'Experimental only. SENDING expiry or ambiguous transport becomes UNKNOWN and is never automatically retried; Telegram supplies no idempotency key.';
