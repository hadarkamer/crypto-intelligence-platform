-- Legacy Formula live-alert storage v1.
-- Additive only. This migration supplies delivery/subscription storage; it no
-- longer authorizes automatic Shadow-to-LIVE promotion. Migration 005 adds the
-- separate explicit human approval record required by the runtime.

ALTER TABLE research_formulas
    ADD COLUMN IF NOT EXISTS shadow_validation_metrics JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE research_formulas
    ADD COLUMN IF NOT EXISTS shadow_validated_at_utc TIMESTAMPTZ;

ALTER TABLE research_formulas
    ADD COLUMN IF NOT EXISTS live_alert_policy_version TEXT;

CREATE TABLE IF NOT EXISTS research_formula_alert_subscriptions (
    chat_id BIGINT PRIMARY KEY,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    requested_by_user_id BIGINT,
    subscribed_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_formula_alert_subscriptions_active
    ON research_formula_alert_subscriptions (active, updated_at_utc DESC);

CREATE TABLE IF NOT EXISTS research_formula_live_deliveries (
    delivery_id BIGSERIAL PRIMARY KEY,
    formula_id BIGINT NOT NULL REFERENCES research_formulas(formula_id) ON DELETE CASCADE,
    event_id BIGINT NOT NULL REFERENCES research_events(event_id) ON DELETE CASCADE,
    chat_id BIGINT NOT NULL REFERENCES research_formula_alert_subscriptions(chat_id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'PENDING' CHECK (
        status IN ('PENDING', 'SENT', 'FAILED')
    ),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error TEXT,
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_attempt_at_utc TIMESTAMPTZ,
    sent_at_utc TIMESTAMPTZ,
    -- One AI trade alert per underlying bot event per destination. If several
    -- LIVE formulas match, the highest-ranked formula is queued first.
    UNIQUE (event_id, chat_id)
);

CREATE INDEX IF NOT EXISTS idx_formula_live_deliveries_pending
    ON research_formula_live_deliveries (status, created_at_utc, delivery_id);
