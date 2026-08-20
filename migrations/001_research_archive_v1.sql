-- Research Archive v1
-- DESIGN/MIGRATION ONLY. This file is not executed automatically by Watch,
-- ai_candidate_main.py, or any recurring task.
-- Apply only after explicit approval and against the approved research database.

BEGIN;

CREATE TABLE IF NOT EXISTS research_events (
    event_id BIGSERIAL PRIMARY KEY,
    schema_version TEXT NOT NULL,
    event_kind TEXT NOT NULL CHECK (event_kind IN ('ALERT', 'SIGNAL_STATE_CHANGE')),
    event_type TEXT NOT NULL,
    alert_time_utc TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('LONG', 'SHORT', 'NEUTRAL')),
    timeframe TEXT,
    score DOUBLE PRECISION,
    current_price DOUBLE PRECISION,
    target_price DOUBLE PRECISION,
    initial_target_distance_pct DOUBLE PRECISION,
    categories JSONB NOT NULL DEFAULT '[]'::jsonb,
    setup_key CHAR(64) NOT NULL,
    event_fingerprint CHAR(64) NOT NULL UNIQUE,
    strategy_version TEXT NOT NULL,
    code_version TEXT NOT NULL,
    capture_stage TEXT NOT NULL DEFAULT 'OBSERVED',
    delivery_status TEXT NOT NULL DEFAULT 'UNKNOWN'
        CHECK (delivery_status IN ('UNKNOWN', 'NOT_APPLICABLE', 'APPROVED_FOR_DELIVERY', 'DELIVERED', 'DELIVERY_FAILED')),
    engine_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_research_events_symbol_time
    ON research_events (symbol, alert_time_utc DESC);
CREATE INDEX IF NOT EXISTS idx_research_events_type_time
    ON research_events (event_type, alert_time_utc DESC);
CREATE INDEX IF NOT EXISTS idx_research_events_setup_time
    ON research_events (setup_key, alert_time_utc ASC);
CREATE INDEX IF NOT EXISTS idx_research_events_kind_time
    ON research_events (event_kind, alert_time_utc DESC);

CREATE TABLE IF NOT EXISTS research_alert_outcomes (
    event_id BIGINT NOT NULL REFERENCES research_events(event_id) ON DELETE CASCADE,
    horizon_minutes INTEGER NOT NULL CHECK (horizon_minutes > 0),
    measured_at_utc TIMESTAMPTZ NOT NULL,
    reference_price DOUBLE PRECISION NOT NULL,
    price_at_horizon DOUBLE PRECISION,
    raw_return_pct DOUBLE PRECISION,
    directional_return_pct DOUBLE PRECISION,
    mfe_pct DOUBLE PRECISION,
    mae_pct DOUBLE PRECISION,
    time_to_first_progress_seconds INTEGER,
    time_to_mfe_seconds INTEGER,
    closest_target_distance_pct DOUBLE PRECISION,
    target_progress_ratio DOUBLE PRECISION,
    target_reached BOOLEAN,
    outcome_method_version TEXT NOT NULL,
    price_source TEXT,
    data_quality_status TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, horizon_minutes)
);

CREATE INDEX IF NOT EXISTS idx_research_outcomes_horizon
    ON research_alert_outcomes (horizon_minutes, measured_at_utc DESC);

COMMIT;
