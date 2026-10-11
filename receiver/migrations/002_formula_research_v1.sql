-- Formula Research v1
-- Additive schema only. Applied manually through research_formula_schema_admin.py.
-- Automatic research may advance a formula no further than SHADOW. APPROVED
-- and LIVE require explicit human approval and separate live-alert enablement.

CREATE TABLE IF NOT EXISTS research_formula_runs (
    run_id BIGSERIAL PRIMARY KEY,
    engine_version TEXT NOT NULL,
    feature_schema_version TEXT NOT NULL,
    outcome_method_version TEXT NOT NULL,
    horizon_minutes INTEGER NOT NULL CHECK (horizon_minutes IN (60, 240, 720, 1440)),
    lookback_days INTEGER NOT NULL CHECK (lookback_days > 0),
    status TEXT NOT NULL CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED')),
    dataset_start_utc TIMESTAMPTZ,
    dataset_end_utc TIMESTAMPTZ,
    holdout_start_utc TIMESTAMPTZ,
    sample_size INTEGER NOT NULL DEFAULT 0,
    discovery_sample_size INTEGER NOT NULL DEFAULT 0,
    holdout_sample_size INTEGER NOT NULL DEFAULT 0,
    candidates_evaluated INTEGER NOT NULL DEFAULT 0,
    formulas_persisted INTEGER NOT NULL DEFAULT 0,
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    coverage JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_text TEXT,
    started_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at_utc TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_research_formula_runs_horizon_time
    ON research_formula_runs (horizon_minutes, started_at_utc DESC);

CREATE TABLE IF NOT EXISTS research_formulas (
    formula_id BIGSERIAL PRIMARY KEY,
    formula_key CHAR(64) NOT NULL UNIQUE,
    formula_version INTEGER NOT NULL DEFAULT 1 CHECK (formula_version > 0),
    formula_schema_version TEXT NOT NULL,
    engine_version TEXT NOT NULL,
    feature_schema_version TEXT NOT NULL,
    outcome_method_version TEXT NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('LONG', 'SHORT')),
    horizon_minutes INTEGER NOT NULL CHECK (horizon_minutes IN (60, 240, 720, 1440)),
    conditions JSONB NOT NULL,
    condition_count INTEGER NOT NULL CHECK (condition_count BETWEEN 1 AND 10),
    formula_text TEXT NOT NULL,
    current_stage TEXT NOT NULL CHECK (
        current_stage IN ('DISCOVERED', 'BACKTESTED', 'HOLDOUT_PASSED', 'SHADOW', 'APPROVED', 'LIVE', 'RETIRED')
    ),
    first_seen_run_id BIGINT REFERENCES research_formula_runs(run_id),
    latest_evaluation_run_id BIGINT REFERENCES research_formula_runs(run_id),
    shadow_started_at_utc TIMESTAMPTZ,
    last_shadow_event_id BIGINT NOT NULL DEFAULT 0,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    live_alert_approved BOOLEAN NOT NULL DEFAULT FALSE,
    live_alert_approved_at_utc TIMESTAMPTZ,
    live_alert_approved_by TEXT,
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (
        (live_alert_approved = FALSE AND live_alert_approved_at_utc IS NULL AND live_alert_approved_by IS NULL)
        OR
        (live_alert_approved = TRUE AND live_alert_approved_at_utc IS NOT NULL AND live_alert_approved_by IS NOT NULL)
    ),
    CHECK (current_stage NOT IN ('APPROVED', 'LIVE') OR live_alert_approved = TRUE)
);

CREATE INDEX IF NOT EXISTS idx_research_formulas_stage_rank
    ON research_formulas (current_stage, active, horizon_minutes, direction);

CREATE TABLE IF NOT EXISTS research_formula_evaluations (
    evaluation_id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES research_formula_runs(run_id) ON DELETE CASCADE,
    formula_id BIGINT NOT NULL REFERENCES research_formulas(formula_id) ON DELETE CASCADE,
    rank_in_run INTEGER NOT NULL CHECK (rank_in_run > 0),
    ranking_score DOUBLE PRECISION NOT NULL,
    discovery_metrics JSONB NOT NULL,
    holdout_metrics JSONB NOT NULL,
    multiple_testing JSONB NOT NULL,
    recommended_stage TEXT NOT NULL,
    gate_notes JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, formula_id)
);

CREATE INDEX IF NOT EXISTS idx_research_formula_evaluations_formula_time
    ON research_formula_evaluations (formula_id, created_at_utc DESC);

CREATE TABLE IF NOT EXISTS research_formula_stage_history (
    stage_change_id BIGSERIAL PRIMARY KEY,
    formula_id BIGINT NOT NULL REFERENCES research_formulas(formula_id) ON DELETE CASCADE,
    run_id BIGINT REFERENCES research_formula_runs(run_id) ON DELETE SET NULL,
    from_stage TEXT,
    to_stage TEXT NOT NULL,
    reason TEXT NOT NULL,
    actor TEXT NOT NULL,
    changed_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (formula_id, run_id, to_stage)
);

CREATE TABLE IF NOT EXISTS research_formula_shadow_checks (
    formula_id BIGINT NOT NULL REFERENCES research_formulas(formula_id) ON DELETE CASCADE,
    event_id BIGINT NOT NULL REFERENCES research_events(event_id) ON DELETE CASCADE,
    matched BOOLEAN NOT NULL,
    feature_schema_version TEXT NOT NULL,
    evaluated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (formula_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_research_formula_shadow_checks_event
    ON research_formula_shadow_checks (event_id, evaluated_at_utc DESC);

CREATE TABLE IF NOT EXISTS research_formula_shadow_hits (
    shadow_hit_id BIGSERIAL PRIMARY KEY,
    formula_id BIGINT NOT NULL REFERENCES research_formulas(formula_id) ON DELETE CASCADE,
    event_id BIGINT NOT NULL REFERENCES research_events(event_id) ON DELETE CASCADE,
    matched_at_utc TIMESTAMPTZ NOT NULL,
    input_snapshot JSONB NOT NULL,
    delivery_status TEXT NOT NULL DEFAULT 'NOT_SENT' CHECK (delivery_status = 'NOT_SENT'),
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (formula_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_research_formula_shadow_hits_time
    ON research_formula_shadow_hits (matched_at_utc DESC);

-- Telegram exports are evidence of old messages, not reconstructed immutable
-- Research Events. They remain isolated so formula training cannot silently
-- mistake a partial import for a complete delivered-alert snapshot.
CREATE TABLE IF NOT EXISTS research_legacy_alert_messages (
    legacy_message_id BIGSERIAL PRIMARY KEY,
    source_fingerprint CHAR(64) NOT NULL UNIQUE,
    source_kind TEXT NOT NULL DEFAULT 'TELEGRAM_DESKTOP_JSON',
    source_chat TEXT,
    source_message_id TEXT,
    message_time_utc TIMESTAMPTZ NOT NULL,
    message_text TEXT NOT NULL,
    parsed_symbol TEXT,
    parsed_direction TEXT CHECK (parsed_direction IN ('LONG', 'SHORT') OR parsed_direction IS NULL),
    parsed_event_type TEXT,
    parse_status TEXT NOT NULL CHECK (parse_status IN ('UNPARSED', 'PARTIAL', 'REVIEWED')),
    parsed_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    imported_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_research_legacy_alert_messages_time
    ON research_legacy_alert_messages (message_time_utc DESC);
