-- Additive recurring research over ordered v7; never changes legacy Formula lifecycle.
CREATE TABLE IF NOT EXISTS research_ordered_formula_candidates (
    candidate_key TEXT PRIMARY KEY,
    formula_version TEXT NOT NULL,
    definition_sha256 TEXT NOT NULL,
    definition JSONB NOT NULL CHECK (jsonb_typeof(definition) = 'object'),
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS research_ordered_formula_matches (
    candidate_key TEXT NOT NULL REFERENCES research_ordered_formula_candidates(candidate_key),
    event_id BIGINT NOT NULL REFERENCES research_events(event_id),
    symbol TEXT NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('LONG','SHORT')),
    alert_time_utc TIMESTAMPTZ NOT NULL,
    snapshot_id TEXT NOT NULL,
    entry_price DOUBLE PRECISION,
    decision_features JSONB NOT NULL CHECK (jsonb_typeof(decision_features) = 'object'),
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(candidate_key,event_id)
);
CREATE INDEX IF NOT EXISTS idx_ordered_formula_matches_scope ON research_ordered_formula_matches(candidate_key,direction,symbol,alert_time_utc,event_id);
CREATE TABLE IF NOT EXISTS research_ordered_formula_scopes (
    scope_key TEXT PRIMARY KEY,
    candidate_key TEXT NOT NULL REFERENCES research_ordered_formula_candidates(candidate_key),
    symbol TEXT NOT NULL,
    direction TEXT NOT NULL CHECK(direction IN ('LONG','SHORT')),
    window_minutes INTEGER NOT NULL CHECK(window_minutes IN (60,240,720,1440)),
    threshold_bps INTEGER NOT NULL CHECK(threshold_bps IN (25,50,75,100,125,150,175,200)),
    last_evaluated_at_utc TIMESTAMPTZ,
    result JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE(candidate_key,symbol,direction,window_minutes,threshold_bps)
);
CREATE INDEX IF NOT EXISTS idx_ordered_formula_scope_schedule ON research_ordered_formula_scopes(last_evaluated_at_utc ASC NULLS FIRST,scope_key);
CREATE TABLE IF NOT EXISTS research_ordered_formula_episodes (
    episode_id TEXT PRIMARY KEY,
    scope_key TEXT NOT NULL REFERENCES research_ordered_formula_scopes(scope_key),
    btc_parent_movement_id TEXT NOT NULL,
    representative_event_ids JSONB NOT NULL CHECK(jsonb_typeof(representative_event_ids)='array'),
    evidence JSONB NOT NULL CHECK(jsonb_typeof(evidence)='object'),
    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(scope_key,btc_parent_movement_id)
);
CREATE TABLE IF NOT EXISTS research_ordered_formula_trials (
    trial_id TEXT PRIMARY KEY,
    scope_key TEXT NOT NULL REFERENCES research_ordered_formula_scopes(scope_key),
    evidence_sha256 TEXT NOT NULL,
    result JSONB NOT NULL CHECK(jsonb_typeof(result)='object'),
    evaluated_at_utc TIMESTAMPTZ NOT NULL,
    UNIQUE(scope_key,evidence_sha256)
);
CREATE TABLE IF NOT EXISTS research_ordered_formula_worker_state (
    worker_key TEXT PRIMARY KEY,
    last_event_id BIGINT NOT NULL DEFAULT 0,
    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS research_ordered_formula_event_checks (
    event_id BIGINT PRIMARY KEY REFERENCES research_events(event_id),
    checked_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
ALTER TABLE research_ordered_formula_worker_state
    ADD COLUMN IF NOT EXISTS initial_scan_complete BOOLEAN NOT NULL DEFAULT FALSE;
-- Shared destination outbox for atomic source+Sheet staging. A newer generation
-- invalidates the old claim; expired leases retry without losing committed data.
CREATE TABLE IF NOT EXISTS research_sheet_upsert_outbox (
    sheet_name TEXT NOT NULL,
    row_key TEXT NOT NULL,
    payload JSONB NOT NULL CHECK(jsonb_typeof(payload)='object'),
    payload_sha256 TEXT NOT NULL,
    sync_status TEXT NOT NULL DEFAULT 'PENDING' CHECK(sync_status IN ('PENDING','IN_FLIGHT','RETRY','SYNCED')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts>=0),
    next_attempt_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    claim_token UUID,
    claimed_payload_sha256 TEXT,
    lease_expires_at_utc TIMESTAMPTZ,
    synced_at_utc TIMESTAMPTZ,
    last_error TEXT,
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(sheet_name,row_key),
    CHECK ((sync_status='IN_FLIGHT') = (claim_token IS NOT NULL)),
    CHECK ((sync_status='SYNCED') = (synced_at_utc IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS idx_research_sheet_upsert_due ON research_sheet_upsert_outbox(next_attempt_at_utc,updated_at_utc) WHERE sync_status IN ('PENDING','RETRY');
CREATE INDEX IF NOT EXISTS idx_research_sheet_upsert_lease ON research_sheet_upsert_outbox(lease_expires_at_utc) WHERE sync_status='IN_FLIGHT';
COMMENT ON TABLE research_ordered_formula_trials IS 'Versioned descriptive v7 research trials. Count gates are not statistical approval or live trading permission.';
