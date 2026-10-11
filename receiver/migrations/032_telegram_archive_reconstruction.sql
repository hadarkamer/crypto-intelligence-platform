-- Versioned archive-only delayed-entry research. No FK into LIVE event tables;
-- no archive field is promoted into a native snapshot or default formula.
CREATE TABLE IF NOT EXISTS research_archive_reconstruction_runs (
    run_key TEXT PRIMARY KEY,
    contract JSONB NOT NULL,
    source_scope TEXT NOT NULL DEFAULT 'ARCHIVE_ONLY' CHECK(source_scope='ARCHIVE_ONLY'),
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS research_archive_reconstructed_events (
    run_key TEXT NOT NULL REFERENCES research_archive_reconstruction_runs(run_key),
    event_key TEXT NOT NULL,
    source_time_utc TIMESTAMPTZ,
    symbol TEXT,
    reconstruction_status TEXT NOT NULL,
    calculation_status TEXT NOT NULL,
    event_payload JSONB NOT NULL,
    PRIMARY KEY(run_key,event_key)
);
CREATE TABLE IF NOT EXISTS research_archive_delayed_entry_outcomes (
    run_key TEXT NOT NULL,
    event_key TEXT NOT NULL,
    signal_variant TEXT NOT NULL CHECK(signal_variant IN ('NORMAL','INVERSE')),
    window_minutes INTEGER NOT NULL CHECK(window_minutes IN(60,240,720,1440)),
    threshold_bps INTEGER NOT NULL CHECK(threshold_bps IN(25,50,75,100,125,150,175,200)),
    outcome_id TEXT NOT NULL,
    status TEXT NOT NULL,
    outcome_payload JSONB NOT NULL,
    PRIMARY KEY(run_key,event_key,signal_variant,window_minutes,threshold_bps),
    FOREIGN KEY(run_key,event_key) REFERENCES research_archive_reconstructed_events(run_key,event_key)
);
CREATE TABLE IF NOT EXISTS research_archive_common_window_metrics (
    run_key TEXT NOT NULL,
    event_key TEXT NOT NULL,
    signal_variant TEXT NOT NULL CHECK(signal_variant IN ('NORMAL','INVERSE')),
    window_minutes INTEGER NOT NULL CHECK(window_minutes IN(60,240,720,1440)),
    status TEXT NOT NULL,
    metrics_payload JSONB NOT NULL,
    PRIMARY KEY(run_key,event_key,signal_variant,window_minutes),
    FOREIGN KEY(run_key,event_key) REFERENCES research_archive_reconstructed_events(run_key,event_key)
);
CREATE TABLE IF NOT EXISTS research_archive_btc_parents (
    run_key TEXT NOT NULL REFERENCES research_archive_reconstruction_runs(run_key),
    btc_parent_movement_id TEXT NOT NULL,
    parent_payload JSONB NOT NULL,
    PRIMARY KEY(run_key,btc_parent_movement_id)
);
CREATE INDEX IF NOT EXISTS idx_archive_reconstruction_pending
    ON research_archive_reconstructed_events(run_key,calculation_status,source_time_utc,event_key);
