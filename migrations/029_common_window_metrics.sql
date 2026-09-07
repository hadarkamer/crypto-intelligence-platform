-- Additive full-horizon excursions. First Touch v7 labels remain immutable.
CREATE TABLE IF NOT EXISTS research_common_window_metrics (
    event_id BIGINT NOT NULL REFERENCES research_events(event_id),
    window_minutes INTEGER NOT NULL CHECK(window_minutes IN (60,240,720,1440)),
    method_version TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('PENDING','OPEN','DATA_MISSING','READY')),
    measurement_start_utc TIMESTAMPTZ NOT NULL,
    window_end_utc TIMESTAMPTZ NOT NULL,
    next_attempt_at_utc TIMESTAMPTZ NOT NULL,
    result JSONB NOT NULL CHECK(jsonb_typeof(result)='object'),
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(event_id,window_minutes,method_version),
    CHECK(window_end_utc=measurement_start_utc+window_minutes*INTERVAL '1 minute')
);
CREATE INDEX IF NOT EXISTS idx_common_window_metrics_due
    ON research_common_window_metrics(next_attempt_at_utc,event_id,window_minutes)
    WHERE status IN ('PENDING','OPEN','DATA_MISSING');
CREATE TABLE IF NOT EXISTS research_common_window_metrics_cursor (
    method_version TEXT PRIMARY KEY,
    last_event_id BIGINT NOT NULL DEFAULT 0
);
COMMENT ON TABLE research_common_window_metrics IS
    'Fixed horizon closed Spot 1m extrema for every outcome status. Never stop-at-FirstTouch. READY requires complete eligible 1m path; partial boundary minutes disclosed. Threshold-independent path joins each exact threshold separately.';
