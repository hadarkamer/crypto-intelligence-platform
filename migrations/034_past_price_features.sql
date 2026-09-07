-- Causal historical features are additive; no alert or outcome is rewritten.
CREATE TABLE IF NOT EXISTS research_past_price_features (
    event_id BIGINT NOT NULL REFERENCES research_events(event_id),
    method_version TEXT NOT NULL,
    event_time_utc TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('PENDING','PARTIAL','DATA_MISSING','READY')),
    next_attempt_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    feature_sha256 TEXT,
    result JSONB NOT NULL DEFAULT '{}'::jsonb CHECK(jsonb_typeof(result)='object'),
    search_refresh_pending BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(event_id,method_version)
);
CREATE INDEX IF NOT EXISTS idx_past_price_features_due
    ON research_past_price_features(next_attempt_at_utc,event_id)
    WHERE status IN ('PENDING','PARTIAL','DATA_MISSING');
CREATE INDEX IF NOT EXISTS idx_past_price_features_search_refresh
    ON research_past_price_features(updated_at_utc,event_id)
    WHERE search_refresh_pending=TRUE;
CREATE INDEX IF NOT EXISTS idx_past_price_features_fresh
    ON research_past_price_features(event_time_utc DESC,next_attempt_at_utc,event_id)
    WHERE status IN ('PENDING','PARTIAL','DATA_MISSING');
CREATE TABLE IF NOT EXISTS research_past_price_features_cursor (
    method_version TEXT PRIMARY KEY,last_event_id BIGINT NOT NULL DEFAULT 0,
    next_lane INTEGER NOT NULL DEFAULT 0 CHECK(next_lane IN(0,1))
);
COMMENT ON TABLE research_past_price_features IS
    'Versioned pre-entry closed Spot 1m lookbacks, explicit per-window coverage and provenance. Exact return sign is descriptive, not a regime. Late enrichment signals a bounded source search refresh; no future labels classify past movement.';
