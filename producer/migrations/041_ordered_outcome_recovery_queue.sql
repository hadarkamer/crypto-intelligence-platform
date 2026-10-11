-- Operational priority for captured Max Pain alerts; no research labels change.
CREATE TABLE IF NOT EXISTS research_ordered_outcome_recovery (
    event_id BIGINT PRIMARY KEY REFERENCES research_events(event_id),
    request_key TEXT NOT NULL,
    priority SMALLINT NOT NULL DEFAULT 0 CHECK(priority BETWEEN 0 AND 2),
    requested_through_utc TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING'
        CHECK(status IN ('PENDING','OPEN','RETRY','COMPLETE','BLOCKED','DERIVED_OPEN','SUPPLEMENTED')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts>=0),
    next_attempt_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    refreshed_through_utc TIMESTAMPTZ,
    last_error TEXT,
    derivation_reference TEXT,
    sheet_pending BOOLEAN NOT NULL DEFAULT FALSE,
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_ordered_outcome_recovery_due
    ON research_ordered_outcome_recovery(priority DESC,next_attempt_at_utc,event_id)
    WHERE status IN ('PENDING','OPEN','RETRY','DERIVED_OPEN');
CREATE INDEX IF NOT EXISTS idx_ordered_outcome_recovery_sheet
    ON research_ordered_outcome_recovery(priority DESC,updated_at_utc,event_id)
    WHERE sheet_pending;
CREATE TABLE IF NOT EXISTS research_ordered_outcome_recovery_state (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK(singleton),
    last_source_event_id BIGINT NOT NULL DEFAULT 0 CHECK(last_source_event_id>=0),
    high_water_event_id BIGINT NOT NULL DEFAULT 0 CHECK(high_water_event_id>=last_source_event_id),
    next_lane INTEGER NOT NULL DEFAULT 0 CHECK(next_lane IN (0,1)),
    next_delivery_lane INTEGER NOT NULL DEFAULT 0 CHECK(next_delivery_lane IN (0,1))
);
INSERT INTO research_ordered_outcome_recovery_state(singleton) VALUES(TRUE)
    ON CONFLICT DO NOTHING;
CREATE INDEX IF NOT EXISTS idx_research_events_maxpain65_recovery
    ON research_events(event_id)
    WHERE event_kind='ALERT' AND delivery_status='DELIVERED'
      AND event_type='MAX_PAIN_ALERT' AND score>=65
      AND direction IN ('LONG','SHORT');
COMMENT ON TABLE research_ordered_outcome_recovery IS
    'Canonical v7 and four fixed-window metrics, using unchanged source provenance. OPEN means current prefix verified but fixed horizons still mature. COMPLETE means all four common windows READY. BLOCKED retains provenance failure. No full-BTC-wave label is implied.';
