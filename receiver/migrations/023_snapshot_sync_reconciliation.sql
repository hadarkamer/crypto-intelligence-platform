-- Committed source coverage for recoverable snapshot/Telegram Sheet exports.
-- Payloads and confirmed delivery live in research_sheet_upsert_outbox (022).
CREATE TABLE IF NOT EXISTS research_snapshot_sheet_sources (
    source_event_id BIGINT NOT NULL REFERENCES research_events(event_id),
    reconcile_version TEXT NOT NULL,
    snapshot_id TEXT NOT NULL CHECK (BTRIM(snapshot_id) <> ''),
    source_status TEXT NOT NULL CHECK (source_status IN ('STAGED', 'REJECTED', 'DEFERRED')),
    rejection_reason TEXT,
    btc_parent_movement_id TEXT,
    staged_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    next_attempt_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_event_id, reconcile_version),
    CHECK (source_status <> 'REJECTED' OR rejection_reason IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS research_events_sheet_snapshot_group_v1
    ON research_events (
        (COALESCE(NULLIF(engine_snapshot->>'sheet_snapshot_id', ''), event_fingerprint)),
        event_id
    )
    WHERE event_kind='ALERT' AND delivery_status='DELIVERED';

CREATE INDEX IF NOT EXISTS research_events_sheet_source_candidates_v1
    ON research_events (event_id DESC)
    WHERE (event_kind='ALERT' AND delivery_status='DELIVERED')
       OR (event_kind='DECISION_SAMPLE' AND event_type='PROSPECTIVE_NEUTRAL_30M');

COMMENT ON TABLE research_snapshot_sheet_sources IS
    'Immutable source IDs staged transactionally to the shared Sheets outbox. A new child replays its complete Watch/symbol/side group; staged does not imply confirmed delivery.';
