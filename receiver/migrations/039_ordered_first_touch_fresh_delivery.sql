-- Operational delivery fairness only; outcome labels, barriers and source rows
-- stay immutable. The expression reads frozen evidence, never retry/ACK time.
-- Reuses migration026's timezone-qualified, immutable safe timestamp parser.
CREATE TABLE IF NOT EXISTS research_ordered_first_touch_delivery_cursor (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK(singleton),
    next_slot BIGINT NOT NULL DEFAULT 0 CHECK(next_slot>=0)
);
INSERT INTO research_ordered_first_touch_delivery_cursor(singleton)
    VALUES (TRUE) ON CONFLICT DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_ordered_first_touch_sync_fresh_observed
    ON research_ordered_first_touch_sync_outbox (
        destination,
        research_sheet_source_timestamp(payload->'row'->>'observed_through_utc') DESC,
        research_sheet_source_timestamp(payload->'row'->>'measurement_start_utc') DESC,
        next_attempt_at_utc ASC, created_at_utc ASC,
        event_id, window_minutes, threshold_bps
    )
    WHERE method_version='ordered-first-touch-v7'
      AND sync_status IN ('PENDING','RETRY','IN_FLIGHT')
      AND research_sheet_source_timestamp(payload->'row'->>'observed_through_utc') IS NOT NULL;
