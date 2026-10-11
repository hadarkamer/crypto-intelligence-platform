-- Additive operational index only; no outcomes, rules or thresholds change.
-- The v7 due-event loader uses this same literal partial predicate and reads
-- at most batch_limit * 32 eligible rows before grouping into distinct events.
-- Without the index even an empty DATA_MISSING lane scans the entire large
-- outcome heap on every poll.
CREATE INDEX IF NOT EXISTS idx_ordered_first_touch_missing_queue
    ON research_ordered_first_touch_outcomes (
        updated_at_utc, event_id, window_minutes, threshold_bps
    )
    WHERE method_version = 'ordered-first-touch-v7'
      AND status = 'DATA_MISSING';
