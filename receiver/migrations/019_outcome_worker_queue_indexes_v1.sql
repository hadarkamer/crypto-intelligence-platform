-- Outcome worker queue indexes v1
--
-- Keep the minute-level open First Touch queue bounded and index-driven.  No
-- research rows are rewritten; these indexes only accelerate eligibility and
-- pending-state lookups used by research_outcome_worker.

CREATE INDEX IF NOT EXISTS idx_research_events_delivered_alert_due
    ON research_events (alert_time_utc ASC, event_id ASC)
    WHERE event_kind = 'ALERT' AND delivery_status = 'DELIVERED';

CREATE INDEX IF NOT EXISTS idx_research_events_decision_sample_due
    ON research_events (alert_time_utc ASC, event_id ASC)
    WHERE event_kind = 'DECISION_SAMPLE'
      AND delivery_status = 'NOT_APPLICABLE';

CREATE INDEX IF NOT EXISTS idx_shadow_checks_open_first_touch_queue
    ON research_formula_shadow_checks (
        event_id, formula_id, prospective_anchor_slot_id
    )
    WHERE matched = TRUE
      AND evaluation_status = 'MATCHED'
      AND authoritative_verified = TRUE
      AND prospective_anchor_slot_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_formulas_active_shadow_horizon
    ON research_formulas (formula_id, horizon_minutes)
    WHERE active = TRUE AND current_stage = 'SHADOW';

CREATE INDEX IF NOT EXISTS idx_first_touch_pending_event_horizon
    ON research_first_touch_outcomes (
        event_id, horizon_minutes, observed_through_utc DESC
    )
    WHERE method_version = 'no-dwell-first-touch-v6'
      AND status = 'PENDING';

CREATE INDEX IF NOT EXISTS idx_first_touch_terminal_event_horizon
    ON research_first_touch_outcomes (event_id, horizon_minutes)
    WHERE method_version = 'no-dwell-first-touch-v6'
      AND status IN ('HIT', 'MISS');

COMMENT ON INDEX idx_shadow_checks_open_first_touch_queue IS
    'Supports the reserved minute-level First Touch queue without scanning unmatched Shadow checks.';
