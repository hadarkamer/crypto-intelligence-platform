-- Scheduling state only; formula definitions and research gates stay intact.
CREATE TABLE IF NOT EXISTS research_ordered_scope_schedule_state (
    scheduler_key TEXT PRIMARY KEY,
    next_ticket BIGINT NOT NULL DEFAULT 0 CHECK (next_ticket >= 0),
    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
