-- Finite durable laps prevent a growing live tail from starving old rows.
-- Advancement commits with source selection; unfinished rows remain eligible
-- on the next lap even if later price retrieval fails.
CREATE TABLE IF NOT EXISTS research_event_scan_cursors (
    queue_key TEXT PRIMARY KEY,
    last_event_id BIGINT NOT NULL DEFAULT 0 CHECK (last_event_id >= 0),
    high_water_event_id BIGINT NOT NULL DEFAULT 0 CHECK (high_water_event_id >= 0),
    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (last_event_id <= high_water_event_id)
);

-- Reconciliation starts from a bounded source page rather than sorting all
-- candidate matches. The existing candidate-first primary key is retained.
CREATE INDEX IF NOT EXISTS idx_ordered_formula_matches_event_candidate
    ON research_ordered_formula_matches(event_id, candidate_key);
