-- Separate observation population; no LIVE events, labels or Sheet publication.
CREATE INDEX IF NOT EXISTS idx_research_watch_captured_sets
    ON research_max_pain_snapshot_sets(snapshot_set_id)
    WHERE source_metadata#>'{capture_metadata,operational_scores}' IS NOT NULL;

CREATE TABLE IF NOT EXISTS research_watch_scan_intake_state (
    consumer_version TEXT PRIMARY KEY,
    activated_at_utc TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    scan_cursor BIGINT NOT NULL DEFAULT 0 CHECK (scan_cursor >= 0),
    scan_high_water BIGINT NOT NULL DEFAULT 0 CHECK (scan_high_water >= 0),
    completed_laps BIGINT NOT NULL DEFAULT 0 CHECK (completed_laps >= 0),
    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS research_watch_scan_intakes (
    consumer_version TEXT NOT NULL REFERENCES research_watch_scan_intake_state(consumer_version),
    snapshot_set_id BIGINT NOT NULL REFERENCES research_max_pain_snapshot_sets(snapshot_set_id),
    snapshot_key CHAR(64) NOT NULL,
    parent_payload_sha256 CHAR(64) NOT NULL,
    watch_scan_id TEXT NOT NULL,
    source_version TEXT,
    bundle_sha256 TEXT,
    capture_status TEXT,
    intake_status TEXT NOT NULL CHECK (intake_status IN ('ACCEPTED','REJECTED')),
    rejection_reason TEXT,
    observed_at_utc TIMESTAMPTZ,
    source_available_at_utc TIMESTAMPTZ NOT NULL,
    source_created_at_utc TIMESTAMPTZ NOT NULL,
    usable_from_utc TIMESTAMPTZ,
    capture_phase TEXT NOT NULL CHECK (capture_phase IN ('HISTORICAL_CAPTURE','FORWARD_CAPTURE')),
    coin_count INTEGER NOT NULL DEFAULT 0,
    score_slot_count INTEGER NOT NULL DEFAULT 0,
    scored_slots INTEGER NOT NULL DEFAULT 0,
    below_65_slots INTEGER NOT NULL DEFAULT 0,
    unavailable_models INTEGER NOT NULL DEFAULT 0,
    source_time_error_count INTEGER NOT NULL DEFAULT 0,
    ingested_at_utc TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (consumer_version,snapshot_set_id),
    CHECK ((intake_status='ACCEPTED') = (rejection_reason IS NULL)),
    CHECK (intake_status <> 'ACCEPTED' OR (
        coin_count=8 AND score_slot_count=112 AND observed_at_utc IS NOT NULL
        AND usable_from_utc >= source_available_at_utc
        AND usable_from_utc >= source_created_at_utc
        AND usable_from_utc >= observed_at_utc AND usable_from_utc <= ingested_at_utc
    ))
);

-- The original JSON is append-only. Views expose it without copying each
-- 200-KiB bundle or repeating the model evidence in 14 stored slot records.
CREATE OR REPLACE VIEW research_watch_scan_observations AS
SELECT i.*, 'watch-all-scan-observations-v1'::text AS population_version,
       c.key AS symbol, c.value->>'status' AS coin_capture_status,
       c.value->'maxpain' AS maxpain_slots, c.value->'models' AS models,
       c.value->'sources' AS sources, c.value->'source_time_errors' AS source_time_errors,
       s.source_metadata#>'{capture_metadata,operational_scores,code_sha256}' AS code_sha256,
       s.source_metadata#>>'{capture_metadata,operational_scores,input_universe_sha256}' AS input_universe_sha256,
       'NOT_EVALUATED'::text AS outcome_status,
       FALSE AS is_delivered_alert, FALSE AS qualifies_as_prospective_formula_evidence
FROM research_watch_scan_intakes i
JOIN research_max_pain_snapshot_sets s USING (snapshot_set_id)
CROSS JOIN LATERAL jsonb_each(
    CASE WHEN jsonb_typeof(s.source_metadata#>'{capture_metadata,operational_scores,coins}')='object'
         THEN s.source_metadata#>'{capture_metadata,operational_scores,coins}' ELSE '{}'::jsonb END) c
WHERE i.intake_status='ACCEPTED'
  AND i.bundle_sha256=s.source_metadata#>>'{capture_metadata,operational_scores,payload_sha256}'
  AND i.parent_payload_sha256=s.payload_sha256;

CREATE OR REPLACE VIEW research_watch_scan_score_slots AS
SELECT o.consumer_version,o.population_version,o.snapshot_set_id,o.snapshot_key,
       o.parent_payload_sha256,o.bundle_sha256,o.watch_scan_id,o.symbol,
       o.observed_at_utc,o.source_available_at_utc,o.source_created_at_utc,
       o.usable_from_utc,o.ingested_at_utc,o.capture_phase,
       slot->>'timeframe' AS timeframe,slot->>'source_side' AS source_side,
       CASE slot->>'source_side' WHEN 'LONG' THEN 'SHORT' ELSE 'LONG' END AS analysis_direction,
       slot->>'status' AS slot_status,
       CASE WHEN slot->>'status'='SCORED' THEN (slot->>'score')::double precision END AS score,
       slot AS maxpain_features,o.models,o.sources,o.source_time_errors,
       o.code_sha256,o.input_universe_sha256,o.outcome_status,
       o.is_delivered_alert,o.qualifies_as_prospective_formula_evidence
FROM research_watch_scan_observations o
CROSS JOIN LATERAL jsonb_array_elements(
    CASE WHEN jsonb_typeof(o.maxpain_slots)='array' THEN o.maxpain_slots ELSE '[]'::jsonb END) slot;
