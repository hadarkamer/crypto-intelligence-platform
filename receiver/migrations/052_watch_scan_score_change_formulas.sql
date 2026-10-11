-- Extend existing coverage for the explicitly versioned all-scan score-change
-- population. Existing evidence and the active pointer remain untouched.
ALTER TABLE research_watch_scan_formula_samples
    DROP CONSTRAINT IF EXISTS watch_formula_versioned_payload_coverage;
ALTER TABLE research_watch_scan_formula_samples ADD CONSTRAINT watch_formula_versioned_payload_coverage
CHECK(status<>'READY' OR ((payload->>'version'=evaluation_version
    AND payload->>'consumer_version'=consumer_version
    AND (payload->>'snapshot_set_id')::bigint=snapshot_set_id
    AND payload->>'symbol'=symbol
    AND CASE evaluation_version
        WHEN 'watch-scan-formulas-v1' THEN jsonb_array_length(payload->'evaluations')=68
        WHEN 'watch-scan-formulas-v2-maxpain' THEN jsonb_array_length(payload->'evaluations')=164
        WHEN 'watch-scan-formulas-v3-btc-context' THEN jsonb_array_length(payload->'evaluations')=212
        WHEN 'watch-scan-formulas-v4-asset-context' THEN jsonb_array_length(payload->'evaluations')=316
        WHEN 'watch-scan-formulas-v5-score-change' THEN jsonb_array_length(payload->'evaluations')=340
        ELSE false END) IS TRUE));

-- Bounded predecessor selection uses immutable source availability, not job order.
CREATE INDEX IF NOT EXISTS idx_watch_scan_accepted_usable
    ON research_watch_scan_intakes(consumer_version,usable_from_utc DESC)
    WHERE intake_status='ACCEPTED';
