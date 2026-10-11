-- Extend frozen feature payload coverage for the BTC-context adapter only.
-- Existing samples, catalogs, measurements, views and active pointer remain.
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
        ELSE false END) IS TRUE));
