-- Immutable operational evidence only. Existing scores, cohorts, outcomes and
-- reporting pointers are unchanged. A source-bound block is not a delivered
-- Combined event and must pass the Python canonical/semantic validator before
-- any future formula adapter consumes it.
CREATE OR REPLACE VIEW research_watch_scan_decision_captures AS
SELECT i.consumer_version,i.snapshot_set_id,i.snapshot_key,i.parent_payload_sha256,
    i.bundle_sha256,i.watch_scan_id,coin.symbol,i.observed_at_utc,
    i.source_available_at_utc,i.source_created_at_utc,i.usable_from_utc,
    i.capture_phase,'watch-all-scan-observations-v1'::text AS population_version,
    d.block->>'version' AS capture_version,
    d.block->>'population' AS capture_population_version,
    CASE WHEN d.block IS NULL OR d.block='null'::jsonb THEN 'NOT_CAPTURED'
         ELSE COALESCE(d.block->>'status','INVALID_CAPTURE') END AS capture_status,
    CASE
        WHEN d.block IS NULL OR d.block='null'::jsonb THEN 'NOT_CAPTURED'
        WHEN jsonb_typeof(d.block) IS DISTINCT FROM 'object' THEN 'INVALID_BINDING'
        WHEN d.block->>'version' IS DISTINCT FROM 'watch-operational-decisions-v1'
            THEN 'UNSUPPORTED_VERSION'
        WHEN d.block->>'cycle_id' IS DISTINCT FROM i.watch_scan_id THEN 'INVALID_BINDING'
        WHEN d.block->>'status'='FAILED' THEN 'CAPTURE_FAILED'
        WHEN d.block->>'hash_version' IS DISTINCT FROM 'json-integer-float-zero-normalized-v1'
            OR d.block->>'population' IS DISTINCT FROM 'watch-all-scan-operational-decisions-v1'
            THEN 'UNSUPPORTED_VERSION'
        WHEN d.block->>'status' NOT IN ('COMPLETE','PARTIAL')
            OR d.block->>'status' IS NULL
            OR d.block->>'source_score_sha256' IS DISTINCT FROM i.bundle_sha256
            OR d.block->>'input_universe_sha256' IS DISTINCT FROM d.scores->>'input_universe_sha256'
            OR (d.block->>'payload_sha256' ~ '^[0-9a-f]{64}$') IS NOT TRUE
            OR jsonb_typeof(d.block->'coins') IS DISTINCT FROM 'object'
            OR jsonb_typeof(d.block->'coins'->coin.symbol) IS DISTINCT FROM 'object'
            THEN 'INVALID_BINDING'
        ELSE 'SOURCE_BOUND'
    END AS binding_status,
    d.block->>'payload_sha256' AS decision_payload_sha256,
    -- Deliberately text: corrupt/unknown evidence cannot make the whole view
    -- throw through an unsafe timestamp cast. The pure validator checks time.
    d.block->>'computed_at_utc' AS decision_computed_at_utc,
    d.block AS decision_bundle,d.block->'coins'->coin.symbol AS coin_context,
    TRUE AS consumer_validation_required,
    FALSE AS is_delivered_alert,FALSE AS qualifies_as_prospective_formula_evidence
FROM research_watch_scan_intakes i
JOIN research_max_pain_snapshot_sets s USING(snapshot_set_id)
CROSS JOIN (VALUES ('BTC'),('ETH'),('SOL'),('HYPE'),('DOGE'),('ZEC'),('BNB'),('XRP')) coin(symbol)
CROSS JOIN LATERAL (SELECT
    s.source_metadata#>'{capture_metadata,operational_decisions}' AS block,
    s.source_metadata#>'{capture_metadata,operational_scores}' AS scores) d
WHERE i.consumer_version='watch-all-scan-intake-v1' AND i.intake_status='ACCEPTED'
    AND i.parent_payload_sha256=s.payload_sha256
    AND i.bundle_sha256=d.scores->>'payload_sha256';
