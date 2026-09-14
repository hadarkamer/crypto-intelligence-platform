-- Read-only. No JSON evaluation expansion or outcome/comparison view scan.
WITH accepted AS MATERIALIZED (
    SELECT consumer_version,snapshot_set_id,usable_from_utc,bundle_sha256,parent_payload_sha256
    FROM research_watch_scan_intakes
    WHERE consumer_version='watch-all-scan-intake-v1' AND intake_status='ACCEPTED'
), tf AS MATERIALIZED (
    SELECT f.* FROM research_watch_scan_tf_formula_samples f
    WHERE evaluation_version='watch-scan-timeframe-formulas-v1'
), coverage AS (
    SELECT count(*) AS definitions,count(*) FILTER(WHERE supported) AS supported,
        count(*) FILTER(WHERE support_dimension='COIN') AS coin,
        count(*) FILTER(WHERE support_dimension='SELECTED_TIMEFRAME') AS selected_timeframe,
        count(*) FILTER(WHERE support_dimension='UNSUPPORTED') AS unsupported,
        count(*) FILTER(WHERE NOT definition_consistent) AS definition_drift
    FROM research_watch_scan_formula_coverage
), samples AS (
    SELECT count(*) AS total,count(*) FILTER(WHERE f.status='READY') AS ready,
        count(*) FILTER(WHERE f.status='WAITING') AS waiting,
        count(*) FILTER(WHERE f.status='ERROR') AS errors,
        count(*) FILTER(WHERE f.next_attempt_at_utc<=clock_timestamp()) AS due,
        count(*) FILTER(WHERE f.status='READY' AND
            jsonb_array_length(f.payload->'evaluations') IS DISTINCT FROM 476) AS bad_ready_grid,
        count(*) FILTER(WHERE f.status='READY' AND (
            i.snapshot_set_id IS NULL OR f.usable_from_utc IS DISTINCT FROM i.usable_from_utc
            OR f.payload->>'bundle_sha256' IS DISTINCT FROM i.bundle_sha256
            OR f.payload->>'parent_payload_sha256' IS DISTINCT FROM i.parent_payload_sha256
            OR f.payload->>'version' IS DISTINCT FROM f.evaluation_version
            OR f.payload->>'consumer_version' IS DISTINCT FROM f.consumer_version
            OR (f.payload->>'snapshot_set_id')::bigint IS DISTINCT FROM f.snapshot_set_id
            OR f.payload->>'symbol' IS DISTINCT FROM f.symbol
            OR (f.payload->>'usable_from_utc')::timestamptz IS DISTINCT FROM f.usable_from_utc
        )) AS bad_ready_lineage
    FROM tf f LEFT JOIN accepted i USING(consumer_version,snapshot_set_id)
), missing AS (
    SELECT count(*) AS missing_or_not_ready
    FROM accepted i
    CROSS JOIN (VALUES ('BTC'),('ETH'),('SOL'),('BNB'),('XRP'),('DOGE'),('ZEC'),('HYPE')) coin(symbol)
    LEFT JOIN tf f ON f.consumer_version=i.consumer_version AND f.snapshot_set_id=i.snapshot_set_id
        AND f.symbol=coin.symbol
    WHERE f.status IS DISTINCT FROM 'READY'
), old AS (
    SELECT f.evaluation_version,count(*) AS samples,count(*) FILTER(WHERE f.status='READY') AS ready,
        count(*) FILTER(WHERE f.status='READY' AND
            jsonb_array_length(f.payload->'evaluations') IS DISTINCT FROM 340) AS bad_v5_grid
    FROM research_watch_scan_formula_samples f
    JOIN research_watch_scan_formula_runtime r ON r.singleton AND r.active_evaluation_version=f.evaluation_version
    GROUP BY f.evaluation_version
)
SELECT jsonb_build_object(
    'accepted_scans',(SELECT count(*) FROM accepted),
    'expected_tf_coin_samples',(SELECT count(*)*8 FROM accepted),
    'latest_source_usable_utc',(SELECT max(usable_from_utc) FROM accepted),
    'tf_runtime',(SELECT to_jsonb(r) FROM research_watch_scan_tf_formula_runtime r WHERE singleton),
    'tf_state',(SELECT to_jsonb(s) FROM research_watch_scan_tf_formula_state s
        WHERE evaluation_version='watch-scan-timeframe-formulas-v1'),
    'tf_catalog',(SELECT jsonb_build_object('definitions',count(*),'supported',count(*) FILTER(WHERE supported),
        'normal',count(*) FILTER(WHERE orientation='NORMAL'),'inverse',count(*) FILTER(WHERE orientation='INVERSE'))
        FROM research_watch_scan_tf_formula_catalog WHERE evaluation_version='watch-scan-timeframe-formulas-v1'),
    'tf_samples',(SELECT to_jsonb(s) FROM samples s),
    'missing_or_not_ready',(SELECT missing_or_not_ready FROM missing),
    'coverage',(SELECT to_jsonb(c) FROM coverage c),
    'active_coin_samples',(SELECT to_jsonb(o) FROM old o)
) AS verification;
