-- Read-only. Expand at most the 8 coins of one completed scan: 3,808 decisions.
WITH latest AS MATERIALIZED (
    SELECT snapshot_set_id
    FROM research_watch_scan_tf_formula_samples
    WHERE evaluation_version='watch-scan-timeframe-formulas-v1'
        AND consumer_version='watch-all-scan-intake-v1' AND status='READY'
    GROUP BY snapshot_set_id HAVING count(*)=8
    ORDER BY max(usable_from_utc) DESC,snapshot_set_id DESC LIMIT 1
), source AS MATERIALIZED (
    SELECT f.symbol,f.snapshot_set_id,f.payload
    FROM research_watch_scan_tf_formula_samples f JOIN latest USING(snapshot_set_id)
    WHERE f.evaluation_version='watch-scan-timeframe-formulas-v1'
        AND f.consumer_version='watch-all-scan-intake-v1' AND f.status='READY'
), decisions AS MATERIALIZED (
    SELECT s.symbol,s.snapshot_set_id,e.*,c.orientation,c.definition->>'base_candidate_key' AS base_candidate_key
    FROM source s
    CROSS JOIN LATERAL jsonb_to_recordset(s.payload->'evaluations')
        e(candidate_key text,timeframe text,base_direction text,analysis_direction text,
          match_status text,missing_features jsonb,selection_status text)
    JOIN research_watch_scan_tf_formula_catalog c
        ON c.evaluation_version='watch-scan-timeframe-formulas-v1' AND c.candidate_key=e.candidate_key
), pairs AS (
    SELECT inverse.symbol,inverse.candidate_key,
        normal.candidate_key IS NULL AS missing_normal,
        (inverse.match_status,inverse.missing_features,inverse.selection_status)
            IS DISTINCT FROM (normal.match_status,normal.missing_features,normal.selection_status) AS mismatch
    FROM decisions inverse LEFT JOIN decisions normal
        ON normal.symbol=inverse.symbol AND normal.timeframe=inverse.timeframe
        AND normal.base_direction=inverse.base_direction AND normal.candidate_key=inverse.base_candidate_key
        AND normal.orientation='NORMAL'
    WHERE inverse.orientation='INVERSE'
)
SELECT (SELECT snapshot_set_id FROM latest) AS snapshot_set_id,
    count(*) AS decisions,count(DISTINCT (symbol,candidate_key,timeframe,base_direction)) AS unique_grid,
    count(*) FILTER(WHERE selection_status='SELECTED') AS selected,
    count(*) FILTER(WHERE selection_status='NOT_SELECTED') AS not_selected,
    count(*) FILTER(WHERE selection_status='UNKNOWN') AS unknown_selection,
    count(*) FILTER(WHERE match_status='MATCH') AS matches,
    count(*) FILTER(WHERE match_status='NO_MATCH') AS controls,
    count(*) FILTER(WHERE match_status='UNKNOWN') AS unknown,
    count(*) FILTER(WHERE match_status='NOT_APPLICABLE') AS not_applicable,
    count(*) FILTER(WHERE (
        (selection_status='SELECTED' AND match_status IN ('MATCH','NO_MATCH','UNKNOWN'))
        OR (selection_status='NOT_SELECTED' AND match_status='NOT_APPLICABLE')
        OR (selection_status='UNKNOWN' AND match_status='UNKNOWN')) IS NOT TRUE) AS invalid_selection_status,
    count(*) FILTER(WHERE analysis_direction IS DISTINCT FROM
        CASE WHEN orientation='INVERSE' THEN CASE base_direction WHEN 'LONG' THEN 'SHORT' ELSE 'LONG' END
             ELSE base_direction END) AS invalid_orientation,
    (SELECT count(*) FROM pairs) AS inverse_pairs,
    (SELECT count(*) FROM pairs WHERE missing_normal) AS missing_normal_pairs,
    (SELECT count(*) FROM pairs WHERE mismatch) AS inverse_predicate_mismatches
FROM decisions;
