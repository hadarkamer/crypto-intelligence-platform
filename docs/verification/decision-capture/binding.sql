SELECT
    CASE WHEN snapshot_set_id<=1306 THEN 'AT_OR_BEFORE_1306' ELSE 'AFTER_1306' END AS source_range,
    capture_version,capture_status,binding_status,
    count(DISTINCT snapshot_set_id) AS scans,count(*) AS coin_rows,
    count(*) FILTER(WHERE consumer_validation_required IS NOT TRUE
        OR is_delivered_alert IS NOT FALSE
        OR qualifies_as_prospective_formula_evidence IS NOT FALSE) AS invalid_evidence_flags
FROM research_watch_scan_decision_captures
GROUP BY 1,2,3,4 ORDER BY 1,2,3,4;
