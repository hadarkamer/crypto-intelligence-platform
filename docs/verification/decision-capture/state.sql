SELECT jsonb_build_object(
 'checked_at_utc',clock_timestamp(),
 'accepted_scans',(SELECT count(*) FROM research_watch_scan_intakes WHERE consumer_version='watch-all-scan-intake-v1' AND intake_status='ACCEPTED'),
 'coverage',(SELECT jsonb_build_object('definitions',count(*),'supported',count(*) FILTER(WHERE supported),'definition_drift',count(*) FILTER(WHERE NOT definition_consistent)) FROM research_watch_scan_formula_coverage),
 'coin_runtime',(SELECT active_evaluation_version FROM research_watch_scan_formula_runtime WHERE singleton),
 'timeframe_runtime',(SELECT active_evaluation_version FROM research_watch_scan_tf_formula_runtime WHERE singleton),
 'coin_samples',(SELECT jsonb_build_object('ready',count(*) FILTER(WHERE status='READY'),'errors',count(*) FILTER(WHERE status='ERROR')) FROM research_watch_scan_formula_samples WHERE evaluation_version='watch-scan-formulas-v5-score-change'),
 'timeframe_samples',(SELECT jsonb_build_object('ready',count(*) FILTER(WHERE status='READY'),'errors',count(*) FILTER(WHERE status='ERROR')) FROM research_watch_scan_tf_formula_samples WHERE evaluation_version='watch-scan-timeframe-formulas-v1'),
 'independent_live_btc_waves',(SELECT count(DISTINCT payload#>>'{membership,btc_parent_movement_id}') FROM research_watch_scan_measurements WHERE measurement_version='watch-scan-measurements-v1' AND payload#>>'{membership,membership_status}'='LIVE'),
 'dual_receipts',(SELECT count(*) FROM dual_cvd65_receipts),
 'last_dual_source',(SELECT max(source_at_utc) FROM dual_cvd65_receipts),
 'last_dual_result',(SELECT result FROM dual_cvd65_receipts ORDER BY source_at_utc DESC LIMIT 1),
 'dual_intents',(SELECT count(*) FROM dual_cvd65_intents)
) AS verification;
