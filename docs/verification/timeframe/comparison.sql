-- Read-only. Set a short statement timeout in the executing connection.
-- A zero-row result is valid if this selected-timeframe slice has no anchor.
SELECT evaluation_version,candidate_key,timeframe,symbol_scope,base_direction,analysis_direction,
    window_minutes,threshold_bps,distinct_waves,matched_waves,control_waves,shared_waves,
    blocked_arms,matched_success,matched_failure,control_success,control_failure,
    open_arms,missing_arms,ambiguous_arms,no_touch_arms,
    statistical_test_performed,qualifies_as_prospective_formula_evidence
FROM research_watch_scan_tf_formula_comparisons
WHERE evaluation_version='watch-scan-timeframe-formulas-v1'
    AND candidate_key='captured-question-search-v3-experimental-binding:LIQUIDITY_BALANCED'
    AND timeframe='12h' AND symbol_scope='BTC' AND base_direction='LONG'
    AND window_minutes=60 AND threshold_bps=25;
