-- Separate descriptive research population. No alert/discovery/delivery writes.
CREATE TABLE IF NOT EXISTS research_watch_scan_formula_catalog (
    evaluation_version TEXT NOT NULL,
    candidate_key TEXT NOT NULL,
    feature_version TEXT NOT NULL,
    definition JSONB NOT NULL CHECK(jsonb_typeof(definition)='object'),
    definition_sha256 TEXT NOT NULL CHECK(definition_sha256 ~ '^[0-9a-f]{64}$'),
    orientation TEXT NOT NULL CHECK(orientation IN ('NORMAL','INVERSE')),
    supported BOOLEAN NOT NULL,
    unsupported_features JSONB NOT NULL CHECK(jsonb_typeof(unsupported_features)='array'),
    population_version TEXT NOT NULL DEFAULT 'watch-all-scan-observations-v1',
    measurement_version TEXT NOT NULL DEFAULT 'watch-scan-measurements-v1',
    entry_version TEXT NOT NULL DEFAULT 'watch-next-full-minute-route-open-v1',
    outcome_method_version TEXT NOT NULL DEFAULT 'ordered-first-touch-v7',
    parent_policy_version TEXT NOT NULL DEFAULT 'btc-parent-close-reversal-200bps-v1',
    selection_version TEXT NOT NULL DEFAULT 'watch-first-arm-cohort-unknown-blocks-v1',
    registered_at_utc TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(evaluation_version,candidate_key),
    CHECK(supported=(jsonb_array_length(unsupported_features)=0))
);
CREATE TABLE IF NOT EXISTS research_watch_scan_formula_state (
    evaluation_version TEXT PRIMARY KEY,
    scan_cursor BIGINT NOT NULL DEFAULT 0,
    scan_high_water BIGINT NOT NULL DEFAULT 0,
    completed_laps BIGINT NOT NULL DEFAULT 0,
    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS research_watch_scan_formula_samples (
    evaluation_version TEXT NOT NULL,
    consumer_version TEXT NOT NULL,
    snapshot_set_id BIGINT NOT NULL,
    symbol TEXT NOT NULL CHECK(symbol IN ('BTC','ETH','SOL','BNB','XRP','DOGE','ZEC','HYPE')),
    usable_from_utc TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL DEFAULT 'WAITING' CHECK(status IN ('WAITING','READY','ERROR')),
    payload JSONB NOT NULL DEFAULT '{}'::jsonb CHECK(jsonb_typeof(payload)='object'),
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at_utc TIMESTAMPTZ,
    last_error TEXT,
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(evaluation_version,consumer_version,snapshot_set_id,symbol),
    FOREIGN KEY(consumer_version,snapshot_set_id)
        REFERENCES research_watch_scan_intakes(consumer_version,snapshot_set_id),
    CHECK(status<>'READY' OR (payload->>'version'=evaluation_version
        AND payload->>'consumer_version'=consumer_version
        AND (payload->>'snapshot_set_id')::bigint=snapshot_set_id
        AND payload->>'symbol'=symbol AND jsonb_array_length(payload->'evaluations')=68))
);
CREATE INDEX IF NOT EXISTS idx_watch_scan_formula_due
    ON research_watch_scan_formula_samples(next_attempt_at_utc,snapshot_set_id,symbol)
    WHERE next_attempt_at_utc IS NOT NULL;

CREATE OR REPLACE FUNCTION research_freeze_watch_formula_v1()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_TABLE_NAME='research_watch_scan_formula_catalog' THEN
        IF NEW IS DISTINCT FROM OLD THEN
            RAISE EXCEPTION 'Watch formula definition and support contract cannot change';
        END IF;
    ELSIF (NEW.evaluation_version,NEW.consumer_version,NEW.snapshot_set_id,NEW.symbol,
        NEW.usable_from_utc) IS DISTINCT FROM
        (OLD.evaluation_version,OLD.consumer_version,OLD.snapshot_set_id,OLD.symbol,OLD.usable_from_utc)
        OR (OLD.status='READY' AND (NEW.status,NEW.payload) IS DISTINCT FROM (OLD.status,OLD.payload)) THEN
        RAISE EXCEPTION 'Completed Watch formula features and decisions cannot change';
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS freeze_watch_formula_catalog_v1 ON research_watch_scan_formula_catalog;
CREATE TRIGGER freeze_watch_formula_catalog_v1 BEFORE UPDATE ON research_watch_scan_formula_catalog
    FOR EACH ROW EXECUTE FUNCTION research_freeze_watch_formula_v1();
DROP TRIGGER IF EXISTS freeze_watch_formula_samples_v1 ON research_watch_scan_formula_samples;
CREATE TRIGGER freeze_watch_formula_samples_v1 BEFORE UPDATE ON research_watch_scan_formula_samples
    FOR EACH ROW EXECUTE FUNCTION research_freeze_watch_formula_v1();

CREATE OR REPLACE VIEW research_watch_scan_formula_evaluations AS
SELECT f.evaluation_version,f.consumer_version,f.snapshot_set_id,f.symbol,f.usable_from_utc,
    f.payload->>'feature_version' AS feature_version,
    f.payload->>'feature_sha256' AS feature_sha256,
    f.payload->>'bundle_sha256' AS bundle_sha256,
    f.payload->>'parent_payload_sha256' AS parent_payload_sha256,
    e.candidate_key,e.base_direction,e.analysis_direction,e.match_status,e.missing_features,
    c.definition_sha256,c.orientation,FALSE AS qualifies_as_prospective_formula_evidence
FROM research_watch_scan_formula_samples f
CROSS JOIN LATERAL jsonb_to_recordset(COALESCE(f.payload->'evaluations','[]'::jsonb))
    e(candidate_key text,base_direction text,analysis_direction text,match_status text,missing_features jsonb)
JOIN research_watch_scan_formula_catalog c ON c.evaluation_version=f.evaluation_version
    AND c.candidate_key=e.candidate_key AND c.supported
WHERE f.status='READY';

-- Only source identity and causal BTC membership enter selection. Price labels,
-- path quality, maturity, and gains/losses never choose a replacement anchor.
-- Pending/failed feature jobs remain UNKNOWN, including a pending tied coin.
CREATE OR REPLACE VIEW research_watch_scan_formula_wave_population AS
SELECT c.evaluation_version,c.candidate_key,c.definition_sha256,c.orientation,
    c.feature_version,c.population_version,c.measurement_version,c.entry_version,
    c.outcome_method_version,c.parent_policy_version,c.selection_version,
    m.consumer_version,m.snapshot_set_id,m.symbol,scope.symbol_scope,m.usable_from_utc,m.entry_time_utc,m.price_route,
    m.payload#>>'{membership,btc_parent_movement_id}' AS btc_parent_movement_id,
    e.base_direction,
    CASE WHEN c.orientation='INVERSE' THEN
        CASE e.base_direction WHEN 'LONG' THEN 'SHORT' ELSE 'LONG' END ELSE e.base_direction END AS analysis_direction,
    COALESCE(e.match_status,'UNKNOWN') AS match_status,
    COALESCE(e.missing_features,'["PENDING_OR_INCONSISTENT_FEATURE_SAMPLE"]'::jsonb) AS missing_features
FROM research_watch_scan_measurements m
CROSS JOIN LATERAL (VALUES ('ALL'::text),(m.symbol)) scope(symbol_scope)
JOIN research_watch_scan_intakes i USING(consumer_version,snapshot_set_id)
LEFT JOIN research_watch_scan_formula_samples f ON f.evaluation_version='watch-scan-formulas-v1'
    AND f.consumer_version=m.consumer_version AND f.snapshot_set_id=m.snapshot_set_id AND f.symbol=m.symbol
CROSS JOIN LATERAL jsonb_to_recordset(CASE WHEN f.status='READY'
    AND f.payload->>'bundle_sha256'=i.bundle_sha256
    AND f.payload->>'parent_payload_sha256'=i.parent_payload_sha256
    AND f.payload->>'feature_version'='watch-captured-total-model-features-v1'
    AND f.usable_from_utc=m.usable_from_utc THEN f.payload->'evaluations'
    ELSE '[{"base_direction":"LONG"},{"base_direction":"SHORT"}]'::jsonb END)
    e(candidate_key text,base_direction text,analysis_direction text,match_status text,missing_features jsonb)
JOIN research_watch_scan_formula_catalog c ON c.evaluation_version='watch-scan-formulas-v1'
    AND c.supported AND (e.candidate_key IS NULL OR c.candidate_key=e.candidate_key)
WHERE m.measurement_version=c.measurement_version
    AND m.consumer_version='watch-all-scan-intake-v1' AND i.intake_status='ACCEPTED'
    AND m.payload->>'version'=m.measurement_version
    AND m.payload->>'population_version'=c.population_version
    AND m.payload->>'consumer_version'=m.consumer_version
    AND (m.payload->>'snapshot_set_id')::bigint=m.snapshot_set_id
    AND m.payload->>'symbol'=m.symbol
    AND m.payload->>'price_route'=m.price_route
    AND (m.payload->>'entry_time_utc')::timestamptz=m.entry_time_utc
    AND m.usable_from_utc=i.usable_from_utc
    AND m.payload->>'bundle_sha256'=i.bundle_sha256
    AND m.payload->>'parent_payload_sha256'=i.parent_payload_sha256
    AND m.payload->>'entry_version'=c.entry_version
    AND m.payload->>'outcome_method_version'=c.outcome_method_version
    AND m.payload#>>'{membership,episode_policy_version}'=c.parent_policy_version
    AND m.payload#>>'{membership,membership_status}'='LIVE'
    AND m.payload#>>'{membership,btc_parent_movement_id}' IS NOT NULL
    AND (m.payload#>>'{membership,decision_time_utc}')::timestamptz=m.usable_from_utc;

CREATE OR REPLACE VIEW research_watch_scan_formula_wave_anchors AS
SELECT p.*,(earliest_unknown_utc IS NULL OR earliest_unknown_utc>usable_from_utc) AS anchor_eligible
FROM (
    SELECT p.*,
        MIN(usable_from_utc) FILTER(WHERE match_status='MATCH') OVER cohort AS first_match_utc,
        MIN(usable_from_utc) FILTER(WHERE match_status='NO_MATCH') OVER cohort AS first_control_utc,
        MIN(usable_from_utc) FILTER(WHERE match_status='UNKNOWN') OVER cohort AS earliest_unknown_utc
    FROM research_watch_scan_formula_wave_population p
    WINDOW cohort AS (PARTITION BY evaluation_version,candidate_key,symbol_scope,base_direction,btc_parent_movement_id)
) p
WHERE (match_status='MATCH' AND usable_from_utc=first_match_utc)
   OR (match_status='NO_MATCH' AND usable_from_utc=first_control_utc);

-- One row per BTC wave and comparison arm (all earliest tied members required).
-- Four windows and eight barriers reuse the same frozen feature decision.
CREATE OR REPLACE VIEW research_watch_scan_formula_wave_outcomes AS
WITH members AS (
    SELECT a.evaluation_version,a.candidate_key,a.symbol_scope,a.base_direction,a.analysis_direction,
        a.btc_parent_movement_id,a.match_status,a.usable_from_utc,a.symbol,a.snapshot_set_id,
        a.anchor_eligible,w.window_minutes,t.threshold_bps,r.record,l.label,
        CASE
            WHEN NOT a.anchor_eligible THEN 'DATA_MISSING'
            WHEN m.status='NOT_YET_ENTRY' THEN 'OPEN'
            WHEN r.record->>'status'='OPEN' THEN 'OPEN'
            WHEN r.record->>'status' IS DISTINCT FROM 'READY' THEN 'DATA_MISSING'
            WHEN l.label IS NULL OR (l.label->>'path_complete')::boolean IS NOT TRUE THEN 'DATA_MISSING'
            WHEN l.label->>'status' IN ('SUCCESS','FAILURE','OPEN','DATA_MISSING') THEN l.label->>'status'
            WHEN l.label->>'terminal_reason'='SAME_CANDLE_BOTH' THEN 'AMBIGUOUS'
            WHEN l.label->>'terminal_reason'='OBSERVATION_WINDOW_CLOSED_NO_TOUCH' THEN 'NO_TOUCH'
            ELSE 'DATA_MISSING' END AS member_status,
        m.updated_at_utc AS measurement_updated_at_utc
    FROM research_watch_scan_formula_wave_anchors a
    CROSS JOIN (VALUES (60),(240),(720),(1440)) w(window_minutes)
    CROSS JOIN (VALUES (25),(50),(75),(100),(125),(150),(175),(200)) t(threshold_bps)
    JOIN research_watch_scan_measurements m ON m.measurement_version=a.measurement_version
        AND m.consumer_version=a.consumer_version AND m.snapshot_set_id=a.snapshot_set_id AND m.symbol=a.symbol
    LEFT JOIN LATERAL (SELECT value AS record FROM jsonb_array_elements(COALESCE(m.payload->'records','[]'::jsonb))
        WHERE value->>'direction'=a.analysis_direction AND (value->>'window_minutes')::int=w.window_minutes) r ON true
    LEFT JOIN LATERAL (SELECT value AS label FROM jsonb_array_elements(COALESCE(r.record->'labels','[]'::jsonb))
        WHERE (value->>'threshold_bps')::int=t.threshold_bps) l ON true
)
SELECT evaluation_version,candidate_key,symbol_scope,base_direction,analysis_direction,btc_parent_movement_id,
    match_status,window_minutes,threshold_bps,MIN(usable_from_utc) AS cohort_time_utc,
    COUNT(*) AS cohort_members,BOOL_AND(anchor_eligible) AS anchor_eligible,
    CASE WHEN BOOL_OR(member_status='DATA_MISSING') THEN 'DATA_MISSING'
         WHEN BOOL_OR(member_status='OPEN') THEN 'OPEN'
         WHEN BOOL_OR(member_status='AMBIGUOUS') THEN 'AMBIGUOUS'
         WHEN BOOL_OR(member_status='NO_TOUCH') THEN 'NO_TOUCH'
         WHEN BOOL_OR(member_status='FAILURE') THEN 'FAILURE' ELSE 'SUCCESS' END AS outcome_status,
    CASE WHEN BOOL_AND(anchor_eligible AND COALESCE(record->>'status'='READY',false)) THEN MIN((record->>'mfe_pct')::double precision) END AS full_window_mfe_pct,
    CASE WHEN BOOL_AND(anchor_eligible AND COALESCE(record->>'status'='READY',false)) THEN MAX((record->>'mae_pct')::double precision) END AS full_window_mae_pct,
    MIN(measurement_updated_at_utc) AS oldest_measurement_update_utc,
    FALSE AS qualifies_as_prospective_formula_evidence
FROM members GROUP BY evaluation_version,candidate_key,symbol_scope,base_direction,analysis_direction,
    btc_parent_movement_id,match_status,window_minutes,threshold_bps;

-- Descriptive comparison only. Arms can share a wave, so the union count is
-- explicit; neither arm sizes nor windows, directions, aliases or barriers add
-- independent cases. No acceptance/promotion/discovery code consumes this view.
CREATE OR REPLACE VIEW research_watch_scan_formula_comparisons AS
SELECT o.evaluation_version,o.candidate_key,c.definition_sha256,c.orientation,c.feature_version,
    c.population_version,c.measurement_version,c.entry_version,c.outcome_method_version,
    c.parent_policy_version,c.selection_version,o.symbol_scope,o.base_direction,o.analysis_direction,o.window_minutes,o.threshold_bps,
    COUNT(DISTINCT btc_parent_movement_id) AS distinct_waves,
    COUNT(*) FILTER(WHERE match_status='MATCH') AS matched_waves,
    COUNT(*) FILTER(WHERE match_status='NO_MATCH') AS control_waves,
    COUNT(*)-COUNT(DISTINCT btc_parent_movement_id) AS shared_waves,
    COUNT(*) FILTER(WHERE NOT anchor_eligible) AS blocked_arms,
    COUNT(*) FILTER(WHERE match_status='MATCH' AND outcome_status='SUCCESS') AS matched_success,
    COUNT(*) FILTER(WHERE match_status='MATCH' AND outcome_status='FAILURE') AS matched_failure,
    COUNT(*) FILTER(WHERE match_status='NO_MATCH' AND outcome_status='SUCCESS') AS control_success,
    COUNT(*) FILTER(WHERE match_status='NO_MATCH' AND outcome_status='FAILURE') AS control_failure,
    COUNT(*) FILTER(WHERE outcome_status='OPEN') AS open_arms,
    COUNT(*) FILTER(WHERE outcome_status='DATA_MISSING') AS missing_arms,
    COUNT(*) FILTER(WHERE outcome_status='AMBIGUOUS') AS ambiguous_arms,
    COUNT(*) FILTER(WHERE outcome_status='NO_TOUCH') AS no_touch_arms,
    100.0*COUNT(*) FILTER(WHERE match_status='MATCH' AND outcome_status='SUCCESS')/
        NULLIF(COUNT(*) FILTER(WHERE match_status='MATCH' AND outcome_status IN ('SUCCESS','FAILURE')),0)
        AS matched_decisive_success_pct,
    100.0*COUNT(*) FILTER(WHERE match_status='NO_MATCH' AND outcome_status='SUCCESS')/
        NULLIF(COUNT(*) FILTER(WHERE match_status='NO_MATCH' AND outcome_status IN ('SUCCESS','FAILURE')),0)
        AS control_decisive_success_pct,
    FALSE AS statistical_test_performed,FALSE AS qualifies_as_prospective_formula_evidence,
    'DESCRIPTIVE_SHARED_WAVE_ARMS'::text AS evidence_status
FROM research_watch_scan_formula_wave_outcomes o
JOIN research_watch_scan_formula_catalog c USING(evaluation_version,candidate_key)
GROUP BY o.evaluation_version,o.candidate_key,c.definition_sha256,c.orientation,c.feature_version,
    c.population_version,c.measurement_version,c.entry_version,c.outcome_method_version,
    c.parent_policy_version,c.selection_version,o.symbol_scope,o.base_direction,o.analysis_direction,o.window_minutes,o.threshold_bps;
