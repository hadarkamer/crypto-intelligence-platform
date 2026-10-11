-- Preserve v1 evidence and publish v2 only after its accepted-source backlog
-- is complete. No source, scoring, price, alert or discovery table is changed.
CREATE TABLE IF NOT EXISTS research_watch_scan_formula_runtime (
    singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK(singleton),
    active_evaluation_version TEXT NOT NULL DEFAULT 'watch-scan-formulas-v1',
    activated_at_utc TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
INSERT INTO research_watch_scan_formula_runtime(singleton) VALUES(true) ON CONFLICT DO NOTHING;

-- Replace only the original payload-coverage check, retaining identity, route,
-- FK and immutability protections. Explicit counts bind each adapter version.
DO $$ DECLARE old_check record;
BEGIN
    FOR old_check IN SELECT conname FROM pg_constraint
        WHERE conrelid='research_watch_scan_formula_samples'::regclass AND contype='c'
          AND pg_get_constraintdef(oid) LIKE '%jsonb_array_length%'
    LOOP
        EXECUTE format('ALTER TABLE research_watch_scan_formula_samples DROP CONSTRAINT %I',old_check.conname);
    END LOOP;
END $$;
ALTER TABLE research_watch_scan_formula_samples ADD CONSTRAINT watch_formula_versioned_payload_coverage
CHECK(status<>'READY' OR ((payload->>'version'=evaluation_version
    AND payload->>'consumer_version'=consumer_version
    AND (payload->>'snapshot_set_id')::bigint=snapshot_set_id
    AND payload->>'symbol'=symbol
    AND CASE evaluation_version
        WHEN 'watch-scan-formulas-v1' THEN jsonb_array_length(payload->'evaluations')=68
        WHEN 'watch-scan-formulas-v2-maxpain' THEN jsonb_array_length(payload->'evaluations')=164
        ELSE false END) IS TRUE));

-- Renaming preserves the dependency graph and exact v1 cohort/outcome rules.
-- The by-version views retain all versions; their original names below expose
-- only the active version. Reapplying this migration keeps the active pointer.
DO $$ DECLARE relation_name text;
BEGIN
    FOREACH relation_name IN ARRAY ARRAY[
        'research_watch_scan_formula_evaluations',
        'research_watch_scan_formula_wave_population',
        'research_watch_scan_formula_wave_anchors',
        'research_watch_scan_formula_wave_outcomes',
        'research_watch_scan_formula_comparisons']
    LOOP
        IF to_regclass(relation_name||'_by_version') IS NULL THEN
            EXECUTE format('ALTER VIEW %I RENAME TO %I',relation_name,relation_name||'_by_version');
        END IF;
    END LOOP;
END $$;

CREATE OR REPLACE VIEW research_watch_scan_formula_wave_population_by_version AS
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
JOIN (SELECT DISTINCT evaluation_version,feature_version,measurement_version
      FROM research_watch_scan_formula_catalog WHERE supported) version
    ON version.measurement_version=m.measurement_version
LEFT JOIN research_watch_scan_formula_samples f ON f.evaluation_version=version.evaluation_version
    AND f.consumer_version=m.consumer_version AND f.snapshot_set_id=m.snapshot_set_id AND f.symbol=m.symbol
CROSS JOIN LATERAL jsonb_to_recordset(CASE WHEN f.status='READY'
    AND f.payload->>'bundle_sha256'=i.bundle_sha256
    AND f.payload->>'parent_payload_sha256'=i.parent_payload_sha256
    AND f.payload->>'feature_version'=version.feature_version
    AND f.usable_from_utc=m.usable_from_utc THEN f.payload->'evaluations'
    ELSE '[{"base_direction":"LONG"},{"base_direction":"SHORT"}]'::jsonb END)
    e(candidate_key text,base_direction text,analysis_direction text,match_status text,missing_features jsonb)
JOIN research_watch_scan_formula_catalog c ON c.evaluation_version=version.evaluation_version
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

CREATE OR REPLACE VIEW research_watch_scan_formula_evaluations AS
SELECT v.* FROM research_watch_scan_formula_evaluations_by_version v
WHERE v.evaluation_version=(SELECT active_evaluation_version
    FROM research_watch_scan_formula_runtime WHERE singleton=true);

CREATE OR REPLACE VIEW research_watch_scan_formula_wave_population AS
SELECT v.* FROM research_watch_scan_formula_wave_population_by_version v
WHERE v.evaluation_version=(SELECT active_evaluation_version
    FROM research_watch_scan_formula_runtime WHERE singleton=true);

CREATE OR REPLACE VIEW research_watch_scan_formula_wave_anchors AS
SELECT v.* FROM research_watch_scan_formula_wave_anchors_by_version v
WHERE v.evaluation_version=(SELECT active_evaluation_version
    FROM research_watch_scan_formula_runtime WHERE singleton=true);

CREATE OR REPLACE VIEW research_watch_scan_formula_wave_outcomes AS
SELECT v.* FROM research_watch_scan_formula_wave_outcomes_by_version v
WHERE v.evaluation_version=(SELECT active_evaluation_version
    FROM research_watch_scan_formula_runtime WHERE singleton=true);

CREATE OR REPLACE VIEW research_watch_scan_formula_comparisons AS
SELECT v.* FROM research_watch_scan_formula_comparisons_by_version v
WHERE v.evaluation_version=(SELECT active_evaluation_version
    FROM research_watch_scan_formula_runtime WHERE singleton=true);
