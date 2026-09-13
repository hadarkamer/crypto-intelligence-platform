-- Versioned all-scan measurements. No alert, formula, or delivery table writes.
CREATE TABLE IF NOT EXISTS research_watch_scan_measurement_state (
    measurement_version TEXT PRIMARY KEY,
    scan_cursor BIGINT NOT NULL DEFAULT 0,
    scan_high_water BIGINT NOT NULL DEFAULT 0,
    completed_laps BIGINT NOT NULL DEFAULT 0,
    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS research_watch_scan_measurements (
    measurement_version TEXT NOT NULL,
    consumer_version TEXT NOT NULL,
    snapshot_set_id BIGINT NOT NULL,
    symbol TEXT NOT NULL CHECK (symbol IN ('BTC','ETH','SOL','BNB','XRP','DOGE','ZEC','HYPE')),
    usable_from_utc TIMESTAMPTZ NOT NULL,
    entry_time_utc TIMESTAMPTZ NOT NULL,
    price_route TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'WAITING',
    payload JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(payload)='object'),
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at_utc TIMESTAMPTZ,
    last_error TEXT,
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(measurement_version,consumer_version,snapshot_set_id,symbol),
    FOREIGN KEY(consumer_version,snapshot_set_id)
        REFERENCES research_watch_scan_intakes(consumer_version,snapshot_set_id),
    CHECK (entry_time_utc=date_trunc('minute',usable_from_utc)+INTERVAL '1 minute'),
    CHECK ((symbol='HYPE' AND price_route='HYPERLIQUID_HYPE_PERP_TRADE_1M')
        OR (symbol<>'HYPE' AND price_route='BINANCE_SPOT_TRADE_1M')),
    CHECK (status IN ('WAITING','NOT_YET_ENTRY','DATA_MISSING_ENTRY','INVALID_OBSERVATION',
        'INVALID_SOURCE','INVALID_PATH','OPEN','DATA_MISSING','READY','ERROR'))
);
CREATE INDEX IF NOT EXISTS idx_watch_scan_measurement_due
    ON research_watch_scan_measurements(next_attempt_at_utc,snapshot_set_id,symbol)
    WHERE next_attempt_at_utc IS NOT NULL;

CREATE OR REPLACE FUNCTION research_freeze_watch_measurement_v1()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE old_record jsonb; new_record jsonb;
BEGIN
    IF (NEW.measurement_version,NEW.consumer_version,NEW.snapshot_set_id,NEW.symbol,
        NEW.usable_from_utc,NEW.entry_time_utc,NEW.price_route) IS DISTINCT FROM
       (OLD.measurement_version,OLD.consumer_version,OLD.snapshot_set_id,OLD.symbol,
        OLD.usable_from_utc,OLD.entry_time_utc,OLD.price_route) THEN
        RAISE EXCEPTION 'Watch measurement identity cannot change';
    END IF;
    IF OLD.payload->>'reference_price' IS NOT NULL AND
       (NEW.payload->'reference_price',NEW.payload->'price_source',
        NEW.payload->'bundle_sha256',NEW.payload->'parent_payload_sha256',
        NEW.payload->'entry_version',NEW.payload->'outcome_method_version') IS DISTINCT FROM
       (OLD.payload->'reference_price',OLD.payload->'price_source',
        OLD.payload->'bundle_sha256',OLD.payload->'parent_payload_sha256',
        OLD.payload->'entry_version',OLD.payload->'outcome_method_version') THEN
        RAISE EXCEPTION 'Watch measurement entry provenance cannot change';
    END IF;
    IF OLD.payload#>>'{membership,membership_status}' IN ('LIVE','BOUNDARY_UNVERIFIED')
       AND NEW.payload->'membership' IS DISTINCT FROM OLD.payload->'membership' THEN
        RAISE EXCEPTION 'Confirmed Watch BTC membership cannot change';
    END IF;
    FOR old_record IN SELECT value FROM jsonb_array_elements(COALESCE(OLD.payload->'records','[]'::jsonb))
        WHERE value->>'status'='READY'
    LOOP
        SELECT value INTO new_record FROM jsonb_array_elements(COALESCE(NEW.payload->'records','[]'::jsonb))
            WHERE value->>'direction'=old_record->>'direction'
              AND value->>'window_minutes'=old_record->>'window_minutes';
        IF new_record IS DISTINCT FROM old_record THEN
            RAISE EXCEPTION 'Completed Watch measurement cannot change';
        END IF;
    END LOOP;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS freeze_watch_measurement_v1 ON research_watch_scan_measurements;
CREATE TRIGGER freeze_watch_measurement_v1 BEFORE UPDATE ON research_watch_scan_measurements
    FOR EACH ROW EXECUTE FUNCTION research_freeze_watch_measurement_v1();

CREATE OR REPLACE VIEW research_watch_scan_window_results AS
SELECT m.measurement_version,m.consumer_version,m.snapshot_set_id,m.symbol,m.price_route,
       m.usable_from_utc,m.entry_time_utc,m.payload->>'entry_version' AS entry_version,
       m.payload->>'outcome_method_version' AS outcome_method_version,
       (m.payload->>'reference_price')::double precision AS reference_price,
       m.payload->'price_source' AS price_source,
       m.payload->>'bundle_sha256' AS bundle_sha256,
       m.payload->>'parent_payload_sha256' AS parent_payload_sha256,
       m.payload->>'capture_phase' AS capture_phase,
       m.payload->'membership' AS membership,
       m.payload#>>'{membership,btc_parent_movement_id}' AS btc_parent_movement_id,
       m.payload#>>'{membership,membership_status}' AS btc_membership_status,
       r->>'direction' AS analysis_direction,(r->>'window_minutes')::integer AS window_minutes,
       r->>'status' AS window_status,(r->>'window_end_utc')::timestamptz AS window_end_utc,
       (r->>'mfe_pct')::double precision AS mfe_pct,(r->>'mae_pct')::double precision AS mae_pct,
       (r->>'asymmetry_ratio')::double precision AS asymmetry_ratio,
       r->>'asymmetry_status' AS asymmetry_status,r->>'path_sha256' AS path_sha256,
       r->'labels' AS labels,FALSE AS qualifies_as_prospective_formula_evidence
FROM research_watch_scan_measurements m
CROSS JOIN LATERAL jsonb_array_elements(COALESCE(m.payload->'records','[]'::jsonb)) r;

CREATE OR REPLACE VIEW research_watch_scan_threshold_results AS
SELECT r.*, (label->>'threshold_bps')::integer AS threshold_bps,
       label->>'status' AS outcome_status,label->>'first_touch_side' AS first_touch_side,
       label->>'terminal_reason' AS terminal_reason,(label->>'success')::boolean AS success,
       label AS outcome
FROM research_watch_scan_window_results r
CROSS JOIN LATERAL jsonb_array_elements(r.labels) label;

-- Seven MaxPain horizons share the same directional price measurement. These
-- joined rows are feature links, not extra independent occurrences.
CREATE OR REPLACE VIEW research_watch_scan_measured_score_slots AS
SELECT s.consumer_version,s.population_version,s.snapshot_set_id,s.snapshot_key,
       s.parent_payload_sha256,s.bundle_sha256,s.watch_scan_id,s.symbol,
       s.observed_at_utc,s.source_available_at_utc,s.source_created_at_utc,
       s.usable_from_utc,s.ingested_at_utc,s.capture_phase,s.timeframe,s.source_side,
       s.analysis_direction,s.slot_status,s.score,s.maxpain_features,s.models,s.sources,
       s.source_time_errors,s.code_sha256,s.input_universe_sha256,s.is_delivered_alert,
       s.qualifies_as_prospective_formula_evidence,
       r.measurement_version,r.window_minutes,r.window_status,
       r.entry_time_utc,r.reference_price,r.price_route,r.btc_parent_movement_id,
       r.btc_membership_status,r.mfe_pct,r.mae_pct,r.asymmetry_ratio,r.labels
FROM research_watch_scan_score_slots s JOIN research_watch_scan_window_results r
    USING(consumer_version,snapshot_set_id,symbol,analysis_direction)
WHERE s.bundle_sha256=r.bundle_sha256 AND s.parent_payload_sha256=r.parent_payload_sha256;
