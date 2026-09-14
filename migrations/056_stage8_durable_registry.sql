-- Durable, research-only Stage-8 registration and evidence boundary.
--
-- This migration creates no role: role lifecycle is intentionally owned by the
-- database administrator.  PUBLIC receives nothing.  If the five documented
-- least-privilege roles already exist, the grants at the end are applied.
-- Applying this migration alone does not register a binding or start a freeze.

CREATE OR REPLACE FUNCTION research_stage8_canonical_json_v1(value JSONB)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
DECLARE
    kind TEXT := jsonb_typeof(value);
    rendered TEXT;
BEGIN
    IF kind = 'object' THEN
        SELECT '{' || COALESCE(string_agg(
            to_jsonb(item.key)::text || ':' || research_stage8_canonical_json_v1(item.value),
            ',' ORDER BY item.key COLLATE "C"), '') || '}'
        INTO rendered
        FROM jsonb_each(value) AS item;
        RETURN rendered;
    ELSIF kind = 'array' THEN
        SELECT '[' || COALESCE(string_agg(
            research_stage8_canonical_json_v1(item.value),
            ',' ORDER BY item.ordinality), '') || ']'
        INTO rendered
        FROM jsonb_array_elements(value) WITH ORDINALITY AS item(value, ordinality);
        RETURN rendered;
    END IF;
    RETURN value::text;
END;
$$;

CREATE OR REPLACE FUNCTION research_stage8_json_sha256_v1(value JSONB)
RETURNS TEXT
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
    SELECT encode(sha256(convert_to(research_stage8_canonical_json_v1(value), 'UTF8')), 'hex')
$$;

-- Watch v2 has a Python JSON float contract separate from Stage-8 JSONB hashes.
-- This formatter covers producer-normalized inputs: integral floats have already
-- become JSON integer tokens before the immutable archive write.
CREATE OR REPLACE FUNCTION research_stage8_watch_canonical_json_v1(value JSONB)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE STRICT PARALLEL SAFE
SET extra_float_digits = 3
AS $$
DECLARE
    kind TEXT := jsonb_typeof(value);
    rendered TEXT;
    raw TEXT;
    floating DOUBLE PRECISION;
BEGIN
    IF kind = 'object' THEN
        SELECT '{' || COALESCE(string_agg(
            to_jsonb(item.key)::text || ':'
                || research_stage8_watch_canonical_json_v1(item.value),
            ',' ORDER BY item.key COLLATE "C"), '') || '}'
        INTO rendered FROM jsonb_each(value) AS item;
        RETURN rendered;
    ELSIF kind = 'array' THEN
        SELECT '[' || COALESCE(string_agg(
            research_stage8_watch_canonical_json_v1(item.value),
            ',' ORDER BY item.ordinality), '') || ']'
        INTO rendered FROM jsonb_array_elements(value)
            WITH ORDINALITY AS item(value, ordinality);
        RETURN rendered;
    ELSIF kind = 'number' THEN
        raw := value::text;
        IF raw !~ '[.]' THEN
            -- Never convert arbitrary-precision JSON integers through float8.
            RETURN raw;
        END IF;
        floating := raw::double precision;
        IF floating::text IN ('NaN','Infinity','-Infinity') THEN
            RAISE EXCEPTION 'Watch JSON number is not finite';
        END IF;
        IF floating = trunc(floating) THEN
            -- Such a decimal token cannot be emitted by Watch v2, whose
            -- producer changes all integral floats (including -0.0) to int.
            -- Refuse to guess a possibly lossy arbitrary-precision identity.
            RAISE EXCEPTION 'Watch decimal number is not producer-normalized';
        END IF;
        rendered := pg_catalog.to_json(floating)::text;
        -- PostgreSQL starts exponent notation at 1e15; Python starts at 1e16.
        IF abs(floating) >= 0.0001 AND abs(floating) < 1e16
           AND rendered ~ '[eE]' THEN
            rendered := (rendered::numeric)::text;
        END IF;
        RETURN rendered;
    END IF;
    RETURN value::text;
END;
$$;

CREATE OR REPLACE FUNCTION research_stage8_watch_json_sha256_v1(value JSONB)
RETURNS TEXT
LANGUAGE sql
IMMUTABLE STRICT PARALLEL SAFE
AS $$
    SELECT encode(sha256(convert_to(
        research_stage8_watch_canonical_json_v1(value), 'UTF8')), 'hex')
$$;

-- Python round(binary64, 2), without changing the value through decimal text
-- or an inexact binary64 multiplication by 100. Derive exact rational cents
-- from the IEEE754 payload, then round that rational to nearest, ties to even.
CREATE OR REPLACE FUNCTION research_stage8_watch_round2_v1(value DOUBLE PRECISION)
RETURNS DOUBLE PRECISION
LANGUAGE plpgsql
IMMUTABLE STRICT PARALLEL SAFE
AS $$
DECLARE
    bytes BYTEA := float8send(value);
    exponent_bits INTEGER;
    exponent_value INTEGER;
    significand NUMERIC;
    numerator NUMERIC;
    denominator NUMERIC;
    quotient NUMERIC;
    remainder_value NUMERIC;
    sign_value INTEGER;
    byte_index INTEGER;
BEGIN
    exponent_bits := ((get_byte(bytes, 0) & 127) * 16)
        + (get_byte(bytes, 1) >> 4);
    IF exponent_bits = 2047 THEN
        RAISE EXCEPTION 'Watch round requires a finite number';
    END IF;
    sign_value := CASE WHEN (get_byte(bytes, 0) & 128) = 0 THEN 1 ELSE -1 END;
    significand := get_byte(bytes, 1) & 15;
    FOR byte_index IN 2..7 LOOP
        significand := significand * 256 + get_byte(bytes, byte_index);
    END LOOP;
    IF exponent_bits = 0 THEN
        exponent_value := -1074;
    ELSE
        significand := significand + 4503599627370496;
        exponent_value := exponent_bits - 1075;
    END IF;
    IF exponent_value >= 0 THEN RETURN value; END IF;
    numerator := significand * 100;
    denominator := power(2::numeric, -exponent_value);
    quotient := div(numerator, denominator);
    remainder_value := mod(numerator, denominator);
    IF remainder_value * 2 > denominator
       OR (remainder_value * 2 = denominator AND mod(quotient, 2) = 1) THEN
        quotient := quotient + 1;
    END IF;
    RETURN (sign_value * quotient / 100)::double precision;
END;
$$;

-- Source clocks must carry an explicit offset.  This admits the ISO form
-- emitted by the frozen producer (including datetime's space separator), not
-- PostgreSQL's permissive date keywords, session timezone, or normalizations.
CREATE OR REPLACE FUNCTION research_stage8_watch_timestamp_v1(value JSONB)
RETURNS TIMESTAMPTZ
LANGUAGE plpgsql
IMMUTABLE STRICT PARALLEL SAFE
SET TimeZone = 'UTC'
SET DateStyle = 'ISO, YMD'
AS $$
DECLARE raw TEXT := value #>> '{}'; parsed TIMESTAMPTZ;
BEGIN
    IF jsonb_typeof(value) <> 'string'
       OR raw !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]{1,6})?(Z|[+-][0-9]{2}:[0-9]{2})$'
       OR substring(raw FROM 12 FOR 2)::integer > 23
       OR substring(raw FROM 15 FOR 2)::integer > 59
       OR substring(raw FROM 18 FOR 2)::integer > 59 THEN
        RETURN NULL;
    END IF;
    parsed := raw::timestamptz;
    IF NOT isfinite(parsed) OR extract(year FROM parsed) NOT BETWEEN 1 AND 9999 THEN
        RETURN NULL;
    END IF;
    RETURN parsed;
EXCEPTION WHEN OTHERS THEN RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION research_stage8_watch_finite_number_v1(value JSONB)
RETURNS BOOLEAN
LANGUAGE plpgsql
IMMUTABLE PARALLEL SAFE
AS $$
DECLARE number_value DOUBLE PRECISION;
BEGIN
    IF jsonb_typeof(value) IS DISTINCT FROM 'number' THEN RETURN FALSE; END IF;
    number_value := (value::text)::double precision;
    RETURN number_value::text NOT IN ('NaN','Infinity','-Infinity');
EXCEPTION WHEN OTHERS THEN RETURN FALSE;
END;
$$;

CREATE OR REPLACE FUNCTION research_stage8_watch_components_match_v1(
    score JSONB, components JSONB
)
RETURNS BOOLEAN
LANGUAGE plpgsql
IMMUTABLE PARALLEL SAFE
AS $$
DECLARE
    component_name TEXT;
    component JSONB;
    integer_sum NUMERIC := 0;
    float_sum DOUBLE PRECISION := 0;
    float_mode BOOLEAN := FALSE;
BEGIN
    IF NOT research_stage8_watch_finite_number_v1(score)
       OR jsonb_typeof(components) IS DISTINCT FROM 'object' THEN RETURN FALSE; END IF;
    FOREACH component_name IN ARRAY ARRAY[
        'directional_alignment','target_proximity','cluster_confidence','relative_gap'
    ] LOOP
        component := components->component_name;
        IF NOT research_stage8_watch_finite_number_v1(component) THEN RETURN FALSE; END IF;
        -- Python sum starts with an integer and switches to binary64 at the
        -- first float. Preserve both that order and the producer's JSON types.
        IF NOT float_mode AND component::text LIKE '%.%' THEN
            float_sum := integer_sum::double precision;
            float_mode := TRUE;
        END IF;
        IF float_mode THEN
            float_sum := float_sum + (component::text)::double precision;
        ELSE
            integer_sum := integer_sum + (component::text)::numeric;
        END IF;
    END LOOP;
    IF float_mode THEN
        RETURN abs(research_stage8_watch_round2_v1(float_sum)
            - (score::text)::double precision) <= 0.011::double precision;
    ELSIF score::text LIKE '%.%' THEN
        RETURN abs(integer_sum::double precision
            - (score::text)::double precision) <= 0.011::double precision;
    END IF;
    RETURN abs(integer_sum - (score::text)::numeric) <= 0.011::numeric;
EXCEPTION WHEN OTHERS THEN RETURN FALSE;
END;
$$;

-- Full selected-coin capture admission, independent of the candidate's chosen
-- model. A malformed unselected model/window/MaxPain slot cannot be hidden by
-- a rehashed COMPLETE envelope. No invalid latest row is replaced with an
-- older, more convenient capture. Reasons remain fail-closed UNKNOWN facts.
CREATE OR REPLACE FUNCTION research_stage8_watch_capture_errors_v1(
    operational JSONB, symbol TEXT, expected_code_manifest JSONB,
    expected_cycle_id TEXT, decision_at TIMESTAMPTZ,
    available_at TIMESTAMPTZ, created_at TIMESTAMPTZ
)
RETURNS TEXT[]
LANGUAGE plpgsql
IMMUTABLE PARALLEL SAFE
AS $$
DECLARE
    reasons TEXT[] := ARRAY[]::TEXT[];
    unsigned_text TEXT;
    coin JSONB;
    sources JSONB;
    item JSONB;
    model JSONB;
    model_name TEXT;
    family_name TEXT;
    family JSONB;
    references_json JSONB;
    families_json JSONB;
    entry RECORD;
    member JSONB;
    window_json JSONB;
    time_entry RECORD;
    times_json JSONB := '[]'::jsonb;
    timestamp_json JSONB;
    observed_at TIMESTAMPTZ;
    computed_at TIMESTAMPTZ;
    source TEXT;
    pair TEXT;
    market TEXT;
    instrument TEXT;
    expected_timeframes TEXT[] := ARRAY['12h','24h','48h','3d','1w','2w','1m'];
BEGIN
    IF jsonb_typeof(operational) IS DISTINCT FROM 'object' THEN
        RETURN ARRAY['WATCH_OPERATIONAL_ENVELOPE_INVALID'];
    END IF;
    unsigned_text := research_stage8_watch_canonical_json_v1(
        operational - 'payload_sha256');
    IF operational->>'version' IS DISTINCT FROM 'watch-operational-scores-v2'
       OR operational->>'population' IS DISTINCT FROM 'all-top8-watch-scans-before-display-v1'
       OR operational->>'hash_version' IS DISTINCT FROM 'json-integer-float-zero-normalized-v1'
       OR operational->>'status' IS DISTINCT FROM 'COMPLETE'
       OR jsonb_typeof(operational->'cycle_id') IS DISTINCT FROM 'string'
       OR btrim(operational->>'cycle_id') = ''
       OR operational->>'cycle_id' IS DISTINCT FROM expected_cycle_id
       OR operational->'code_sha256' IS DISTINCT FROM expected_code_manifest
       OR COALESCE(operational->>'input_universe_sha256','') !~ '^[0-9a-f]{64}$'
       OR jsonb_typeof(operational->'input_row_count') IS DISTINCT FROM 'number'
       OR COALESCE((operational->'input_row_count')::text,'') !~ '^[0-9]+$'
       OR (operational->>'input_row_count')::numeric < 56
       OR operational->>'source_side_semantics' IS DISTINCT FROM
           'liquidated-side; SHORT target implies price UP, LONG target price DOWN'
       OR operational->'symbols_expected' IS DISTINCT FROM
           '["BTC","ETH","SOL","HYPE","DOGE","ZEC","BNB","XRP"]'::jsonb
       OR jsonb_typeof(operational->'coins') IS DISTINCT FROM 'object'
       OR (SELECT array_agg(key ORDER BY key COLLATE "C")
           FROM jsonb_object_keys(operational->'coins') AS keys(key))
           IS DISTINCT FROM ARRAY['BNB','BTC','DOGE','ETH','HYPE','SOL','XRP','ZEC']::TEXT[]
       OR operational->'maxpain_additive_components' IS DISTINCT FROM
           '["directional_alignment","target_proximity","cluster_confidence","relative_gap"]'::jsonb
       OR operational->>'payload_sha256' IS DISTINCT FROM
           encode(sha256(convert_to(unsigned_text,'UTF8')),'hex') THEN
        reasons := array_append(reasons,'WATCH_OPERATIONAL_ENVELOPE_INVALID');
    END IF;
    IF octet_length(convert_to(unsigned_text,'UTF8')) > 262144 THEN
        reasons := array_append(reasons,'WATCH_CAPTURE_SIZE_LIMIT_EXCEEDED');
    END IF;
    computed_at := research_stage8_watch_timestamp_v1(operational->'computed_at_utc');
    IF computed_at IS NULL OR decision_at IS NULL OR available_at IS NULL OR created_at IS NULL
       OR NOT isfinite(decision_at) OR NOT isfinite(available_at) OR NOT isfinite(created_at)
       OR computed_at > least(decision_at,available_at,created_at)
       OR greatest(available_at,created_at) > decision_at
       OR greatest(available_at,created_at) < decision_at - interval '300 seconds' THEN
        reasons := array_append(reasons,'WATCH_COMPUTED_TIME_INVALID');
    END IF;
    coin := operational->'coins'->symbol;
    IF jsonb_typeof(coin) IS DISTINCT FROM 'object'
       OR coin->>'status' IS DISTINCT FROM 'CAPTURED'
       OR coin->'source_time_errors' IS DISTINCT FROM '[]'::jsonb THEN
        reasons := array_append(reasons,'WATCH_COIN_AUTHORITY_INVALID');
    END IF;
    IF jsonb_typeof(coin->'maxpain') IS DISTINCT FROM 'array'
       OR jsonb_array_length(coin->'maxpain') IS DISTINCT FROM 14
       OR (SELECT count(DISTINCT (slot->>'timeframe',slot->>'source_side'))
           FROM jsonb_array_elements(coin->'maxpain') AS slots(slot)) <> 14
       OR EXISTS (SELECT 1 FROM jsonb_array_elements(coin->'maxpain') AS slots(slot)
           WHERE jsonb_typeof(slot) IS DISTINCT FROM 'object'
              OR COALESCE(slot->>'timeframe','') <> ALL(expected_timeframes)
              OR COALESCE(slot->>'source_side','') NOT IN ('LONG','SHORT')) THEN
        reasons := array_append(reasons,'WATCH_MAXPAIN_SLOT_GRID_INVALID');
    ELSE
        FOR item IN SELECT value FROM jsonb_array_elements(coin->'maxpain') LOOP
            IF item->>'status' IN ('MISSING_INPUT','INACTIVE_TARGET') THEN
                IF item->'score' IS NOT NULL AND item->'score' <> 'null'::jsonb THEN
                    reasons := array_append(reasons,'WATCH_UNAVAILABLE_TARGET_HAS_SCORE');
                END IF;
            ELSIF item->>'status' IS DISTINCT FROM 'SCORED'
               OR NOT research_stage8_watch_components_match_v1(item->'score',item->'components') THEN
                reasons := array_append(reasons,'WATCH_MAXPAIN_SCORE_OR_COMPONENTS_INVALID');
            END IF;
        END LOOP;
    END IF;
    sources := coin->'sources';
    IF jsonb_typeof(sources) IS DISTINCT FROM 'object'
       OR COALESCE(sources->>'derivatives_snapshot_sha256','') !~ '^[0-9a-f]{64}$' THEN
        reasons := array_append(reasons,'WATCH_DERIVATIVES_REFERENCE_INVALID');
    END IF;
    IF jsonb_typeof(sources->'maxpain_operational_rows') IS DISTINCT FROM 'array'
       OR jsonb_array_length(sources->'maxpain_operational_rows') IS DISTINCT FROM 7
       OR (SELECT count(DISTINCT row->>'timeframe') FROM jsonb_array_elements(
           sources->'maxpain_operational_rows') AS rows(row)) <> 7
       OR EXISTS (SELECT 1 FROM jsonb_array_elements(
           sources->'maxpain_operational_rows') AS rows(row)
           WHERE jsonb_typeof(row) IS DISTINCT FROM 'object'
              OR COALESCE(row->>'timeframe','') <> ALL(expected_timeframes)) THEN
        reasons := array_append(reasons,'WATCH_OPERATIONAL_ROWS_INCOMPLETE');
    ELSE
        FOR item IN SELECT value FROM jsonb_array_elements(sources->'maxpain_operational_rows') LOOP
            source := item->>'price_source'; pair := item->>'price_pair';
            market := lower(btrim(COALESCE(item->>'price_market','')));
            instrument := btrim(COALESCE(item->>'price_instrument',''));
            IF jsonb_typeof(item->'price_source') IS DISTINCT FROM 'string'
               OR jsonb_typeof(item->'price_pair') IS DISTINCT FROM 'string'
               OR btrim(source) = '' OR btrim(pair) = '' THEN
                reasons := array_append(reasons,'WATCH_OPERATIONAL_PRICE_IDENTITY_MISSING');
            ELSIF symbol <> 'HYPE' THEN
                IF source = 'binance_spot'
                   AND (item->'price_market' IS NULL OR item->'price_market' = 'null'::jsonb) THEN
                    market := 'spot';
                END IF;
                IF (CASE WHEN source = 'binance_spot' THEN 'binance' ELSE lower(btrim(source)) END) <> 'binance'
                   OR market <> 'spot' OR upper(btrim(pair)) <> symbol || 'USDT'
                   OR instrument <> '' THEN
                    reasons := array_append(reasons,'WATCH_OPERATIONAL_PRICE_IDENTITY_INCOMPATIBLE');
                END IF;
            ELSIF source IN ('hyperliquid','hyperliquid_spot_@107') THEN
                IF market <> 'spot' OR upper(btrim(pair)) <> 'HYPE/USDT' OR instrument <> '@107' THEN
                    reasons := array_append(reasons,'WATCH_HYPE_SPOT_IDENTITY_INCOMPLETE');
                END IF;
            ELSIF regexp_replace(upper(btrim(pair)),'[^[:alnum:]]','','g') <> 'HYPEUSDT' THEN
                reasons := array_append(reasons,'WATCH_HYPE_OPERATIONAL_PAIR_MISMATCH');
            END IF;
            times_json := times_json || jsonb_build_array(
                item->'source_observed_at_utc',item->'price_fetched_at_utc');
        END LOOP;
    END IF;
    times_json := times_json || jsonb_build_array(
        sources->'positioning'->'price_fetched_at', sources->'positioning'->'oi_fetched_at',
        sources->'futures'->'quality'->'candle_close', sources->'spot'->'quality'->'candle_close',
        sources->'timing_observation'->'cvd_observed_at_utc');
    FOREACH model_name IN ARRAY ARRAY['positioning','futures_flow','spot_flow'] LOOP
        family_name := CASE model_name WHEN 'positioning' THEN 'positioning'
            WHEN 'futures_flow' THEN 'futures' ELSE 'spot' END;
        model := coin->'models'->model_name;
        IF jsonb_typeof(model->'available') IS DISTINCT FROM 'boolean'
           OR model->>'capture_status' IS DISTINCT FROM (CASE
               WHEN model->'available' = 'true'::jsonb THEN 'AVAILABLE' ELSE 'UNAVAILABLE' END)
           OR NOT research_stage8_watch_finite_number_v1(model->'score') THEN
            reasons := array_append(reasons,'WATCH_MODEL_AVAILABILITY_OR_SCORE_INVALID:' || model_name);
        END IF;
        family := sources->family_name;
        references_json := CASE WHEN jsonb_typeof(family->'window_references') = 'object'
            THEN family->'window_references' ELSE '{}'::jsonb END;
        families_json := CASE WHEN jsonb_typeof(model->'time_families') = 'object'
            THEN model->'time_families' ELSE '{}'::jsonb END;
        IF model->'available' = 'true'::jsonb AND (
            references_json = '{}'::jsonb OR families_json = '{}'::jsonb
            OR NOT EXISTS (SELECT 1 FROM jsonb_each(references_json)
                WHERE value->'available' = 'true'::jsonb)) THEN
            reasons := array_append(reasons,'WATCH_AVAILABLE_MODEL_WINDOW_EVIDENCE_MISSING:' || model_name);
        END IF;
        FOR entry IN SELECT key,value FROM jsonb_each(families_json) LOOP
            IF jsonb_typeof(entry.value->'members') IS DISTINCT FROM 'array' THEN
                IF model->'available' = 'true'::jsonb
                   OR COALESCE(entry.value->'available_windows','null'::jsonb)
                        NOT IN ('null'::jsonb,'false'::jsonb,'0'::jsonb,'""'::jsonb,'[]'::jsonb,'{}'::jsonb) THEN
                    reasons := array_append(reasons,'WATCH_TIME_FAMILY_MEMBERS_MISSING:' || model_name);
                END IF;
            ELSE
                FOR member IN SELECT value FROM jsonb_array_elements(entry.value->'members') LOOP
                    IF member->'available' = 'true'::jsonb THEN
                        window_json := references_json->(member->>'window');
                        IF window_json->'available' IS DISTINCT FROM 'true'::jsonb THEN
                            reasons := array_append(reasons,'WATCH_AVAILABLE_MEMBER_REFERENCE_MISSING:' || model_name);
                        END IF;
                        times_json := times_json || jsonb_build_array(
                            window_json->'latest_time',window_json->'reference_time');
                    END IF;
                END LOOP;
            END IF;
        END LOOP;
        FOR entry IN SELECT key,value FROM jsonb_each(references_json) LOOP
            window_json := CASE WHEN jsonb_typeof(entry.value) = 'object'
                THEN entry.value ELSE '{}'::jsonb END;
            IF window_json->'available' = 'true'::jsonb THEN
                times_json := times_json || jsonb_build_array(
                    window_json->'latest_time',window_json->'reference_time');
            END IF;
            FOR time_entry IN SELECT key,value FROM jsonb_each(window_json) LOOP
                IF right(time_entry.key,4) = 'time' AND time_entry.value <> 'null'::jsonb THEN
                    times_json := times_json || jsonb_build_array(time_entry.value);
                END IF;
            END LOOP;
        END LOOP;
    END LOOP;
    FOR timestamp_json IN SELECT value FROM jsonb_array_elements(times_json) LOOP
        observed_at := research_stage8_watch_timestamp_v1(timestamp_json);
        IF observed_at IS NULL OR computed_at IS NULL OR observed_at > computed_at THEN
            reasons := array_append(reasons,'WATCH_SOURCE_TIME_UNKNOWN_OR_FUTURE');
        END IF;
    END LOOP;
    RETURN reasons;
EXCEPTION WHEN OTHERS THEN
    RETURN array_append(reasons,'WATCH_CAPTURE_INVALID');
END;
$$;

CREATE OR REPLACE FUNCTION research_stage8_has_forbidden_evidence_key_v1(value JSONB)
RETURNS BOOLEAN
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
DECLARE
    item RECORD;
BEGIN
    IF jsonb_typeof(value) = 'object' THEN
        FOR item IN SELECT key, child FROM jsonb_each(value) AS entry(key, child)
        LOOP
            IF (
                lower(item.key) ~ '(^|_)(outcome|outcomes|label|labels|mfe|mae|probability|asymmetry|pnl|profit|loss|return|research_qualified|atomic_gate_passed)(_|$)'
                AND lower(item.key) NOT IN (
                    'declared_label_price_route','label_route_status',
                    'hype_label_instrument',
                    'operational_and_label_routes_are_separate','label_version'
                )
            )
               OR research_stage8_has_forbidden_evidence_key_v1(item.child) THEN
                RETURN TRUE;
            END IF;
        END LOOP;
    ELSIF jsonb_typeof(value) = 'array' THEN
        FOR item IN SELECT child FROM jsonb_array_elements(value) AS entry(child)
        LOOP
            IF research_stage8_has_forbidden_evidence_key_v1(item.child) THEN
                RETURN TRUE;
            END IF;
        END LOOP;
    END IF;
    RETURN FALSE;
END;
$$;

CREATE OR REPLACE FUNCTION research_stage8_utc_text_v1(value TIMESTAMPTZ)
RETURNS TEXT
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
    SELECT to_char(value AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
$$;

-- Sampler v4 hashes Python JSON after archive readback, not the Watch codec:
-- integral decimal tokens retain .0, while arbitrary-precision integers never
-- pass through binary64. JSONB has already discarded original exponent text.
-- Python str.strip includes Unicode whitespace and the four ASCII information
-- separators; PostgreSQL btrim(text) alone strips only the ordinary space.
CREATE OR REPLACE FUNCTION research_stage8_anchor_strip_v1(value TEXT)
RETURNS TEXT LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
    SELECT btrim(value, U&'\0009\000A\000B\000C\000D\001C\001D\001E\001F\0020\0085\00A0\1680\2000\2001\2002\2003\2004\2005\2006\2007\2008\2009\200A\2028\2029\202F\205F\3000')
$$;

CREATE OR REPLACE FUNCTION research_stage8_anchor_canonical_json_v1(value JSONB)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
SET extra_float_digits = 3
AS $$
DECLARE kind TEXT := jsonb_typeof(value); rendered TEXT; raw TEXT;
        floating DOUBLE PRECISION;
BEGIN
    IF kind = 'object' THEN
        SELECT '{' || COALESCE(string_agg(to_jsonb(item.key)::text || ':' ||
            research_stage8_anchor_canonical_json_v1(item.value),
            ',' ORDER BY item.key COLLATE "C"), '') || '}' INTO rendered
        FROM jsonb_each(value) AS item;
        RETURN rendered;
    ELSIF kind = 'array' THEN
        SELECT '[' || COALESCE(string_agg(
            research_stage8_anchor_canonical_json_v1(item.value),
            ',' ORDER BY item.ordinality), '') || ']' INTO rendered
        FROM jsonb_array_elements(value) WITH ORDINALITY AS item(value, ordinality);
        RETURN rendered;
    ELSIF kind = 'number' THEN
        raw := value::text;
        IF raw !~ '[.]' THEN
            -- Match the pinned Python 3.11 default decimal-int codec limit.
            -- The sign is not a digit; larger JSONB numerics fail closed.
            IF length(ltrim(raw,'-')) > 4300 THEN
                RAISE EXCEPTION 'Anchor JSON integer exceeds Python digit limit';
            END IF;
            RETURN raw;
        END IF;
        floating := raw::double precision;
        IF floating::text IN ('NaN','Infinity','-Infinity') THEN
            RAISE EXCEPTION 'Anchor JSON number is not finite';
        END IF;
        IF floating = 0 THEN RETURN '0.0'; END IF;
        rendered := to_json(floating)::text;
        IF abs(floating) >= 0.0001 AND abs(floating) < 1e16
           AND rendered ~ '[eE]' THEN rendered := (rendered::numeric)::text; END IF;
        IF rendered !~ '[.eE]' THEN rendered := rendered || '.0'; END IF;
        RETURN rendered;
    END IF;
    RETURN value::text;
END;
$$;

-- Python's `value or ''` and str(...).strip() are not SQL NULL coalescing:
-- false, numeric zero and empty containers are falsey, but whitespace strings
-- remain truthy until the subsequent strip operation.
CREATE OR REPLACE FUNCTION research_stage8_anchor_truthy_v1(value JSONB)
RETURNS BOOLEAN LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT value IS NOT NULL AND value <> ALL(
        ARRAY['null','false','0','""','[]','{}']::jsonb[])
$$;

CREATE OR REPLACE FUNCTION research_stage8_anchor_nonempty_v1(value JSONB)
RETURNS BOOLEAN LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT research_stage8_anchor_truthy_v1(value) AND
        (jsonb_typeof(value) <> 'string' OR research_stage8_anchor_strip_v1(value #>> '{}') <> '')
$$;

CREATE OR REPLACE FUNCTION research_stage8_anchor_json_sha256_v1(value JSONB)
RETURNS TEXT LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
    SELECT encode(sha256(convert_to(
        research_stage8_anchor_canonical_json_v1(value), 'UTF8')), 'hex')
$$;

-- Event reference equality uses capture.canonical's integral-float
-- normalization, unlike the sampler input/bundle hash. Decode an integral
-- binary64 exactly instead of the lossy float8-to-numeric cast or its shortest
-- decimal rendering: int(float(1000000000000000100)) is 1000000000000000128.
CREATE OR REPLACE FUNCTION research_stage8_anchor_reference_canonical_json_v1(value JSONB)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE kind TEXT := jsonb_typeof(value); rendered TEXT; raw TEXT;
        floating DOUBLE PRECISION; bytes BYTEA; exponent_bits INTEGER;
        exponent_value INTEGER; significand NUMERIC; sign_value INTEGER; i INTEGER;
BEGIN
    IF kind = 'object' THEN
        SELECT '{' || COALESCE(string_agg(to_jsonb(item.key)::text || ':' ||
            research_stage8_anchor_reference_canonical_json_v1(item.value),
            ',' ORDER BY item.key COLLATE "C"), '') || '}' INTO rendered
        FROM jsonb_each(value) AS item;
        RETURN rendered;
    ELSIF kind = 'array' THEN
        SELECT '[' || COALESCE(string_agg(
            research_stage8_anchor_reference_canonical_json_v1(item.value),
            ',' ORDER BY item.ordinality), '') || ']' INTO rendered
        FROM jsonb_array_elements(value) WITH ORDINALITY AS item(value, ordinality);
        RETURN rendered;
    ELSIF kind = 'number' THEN
        raw := value::text;
        IF raw !~ '[.]' THEN
            IF length(ltrim(raw,'-')) > 4300 THEN
                RAISE EXCEPTION 'Anchor reference JSON integer exceeds Python digit limit';
            END IF;
            RETURN raw;
        END IF;
        floating := raw::double precision;
        IF floating::text IN ('NaN','Infinity','-Infinity') THEN
            RAISE EXCEPTION 'Anchor reference JSON number is not finite';
        END IF;
        IF floating = trunc(floating) THEN
            bytes := float8send(floating);
            exponent_bits := ((get_byte(bytes,0) & 127) * 16) + (get_byte(bytes,1) >> 4);
            significand := get_byte(bytes,1) & 15;
            FOR i IN 2..7 LOOP significand := significand * 256 + get_byte(bytes,i); END LOOP;
            sign_value := CASE WHEN (get_byte(bytes,0) & 128) = 0 THEN 1 ELSE -1 END;
            IF exponent_bits = 0 THEN exponent_value := -1074;
            ELSE significand := significand + 4503599627370496; exponent_value := exponent_bits - 1075; END IF;
            IF exponent_value >= 0 THEN
                RETURN trunc(sign_value * significand * power(2::numeric,exponent_value))::text;
            END IF;
            RETURN div(sign_value * significand,power(2::numeric,-exponent_value))::text;
        END IF;
        RETURN research_stage8_anchor_canonical_json_v1(value);
    END IF;
    RETURN value::text;
END;
$$;

-- Python anchor clocks permit naive ISO timestamps as UTC. Never inherit the
-- caller timezone or accept PostgreSQL date keywords/leap-second rollover.
CREATE OR REPLACE FUNCTION research_stage8_anchor_timestamp_v1(value JSONB)
RETURNS TIMESTAMPTZ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
SET TimeZone = 'UTC' SET DateStyle = 'ISO, YMD'
AS $$
DECLARE raw TEXT := research_stage8_anchor_strip_v1(value #>> '{}'); parsed TIMESTAMPTZ;
BEGIN
    IF jsonb_typeof(value) IS DISTINCT FROM 'string'
       OR raw !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]{1,6})?(Z|[+-][0-9]{2}:[0-9]{2})?$'
       OR substring(raw FROM 12 FOR 2)::integer > 23
       OR substring(raw FROM 15 FOR 2)::integer > 59
       OR substring(raw FROM 18 FOR 2)::integer > 59
       OR (raw ~ '[+-][0-9]{2}:[0-9]{2}$' AND
           (right(raw, 2)::integer > 59
            OR substring(right(raw, 6) FROM 2 FOR 2)::integer > 23)) THEN
        RETURN NULL;
    END IF;
    parsed := raw::timestamptz;
    IF NOT isfinite(parsed) OR extract(year FROM parsed) NOT BETWEEN 1 AND 9999 THEN
        RETURN NULL;
    END IF;
    RETURN parsed;
EXCEPTION WHEN OTHERS THEN RETURN NULL;
END;
$$;

-- The audit's immutable event/slot identities, unlike producer source clocks,
-- require an explicit UTC offset. Keep the two Python timestamp contracts apart.
CREATE OR REPLACE FUNCTION research_stage8_anchor_aware_timestamp_v1(value JSONB)
RETURNS TIMESTAMPTZ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
BEGIN
    IF jsonb_typeof(value) IS DISTINCT FROM 'string'
       OR (value #>> '{}') IS DISTINCT FROM research_stage8_anchor_strip_v1(value #>> '{}')
       OR (value #>> '{}') !~ '(Z|[+-][0-9]{2}:[0-9]{2})$' THEN RETURN NULL; END IF;
    RETURN research_stage8_anchor_timestamp_v1(value);
END;
$$;

CREATE OR REPLACE FUNCTION research_stage8_anchor_finite_number_v1(value JSONB)
RETURNS BOOLEAN LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT research_stage8_watch_finite_number_v1(value)
$$;

-- _number in the frozen-source producer also accepts numeric strings, but
-- never booleans, null, NaN or infinities.
CREATE OR REPLACE FUNCTION research_stage8_anchor_number_v1(value JSONB)
RETURNS DOUBLE PRECISION LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
DECLARE result DOUBLE PRECISION; raw TEXT;
BEGIN
    IF jsonb_typeof(value) IS NULL OR jsonb_typeof(value) NOT IN ('number','string')
       OR research_stage8_anchor_strip_v1(value #>> '{}') = '' THEN RETURN NULL; END IF;
    raw := value #>> '{}';
    IF jsonb_typeof(value) = 'string' THEN
        -- str.strip() removes these ASCII separators, but Python float()
        -- rejects them even at the boundary. Do not erase that distinction.
        IF raw ~ U&'[\001C-\001F]' THEN RETURN NULL; END IF;
        raw := research_stage8_anchor_strip_v1(raw);
        -- PostgreSQL float8 accepts hexadecimal strings that Python float()
        -- rejects. Admit only the producer's ASCII decimal syntax; underscores
        -- and non-ASCII digits intentionally remain fail-closed.
        IF raw !~ '^[+-]?(([0-9]+([.][0-9]*)?)|([.][0-9]+))([eE][+-]?[0-9]+)?$' THEN
            RETURN NULL;
        END IF;
    END IF;
    result := raw::double precision;
    IF result::text IN ('NaN','Infinity','-Infinity') THEN RETURN NULL; END IF;
    RETURN result;
EXCEPTION WHEN OTHERS THEN RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION research_stage8_anchor_round6_v1(value DOUBLE PRECISION)
RETURNS DOUBLE PRECISION LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE bytes BYTEA := float8send(value); exponent_bits INTEGER; exponent_value INTEGER;
        significand NUMERIC; numerator NUMERIC; denominator NUMERIC;
        quotient NUMERIC; remainder_value NUMERIC; sign_value INTEGER; byte_index INTEGER;
BEGIN
    exponent_bits := ((get_byte(bytes,0) & 127) * 16) + (get_byte(bytes,1) >> 4);
    IF exponent_bits = 2047 THEN RAISE EXCEPTION 'Anchor round requires finite input'; END IF;
    sign_value := CASE WHEN (get_byte(bytes,0) & 128) = 0 THEN 1 ELSE -1 END;
    significand := get_byte(bytes,1) & 15;
    FOR byte_index IN 2..7 LOOP significand := significand * 256 + get_byte(bytes,byte_index); END LOOP;
    IF exponent_bits = 0 THEN exponent_value := -1074;
    ELSE significand := significand + 4503599627370496; exponent_value := exponent_bits - 1075; END IF;
    IF exponent_value >= 0 THEN RETURN value; END IF;
    numerator := significand * 1000000;
    denominator := power(2::numeric,-exponent_value);
    quotient := div(numerator,denominator); remainder_value := mod(numerator,denominator);
    IF remainder_value * 2 > denominator OR
       (remainder_value * 2 = denominator AND mod(quotient,2) = 1) THEN quotient := quotient + 1; END IF;
    RETURN (sign_value * quotient / 1000000)::double precision;
END;
$$;

CREATE OR REPLACE FUNCTION research_stage8_anchor_coverage_valid_v1(
    value JSONB, symbol TEXT, decision_time TIMESTAMPTZ)
RETURNS BOOLEAN LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
DECLARE expected JSONB := '{"coverage_policy_version":"prospective-coverage-v3-completed-fully-validated-replay-run:no-dwell-first-touch-v6:historical-raw-opportunity-replay-v2-balanced-prior-session-width","method_version":"no-dwell-first-touch-v6","replay_version":"historical-raw-opportunity-replay-v2-balanced-prior-session-width","coverage_scope_version":"bounded-balanced-coherent-current-replay-all-horizons-v1","movement_width_calibration_version":"prior-only-session-width-v2","canonical_price_method_version":"canonical-spot-1m-ohlc-path-v3","canonical_price_provenance_version":"canonical-spot-reference-provenance-v1"}'::jsonb;
        field RECORD; horizon INTEGER; item JSONB; as_of_time TIMESTAMPTZ;
        completed TIMESTAMPTZ; first_time TIMESTAMPTZ; last_time TIMESTAMPTZ;
BEGIN
    IF jsonb_typeof(value) IS DISTINCT FROM 'object' OR decision_time IS NULL
       OR upper(research_stage8_anchor_strip_v1(value->>'symbol')) IS DISTINCT FROM symbol
       OR value->'eligible' IS DISTINCT FROM 'true'::jsonb
       OR value->'failed_gates' IS DISTINCT FROM '[]'::jsonb
       OR jsonb_typeof(value->'horizons') IS DISTINCT FROM 'object'
       OR jsonb_typeof(value->'replay_run_id') IS DISTINCT FROM 'number'
       OR COALESCE(value->>'replay_run_id','') !~ '^[0-9]+$'
       OR (value->>'replay_run_id')::numeric <= 0 THEN RETURN FALSE; END IF;
    FOR field IN SELECT * FROM jsonb_each(expected) LOOP
        IF value->field.key IS DISTINCT FROM field.value THEN RETURN FALSE; END IF;
    END LOOP;
    as_of_time := research_stage8_anchor_timestamp_v1(value->'as_of_utc');
    completed := research_stage8_anchor_timestamp_v1(value->'replay_completed_at_utc');
    IF as_of_time IS NULL OR completed IS NULL OR as_of_time > decision_time
       OR completed > as_of_time THEN RETURN FALSE; END IF;
    FOREACH horizon IN ARRAY ARRAY[60,240,720,1440] LOOP
        item := value->'horizons'->horizon::text;
        IF jsonb_typeof(item) IS DISTINCT FROM 'object'
           OR item->'eligible' IS DISTINCT FROM 'true'::jsonb
           OR item->'failed_gates' IS DISTINCT FROM '[]'::jsonb
           OR jsonb_typeof(item->'anchors') IS DISTINCT FROM 'number'
           OR COALESCE(item->>'anchors','') !~ '^[0-9]+$'
           OR (item->>'anchors')::numeric < 250
           OR jsonb_typeof(item->'utc_dates') IS DISTINCT FROM 'number'
           OR COALESCE(item->>'utc_dates','') !~ '^[0-9]+$'
           OR (item->>'utc_dates')::numeric < 14
           OR NOT research_stage8_anchor_finite_number_v1(item->'span_hours')
           OR (item->>'span_hours')::double precision < 336 THEN RETURN FALSE; END IF;
        first_time := research_stage8_anchor_timestamp_v1(item->'min_anchor_time_utc');
        last_time := research_stage8_anchor_timestamp_v1(item->'max_anchor_time_utc');
        IF first_time IS NULL OR last_time IS NULL OR first_time > last_time
           OR last_time > as_of_time OR last_time + make_interval(mins => horizon) > completed
           OR abs((item->>'span_hours')::double precision
                  - extract(epoch FROM last_time-first_time)::double precision / 3600.0) > 1e-6 THEN
            RETURN FALSE;
        END IF;
    END LOOP;
    RETURN TRUE;
EXCEPTION WHEN OTHERS THEN RETURN FALSE;
END;
$$;

CREATE OR REPLACE FUNCTION research_stage8_anchor_sources_valid_v1(
    frozen JSONB, timestamps JSONB, provenance JSONB, symbol TEXT,
    slot_open TIMESTAMPTZ, slot_close TIMESTAMPTZ,
    base_eligible TIMESTAMPTZ, decision_time TIMESTAMPTZ)
RETURNS BOOLEAN LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
DECLARE family TEXT; clock JSONB; prov JSONB; values_json JSONB; key TEXT;
        expected_keys TEXT[]; refresh_time TIMESTAMPTZ; observed TIMESTAMPTZ;
        upstream TIMESTAMPTZ; timestamp_mode TEXT; number_value DOUBLE PRECISION;
        exchanges TEXT[]; price_pair TEXT;
BEGIN
    IF jsonb_typeof(frozen) IS DISTINCT FROM 'object'
       OR jsonb_typeof(timestamps) IS DISTINCT FROM 'object'
       OR jsonb_typeof(provenance) IS DISTINCT FROM 'object'
       OR slot_open IS NULL OR slot_close IS NULL OR base_eligible IS NULL
       OR decision_time IS NULL THEN RETURN FALSE; END IF;
    FOREACH family IN ARRAY ARRAY['official_price','price_oi','futures_cvd','spot_cvd'] LOOP
        clock := timestamps->family; prov := provenance->family; values_json := frozen->family;
        expected_keys := CASE family WHEN 'official_price' THEN ARRAY['observed_at_utc','refresh_completed_at_utc']
            WHEN 'price_oi' THEN ARRAY['observation_time_utc','oi_fetched_at_utc','price_fetched_at_utc','refresh_completed_at_utc']
            ELSE ARRAY['refresh_completed_at_utc','source_candle_time_utc'] END;
        IF jsonb_typeof(clock) IS DISTINCT FROM 'object'
           OR jsonb_typeof(prov) IS DISTINCT FROM 'object'
           OR jsonb_typeof(values_json) IS DISTINCT FROM 'object'
           OR (SELECT array_agg(k ORDER BY k COLLATE "C") FROM jsonb_object_keys(clock) AS keys(k))
                IS DISTINCT FROM expected_keys
           OR EXISTS (SELECT 1 FROM jsonb_object_keys(prov) AS keys(k) WHERE k <> ALL(ARRAY[
                'source','quality_status','price_exchange','price_market','price_pair','price_instrument_id',
                'price_timeframe','exchange_list','upstream_source','source_table','source_record_id',
                'price_source','oi_source','fallback_used','fallback_policy','candle_timestamp_mode',
                'refresh_time_semantics','quality_status_basis']))
           OR upper(research_stage8_anchor_strip_v1(COALESCE((CASE
                WHEN research_stage8_anchor_truthy_v1(prov->'quality_status') THEN prov->'quality_status'
                WHEN research_stage8_anchor_truthy_v1(values_json->'quality_status') THEN values_json->'quality_status'
                ELSE values_json->'data_quality_status' END) #>> '{}',''))) <> 'PASS' THEN RETURN FALSE; END IF;
        -- Reconstruction overlays provenance/timestamps onto the frozen values,
        -- exactly as prospective_frozen_source_rows; absent provenance values
        -- can use the producer's frozen value-level provenance fallback.
        prov := (values_json || prov) || jsonb_build_object('source',CASE
            WHEN research_stage8_anchor_truthy_v1(prov->'source') THEN prov->'source'
            ELSE values_json->'source' END);
        refresh_time := research_stage8_anchor_timestamp_v1(clock->'refresh_completed_at_utc');
        IF refresh_time IS NULL OR refresh_time < base_eligible OR refresh_time > decision_time THEN RETURN FALSE; END IF;
        IF family = 'official_price' THEN
            observed := research_stage8_anchor_timestamp_v1(clock->'observed_at_utc');
            -- Python isalnum retains Unicode letters/digits. Never erase them
            -- into a different, apparently canonical ASCII instrument identity.
            IF octet_length(COALESCE(prov->>'price_pair','')) <> length(COALESCE(prov->>'price_pair','')) THEN
                RETURN FALSE;
            END IF;
            price_pair := regexp_replace(upper(research_stage8_anchor_strip_v1(COALESCE(prov->>'price_pair',''))),'[^A-Z0-9]','','g');
            IF observed IS NULL OR observed > decision_time OR decision_time-observed > interval '120 seconds'
               OR refresh_time < observed OR lower(research_stage8_anchor_strip_v1(COALESCE(prov->>'price_timeframe',''))) <> '1m'
               OR prov->'fallback_used' IS DISTINCT FROM 'false'::jsonb
               OR upper(research_stage8_anchor_strip_v1(COALESCE(prov->>'fallback_policy',''))) <> 'PROVIDER_ATTESTED_NO_FALLBACK'
               OR upper(research_stage8_anchor_strip_v1(COALESCE(prov->>'price_market',''))) <> 'SPOT'
               OR price_pair <> symbol || 'USDT' THEN RETURN FALSE; END IF;
            IF symbol = 'HYPE' THEN
                IF lower(research_stage8_anchor_strip_v1(COALESCE(prov->>'source',''))) <> 'hyperliquid_spot_@107'
                   OR upper(research_stage8_anchor_strip_v1(COALESCE(prov->>'price_exchange',''))) <> 'HYPERLIQUID'
                   OR upper(research_stage8_anchor_strip_v1(COALESCE(prov->>'price_instrument_id',''))) <> '@107' THEN RETURN FALSE; END IF;
            ELSIF lower(research_stage8_anchor_strip_v1(COALESCE(prov->>'source',''))) <> 'binance_spot'
               OR upper(research_stage8_anchor_strip_v1(COALESCE(prov->>'price_exchange',''))) <> 'BINANCE' THEN RETURN FALSE;
            END IF;
            expected_keys := ARRAY['price'];
        ELSIF family = 'price_oi' THEN
            observed := research_stage8_anchor_timestamp_v1(clock->'observation_time_utc');
            IF observed IS NULL OR observed < base_eligible OR observed > decision_time
               OR lower(research_stage8_anchor_strip_v1(COALESCE(prov->>'source_table',''))) <> 'oi_regime_snapshots'
               OR NOT research_stage8_anchor_nonempty_v1(prov->'price_source')
               OR NOT research_stage8_anchor_nonempty_v1(prov->'oi_source') THEN RETURN FALSE; END IF;
            FOREACH key IN ARRAY ARRAY['price_fetched_at_utc','oi_fetched_at_utc'] LOOP
                upstream := research_stage8_anchor_timestamp_v1(clock->key);
                IF upstream IS NULL OR upstream < slot_close OR upstream > observed THEN RETURN FALSE; END IF;
            END LOOP;
            expected_keys := ARRAY['price_close','oi_close_usd'];
        ELSE
            observed := research_stage8_anchor_timestamp_v1(clock->'source_candle_time_utc');
            timestamp_mode := lower(research_stage8_anchor_strip_v1(COALESCE(prov->>'candle_timestamp_mode','')));
            IF observed IS NULL OR timestamp_mode NOT IN ('open','close')
               OR observed IS DISTINCT FROM (CASE timestamp_mode WHEN 'open' THEN slot_open ELSE slot_close END)
               OR lower(research_stage8_anchor_strip_v1(COALESCE(prov->>'source',''))) IS DISTINCT FROM
                    (CASE family WHEN 'futures_cvd' THEN 'coinglass_futures_aggregated_cvd'
                                ELSE 'coinglass_spot_aggregated_cvd' END) THEN RETURN FALSE; END IF;
            SELECT array_agg(DISTINCT upper(research_stage8_anchor_strip_v1(part)) ORDER BY upper(research_stage8_anchor_strip_v1(part))) INTO exchanges
            FROM unnest(string_to_array(COALESCE(prov->>'exchange_list',''),',')) AS parts(part)
            WHERE research_stage8_anchor_strip_v1(part) <> '';
            IF exchanges IS DISTINCT FROM ARRAY['BINANCE','BYBIT','OKX'] THEN RETURN FALSE; END IF;
            expected_keys := ARRAY['continuous_cum_vol_delta_usd'];
        END IF;
        FOREACH key IN ARRAY expected_keys LOOP
            number_value := research_stage8_anchor_number_v1(values_json->key);
            IF number_value IS NULL OR (family IN ('official_price','price_oi') AND number_value <= 0) THEN
                RETURN FALSE;
            END IF;
        END LOOP;
    END LOOP;
    RETURN TRUE;
EXCEPTION WHEN OTHERS THEN RETURN FALSE;
END;
$$;

-- Pure anchor feature-width validation. Dependencies supplied by the anchor
-- validator: anchor_timestamp_v1(JSONB), anchor_finite_number_v1(JSONB), and
-- anchor_round6_v1(DOUBLE PRECISION), all prefixed research_stage8_.
-- These functions read neither source tables nor outcomes.
CREATE OR REPLACE FUNCTION research_stage8_anchor_session_ratios_v1(
    window_start TIMESTAMPTZ, window_end TIMESTAMPTZ
)
RETURNS TABLE(active_ratio DOUBLE PRECISION, weekend_ratio DOUBLE PRECISION, segments INTEGER)
LANGUAGE plpgsql
IMMUTABLE SECURITY INVOKER PARALLEL SAFE
AS $$
DECLARE
    current_day DATE;
    last_day DATE;
    boundary TIMESTAMPTZ;
    boundaries TIMESTAMPTZ[];
    left_point TIMESTAMPTZ;
    right_point TIMESTAMPTZ;
    local_point TIMESTAMP;
    local_weekday INTEGER;
    seconds_value DOUBLE PRECISION;
    active_seconds DOUBLE PRECISION := 0.0;
    weekend_seconds DOUBLE PRECISION := 0.0;
    total_seconds DOUBLE PRECISION;
BEGIN
    IF window_start IS NULL OR window_end IS NULL
       OR NOT isfinite(window_start) OR NOT isfinite(window_end) THEN
        RETURN;
    END IF;
    IF window_end <= window_start THEN
        active_ratio := 1.0; weekend_ratio := 0.0; segments := 0;
        RETURN NEXT; RETURN;
    END IF;
    boundaries := ARRAY[window_start, window_end];
    current_day := (window_start AT TIME ZONE 'America/New_York')::date - 1;
    last_day := (window_end AT TIME ZONE 'America/New_York')::date + 1;
    WHILE current_day <= last_day LOOP
        IF extract(isodow FROM current_day) = 5 THEN
            boundary := (current_day + time '20:00') AT TIME ZONE 'America/New_York';
        ELSIF extract(isodow FROM current_day) = 7 THEN
            boundary := (current_day + time '18:00') AT TIME ZONE 'America/New_York';
        ELSE
            boundary := NULL;
        END IF;
        IF boundary > window_start AND boundary < window_end THEN
            boundaries := array_append(boundaries, boundary);
        END IF;
        current_day := current_day + 1;
    END LOOP;
    left_point := window_start;
    FOR right_point IN SELECT DISTINCT p FROM unnest(boundaries) AS t(p) ORDER BY p LOOP
        IF right_point <= left_point THEN CONTINUE; END IF;
        seconds_value := extract(epoch FROM (right_point - left_point))::double precision;
        -- There is no boundary inside this half-open segment, so its left
        -- endpoint has the same session as Python's rounded midpoint.
        local_point := left_point AT TIME ZONE 'America/New_York';
        local_weekday := extract(isodow FROM local_point)::integer;
        IF local_weekday <= 4
           OR (local_weekday = 5 AND local_point::time < time '20:00')
           OR (local_weekday = 7 AND local_point::time >= time '18:00') THEN
            active_seconds := active_seconds + seconds_value;
        ELSE
            weekend_seconds := weekend_seconds + seconds_value;
        END IF;
        left_point := right_point;
    END LOOP;
    total_seconds := active_seconds + weekend_seconds;
    active_ratio := active_seconds / total_seconds;
    weekend_ratio := weekend_seconds / total_seconds;
    -- This is the Python diagnostic 30-minute segment count, not the number
    -- of exact calendar segments used above.
    segments := greatest(1, ceil(total_seconds / 1800.0)::integer);
    RETURN NEXT;
END;
$$;

CREATE OR REPLACE FUNCTION research_stage8_anchor_width_error_v1(
    reference JSONB, expected_symbol TEXT, event_time TIMESTAMPTZ, horizon_minutes INTEGER
)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE SECURITY INVOKER PARALLEL SAFE
AS $$
DECLARE
    field_name TEXT;
    symbol TEXT;
    as_of_utc TIMESTAMPTZ;
    composition_tolerance DOUBLE PRECISION;
    floor_scale DOUBLE PRECISION;
    threshold_scale DOUBLE PRECISION;
    applied BOOLEAN;
    active_ratio DOUBLE PRECISION;
    weekend_ratio DOUBLE PRECISION;
    segments INTEGER;
    composition TEXT;
    stored_active DOUBLE PRECISION;
    stored_weekend DOUBLE PRECISION;
    reason TEXT;
    evidence_names TEXT[] := ARRAY[
        'prior_points', 'session_matched_samples',
        'session_matched_effective_samples', 'active_reference_samples',
        'active_reference_effective_samples',
        'session_matched_abs_return_p90_pct', 'active_reference_abs_return_p90_pct'
    ];
    prior_points NUMERIC;
    matched_samples NUMERIC;
    active_samples NUMERIC;
    matched_effective DOUBLE PRECISION;
    active_effective DOUBLE PRECISION;
    matched_p90 DOUBLE PRECISION;
    active_p90 DOUBLE PRECISION;
    sufficient BOOLEAN;
    expected_scale DOUBLE PRECISION;
    expected_applied BOOLEAN;
    expected_reason TEXT;
BEGIN
    IF jsonb_typeof(reference) IS DISTINCT FROM 'object'
       OR event_time IS NULL OR NOT isfinite(event_time)
       OR horizon_minutes IS NULL THEN
        RETURN 'movement-width reference context is malformed';
    END IF;
    FOREACH field_name IN ARRAY ARRAY[
        'horizon_minutes', 'lookback_days', 'minimum_effective_samples', 'session_segments'
    ] LOOP
        IF jsonb_typeof(reference->field_name) IS DISTINCT FROM 'number'
           OR (reference->>field_name) !~ '^-?(0|[1-9][0-9]*)$' THEN
            RETURN 'movement-width integer fields are malformed';
        END IF;
    END LOOP;
    symbol := upper(research_stage8_anchor_strip_v1(coalesce(expected_symbol, '')));
    IF symbol = '' OR upper(research_stage8_anchor_strip_v1(coalesce(reference->>'symbol', ''))) <> symbol THEN
        RETURN 'movement-width symbol differs from the decision symbol';
    END IF;
    IF reference->>'calibration_version' IS DISTINCT FROM 'prior-only-session-width-v2' THEN
        RETURN 'movement-width calibration version is incompatible';
    END IF;
    IF reference->>'policy' IS DISTINCT FROM
       'prior raw price width; same-symbol session-composition matched; weekend width only' THEN
        RETURN 'movement-width policy is incompatible';
    END IF;
    IF upper(coalesce(reference->>'source_kind', '')) <> 'PRIOR_ONLY_SESSION_CALIBRATION' THEN
        RETURN 'movement-width source is not prior-only';
    END IF;
    IF (reference->>'horizon_minutes')::numeric <> horizon_minutes THEN
        RETURN 'movement-width horizon differs from formula horizon';
    END IF;
    IF research_stage8_anchor_finite_number_v1(reference->'composition_tolerance') IS NOT TRUE THEN
        RETURN 'movement-width calibration parameters are incompatible';
    END IF;
    composition_tolerance := (reference->>'composition_tolerance')::double precision;
    IF (reference->>'lookback_days')::numeric <> 180
       OR (reference->>'minimum_effective_samples')::numeric <> 30
       OR abs(composition_tolerance - 0.25::double precision) > 1e-12::double precision THEN
        RETURN 'movement-width calibration parameters are incompatible';
    END IF;
    as_of_utc := research_stage8_anchor_timestamp_v1(reference->'as_of_utc');
    IF as_of_utc IS NULL THEN RETURN 'movement-width reference context is malformed'; END IF;
    IF as_of_utc > event_time THEN
        RETURN 'movement-width calibration is newer than decision time';
    END IF;
    IF research_stage8_anchor_finite_number_v1(reference->'floor_scale_factor') IS NOT TRUE
       OR research_stage8_anchor_finite_number_v1(reference->'threshold_scale_factor') IS NOT TRUE THEN
        RETURN 'movement-width scale fields are invalid or inconsistent';
    END IF;
    floor_scale := (reference->>'floor_scale_factor')::double precision;
    threshold_scale := (reference->>'threshold_scale_factor')::double precision;
    IF threshold_scale < 0.50 OR threshold_scale > 1.00
       OR abs(floor_scale - threshold_scale) > 1e-12::double precision THEN
        RETURN 'movement-width scale fields are invalid or inconsistent';
    END IF;
    IF jsonb_typeof(reference->'applied') IS DISTINCT FROM 'boolean' THEN
        RETURN 'movement-width applied flag differs from scale';
    END IF;
    applied := (reference->>'applied')::boolean;
    IF applied <> (threshold_scale < 1.0::double precision - 1e-9::double precision) THEN
        RETURN 'movement-width applied flag differs from scale';
    END IF;
    SELECT ratios.active_ratio, ratios.weekend_ratio, ratios.segments
      INTO active_ratio, weekend_ratio, segments
      FROM research_stage8_anchor_session_ratios_v1(
          event_time, event_time + make_interval(mins => horizon_minutes)
      ) AS ratios;
    composition := CASE
        WHEN active_ratio >= 1.0::double precision - 1e-9::double precision THEN 'ACTIVE_ONLY'
        WHEN active_ratio <= 1e-9::double precision THEN 'WEEKEND_ONLY'
        ELSE 'MIXED'
    END;
    IF research_stage8_anchor_finite_number_v1(reference->'session_active_ratio') IS NOT TRUE
       OR research_stage8_anchor_finite_number_v1(reference->'session_weekend_ratio') IS NOT TRUE THEN
        RETURN 'movement-width session context differs from New York calendar';
    END IF;
    stored_active := (reference->>'session_active_ratio')::double precision;
    stored_weekend := (reference->>'session_weekend_ratio')::double precision;
    IF active_ratio IS NULL OR weekend_ratio IS NULL OR segments IS NULL
       OR abs(stored_active - active_ratio) > 1e-6::double precision
       OR abs(stored_weekend - weekend_ratio) > 1e-6::double precision
       OR (reference->>'session_segments')::numeric <> segments
       OR reference->>'session_composition' IS DISTINCT FROM composition THEN
        RETURN 'movement-width session context differs from New York calendar';
    END IF;
    IF weekend_ratio <= 1e-9::double precision
       AND threshold_scale < 1.0::double precision - 1e-9::double precision THEN
        RETURN 'ACTIVE-only horizon cannot relax movement width';
    END IF;
    reason := coalesce(reference->>'reason', '');
    IF reason = 'historical horizon unavailable' THEN
        IF reference ?| evidence_names OR abs(threshold_scale - 1.0::double precision) > 1e-12::double precision THEN
            RETURN 'unavailable movement-width history has forged evidence';
        END IF;
        RETURN NULL;
    END IF;
    IF NOT (reference ?& evidence_names) THEN
        RETURN 'movement-width evidence summary is incomplete';
    END IF;
    FOREACH field_name IN ARRAY ARRAY['prior_points', 'session_matched_samples', 'active_reference_samples'] LOOP
        IF jsonb_typeof(reference->field_name) IS DISTINCT FROM 'number'
           OR (reference->>field_name) !~ '^-?(0|[1-9][0-9]*)$'
           OR (reference->>field_name)::numeric < 0 THEN
            RETURN 'movement-width sample evidence is malformed';
        END IF;
    END LOOP;
    IF research_stage8_anchor_finite_number_v1(reference->'session_matched_effective_samples') IS NOT TRUE
       OR research_stage8_anchor_finite_number_v1(reference->'active_reference_effective_samples') IS NOT TRUE
       OR (reference->'session_matched_abs_return_p90_pct' <> 'null'::jsonb
           AND research_stage8_anchor_finite_number_v1(reference->'session_matched_abs_return_p90_pct') IS NOT TRUE)
       OR (reference->'active_reference_abs_return_p90_pct' <> 'null'::jsonb
           AND research_stage8_anchor_finite_number_v1(reference->'active_reference_abs_return_p90_pct') IS NOT TRUE) THEN
        RETURN 'movement-width sample evidence is malformed';
    END IF;
    prior_points := (reference->>'prior_points')::numeric;
    matched_samples := (reference->>'session_matched_samples')::numeric;
    active_samples := (reference->>'active_reference_samples')::numeric;
    matched_effective := (reference->>'session_matched_effective_samples')::double precision;
    active_effective := (reference->>'active_reference_effective_samples')::double precision;
    matched_p90 := (reference->>'session_matched_abs_return_p90_pct')::double precision;
    active_p90 := (reference->>'active_reference_abs_return_p90_pct')::double precision;
    IF matched_samples > prior_points OR active_samples > prior_points
       OR matched_effective < 0.0 OR active_effective < 0.0
       OR matched_effective > matched_samples::double precision + 1e-6::double precision
       OR active_effective > active_samples::double precision + 1e-6::double precision
       OR (matched_samples = 0) <> (matched_p90 IS NULL)
       OR (active_samples = 0) <> (active_p90 IS NULL)
       OR matched_p90 < 0.0 OR active_p90 < 0.0 THEN
        RETURN 'movement-width sample evidence is malformed';
    END IF;
    sufficient := matched_effective >= 30 AND active_effective >= 30
        AND matched_p90 IS NOT NULL AND matched_p90 >= 0.0
        AND active_p90 IS NOT NULL AND active_p90 > 0.0;
    IF NOT sufficient THEN
        IF reason <> 'insufficient prior-only width calibration evidence' THEN
            RETURN 'insufficient movement-width evidence has an invalid reason';
        END IF;
        IF abs(threshold_scale - 1.0::double precision) > 1e-12::double precision THEN
            RETURN 'insufficient movement-width evidence cannot relax width';
        END IF;
        RETURN NULL;
    END IF;
    IF weekend_ratio <= 1e-9::double precision THEN
        IF reason <> 'ACTIVE-only horizon keeps the static movement width'
           OR abs(threshold_scale - 1.0::double precision) > 1e-12::double precision THEN
            RETURN 'ACTIVE-only movement-width decision is inconsistent';
        END IF;
        RETURN NULL;
    END IF;
    -- Python permits an overflowing positive division to become infinity;
    -- clamp before division in this branch to obtain the same bounded result.
    expected_scale := CASE WHEN matched_p90 >= active_p90 THEN 1.0
        WHEN matched_p90 <= active_p90 * 0.50 THEN 0.50 ELSE
        research_stage8_anchor_round6_v1(greatest(0.50::double precision, matched_p90 / active_p90))
    END;
    expected_applied := expected_scale < 1.0::double precision - 1e-9::double precision;
    expected_reason := CASE WHEN expected_applied THEN
        'weekend/mixed width floor calibrated from prior raw price history'
        ELSE 'session width was not below the ACTIVE reference' END;
    IF abs(threshold_scale - expected_scale) > 1e-12::double precision
       OR applied IS DISTINCT FROM expected_applied OR reason <> expected_reason THEN
        RETURN 'movement-width scale does not match frozen evidence';
    END IF;
    RETURN NULL;
EXCEPTION WHEN OTHERS THEN
    RETURN 'movement-width reference context is malformed';
END;
$$;

CREATE OR REPLACE FUNCTION research_stage8_anchor_width_valid_v1(
    reference JSONB, expected_symbol TEXT, event_time TIMESTAMPTZ, horizon_minutes INTEGER
)
RETURNS BOOLEAN
LANGUAGE sql
IMMUTABLE SECURITY INVOKER PARALLEL SAFE
AS $$
    SELECT research_stage8_anchor_width_error_v1($1, $2, $3, $4) IS NULL;
$$;

-- Pure validation of the v4 producer's outcome-free, model-ABSENT feature bundle.
CREATE OR REPLACE FUNCTION research_stage8_anchor_feature_name_valid_v1(p_name TEXT)
RETURNS BOOLEAN LANGUAGE plpgsql IMMUTABLE SECURITY INVOKER AS $$
DECLARE
    parts TEXT[];
    field TEXT;
    ending TEXT;
BEGIN
    IF p_name IS NULL OR p_name = '' OR p_name <> research_stage8_anchor_strip_v1(p_name) THEN
        RETURN FALSE;
    END IF;
    IF p_name = ANY (ARRAY[
        'event.symbol', 'event.event_type', 'event.source_side', 'event.timeframe',
        'time.is_market_weekend', 'time.market_session', 'time.market_regime',
        'time.market_session_timezone', 'time.market_session_definition',
        'time.market_local_hour', 'time.market_local_minute',
        'time.market_local_weekday', 'time.market_local_weekday_name',
        'time.market_time_bucket', 'historical.event_market_session'
    ]) THEN RETURN TRUE; END IF;
    parts := string_to_array(p_name, '.');
    IF parts[1] = 'latest' THEN
        RETURN cardinality(parts) = 3
            AND parts[2] = ANY (ARRAY['price_oi', 'futures_cvd', 'spot_cvd'])
            AND parts[3] = 'buy_sell_ratio';
    ELSIF parts[1] = ANY (ARRAY['raw', 'aligned', 'aligned_log', 'historical']) THEN
        IF cardinality(parts) <> 3
           OR NOT (parts[2] = ANY (ARRAY['30m', '60m', '240m', '720m', '1440m']))
        THEN RETURN FALSE; END IF;
        field := parts[3];
        IF parts[1] = 'raw' THEN
            RETURN field = ANY (ARRAY[
                'session_active_ratio', 'session_weekend_ratio', 'session_composition',
                'price_change_pct', 'oi_change_pct', 'futures_continuous_cvd_change_usd',
                'spot_continuous_cvd_change_usd', 'futures_api_cvd_change_usd',
                'spot_api_cvd_change_usd', 'spot_to_futures_abs_cvd_ratio',
                'price_oi_state', 'spot_futures_alignment', 'price_spot_alignment',
                'price_futures_alignment'
            ]);
        ELSIF parts[1] = ANY (ARRAY['aligned', 'aligned_log']) THEN
            RETURN field = ANY (ARRAY[
                'price_change_pct', 'futures_continuous_cvd_change_usd',
                'spot_continuous_cvd_change_usd', 'futures_api_cvd_change_usd',
                'spot_api_cvd_change_usd'
            ]);
        ELSE
            RETURN field = ANY (ARRAY[
                'session_active_ratio', 'session_weekend_ratio', 'session_composition'
            ]) OR field ~ '^(price_change_pct|oi_change_pct|futures_continuous_cvd_change_usd|spot_continuous_cvd_change_usd)_(percentile_session_matched|abs_percentile_session_matched|median_session_matched|abs_median_session_matched)$';
        END IF;
    ELSIF parts[1] = 'max_pain' THEN
        IF cardinality(parts) = 3 AND parts[2] = 'aggregate' THEN
            RETURN parts[3] = ANY (ARRAY[
                'upside_active_timeframe_count', 'downside_active_timeframe_count',
                'closer_upside_count', 'closer_downside_count', 'consensus_direction',
                'consensus_count', 'consensus_ratio', 'upside_liquidity_usd',
                'downside_liquidity_usd', 'short_liquidity_usd', 'long_liquidity_usd',
                'upside_downside_liquidity_ratio', 'short_long_liquidity_ratio',
                'liquidity_imbalance_pct', 'median_upside_active_distance_pct',
                'median_downside_active_distance_pct', 'upside_cluster_count_1pct',
                'upside_cluster_spread_pct', 'upside_all_target_spread_pct',
                'downside_cluster_count_1pct', 'downside_cluster_spread_pct',
                'downside_all_target_spread_pct'
            ]);
        ELSIF p_name = 'max_pain.delta.minutes_since_previous_snapshot' THEN
            RETURN TRUE;
        ELSIF cardinality(parts) = 3 AND parts[2] = 'delta' THEN
            field := parts[3];
            FOREACH ending IN ARRAY ARRAY['_change_pct', '_change', '_trend'] LOOP
                IF right(field, length(ending)) = ending THEN
                    RETURN left(field, length(field) - length(ending)) = ANY (ARRAY[
                        'upside_liquidity_usd', 'downside_liquidity_usd',
                        'liquidity_imbalance_pct', 'closer_upside_count',
                        'closer_downside_count', 'upside_cluster_count_1pct',
                        'downside_cluster_count_1pct', 'upside_cluster_spread_pct',
                        'downside_cluster_spread_pct'
                    ]);
                END IF;
            END LOOP;
        ELSIF cardinality(parts) = 4 AND parts[2] = 'delta'
              AND parts[3] = ANY (ARRAY['12h', '24h', '48h', '3d', '1w', '2w', '1m'])
        THEN
            field := parts[4];
            FOREACH ending IN ARRAY ARRAY['_change', '_trend'] LOOP
                IF right(field, length(ending)) = ending THEN
                    RETURN left(field, length(field) - length(ending)) = ANY (ARRAY[
                        'upside_liquidity_usd', 'downside_liquidity_usd',
                        'upside_active_distance_pct', 'downside_active_distance_pct',
                        'short_target_signed_distance_pct', 'long_target_signed_distance_pct'
                    ]);
                END IF;
            END LOOP;
        ELSIF cardinality(parts) = 3
              AND parts[2] = ANY (ARRAY['12h', '24h', '48h', '3d', '1w', '2w', '1m'])
        THEN
            RETURN parts[3] = ANY (ARRAY[
                'short_target_signed_distance_pct', 'long_target_signed_distance_pct',
                'upside_active_distance_pct', 'downside_active_distance_pct',
                'upside_liquidity_usd', 'downside_liquidity_usd',
                'short_liquidity_usd', 'long_liquidity_usd',
                'upside_downside_liquidity_ratio', 'short_long_liquidity_ratio',
                'liquidity_imbalance_pct', 'closer_active_direction'
            ]);
        END IF;
    END IF;
    RETURN FALSE;
END;
$$;

CREATE OR REPLACE FUNCTION research_stage8_anchor_forbidden_bundle_key_v1(p_value JSONB)
RETURNS BOOLEAN LANGUAGE plpgsql IMMUTABLE SECURITY INVOKER AS $$
DECLARE item RECORD; element JSONB;
BEGIN
    IF p_value IS NULL THEN RETURN TRUE; END IF;
    IF jsonb_typeof(p_value) = 'object' THEN
        FOR item IN SELECT key, value FROM jsonb_each(p_value) LOOP
            IF item.key = '' OR lower(research_stage8_anchor_strip_v1(item.key)) = ANY (ARRAY[
                'outcome_label', 'mfe', 'mfe_pct', 'mae', 'mae_pct',
                'full_horizon_mae_pct', 'path_success', 'first_touch_status',
                'price_at_horizon', 'raw_return_pct', 'directional_return_pct',
                'target_reached', 'time_to_mfe_seconds', 'time_to_target_seconds',
                'time_to_first_progress_seconds'
            ]) OR research_stage8_anchor_forbidden_bundle_key_v1(item.value) THEN
                RETURN TRUE;
            END IF;
        END LOOP;
    ELSIF jsonb_typeof(p_value) = 'array' THEN
        FOR element IN SELECT value FROM jsonb_array_elements(p_value) LOOP
            IF research_stage8_anchor_forbidden_bundle_key_v1(element) THEN RETURN TRUE; END IF;
        END LOOP;
    END IF;
    RETURN FALSE;
EXCEPTION WHEN OTHERS THEN RETURN TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION research_stage8_anchor_series_valid_v1(
    p_manifest JSONB, p_decision TIMESTAMPTZ
) RETURNS BOOLEAN LANGUAGE plpgsql IMMUTABLE SECURITY INVOKER AS $$
DECLARE
    n NUMERIC;
    first_time TIMESTAMPTZ;
    last_time TIMESTAMPTZ;
    versions JSONB;
    canonical_versions JSONB;
    version_count INTEGER;
BEGIN
    IF p_manifest IS NULL OR p_decision IS NULL
       OR jsonb_typeof(p_manifest) <> 'object' THEN RETURN FALSE; END IF;
    IF NOT (p_manifest ?& ARRAY['count', 'first_decision_time_utc',
        'last_decision_time_utc', 'sha256', 'sampler_versions'])
       OR (SELECT count(*) FROM jsonb_object_keys(p_manifest)) <> 5
       OR jsonb_typeof(p_manifest->'count') IS DISTINCT FROM 'number'
       OR (p_manifest->>'count') !~ '^(0|[1-9][0-9]*)$'
       OR jsonb_typeof(p_manifest->'sha256') IS DISTINCT FROM 'string'
       OR (p_manifest->>'sha256') !~ '^[0-9a-f]{64}$'
       OR jsonb_typeof(p_manifest->'sampler_versions') IS DISTINCT FROM 'array'
    THEN RETURN FALSE; END IF;
    n := (p_manifest->>'count')::NUMERIC;
    versions := p_manifest->'sampler_versions';
    IF EXISTS (
        SELECT 1 FROM jsonb_array_elements(versions) AS e(value)
        WHERE jsonb_typeof(e.value) <> 'string'
           OR (e.value #>> '{}') NOT IN (
                'prospective-neutral-anchor-v3-max-pain-frozen',
                'prospective-neutral-anchor-v4-decision-features-frozen')
    ) THEN RETURN FALSE; END IF;
    SELECT count(*), coalesce(jsonb_agg(v ORDER BY v COLLATE "C"), '[]'::JSONB)
      INTO version_count, canonical_versions
      FROM (SELECT DISTINCT value #>> '{}' AS v FROM jsonb_array_elements(versions)) AS s;
    IF jsonb_array_length(versions) <> version_count OR versions <> canonical_versions
    THEN RETURN FALSE; END IF;
    IF n = 0 THEN
        RETURN p_manifest->'first_decision_time_utc' = 'null'::JSONB
           AND p_manifest->'last_decision_time_utc' = 'null'::JSONB
           AND versions = '[]'::JSONB;
    END IF;
    first_time := research_stage8_anchor_timestamp_v1(p_manifest->'first_decision_time_utc');
    last_time := research_stage8_anchor_timestamp_v1(p_manifest->'last_decision_time_utc');
    RETURN coalesce(first_time IS NOT NULL AND last_time IS NOT NULL
        AND first_time <= last_time AND last_time <= p_decision
        AND version_count > 0
        AND p_manifest->>'first_decision_time_utc' = research_stage8_utc_text_v1(first_time)
        AND p_manifest->>'last_decision_time_utc' = research_stage8_utc_text_v1(last_time), FALSE);
EXCEPTION WHEN OTHERS THEN RETURN FALSE;
END;
$$;

CREATE OR REPLACE FUNCTION research_stage8_anchor_bundle_valid_v1(
    p_bundle JSONB, p_symbol TEXT, p_decision TIMESTAMPTZ, p_expected_hash TEXT
) RETURNS BOOLEAN LANGUAGE plpgsql IMMUTABLE SECURITY INVOKER AS $$
DECLARE
    bundle_symbol TEXT;
    bundle_time TIMESTAMPTZ;
    features JSONB;
    direction TEXT;
    feature RECORD;
    horizon INTEGER;
    context JSONB;
    session_context JSONB;
    width JSONB;
    active_ratio DOUBLE PRECISION;
    weekend_ratio DOUBLE PRECISION;
    composition TEXT;
BEGIN
    IF p_bundle IS NULL OR p_symbol IS NULL OR p_decision IS NULL OR p_expected_hash IS NULL
       OR jsonb_typeof(p_bundle) <> 'object' THEN RETURN FALSE; END IF;
    IF NOT (p_bundle ?& ARRAY[
        'bundle_schema_version', 'feature_policy_version', 'feature_schema_version',
        'decision_time_utc', 'symbol', 'source_series_manifest', 'features_by_direction',
        'horizon_context', 'model_score_status'])
       OR (SELECT count(*) FROM jsonb_object_keys(p_bundle)) <> 9
       OR p_bundle->>'bundle_schema_version' IS DISTINCT FROM 'prospective-decision-feature-bundle-schema-v1'
       OR p_bundle->>'feature_policy_version' IS DISTINCT FROM 'prospective-decision-feature-bundle-v1'
       OR p_bundle->>'model_score_status' IS DISTINCT FROM 'ABSENT'
       OR NOT research_stage8_anchor_nonempty_v1(p_bundle->'feature_schema_version')
       OR jsonb_typeof(p_bundle->'symbol') IS DISTINCT FROM 'string'
       OR lower(research_stage8_anchor_strip_v1(p_expected_hash)) !~ '^[0-9a-f]{64}$'
    THEN RETURN FALSE; END IF;
    bundle_symbol := upper(research_stage8_anchor_strip_v1(p_bundle->>'symbol'));
    IF bundle_symbol !~ '^[A-Z0-9-]{1,20}$'
       OR replace(bundle_symbol, '-', '') = ''
       OR bundle_symbol <> upper(research_stage8_anchor_strip_v1(p_symbol)) THEN RETURN FALSE; END IF;
    bundle_time := research_stage8_anchor_timestamp_v1(p_bundle->'decision_time_utc');
    IF bundle_time IS NULL OR bundle_time <> p_decision
       OR p_bundle->>'decision_time_utc' IS DISTINCT FROM research_stage8_utc_text_v1(bundle_time)
       OR research_stage8_anchor_forbidden_bundle_key_v1(p_bundle)
       OR NOT research_stage8_anchor_series_valid_v1(p_bundle->'source_series_manifest', bundle_time)
    THEN RETURN FALSE; END IF;
    features := p_bundle->'features_by_direction';
    IF jsonb_typeof(features) IS DISTINCT FROM 'object'
       OR NOT (features ?& ARRAY['LONG', 'SHORT'])
       OR (SELECT count(*) FROM jsonb_object_keys(features)) <> 2
    THEN RETURN FALSE; END IF;
    FOREACH direction IN ARRAY ARRAY['LONG', 'SHORT'] LOOP
        IF jsonb_typeof(features->direction) IS DISTINCT FROM 'object' THEN RETURN FALSE; END IF;
        FOR feature IN SELECT key, value FROM jsonb_each(features->direction) LOOP
            IF NOT research_stage8_anchor_feature_name_valid_v1(feature.key)
               OR jsonb_typeof(feature.value) NOT IN ('boolean', 'number', 'string')
               OR (jsonb_typeof(feature.value) = 'number' AND feature.value::text LIKE '%.%'
                   AND NOT research_stage8_anchor_finite_number_v1(feature.value))
            THEN RETURN FALSE; END IF;
        END LOOP;
    END LOOP;
    IF jsonb_typeof(p_bundle->'horizon_context') IS DISTINCT FROM 'object'
       OR NOT ((p_bundle->'horizon_context') ?& ARRAY['60', '240', '720', '1440'])
       OR (SELECT count(*) FROM jsonb_object_keys(p_bundle->'horizon_context')) <> 4
    THEN RETURN FALSE; END IF;
    FOREACH horizon IN ARRAY ARRAY[60, 240, 720, 1440] LOOP
        context := p_bundle->'horizon_context'->horizon::TEXT;
        IF jsonb_typeof(context) IS DISTINCT FROM 'object'
           OR NOT (context ?& ARRAY['session', 'movement_width_reference'])
           OR (SELECT count(*) FROM jsonb_object_keys(context)) <> 2
        THEN RETURN FALSE; END IF;
        session_context := context->'session';
        IF jsonb_typeof(session_context) IS DISTINCT FROM 'object'
           OR NOT (session_context ?& ARRAY['active_ratio', 'weekend_ratio', 'composition', 'segments'])
           OR (SELECT count(*) FROM jsonb_object_keys(session_context)) <> 4
           OR NOT research_stage8_anchor_finite_number_v1(session_context->'active_ratio')
           OR NOT research_stage8_anchor_finite_number_v1(session_context->'weekend_ratio')
           OR jsonb_typeof(session_context->'segments') IS DISTINCT FROM 'number'
           OR (session_context->>'segments') !~ '^(0|[1-9][0-9]*)$'
        THEN RETURN FALSE; END IF;
        active_ratio := (session_context->>'active_ratio')::DOUBLE PRECISION;
        weekend_ratio := (session_context->>'weekend_ratio')::DOUBLE PRECISION;
        composition := CASE WHEN active_ratio >= 1.0 - 1e-9 THEN 'ACTIVE_ONLY'
                            WHEN active_ratio <= 1e-9 THEN 'WEEKEND_ONLY' ELSE 'MIXED' END;
        IF abs(active_ratio + weekend_ratio - 1.0) > 1e-6
           OR session_context->>'composition' IS DISTINCT FROM composition
        THEN RETURN FALSE; END IF;
        width := context->'movement_width_reference';
        IF research_stage8_anchor_width_valid_v1(width, bundle_symbol, bundle_time, horizon) IS DISTINCT FROM TRUE
           OR width->>'as_of_utc' IS DISTINCT FROM research_stage8_utc_text_v1(
                research_stage8_anchor_timestamp_v1(width->'as_of_utc'))
        THEN RETURN FALSE; END IF;
        IF abs(active_ratio - (width->>'session_active_ratio')::DOUBLE PRECISION) > 1e-6
           OR abs(weekend_ratio - (width->>'session_weekend_ratio')::DOUBLE PRECISION) > 1e-6
           OR session_context->>'composition' IS DISTINCT FROM width->>'session_composition'
           OR (session_context->>'segments')::NUMERIC IS DISTINCT FROM (width->>'session_segments')::NUMERIC
        THEN RETURN FALSE; END IF;
    END LOOP;
    RETURN coalesce(research_stage8_anchor_json_sha256_v1(p_bundle) = lower(research_stage8_anchor_strip_v1(p_expected_hash)), FALSE);
EXCEPTION WHEN OTHERS THEN RETURN FALSE;
END;
$$;

-- All inputs below are read by the pinned derivation from the durable source
-- graph. This pure function grants no authority to a caller-supplied receipt.
CREATE OR REPLACE FUNCTION research_stage8_anchor_errors_v1(
    attempt JSONB, slot JSONB, events JSONB)
RETURNS TEXT[] LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
SET TimeZone = 'UTC'
AS $$
DECLARE errors TEXT[] := ARRAY[]::TEXT[]; field TEXT; direction TEXT;
        opened TIMESTAMPTZ; closed TIMESTAMPTZ; base_time TIMESTAMPTZ;
        expires TIMESTAMPTZ; decision TIMESTAMPTZ; event JSONB; ref JSONB;
        input_payload JSONB; fingerprint TEXT; anchor_key TEXT;
        expected_event_id JSONB; official_price DOUBLE PRECISION; event_price DOUBLE PRECISION;
        symbol TEXT; policy TEXT := 'prospective-coverage-v3-completed-fully-validated-replay-run:no-dwell-first-touch-v6:historical-raw-opportunity-replay-v2-balanced-prior-session-width';
BEGIN
    IF jsonb_typeof(attempt) IS DISTINCT FROM 'object'
       OR attempt->>'sampler_version' IS DISTINCT FROM 'prospective-neutral-anchor-v4-decision-features-frozen'
       OR attempt->>'coverage_policy_version' IS DISTINCT FROM policy
       OR COALESCE(attempt->>'evaluation_status','') NOT IN ('EVALUABLE','UNEVALUABLE','COVERAGE_EXCLUDED')
       OR NOT research_stage8_anchor_nonempty_v1(attempt->'evaluation_reason')
       OR jsonb_typeof(attempt->'attempt_id') IS DISTINCT FROM 'number'
       OR COALESCE(attempt->>'attempt_id','') !~ '^[0-9]+$'
       OR (attempt->>'attempt_id')::numeric NOT BETWEEN 1 AND 9223372036854775807 THEN
        RETURN ARRAY['ANCHOR_ATTEMPT_AUTHORITY_INVALID'];
    END IF;
    IF attempt->>'evaluation_status' <> 'EVALUABLE' THEN
        IF COALESCE(attempt->'decision_time_utc','null'::jsonb) <> 'null'::jsonb THEN
            RETURN ARRAY['ANCHOR_NON_EVALUABLE_HAS_DECISION'];
        END IF;
        RETURN errors;
    END IF;
    IF jsonb_typeof(slot) IS DISTINCT FROM 'object'
       OR jsonb_typeof(slot->'anchor_slot_id') IS DISTINCT FROM 'number'
       OR COALESCE(slot->>'anchor_slot_id','') !~ '^[0-9]+$'
       OR (slot->>'anchor_slot_id')::numeric NOT BETWEEN 1 AND 9223372036854775807 THEN
        RETURN ARRAY['ANCHOR_SLOT_INVALID'];
    END IF;
    FOREACH field IN ARRAY ARRAY['sampler_version','coverage_policy_version','symbol','interval_minutes',
        'feature_bundle_policy_version','feature_bundle_sha256','input_fingerprint',
        'coverage_snapshot','source_timestamps','source_provenance','frozen_inputs'] LOOP
        IF research_stage8_anchor_canonical_json_v1(attempt->field) IS DISTINCT FROM
           research_stage8_anchor_canonical_json_v1(slot->field) THEN
            errors := array_append(errors,'ANCHOR_ATTEMPT_SLOT_MISMATCH:' || field);
        END IF;
    END LOOP;
    FOREACH field IN ARRAY ARRAY['source_candle_open_utc','source_candle_close_utc',
        'base_eligible_at_utc','expires_at_utc','decision_time_utc'] LOOP
        IF research_stage8_anchor_aware_timestamp_v1(attempt->field) IS NULL
           OR research_stage8_anchor_aware_timestamp_v1(attempt->field) IS DISTINCT FROM
              research_stage8_anchor_aware_timestamp_v1(slot->field) THEN
            errors := array_append(errors,'ANCHOR_ATTEMPT_SLOT_TIME_MISMATCH:' || field);
        END IF;
    END LOOP;
    opened := research_stage8_anchor_aware_timestamp_v1(slot->'source_candle_open_utc');
    closed := research_stage8_anchor_aware_timestamp_v1(slot->'source_candle_close_utc');
    base_time := research_stage8_anchor_aware_timestamp_v1(slot->'base_eligible_at_utc');
    expires := research_stage8_anchor_aware_timestamp_v1(slot->'expires_at_utc');
    decision := research_stage8_anchor_aware_timestamp_v1(slot->'decision_time_utc');
    symbol := slot->>'symbol';
    IF opened IS NULL OR decision IS NULL
       OR slot->'interval_minutes' IS DISTINCT FROM '30'::jsonb
       OR date_trunc('minute',opened) IS DISTINCT FROM opened
       OR extract(minute FROM opened)::integer % 30 <> 0
       OR closed IS DISTINCT FROM opened + interval '30 minutes'
       OR base_time IS DISTINCT FROM opened + interval '32 minutes'
       OR expires IS DISTINCT FROM opened + interval '62 minutes'
       OR decision < base_time OR decision >= expires
       OR research_stage8_anchor_aware_timestamp_v1(attempt->'checked_at_utc') IS DISTINCT FROM decision
       OR attempt->'missing_sources' IS DISTINCT FROM '[]'::jsonb
       OR symbol IS NULL OR symbol !~ '^[A-Z0-9-]{1,20}$'
       OR replace(symbol,'-','') = '' THEN
        errors := array_append(errors,'ANCHOR_DECISION_INTERVAL_INVALID');
    END IF;
    IF NOT research_stage8_anchor_coverage_valid_v1(slot->'coverage_snapshot',symbol,decision) THEN
        errors := array_append(errors,'ANCHOR_COVERAGE_INVALID');
    END IF;
    IF NOT research_stage8_anchor_sources_valid_v1(slot->'frozen_inputs',slot->'source_timestamps',
            slot->'source_provenance',symbol,opened,closed,base_time,decision) THEN
        errors := array_append(errors,'ANCHOR_SOURCE_INVALID');
    END IF;
    IF slot->>'feature_bundle_policy_version' IS DISTINCT FROM 'prospective-decision-feature-bundle-v1'
       OR NOT research_stage8_anchor_bundle_valid_v1(slot->'decision_feature_bundle',symbol,
            decision,slot->>'feature_bundle_sha256') THEN
        errors := array_append(errors,'ANCHOR_FEATURE_BUNDLE_INVALID');
    END IF;
    input_payload := jsonb_build_object(
        'sampler_version',slot->'sampler_version','coverage_policy_version',slot->'coverage_policy_version',
        'coverage_snapshot',slot->'coverage_snapshot','symbol',symbol,
        'source_candle_open_utc',research_stage8_utc_text_v1(opened),
        'source_candle_close_utc',research_stage8_utc_text_v1(closed),
        'base_eligible_at_utc',research_stage8_utc_text_v1(base_time),
        'expires_at_utc',research_stage8_utc_text_v1(expires),
        'evaluation_status','EVALUABLE','decision_time_utc',research_stage8_utc_text_v1(decision),
        'source_timestamps',slot->'source_timestamps','source_provenance',slot->'source_provenance',
        'frozen_formula_visible_inputs',slot->'frozen_inputs',
        'feature_bundle_policy_version',slot->'feature_bundle_policy_version',
        'feature_bundle_sha256',slot->'feature_bundle_sha256');
    fingerprint := research_stage8_anchor_json_sha256_v1(input_payload);
    IF fingerprint IS DISTINCT FROM research_stage8_anchor_strip_v1(slot->>'input_fingerprint')
       OR fingerprint IS DISTINCT FROM research_stage8_anchor_strip_v1(attempt->>'input_fingerprint') THEN
        errors := array_append(errors,'ANCHOR_INPUT_FINGERPRINT_MISMATCH');
    END IF;
    IF jsonb_typeof(events) IS DISTINCT FROM 'array' OR jsonb_array_length(events) <> 2
       OR slot->'long_event_id' IS NOT DISTINCT FROM slot->'short_event_id' THEN
        RETURN array_append(errors,'ANCHOR_EXACT_EVENT_PAIR_MISMATCH');
    END IF;
    anchor_key := research_stage8_anchor_json_sha256_v1(jsonb_build_object(
        'sampler_version',slot->'sampler_version','symbol',symbol,
        'source_candle_open_utc',research_stage8_utc_text_v1(opened)));
    official_price := research_stage8_anchor_number_v1(slot->'frozen_inputs'->'official_price'->'price');
    FOREACH direction IN ARRAY ARRAY['LONG','SHORT'] LOOP
        expected_event_id := slot->CASE direction WHEN 'LONG' THEN 'long_event_id' ELSE 'short_event_id' END;
        IF jsonb_typeof(expected_event_id) IS DISTINCT FROM 'number'
           OR COALESCE(expected_event_id #>> '{}','') !~ '^[0-9]+$'
           OR (expected_event_id #>> '{}')::numeric NOT BETWEEN 1 AND 9223372036854775807
           OR (SELECT count(*) FROM jsonb_array_elements(events) AS e(value)
               WHERE e.value->'event_id' = expected_event_id) <> 1 THEN
            errors := array_append(errors,'ANCHOR_EVENT_ID_INVALID:' || direction);
            CONTINUE;
        END IF;
        SELECT value INTO event FROM jsonb_array_elements(events) AS e(value)
        WHERE value->'event_id' = expected_event_id;
        ref := event->'engine_snapshot'->'prospective_anchor';
        IF jsonb_typeof(event) IS DISTINCT FROM 'object'
           OR event->>'schema_version' IS DISTINCT FROM 'research-event-v1'
           OR event->>'direction' IS DISTINCT FROM direction OR event->>'symbol' IS DISTINCT FROM symbol
           OR event->>'event_kind' IS DISTINCT FROM 'DECISION_SAMPLE'
           OR event->>'event_type' IS DISTINCT FROM 'PROSPECTIVE_NEUTRAL_30M'
           OR event->>'source_side' IS DISTINCT FROM 'RAW_NEUTRAL'
           OR event->>'timeframe' IS DISTINCT FROM '30m'
           OR event->>'capture_stage' IS DISTINCT FROM 'SILENT_NEUTRAL_ANCHOR'
           OR event->>'strategy_version' IS DISTINCT FROM 'formula-prospective-neutral-v4'
           OR event->>'delivery_status' IS DISTINCT FROM 'NOT_APPLICABLE'
           OR length(COALESCE(event->>'setup_key','')) <> 64
           OR octet_length(convert_to(research_stage8_anchor_canonical_json_v1(
                event->'engine_snapshot'),'UTF8')) > 32000
           OR research_stage8_anchor_aware_timestamp_v1(event->'alert_time_utc') IS DISTINCT FROM decision
           OR jsonb_typeof(ref) IS DISTINCT FROM 'object'
           OR ref ? 'decision_feature_bundle'
           OR ref->>'anchor_key' IS DISTINCT FROM anchor_key
           OR ref->>'sampling_frame' IS DISTINCT FROM 'NEUTRAL_30M_BOTH_DIRECTIONS'
           OR ref->>'delivery_status' IS DISTINCT FROM 'NOT_APPLICABLE'
           OR ref->'telegram_delivery_allowed' IS DISTINCT FROM 'false'::jsonb
           OR ref->'trade_execution_allowed' IS DISTINCT FROM 'false'::jsonb
           OR ref->'coverage_eligible' IS DISTINCT FROM 'true'::jsonb
           OR COALESCE(event->'score','null'::jsonb) <> 'null'::jsonb
           OR COALESCE(event->'target_price','null'::jsonb) <> 'null'::jsonb
           OR COALESCE(event->'initial_target_distance_pct','null'::jsonb) <> 'null'::jsonb THEN
            errors := array_append(errors,'ANCHOR_EVENT_AUTHORITY_INVALID:' || direction);
        END IF;
        IF event->>'event_fingerprint' IS DISTINCT FROM research_stage8_anchor_json_sha256_v1(jsonb_build_object(
            'sampler_version',slot->'sampler_version','event_type','PROSPECTIVE_NEUTRAL_30M',
            'symbol',symbol,'direction',direction,'source_candle_open_utc',research_stage8_utc_text_v1(opened))) THEN
            errors := array_append(errors,'ANCHOR_EVENT_FINGERPRINT_MISMATCH:' || direction);
        END IF;
        FOREACH field IN ARRAY ARRAY['sampler_version','coverage_policy_version','coverage_snapshot',
            'input_fingerprint','source_timestamps','source_provenance','frozen_inputs',
            'feature_bundle_policy_version','feature_bundle_sha256'] LOOP
            IF research_stage8_anchor_reference_canonical_json_v1(ref->field) IS DISTINCT FROM
               research_stage8_anchor_reference_canonical_json_v1(slot->field) THEN
                errors := array_append(errors,'ANCHOR_EVENT_REFERENCE_MISMATCH:' || direction || ':' || field);
            END IF;
        END LOOP;
        FOREACH field IN ARRAY ARRAY['source_candle_open_utc','source_candle_close_utc',
            'base_eligible_at_utc','expires_at_utc','decision_time_utc'] LOOP
            IF research_stage8_anchor_aware_timestamp_v1(ref->field) IS NULL
               OR research_stage8_anchor_aware_timestamp_v1(ref->field) IS DISTINCT FROM
                  research_stage8_anchor_aware_timestamp_v1(slot->field) THEN
                errors := array_append(errors,'ANCHOR_EVENT_REFERENCE_TIME_MISMATCH:' || direction || ':' || field);
            END IF;
        END LOOP;
        event_price := research_stage8_anchor_number_v1(event->'current_price');
        IF official_price IS NULL OR event_price IS NULL
           OR abs(event_price-official_price) > 1e-12 * greatest(abs(event_price),abs(official_price)) THEN
            errors := array_append(errors,'ANCHOR_REFERENCE_PRICE_MISMATCH:' || direction);
        END IF;
    END LOOP;
    RETURN errors;
EXCEPTION WHEN OTHERS THEN RETURN array_append(errors,'ANCHOR_AUTHORITY_MALFORMED');
END;
$$;

CREATE OR REPLACE FUNCTION research_stage8_expected_scope_v1(value TEXT)
RETURNS JSONB
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
    SELECT CASE value
        WHEN 'BINANCE_BTC' THEN '{"scope_id":"BINANCE_BTC","symbols":["BTC"],"price_route":"BINANCE_SPOT_1M"}'::jsonb
        WHEN 'BINANCE_ETH' THEN '{"scope_id":"BINANCE_ETH","symbols":["ETH"],"price_route":"BINANCE_SPOT_1M"}'::jsonb
        WHEN 'BINANCE_SOL' THEN '{"scope_id":"BINANCE_SOL","symbols":["SOL"],"price_route":"BINANCE_SPOT_1M"}'::jsonb
        WHEN 'BINANCE_DOGE' THEN '{"scope_id":"BINANCE_DOGE","symbols":["DOGE"],"price_route":"BINANCE_SPOT_1M"}'::jsonb
        WHEN 'BINANCE_ZEC' THEN '{"scope_id":"BINANCE_ZEC","symbols":["ZEC"],"price_route":"BINANCE_SPOT_1M"}'::jsonb
        WHEN 'BINANCE_BNB' THEN '{"scope_id":"BINANCE_BNB","symbols":["BNB"],"price_route":"BINANCE_SPOT_1M"}'::jsonb
        WHEN 'BINANCE_XRP' THEN '{"scope_id":"BINANCE_XRP","symbols":["XRP"],"price_route":"BINANCE_SPOT_1M"}'::jsonb
        WHEN 'ALL_BINANCE7' THEN '{"scope_id":"ALL_BINANCE7","symbols":["BTC","ETH","SOL","DOGE","ZEC","BNB","XRP"],"price_route":"BINANCE_SPOT_1M"}'::jsonb
        WHEN 'HYPE_SPOT_107' THEN '{"scope_id":"HYPE_SPOT_107","symbols":["HYPE"],"price_route":"HYPERLIQUID_SPOT_@107_1M"}'::jsonb
        ELSE NULL
    END
$$;

CREATE OR REPLACE FUNCTION research_stage8_expected_candidate_v1(value TEXT)
RETURNS JSONB
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
    SELECT CASE value
        WHEN 'POSITIONING_ALIGNED65_LONG' THEN '{"candidate_id":"POSITIONING_ALIGNED65_LONG","model":"positioning","direction":"LONG","feature":"watch.models.positioning.aligned_score","operator":">=","value":65}'::jsonb
        WHEN 'POSITIONING_ALIGNED65_SHORT' THEN '{"candidate_id":"POSITIONING_ALIGNED65_SHORT","model":"positioning","direction":"SHORT","feature":"watch.models.positioning.aligned_score","operator":">=","value":65}'::jsonb
        WHEN 'FUTURES_FLOW_ALIGNED65_LONG' THEN '{"candidate_id":"FUTURES_FLOW_ALIGNED65_LONG","model":"futures_flow","direction":"LONG","feature":"watch.models.futures_flow.aligned_score","operator":">=","value":65}'::jsonb
        WHEN 'FUTURES_FLOW_ALIGNED65_SHORT' THEN '{"candidate_id":"FUTURES_FLOW_ALIGNED65_SHORT","model":"futures_flow","direction":"SHORT","feature":"watch.models.futures_flow.aligned_score","operator":">=","value":65}'::jsonb
        WHEN 'SPOT_FLOW_ALIGNED65_LONG' THEN '{"candidate_id":"SPOT_FLOW_ALIGNED65_LONG","model":"spot_flow","direction":"LONG","feature":"watch.models.spot_flow.aligned_score","operator":">=","value":65}'::jsonb
        WHEN 'SPOT_FLOW_ALIGNED65_SHORT' THEN '{"candidate_id":"SPOT_FLOW_ALIGNED65_SHORT","model":"spot_flow","direction":"SHORT","feature":"watch.models.spot_flow.aligned_score","operator":">=","value":65}'::jsonb
        ELSE NULL
    END
$$;

CREATE TABLE IF NOT EXISTS research_stage8_binding_registry (
    exact_binding JSONB NOT NULL CHECK (jsonb_typeof(exact_binding) = 'object'),
    exact_binding_sha256 TEXT PRIMARY KEY CHECK (exact_binding_sha256 ~ '^[0-9a-f]{64}$'),
    manifest_sha256 TEXT NOT NULL CHECK (manifest_sha256 = '5a3ee3af6a73467f3ead09fbe9684a8f60101f97fa7064472b228a31468e6bef'),
    contract_version TEXT NOT NULL CHECK (contract_version = 'stage8-operational-model-contract-v1'),
    hash_version TEXT NOT NULL CHECK (hash_version = 'stage8-strict-json-sha256-v1'),
    source_version TEXT NOT NULL CHECK (source_version = 'stage8-neutral-v4-watch-v2-source-v1'),
    source_audit_version TEXT NOT NULL CHECK (source_audit_version = 'operational-score-source-audit-v2'),
    projection_version TEXT NOT NULL CHECK (projection_version = 'stage8-watch-signed-model-sidecar-v1'),
    candidate_version TEXT NOT NULL CHECK (candidate_version = 'stage8-first-tranche-single-model-aligned65-60m-v1'),
    label_version TEXT NOT NULL CHECK (label_version = 'stage8-ordered-v7-60m-eight-thresholds-full-window-v1'),
    independence_version TEXT NOT NULL CHECK (independence_version = 'stage8-earliest-match-per-btc-parent-v1'),
    acceptance_version TEXT NOT NULL CHECK (acceptance_version = 'stage8-five-parent-probability-or-asymmetry-v1'),
    parent_policy_version TEXT NOT NULL CHECK (parent_policy_version = 'btc-parent-close-reversal-200bps-v1'),
    scope_id TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    window_minutes INTEGER NOT NULL CHECK (window_minutes = 60),
    threshold_bps INTEGER NOT NULL CHECK (threshold_bps IN (25,50,75,100,125,150,175,200)),
    implementation_artifacts JSONB NOT NULL CHECK (jsonb_typeof(implementation_artifacts) = 'object'),
    implementation_artifacts_sha256 TEXT NOT NULL CHECK (implementation_artifacts_sha256 ~ '^[0-9a-f]{64}$'),
    expected_watch_code_manifest JSONB NOT NULL CHECK (jsonb_typeof(expected_watch_code_manifest) = 'object'),
    expected_watch_code_manifest_sha256 TEXT NOT NULL CHECK (expected_watch_code_manifest_sha256 ~ '^[0-9a-f]{64}$'),
    verifier_profile JSONB NOT NULL CHECK (jsonb_typeof(verifier_profile) = 'object'),
    verifier_profile_sha256 TEXT NOT NULL CHECK (verifier_profile_sha256 ~ '^[0-9a-f]{64}$'),
    frozen_at_utc TIMESTAMPTZ NOT NULL,
    freeze_id TEXT NOT NULL UNIQUE CHECK (freeze_id ~ '^[0-9a-f]{64}$'),
    registry_record JSONB NOT NULL CHECK (jsonb_typeof(registry_record) = 'object'),
    registry_record_sha256 TEXT NOT NULL UNIQUE CHECK (registry_record_sha256 ~ '^[0-9a-f]{64}$'),
    registered_by TEXT NOT NULL,
    UNIQUE(scope_id, candidate_id, window_minutes, threshold_bps)
);

CREATE TABLE IF NOT EXISTS research_stage8_projection_fact_batches (
    fact_batch_record_sha256 TEXT PRIMARY KEY CHECK (fact_batch_record_sha256 ~ '^[0-9a-f]{64}$'),
    exact_binding_sha256 TEXT NOT NULL REFERENCES research_stage8_binding_registry(exact_binding_sha256),
    freeze_id TEXT NOT NULL CHECK (freeze_id ~ '^[0-9a-f]{64}$'),
    registry_record_sha256 TEXT NOT NULL CHECK (registry_record_sha256 ~ '^[0-9a-f]{64}$'),
    verifier_profile_sha256 TEXT NOT NULL CHECK (verifier_profile_sha256 ~ '^[0-9a-f]{64}$'),
    registry_verification_receipt_sha256 TEXT NOT NULL CHECK (registry_verification_receipt_sha256 ~ '^[0-9a-f]{64}$'),
    projection_adapter_version TEXT NOT NULL,
    observed_projection_source_sha256 TEXT NOT NULL CHECK (observed_projection_source_sha256 ~ '^[0-9a-f]{64}$'),
    observed_projection_adapter_source_sha256 TEXT NOT NULL CHECK (observed_projection_adapter_source_sha256 ~ '^[0-9a-f]{64}$'),
    observed_registry_adapter_source_sha256 TEXT NOT NULL CHECK (observed_registry_adapter_source_sha256 ~ '^[0-9a-f]{64}$'),
    observed_registry_migration_sha256 TEXT NOT NULL CHECK (observed_registry_migration_sha256 ~ '^[0-9a-f]{64}$'),
    projection_source_manifest JSONB NOT NULL CHECK (jsonb_typeof(projection_source_manifest) = 'object'),
    projection_source_manifest_sha256 TEXT NOT NULL CHECK (projection_source_manifest_sha256 ~ '^[0-9a-f]{64}$'),
    adapter_query_binding_sha256 TEXT NOT NULL CHECK (adapter_query_binding_sha256 ~ '^[0-9a-f]{64}$'),
    adapter_population_receipt JSONB NOT NULL CHECK (jsonb_typeof(adapter_population_receipt) = 'object'),
    adapter_population_receipt_sha256 TEXT NOT NULL CHECK (adapter_population_receipt_sha256 ~ '^[0-9a-f]{64}$'),
    adapter_authority_receipt JSONB NOT NULL CHECK (jsonb_typeof(adapter_authority_receipt) = 'object'),
    adapter_authority_receipt_sha256 TEXT NOT NULL CHECK (adapter_authority_receipt_sha256 ~ '^[0-9a-f]{64}$'),
    adapter_result_sha256 TEXT NOT NULL CHECK (adapter_result_sha256 ~ '^[0-9a-f]{64}$'),
    coverage_query_scope JSONB NOT NULL CHECK (jsonb_typeof(coverage_query_scope) = 'object'),
    coverage_query_sha256 TEXT NOT NULL CHECK (coverage_query_sha256 ~ '^[0-9a-f]{64}$'),
    outcome_free_population_receipt_sha256 TEXT NOT NULL CHECK (outcome_free_population_receipt_sha256 ~ '^[0-9a-f]{64}$'),
    coverage_attempt_population_sha256 TEXT NOT NULL CHECK (coverage_attempt_population_sha256 ~ '^[0-9a-f]{64}$'),
    coverage_source_high_water_attempt_id BIGINT NOT NULL CHECK (coverage_source_high_water_attempt_id >= 0),
    watch_archive_high_water_snapshot_set_id BIGINT CHECK (
        watch_archive_high_water_snapshot_set_id IS NULL
        OR watch_archive_high_water_snapshot_set_id > 0
    ),
    attempt_ids JSONB NOT NULL CHECK (jsonb_typeof(attempt_ids) = 'array'),
    attempt_count INTEGER NOT NULL CHECK (attempt_count > 0),
    persisted_at_utc TIMESTAMPTZ NOT NULL,
    fact_batch_record JSONB NOT NULL CHECK (jsonb_typeof(fact_batch_record) = 'object'),
    persisted_by TEXT NOT NULL,
    UNIQUE(exact_binding_sha256, outcome_free_population_receipt_sha256,
           adapter_population_receipt_sha256)
);

CREATE TABLE IF NOT EXISTS research_stage8_projected_fact_ledger (
    fact_record_sha256 TEXT PRIMARY KEY CHECK (fact_record_sha256 ~ '^[0-9a-f]{64}$'),
    fact_batch_record_sha256 TEXT NOT NULL REFERENCES research_stage8_projection_fact_batches(fact_batch_record_sha256),
    exact_binding_sha256 TEXT NOT NULL REFERENCES research_stage8_binding_registry(exact_binding_sha256),
    attempt_id BIGINT NOT NULL REFERENCES research_prospective_anchor_attempts(attempt_id),
    attempt_fingerprint TEXT NOT NULL CHECK (attempt_fingerprint ~ '^[0-9a-f]{64}$'),
    anchor_slot_id BIGINT REFERENCES research_prospective_anchor_slots(anchor_slot_id),
    event_id BIGINT REFERENCES research_events(event_id),
    event_fingerprint TEXT CHECK (event_fingerprint IS NULL OR event_fingerprint ~ '^[0-9a-f]{64}$'),
    symbol TEXT,
    direction TEXT CHECK (direction IS NULL OR direction IN ('LONG','SHORT')),
    decision_time_utc TIMESTAMPTZ,
    knowledge_status TEXT NOT NULL CHECK (knowledge_status IN ('KNOWN','UNKNOWN')),
    candidate_match BOOLEAN,
    CONSTRAINT research_stage8_fact_knowledge_match_v1 CHECK (
        (knowledge_status = 'KNOWN' AND candidate_match IS NOT NULL)
        OR (knowledge_status = 'UNKNOWN' AND candidate_match IS NULL)
    ),
    fact JSONB NOT NULL CHECK (jsonb_typeof(fact) = 'object'),
    fact_sha256 TEXT NOT NULL CHECK (fact_sha256 ~ '^[0-9a-f]{64}$'),
    fact_authority JSONB NOT NULL CHECK (jsonb_typeof(fact_authority) = 'object'),
    fact_authority_sha256 TEXT NOT NULL CHECK (fact_authority_sha256 ~ '^[0-9a-f]{64}$'),
    watch_selection_attestation JSONB,
    watch_selection_attestation_sha256 TEXT CHECK (watch_selection_attestation_sha256 IS NULL OR watch_selection_attestation_sha256 ~ '^[0-9a-f]{64}$'),
    observed_watch_code_manifest_sha256 TEXT CHECK (observed_watch_code_manifest_sha256 IS NULL OR observed_watch_code_manifest_sha256 ~ '^[0-9a-f]{64}$'),
    parent_membership_evidence JSONB,
    parent_membership_evidence_sha256 TEXT CHECK (parent_membership_evidence_sha256 IS NULL OR parent_membership_evidence_sha256 ~ '^[0-9a-f]{64}$'),
    noneligibility_proof JSONB,
    noneligibility_proof_sha256 TEXT CHECK (noneligibility_proof_sha256 IS NULL OR noneligibility_proof_sha256 ~ '^[0-9a-f]{64}$'),
    selection_fact_identity JSONB NOT NULL
        CHECK (jsonb_typeof(selection_fact_identity) = 'object'),
    selection_fact_identity_sha256 TEXT NOT NULL
        CHECK (selection_fact_identity_sha256 ~ '^[0-9a-f]{64}$'),
    server_projection_attestation JSONB NOT NULL
        CHECK (jsonb_typeof(server_projection_attestation) = 'object'),
    server_projection_attestation_sha256 TEXT NOT NULL
        CHECK (server_projection_attestation_sha256 ~ '^[0-9a-f]{64}$'),
    server_projection_status TEXT NOT NULL CHECK (
        server_projection_status IN ('VERIFIED','PROVEN_NONELIGIBLE','UNKNOWN')
    ),
    persisted_at_utc TIMESTAMPTZ NOT NULL,
    fact_record JSONB NOT NULL CHECK (jsonb_typeof(fact_record) = 'object'),
    persisted_by TEXT NOT NULL,
    UNIQUE(fact_batch_record_sha256, attempt_id),
    UNIQUE(fact_batch_record_sha256, attempt_fingerprint)
);

CREATE TABLE IF NOT EXISTS research_stage8_projection_fact_batch_seals (
    fact_batch_record_sha256 TEXT PRIMARY KEY REFERENCES research_stage8_projection_fact_batches(fact_batch_record_sha256),
    fact_count INTEGER NOT NULL CHECK (fact_count > 0),
    fact_records_sha256 TEXT NOT NULL CHECK (fact_records_sha256 ~ '^[0-9a-f]{64}$'),
    sealed_at_utc TIMESTAMPTZ NOT NULL,
    seal_record JSONB NOT NULL CHECK (jsonb_typeof(seal_record) = 'object'),
    seal_record_sha256 TEXT NOT NULL UNIQUE CHECK (seal_record_sha256 ~ '^[0-9a-f]{64}$'),
    sealed_by TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS research_stage8_selection_receipts (
    selection_record_sha256 TEXT PRIMARY KEY CHECK (selection_record_sha256 ~ '^[0-9a-f]{64}$'),
    fact_batch_record_sha256 TEXT NOT NULL REFERENCES research_stage8_projection_fact_batch_seals(fact_batch_record_sha256),
    exact_binding_sha256 TEXT NOT NULL REFERENCES research_stage8_binding_registry(exact_binding_sha256),
    freeze_id TEXT NOT NULL CHECK (freeze_id ~ '^[0-9a-f]{64}$'),
    registry_record_sha256 TEXT NOT NULL CHECK (registry_record_sha256 ~ '^[0-9a-f]{64}$'),
    verifier_profile_sha256 TEXT NOT NULL CHECK (verifier_profile_sha256 ~ '^[0-9a-f]{64}$'),
    registry_verification_receipt_sha256 TEXT NOT NULL CHECK (registry_verification_receipt_sha256 ~ '^[0-9a-f]{64}$'),
    selector_version TEXT NOT NULL,
    observed_projection_source_sha256 TEXT NOT NULL CHECK (observed_projection_source_sha256 ~ '^[0-9a-f]{64}$'),
    observed_selector_source_sha256 TEXT NOT NULL CHECK (observed_selector_source_sha256 ~ '^[0-9a-f]{64}$'),
    observed_watch_code_manifest_sha256 TEXT NOT NULL CHECK (observed_watch_code_manifest_sha256 ~ '^[0-9a-f]{64}$'),
    cohort_query_sha256 TEXT NOT NULL CHECK (cohort_query_sha256 ~ '^[0-9a-f]{64}$'),
    outcome_free_population_receipt_sha256 TEXT NOT NULL CHECK (outcome_free_population_receipt_sha256 ~ '^[0-9a-f]{64}$'),
    source_high_water_attempt_id BIGINT NOT NULL CHECK (source_high_water_attempt_id >= 0),
    representative_count INTEGER NOT NULL CHECK (representative_count >= 0),
    representative_set_sha256 TEXT NOT NULL CHECK (representative_set_sha256 ~ '^[0-9a-f]{64}$'),
    representative_identities JSONB NOT NULL CHECK (jsonb_typeof(representative_identities) = 'array'),
    representative_identities_sha256 TEXT NOT NULL CHECK (representative_identities_sha256 ~ '^[0-9a-f]{64}$'),
    selection_attestation JSONB NOT NULL CHECK (jsonb_typeof(selection_attestation) = 'object'),
    selection_attestation_sha256 TEXT NOT NULL CHECK (selection_attestation_sha256 ~ '^[0-9a-f]{64}$'),
    persisted_at_utc TIMESTAMPTZ NOT NULL,
    selection_record JSONB NOT NULL CHECK (jsonb_typeof(selection_record) = 'object'),
    persisted_by TEXT NOT NULL,
    UNIQUE(exact_binding_sha256, cohort_query_sha256, outcome_free_population_receipt_sha256, source_high_water_attempt_id)
);

CREATE TABLE IF NOT EXISTS research_stage8_evaluation_receipts (
    evaluation_record_sha256 TEXT PRIMARY KEY CHECK (evaluation_record_sha256 ~ '^[0-9a-f]{64}$'),
    exact_binding_sha256 TEXT NOT NULL REFERENCES research_stage8_binding_registry(exact_binding_sha256),
    selection_record_sha256 TEXT NOT NULL REFERENCES research_stage8_selection_receipts(selection_record_sha256),
    outcome_adapter_version TEXT NOT NULL,
    observed_outcome_adapter_source_sha256 TEXT NOT NULL
        CHECK (observed_outcome_adapter_source_sha256 ~ '^[0-9a-f]{64}$'),
    outcome_source_manifest JSONB NOT NULL
        CHECK (jsonb_typeof(outcome_source_manifest) = 'object'),
    outcome_source_manifest_sha256 TEXT NOT NULL
        CHECK (outcome_source_manifest_sha256 ~ '^[0-9a-f]{64}$'),
    transaction_identity_sha256 TEXT NOT NULL
        CHECK (transaction_identity_sha256 ~ '^[0-9a-f]{64}$'),
    fact_replay_receipt JSONB NOT NULL
        CHECK (jsonb_typeof(fact_replay_receipt) = 'object'),
    fact_replay_receipt_sha256 TEXT NOT NULL
        CHECK (fact_replay_receipt_sha256 ~ '^[0-9a-f]{64}$'),
    evidence_receipt JSONB NOT NULL
        CHECK (jsonb_typeof(evidence_receipt) = 'object'),
    evidence_receipt_sha256 TEXT NOT NULL
        CHECK (evidence_receipt_sha256 ~ '^[0-9a-f]{64}$'),
    evaluation JSONB NOT NULL CHECK (jsonb_typeof(evaluation) = 'object'),
    evaluation_sha256 TEXT NOT NULL CHECK (evaluation_sha256 ~ '^[0-9a-f]{64}$'),
    persistence_payload JSONB NOT NULL
        CHECK (jsonb_typeof(persistence_payload) = 'object'),
    persistence_payload_sha256 TEXT NOT NULL
        CHECK (persistence_payload_sha256 ~ '^[0-9a-f]{64}$'),
    server_replay_attestation JSONB NOT NULL
        CHECK (jsonb_typeof(server_replay_attestation) = 'object'),
    server_replay_attestation_sha256 TEXT NOT NULL
        CHECK (server_replay_attestation_sha256 ~ '^[0-9a-f]{64}$'),
    server_replay_verified BOOLEAN NOT NULL,
    atomic_gate_passed BOOLEAN NOT NULL,
    research_qualified BOOLEAN NOT NULL,
    result_scope TEXT NOT NULL CHECK (result_scope = 'EXPERIMENTAL_RESEARCH_ONLY'),
    live_authorized BOOLEAN NOT NULL CHECK (live_authorized IS FALSE),
    telegram_authorized BOOLEAN NOT NULL CHECK (telegram_authorized IS FALSE),
    trade_authorized BOOLEAN NOT NULL CHECK (trade_authorized IS FALSE),
    persisted_at_utc TIMESTAMPTZ NOT NULL,
    evaluation_record JSONB NOT NULL CHECK (jsonb_typeof(evaluation_record) = 'object'),
    persisted_by TEXT NOT NULL,
    UNIQUE(selection_record_sha256, persistence_payload_sha256)
);

CREATE OR REPLACE FUNCTION research_stage8_registry_insert_guard_v1()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    inner_binding JSONB;
    expected_inner JSONB;
    expected_outer JSONB;
    artifact_keys TEXT[];
    watch_keys TEXT[];
    frozen_text TEXT;
BEGIN
    -- Resolve every unqualified trusted object in the trigger's own schema;
    -- explicitly placing pg_temp last prevents TEMP-table shadowing by a
    -- least-privilege writer even when its session search_path is hostile.
    PERFORM pg_catalog.set_config(
        'search_path',
        pg_catalog.quote_ident(TG_TABLE_SCHEMA) || ',pg_catalog,pg_temp',
        true
    );
    IF NEW.exact_binding_sha256 IS NOT NULL OR NEW.manifest_sha256 IS NOT NULL
       OR NEW.contract_version IS NOT NULL OR NEW.frozen_at_utc IS NOT NULL
       OR NEW.freeze_id IS NOT NULL OR NEW.registry_record IS NOT NULL
       OR NEW.registry_record_sha256 IS NOT NULL OR NEW.registered_by IS NOT NULL THEN
        RAISE EXCEPTION 'Stage-8 registry server-owned fields cannot be supplied';
    END IF;
    IF jsonb_typeof(NEW.exact_binding) <> 'object'
       OR jsonb_typeof(NEW.exact_binding->'binding') <> 'object' THEN
        RAISE EXCEPTION 'Stage-8 exact binding is malformed';
    END IF;
    inner_binding := NEW.exact_binding->'binding';
    NEW.scope_id := inner_binding->'scope'->>'scope_id';
    NEW.candidate_id := inner_binding->'candidate'->>'candidate_id';
    BEGIN
        NEW.window_minutes := (inner_binding->>'window_minutes')::integer;
        NEW.threshold_bps := (inner_binding->>'threshold_bps')::integer;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'Stage-8 exact binding has invalid numeric axes';
    END;
    expected_inner := jsonb_build_object(
        'version', 'stage8-operational-model-contract-v1',
        'manifest_sha256', '5a3ee3af6a73467f3ead09fbe9684a8f60101f97fa7064472b228a31468e6bef',
        'scope', research_stage8_expected_scope_v1(NEW.scope_id),
        'candidate', research_stage8_expected_candidate_v1(NEW.candidate_id),
        'source_version', 'stage8-neutral-v4-watch-v2-source-v1',
        'projection_version', 'stage8-watch-signed-model-sidecar-v1',
        'label_version', 'stage8-ordered-v7-60m-eight-thresholds-full-window-v1',
        'independence_version', 'stage8-earliest-match-per-btc-parent-v1',
        'acceptance_version', 'stage8-five-parent-probability-or-asymmetry-v1',
        'window_minutes', NEW.window_minutes,
        'threshold_bps', NEW.threshold_bps
    );
    expected_outer := jsonb_build_object(
        'binding', expected_inner,
        'binding_sha256', research_stage8_json_sha256_v1(expected_inner)
    );
    IF research_stage8_expected_scope_v1(NEW.scope_id) IS NULL
       OR research_stage8_expected_candidate_v1(NEW.candidate_id) IS NULL
       OR NEW.window_minutes <> 60
       OR NEW.threshold_bps NOT IN (25,50,75,100,125,150,175,200)
       OR NEW.exact_binding <> expected_outer THEN
        RAISE EXCEPTION 'Stage-8 exact binding is not in the frozen manifest';
    END IF;

    SELECT array_agg(key ORDER BY key) INTO artifact_keys
    FROM jsonb_object_keys(COALESCE(NEW.implementation_artifacts->'files', '{}'::jsonb)) AS key;
    IF NEW.implementation_artifacts->>'version'
            IS DISTINCT FROM 'stage8-implementation-artifact-manifest-v1'
       OR artifact_keys IS DISTINCT FROM ARRAY[
            'acceptance','canonical_price_path','common_window_metrics','contract',
            'coverage_receipt','outcome_db_adapter','projection',
            'projection_db_adapter','registry_adapter','registry_migration',
            'selector','source_audit','watch_capture'
       ]::text[]
       OR NEW.implementation_artifacts->'files'->'canonical_price_path'->>'path'
            IS DISTINCT FROM 'canonical_price_path.py'
       OR NEW.implementation_artifacts->'files'->'canonical_price_path'->>'version'
            IS DISTINCT FROM 'canonical-spot-1m-ohlc-path-v3'
       OR NEW.implementation_artifacts->'files'->'common_window_metrics'->>'path'
            IS DISTINCT FROM 'research_common_window_metrics.py'
       OR NEW.implementation_artifacts->'files'->'common_window_metrics'->>'version'
            IS DISTINCT FROM 'common-window-spot-1m-v1'
       OR NEW.implementation_artifacts->'files'->'contract'->>'path'
            IS DISTINCT FROM 'research_stage8_contract.py'
       OR NEW.implementation_artifacts->'files'->'contract'->>'version'
            IS DISTINCT FROM 'stage8-operational-model-contract-v1'
       OR NEW.implementation_artifacts->'files'->'coverage_receipt'->>'path'
            IS DISTINCT FROM 'research_stage8_coverage_receipt.py'
       OR NEW.implementation_artifacts->'files'->'coverage_receipt'->>'version'
            IS DISTINCT FROM 'stage8-bounded-coverage-receipt-v1'
       OR NEW.implementation_artifacts->'files'->'source_audit'->>'path'
            IS DISTINCT FROM 'research_operational_score_source_audit.py'
       OR NEW.implementation_artifacts->'files'->'source_audit'->>'version'
            IS DISTINCT FROM 'operational-score-source-audit-v2'
       OR NEW.implementation_artifacts->'files'->'watch_capture'->>'path'
            IS DISTINCT FROM 'research_watch_score_capture.py'
       OR NEW.implementation_artifacts->'files'->'watch_capture'->>'version'
            IS DISTINCT FROM 'watch-operational-scores-v2'
       OR NEW.implementation_artifacts->'files'->'projection'->>'path'
            IS DISTINCT FROM 'research_stage8_feature_projection.py'
       OR NEW.implementation_artifacts->'files'->'projection'->>'version'
            IS DISTINCT FROM 'stage8-watch-signed-model-sidecar-v1'
       OR NEW.implementation_artifacts->'files'->'projection_db_adapter'->>'path'
            IS DISTINCT FROM 'research_stage8_projection_db_adapter.py'
       OR NEW.implementation_artifacts->'files'->'projection_db_adapter'->>'version'
            IS DISTINCT FROM 'stage8-projection-postgres-adapter-v1'
       OR NEW.implementation_artifacts->'files'->'outcome_db_adapter'->>'path'
            IS DISTINCT FROM 'research_stage8_outcome_db_adapter.py'
       OR NEW.implementation_artifacts->'files'->'outcome_db_adapter'->>'version'
            IS DISTINCT FROM 'stage8-durable-outcome-db-adapter-v1'
       OR NEW.implementation_artifacts->'files'->'registry_adapter'->>'path'
            IS DISTINCT FROM 'research_stage8_registry.py'
       OR NEW.implementation_artifacts->'files'->'registry_adapter'->>'version'
            IS DISTINCT FROM 'stage8-durable-registry-adapter-v1'
       OR NEW.implementation_artifacts->'files'->'registry_migration'->>'path'
            IS DISTINCT FROM 'migrations/056_stage8_durable_registry.sql'
       OR NEW.implementation_artifacts->'files'->'registry_migration'->>'version'
            IS DISTINCT FROM '056-stage8-durable-registry-v1'
       OR NEW.implementation_artifacts->'files'->'selector'->>'path'
            IS DISTINCT FROM 'research_stage8_representative_selector.py'
       OR NEW.implementation_artifacts->'files'->'selector'->>'version'
            IS DISTINCT FROM 'stage8-outcome-blind-representative-selector-v1'
       OR NEW.implementation_artifacts->'files'->'acceptance'->>'path'
            IS DISTINCT FROM 'research_stage8_acceptance.py'
       OR NEW.implementation_artifacts->'files'->'acceptance'->>'version'
            IS DISTINCT FROM 'stage8-experimental-acceptance-evaluator-v1'
       OR EXISTS (
            SELECT 1 FROM jsonb_each(NEW.implementation_artifacts->'files') AS artifact
            WHERE COALESCE(artifact.value->>'sha256','') !~ '^[0-9a-f]{64}$'
               OR (SELECT array_agg(key ORDER BY key)
                   FROM jsonb_object_keys(artifact.value) AS keys(key))
                    IS DISTINCT FROM ARRAY['path','sha256','version']::TEXT[]
       ) THEN
        RAISE EXCEPTION 'Stage-8 implementation artifact manifest is invalid';
    END IF;
    SELECT array_agg(key ORDER BY key) INTO watch_keys
    FROM jsonb_object_keys(NEW.expected_watch_code_manifest) AS key;
    IF watch_keys IS DISTINCT FROM ARRAY[
          'alert_engine.py','coinglass_flow_engine.py','coinglass_oi_regime_service.py',
          'live_price_provider.py','market_confidence_engine.py','time_family_engine.py'
       ]::text[]
       OR EXISTS (
            SELECT 1 FROM jsonb_each_text(NEW.expected_watch_code_manifest) AS code
            WHERE code.value !~ '^[0-9a-f]{64}$'
       ) THEN
        RAISE EXCEPTION 'Stage-8 expected Watch code manifest is invalid';
    END IF;

    NEW.exact_binding_sha256 := expected_outer->>'binding_sha256';
    NEW.manifest_sha256 := '5a3ee3af6a73467f3ead09fbe9684a8f60101f97fa7064472b228a31468e6bef';
    NEW.contract_version := 'stage8-operational-model-contract-v1';
    NEW.hash_version := 'stage8-strict-json-sha256-v1';
    NEW.source_version := 'stage8-neutral-v4-watch-v2-source-v1';
    NEW.source_audit_version := 'operational-score-source-audit-v2';
    NEW.projection_version := 'stage8-watch-signed-model-sidecar-v1';
    NEW.candidate_version := 'stage8-first-tranche-single-model-aligned65-60m-v1';
    NEW.label_version := 'stage8-ordered-v7-60m-eight-thresholds-full-window-v1';
    NEW.independence_version := 'stage8-earliest-match-per-btc-parent-v1';
    NEW.acceptance_version := 'stage8-five-parent-probability-or-asymmetry-v1';
    NEW.parent_policy_version := 'btc-parent-close-reversal-200bps-v1';
    NEW.implementation_artifacts_sha256 := research_stage8_json_sha256_v1(NEW.implementation_artifacts);
    NEW.expected_watch_code_manifest_sha256 := research_stage8_json_sha256_v1(NEW.expected_watch_code_manifest);
    NEW.verifier_profile := jsonb_build_object(
        'version', 'stage8-durable-verifier-profile-v1',
        'manifest_sha256', NEW.manifest_sha256,
        'implementation_artifacts_sha256', NEW.implementation_artifacts_sha256,
        'expected_watch_code_manifest_sha256', NEW.expected_watch_code_manifest_sha256
    );
    NEW.verifier_profile_sha256 := research_stage8_json_sha256_v1(NEW.verifier_profile);
    NEW.frozen_at_utc := clock_timestamp();
    frozen_text := research_stage8_utc_text_v1(NEW.frozen_at_utc);
    NEW.freeze_id := research_stage8_json_sha256_v1(jsonb_build_object(
        'version', 'stage8-db-clock-freeze-id-v1',
        'exact_binding_sha256', NEW.exact_binding_sha256,
        'frozen_at_utc', frozen_text,
        'verifier_profile_sha256', NEW.verifier_profile_sha256
    ));
    NEW.registered_by := current_user;
    NEW.registry_record := jsonb_build_object(
        'version', 'stage8-durable-registry-record-v1',
        'exact_binding', NEW.exact_binding,
        'manifest_sha256', NEW.manifest_sha256,
        'hash_version', NEW.hash_version,
        'source_audit_version', NEW.source_audit_version,
        'candidate_version', NEW.candidate_version,
        'parent_policy_version', NEW.parent_policy_version,
        'implementation_artifacts', NEW.implementation_artifacts,
        'expected_watch_code_manifest', NEW.expected_watch_code_manifest,
        'verifier_profile', NEW.verifier_profile,
        'freeze_id', NEW.freeze_id,
        'frozen_at_utc', frozen_text,
        'registered_by', NEW.registered_by
    );
    NEW.registry_record_sha256 := research_stage8_json_sha256_v1(NEW.registry_record);
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION research_stage8_fact_batch_insert_guard_v1()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    registry research_stage8_binding_registry%ROWTYPE;
    normalized_attempt_ids JSONB;
    authoritative_attempt_ids JSONB;
    normalized_scope_symbols JSONB;
    database_high_water BIGINT;
    watch_archive_high_water BIGINT;
    batch_cutoff TIMESTAMPTZ;
    persisted_text TEXT;
BEGIN
    PERFORM pg_catalog.set_config(
        'search_path',
        pg_catalog.quote_ident(TG_TABLE_SCHEMA) || ',pg_catalog,pg_temp',
        true
    );
    IF lower(pg_catalog.current_setting('transaction_isolation'))
            IS DISTINCT FROM 'repeatable read' THEN
        RAISE EXCEPTION
            'Stage-8 fact batches require an actual REPEATABLE READ transaction';
    END IF;
    IF NEW.fact_batch_record_sha256 IS NOT NULL OR NEW.persisted_at_utc IS NOT NULL
       OR NEW.watch_archive_high_water_snapshot_set_id IS NOT NULL
       OR NEW.fact_batch_record IS NOT NULL OR NEW.persisted_by IS NOT NULL THEN
        RAISE EXCEPTION 'Stage-8 fact-batch server-owned fields cannot be supplied';
    END IF;
    SELECT * INTO STRICT registry FROM research_stage8_binding_registry
    WHERE exact_binding_sha256 = NEW.exact_binding_sha256;
    batch_cutoff := clock_timestamp();
    IF NEW.freeze_id IS DISTINCT FROM registry.freeze_id
       OR NEW.registry_record_sha256 IS DISTINCT FROM registry.registry_record_sha256
       OR NEW.verifier_profile_sha256 IS DISTINCT FROM registry.verifier_profile_sha256
       OR NEW.projection_adapter_version
            IS DISTINCT FROM registry.implementation_artifacts->'files'->'projection_db_adapter'->>'version'
       OR NEW.observed_projection_source_sha256
            IS DISTINCT FROM registry.implementation_artifacts->'files'->'projection'->>'sha256'
       OR NEW.observed_projection_adapter_source_sha256
            IS DISTINCT FROM registry.implementation_artifacts->'files'->'projection_db_adapter'->>'sha256'
       OR NEW.observed_registry_adapter_source_sha256
            IS DISTINCT FROM registry.implementation_artifacts->'files'->'registry_adapter'->>'sha256'
       OR NEW.observed_registry_migration_sha256
            IS DISTINCT FROM registry.implementation_artifacts->'files'->'registry_migration'->>'sha256'
       OR NEW.projection_source_manifest_sha256
            IS DISTINCT FROM research_stage8_json_sha256_v1(NEW.projection_source_manifest)
       OR NEW.projection_source_manifest->>'research_stage8_feature_projection.py'
            IS DISTINCT FROM NEW.observed_projection_source_sha256
       OR NEW.projection_source_manifest->>'research_stage8_projection_db_adapter.py'
            IS DISTINCT FROM NEW.observed_projection_adapter_source_sha256 THEN
        RAISE EXCEPTION 'Stage-8 fact batch does not match its durable verifier profile';
    END IF;
    IF NEW.adapter_population_receipt_sha256
            IS DISTINCT FROM research_stage8_json_sha256_v1(NEW.adapter_population_receipt - 'population_receipt_sha256')
       OR NEW.adapter_population_receipt->>'population_receipt_sha256'
            IS DISTINCT FROM NEW.adapter_population_receipt_sha256
       OR NEW.adapter_authority_receipt_sha256
            IS DISTINCT FROM research_stage8_json_sha256_v1(NEW.adapter_authority_receipt - 'authority_receipt_sha256')
       OR NEW.adapter_authority_receipt->>'authority_receipt_sha256'
            IS DISTINCT FROM NEW.adapter_authority_receipt_sha256
       OR NEW.adapter_population_receipt->>'query_binding_sha256'
            IS DISTINCT FROM NEW.adapter_query_binding_sha256
       OR NEW.adapter_authority_receipt->>'population_receipt_sha256'
            IS DISTINCT FROM NEW.adapter_population_receipt_sha256
       OR NEW.adapter_authority_receipt->>'projection_source_manifest_sha256'
            IS DISTINCT FROM NEW.projection_source_manifest_sha256
       OR NEW.adapter_population_receipt->'read_only' IS DISTINCT FROM 'true'::jsonb
       OR lower(NEW.adapter_population_receipt->>'transaction_isolation')
            IS DISTINCT FROM 'repeatable read'
       OR NEW.adapter_population_receipt->'population_complete' IS DISTINCT FROM 'true'::jsonb
       OR NEW.adapter_population_receipt->'truncated' IS DISTINCT FROM 'false'::jsonb THEN
        RAISE EXCEPTION 'Stage-8 projection adapter receipts are invalid or incomplete';
    END IF;
    SELECT COALESCE(jsonb_agg(symbol ORDER BY symbol COLLATE "C"), '[]'::jsonb)
    INTO normalized_scope_symbols
    FROM (
        SELECT DISTINCT value AS symbol
        FROM jsonb_array_elements_text(
            registry.exact_binding->'binding'->'scope'->'symbols'
        )
    ) AS symbols;
    IF (SELECT array_agg(key ORDER BY key)
        FROM jsonb_object_keys(NEW.coverage_query_scope) AS keys(key))
       IS DISTINCT FROM ARRAY[
           'adapter_version','capture_version','end_utc','max_capture_age_seconds',
           'outcome_version','page_size','parent_policy','query_sha256',
           'sampler_version','start_utc','symbols','thresholds_bps','windows'
       ]::TEXT[]
       OR NEW.coverage_query_scope->>'query_sha256'
            IS DISTINCT FROM NEW.coverage_query_sha256
       OR research_stage8_json_sha256_v1(NEW.coverage_query_scope - 'query_sha256')
            IS DISTINCT FROM NEW.coverage_query_sha256
       OR NEW.coverage_query_scope->>'adapter_version'
            IS DISTINCT FROM registry.source_audit_version
       OR NEW.coverage_query_scope->>'sampler_version'
            IS DISTINCT FROM 'prospective-neutral-anchor-v4-decision-features-frozen'
       OR NEW.coverage_query_scope->'symbols'
            IS DISTINCT FROM normalized_scope_symbols
       OR NEW.coverage_query_scope->'windows' IS DISTINCT FROM '[60]'::jsonb
       OR NEW.coverage_query_scope->'thresholds_bps'
            IS DISTINCT FROM '[25,50,75,100,125,150,175,200]'::jsonb
       OR NEW.coverage_query_scope->'max_capture_age_seconds'
            IS DISTINCT FROM '300.0'::jsonb
       OR NEW.coverage_query_scope->>'capture_version'
            IS DISTINCT FROM 'watch-operational-scores-v2'
       OR NEW.coverage_query_scope->>'outcome_version'
            IS DISTINCT FROM 'ordered-first-touch-v7'
       OR NEW.coverage_query_scope->>'parent_policy'
            IS DISTINCT FROM registry.parent_policy_version
       OR jsonb_typeof(NEW.coverage_query_scope->'page_size') IS DISTINCT FROM 'number'
       OR (NEW.coverage_query_scope->>'page_size')::integer NOT BETWEEN 1 AND 25
       OR NEW.coverage_query_scope->>'start_utc' IS NULL
       OR NEW.coverage_query_scope->>'end_utc' IS NULL
       OR (NEW.coverage_query_scope->>'start_utc')::timestamptz
            IS DISTINCT FROM registry.frozen_at_utc
       OR (NEW.coverage_query_scope->>'start_utc')::timestamptz
            >= (NEW.coverage_query_scope->>'end_utc')::timestamptz
       OR (NEW.coverage_query_scope->>'end_utc')::timestamptz > batch_cutoff THEN
        RAISE EXCEPTION 'Stage-8 coverage query scope is malformed or differs from the frozen axes';
    END IF;
    BEGIN
        SELECT COALESCE(jsonb_agg(value ORDER BY value), '[]'::jsonb)
        INTO normalized_attempt_ids
        FROM (
            SELECT DISTINCT (item #>> '{}')::bigint AS value
            FROM jsonb_array_elements(NEW.attempt_ids) AS item
        ) AS ids;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'Stage-8 fact-batch attempt IDs are invalid';
    END;
    SELECT COALESCE(MAX(attempt.attempt_id), 0)
    INTO database_high_water
    FROM research_prospective_anchor_attempts AS attempt
    WHERE attempt.sampler_version = NEW.coverage_query_scope->>'sampler_version'
      AND attempt.symbol = ANY(ARRAY(
          SELECT jsonb_array_elements_text(NEW.coverage_query_scope->'symbols')
      ))
      AND attempt.source_candle_open_utc >= registry.frozen_at_utc
      AND attempt.source_candle_open_utc < batch_cutoff;
    SELECT max(snapshot.snapshot_set_id)
    INTO watch_archive_high_water
    FROM research_max_pain_snapshot_sets AS snapshot
    WHERE snapshot.source = 'WATCH_SHARED';
    SELECT COALESCE(jsonb_agg(attempt.attempt_id ORDER BY attempt.attempt_id), '[]'::jsonb)
    INTO authoritative_attempt_ids
    FROM research_prospective_anchor_attempts AS attempt
    WHERE attempt.sampler_version = NEW.coverage_query_scope->>'sampler_version'
      AND attempt.symbol = ANY(ARRAY(
          SELECT jsonb_array_elements_text(NEW.coverage_query_scope->'symbols')
      ))
      AND attempt.source_candle_open_utc
            >= registry.frozen_at_utc
      AND attempt.source_candle_open_utc
            < (NEW.coverage_query_scope->>'end_utc')::timestamptz
      AND attempt.attempt_id <= NEW.coverage_source_high_water_attempt_id;
    IF normalized_attempt_ids IS DISTINCT FROM NEW.attempt_ids
       OR authoritative_attempt_ids IS DISTINCT FROM NEW.attempt_ids
       OR NEW.coverage_source_high_water_attempt_id
            IS DISTINCT FROM database_high_water
       OR EXISTS (
            SELECT 1
            FROM research_prospective_anchor_attempts AS attempt
            WHERE attempt.sampler_version
                    = NEW.coverage_query_scope->>'sampler_version'
              AND attempt.symbol = ANY(ARRAY(
                  SELECT jsonb_array_elements_text(
                      NEW.coverage_query_scope->'symbols'
                  )
              ))
              AND attempt.source_candle_open_utc >= registry.frozen_at_utc
              AND attempt.source_candle_open_utc < batch_cutoff
              AND attempt.source_candle_open_utc
                    >= (NEW.coverage_query_scope->>'end_utc')::timestamptz
       )
       OR jsonb_array_length(NEW.attempt_ids) IS DISTINCT FROM NEW.attempt_count
       OR (NEW.attempt_ids->>-1)::bigint
            IS DISTINCT FROM NEW.coverage_source_high_water_attempt_id
       OR NEW.adapter_population_receipt->'requested_attempt_ids' IS DISTINCT FROM NEW.attempt_ids
       OR NEW.adapter_population_receipt->'found_attempt_ids' IS DISTINCT FROM NEW.attempt_ids
       OR NEW.adapter_population_receipt->'missing_attempt_ids' IS DISTINCT FROM '[]'::jsonb
       OR EXISTS (
            SELECT 1 FROM jsonb_array_elements_text(NEW.attempt_ids) AS item
            LEFT JOIN research_prospective_anchor_attempts AS attempt
              ON attempt.attempt_id = item::bigint
            WHERE attempt.attempt_id IS NULL
       ) THEN
        RAISE EXCEPTION 'Stage-8 fact-batch population is incomplete or not source-backed';
    END IF;
    NEW.persisted_at_utc := batch_cutoff;
    NEW.watch_archive_high_water_snapshot_set_id := watch_archive_high_water;
    NEW.persisted_by := current_user;
    persisted_text := research_stage8_utc_text_v1(NEW.persisted_at_utc);
    NEW.fact_batch_record := jsonb_build_object(
        'version', 'stage8-durable-projection-fact-batch-v1',
        'exact_binding_sha256', NEW.exact_binding_sha256,
        'freeze_id', NEW.freeze_id,
        'registry_record_sha256', NEW.registry_record_sha256,
        'verifier_profile_sha256', NEW.verifier_profile_sha256,
        'registry_verification_receipt_sha256', NEW.registry_verification_receipt_sha256,
        'projection_adapter_version', NEW.projection_adapter_version,
        'observed_projection_source_sha256', NEW.observed_projection_source_sha256,
        'observed_projection_adapter_source_sha256', NEW.observed_projection_adapter_source_sha256,
        'observed_registry_adapter_source_sha256', NEW.observed_registry_adapter_source_sha256,
        'observed_registry_migration_sha256', NEW.observed_registry_migration_sha256,
        'projection_source_manifest_sha256', NEW.projection_source_manifest_sha256,
        'adapter_query_binding_sha256', NEW.adapter_query_binding_sha256,
        'adapter_population_receipt_sha256', NEW.adapter_population_receipt_sha256,
        'adapter_authority_receipt_sha256', NEW.adapter_authority_receipt_sha256,
        'adapter_result_sha256', NEW.adapter_result_sha256,
        'coverage_query_scope', NEW.coverage_query_scope,
        'coverage_query_sha256', NEW.coverage_query_sha256,
        'outcome_free_population_receipt_sha256', NEW.outcome_free_population_receipt_sha256,
        'coverage_attempt_population_sha256', NEW.coverage_attempt_population_sha256,
        'coverage_source_high_water_attempt_id', NEW.coverage_source_high_water_attempt_id,
        'watch_archive_high_water_snapshot_set_id',
            NEW.watch_archive_high_water_snapshot_set_id,
        'attempt_ids', NEW.attempt_ids,
        'attempt_count', NEW.attempt_count,
        'persisted_at_utc', persisted_text,
        'persisted_by', NEW.persisted_by
    );
    NEW.fact_batch_record_sha256 := research_stage8_json_sha256_v1(NEW.fact_batch_record);
    RETURN NEW;
END;
$$;

-- Derive the qualification-critical projection state from trusted database
-- rows.  The function is deliberately SECURITY INVOKER.  Its two trigger
-- callers first pin search_path to TG_TABLE_SCHEMA, and the dedicated writers
-- receive SELECT-only access to the exact source relations below.  No caller
-- fact, Watch receipt, projection receipt, or outcome is an input.
CREATE OR REPLACE FUNCTION research_stage8_derive_projection_attestation_v1(
    trusted_schema TEXT,
    binding_sha256 TEXT,
    requested_attempt_id BIGINT,
    persisted_snapshot_set_id BIGINT DEFAULT NULL,
    replay_exact_snapshot BOOLEAN DEFAULT FALSE
)
RETURNS JSONB
LANGUAGE plpgsql
STABLE
SET TimeZone = 'UTC'
AS $$
DECLARE
    registry research_stage8_binding_registry%ROWTYPE;
    source_attempt research_prospective_anchor_attempts%ROWTYPE;
    source_slot research_prospective_anchor_slots%ROWTYPE;
    source_event research_events%ROWTYPE;
    source_anchor_events JSONB;
    source_snapshot research_max_pain_snapshot_sets%ROWTYPE;
    source_parent research_btc_parent_movements%ROWTYPE;
    source_bar research_btc_price_bars%ROWTYPE;
    slot_found BOOLEAN := FALSE;
    event_found BOOLEAN := FALSE;
    snapshot_found BOOLEAN := FALSE;
    parent_found BOOLEAN := FALSE;
    bar_found BOOLEAN := FALSE;
    candidate JSONB;
    operational JSONB;
    coin JSONB;
    model JSONB;
    model_source JSONB;
    direction TEXT;
    model_name TEXT;
    source_path TEXT;
    expected_family TEXT;
    selected_event_id BIGINT;
    durable_at TIMESTAMPTZ;
    computed_at TIMESTAMPTZ;
    raw_score NUMERIC;
    aligned_score NUMERIC;
    multiplier INTEGER;
    candidate_threshold NUMERIC;
    derived_match BOOLEAN;
    derived_knowledge_status TEXT := 'UNKNOWN';
    parent_class TEXT := 'UNKNOWN';
    status_value TEXT := 'UNKNOWN';
    reasons TEXT[] := ARRAY[]::TEXT[];
    semantics JSONB;
    source_rows JSONB;
    time_family_entry RECORD;
    member_entry JSONB;
    window_entry RECORD;
    window_value JSONB;
    latest_time_value TIMESTAMPTZ;
    reference_time_value TIMESTAMPTZ;
    available_window_found BOOLEAN := FALSE;
BEGIN
    IF pg_catalog.pg_trigger_depth() = 0 THEN
        RAISE EXCEPTION
            'Stage-8 projection authority helper is trigger-internal only';
    END IF;
    IF trusted_schema IS DISTINCT FROM current_schema() THEN
        RAISE EXCEPTION 'Stage-8 projection derivation schema is not pinned';
    END IF;
    SELECT * INTO STRICT registry
    FROM research_stage8_binding_registry
    WHERE exact_binding_sha256 = binding_sha256;
    SELECT * INTO STRICT source_attempt
    FROM research_prospective_anchor_attempts
    WHERE attempt_id = requested_attempt_id;

    IF source_attempt.attempt_id <= 0 THEN
        reasons := array_append(reasons, 'ANCHOR_ATTEMPT_ID_INVALID');
    END IF;

    candidate := registry.exact_binding->'binding'->'candidate';
    direction := candidate->>'direction';
    model_name := candidate->>'model';
    candidate_threshold := (candidate->>'value')::numeric;
    multiplier := CASE direction WHEN 'LONG' THEN 1 WHEN 'SHORT' THEN -1 ELSE 0 END;
    source_path := CASE model_name
        WHEN 'positioning' THEN 'positioning'
        WHEN 'futures_flow' THEN 'futures'
        WHEN 'spot_flow' THEN 'spot'
        ELSE NULL END;
    expected_family := CASE model_name
        WHEN 'positioning' THEN 'Price+OI'
        WHEN 'futures_flow' THEN 'Futures Flow'
        WHEN 'spot_flow' THEN 'Spot Flow'
        ELSE NULL END;

    IF source_attempt.sampler_version
            IS DISTINCT FROM 'prospective-neutral-anchor-v4-decision-features-frozen' THEN
        reasons := array_append(reasons, 'ATTEMPT_SAMPLER_VERSION_MISMATCH');
    END IF;
    IF source_attempt.coverage_policy_version IS DISTINCT FROM
           'prospective-coverage-v3-completed-fully-validated-replay-run:no-dwell-first-touch-v6:historical-raw-opportunity-replay-v2-balanced-prior-session-width'
       OR research_stage8_anchor_strip_v1(COALESCE(source_attempt.evaluation_reason,'')) = '' THEN
        reasons := array_append(reasons,'ATTEMPT_COVERAGE_OR_STATUS_INVALID');
    END IF;
    IF source_attempt.attempt_fingerprint !~ '^[0-9a-f]{64}$' THEN
        reasons := array_append(reasons, 'ATTEMPT_FINGERPRINT_INVALID');
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM jsonb_array_elements_text(
            registry.exact_binding->'binding'->'scope'->'symbols'
        ) AS allowed(symbol)
        WHERE allowed.symbol = source_attempt.symbol
    ) THEN
        reasons := array_append(reasons, 'ATTEMPT_SYMBOL_OUTSIDE_EXACT_SCOPE');
    END IF;
    IF source_attempt.source_candle_open_utc < registry.frozen_at_utc THEN
        reasons := array_append(reasons, 'ATTEMPT_PREDATES_FREEZE');
    END IF;

    IF source_attempt.evaluation_status <> 'EVALUABLE' THEN
        IF source_attempt.evaluation_status IN ('UNEVALUABLE','COVERAGE_EXCLUDED')
           AND source_attempt.decision_time_utc IS NULL
           AND cardinality(reasons) = 0 THEN
            status_value := 'PROVEN_NONELIGIBLE';
            parent_class := 'PROVEN_NOT_CANDIDATE_ELIGIBLE';
        ELSE
            reasons := array_append(reasons, 'ATTEMPT_NONELIGIBILITY_NOT_PROVEN');
        END IF;
    ELSE
        IF source_attempt.decision_time_utc IS NULL THEN
            reasons := array_append(reasons, 'ATTEMPT_DECISION_TIME_MISSING');
        END IF;
        SELECT s.* INTO source_slot
        FROM research_prospective_anchor_slots AS s
        WHERE s.sampler_version = source_attempt.sampler_version
          AND s.symbol = source_attempt.symbol
          AND s.source_candle_open_utc = source_attempt.source_candle_open_utc
        LIMIT 2;
        slot_found := FOUND;
        IF NOT slot_found THEN
            reasons := array_append(reasons, 'ANCHOR_SLOT_MISSING');
        ELSE
            -- Validate the complete persisted neutral pair, including the
            -- opposite-direction event. No caller anchor receipt is read.
            SELECT COALESCE(jsonb_agg(to_jsonb(e) ORDER BY e.event_id), '[]'::jsonb)
            INTO source_anchor_events
            FROM research_events AS e
            WHERE e.event_id IN (source_slot.long_event_id, source_slot.short_event_id);
            reasons := reasons || research_stage8_anchor_errors_v1(
                to_jsonb(source_attempt), to_jsonb(source_slot), source_anchor_events);
            IF source_slot.input_fingerprint
                    IS DISTINCT FROM source_attempt.input_fingerprint
               OR source_slot.decision_time_utc
                    IS DISTINCT FROM source_attempt.decision_time_utc
               OR source_slot.sampler_version IS DISTINCT FROM source_attempt.sampler_version
               OR source_slot.coverage_policy_version IS DISTINCT FROM source_attempt.coverage_policy_version
               OR source_slot.coverage_snapshot IS DISTINCT FROM source_attempt.coverage_snapshot
               OR source_slot.symbol IS DISTINCT FROM source_attempt.symbol
               OR source_slot.interval_minutes IS DISTINCT FROM source_attempt.interval_minutes
               OR source_slot.source_candle_open_utc IS DISTINCT FROM source_attempt.source_candle_open_utc
               OR source_slot.source_candle_close_utc IS DISTINCT FROM source_attempt.source_candle_close_utc
               OR source_slot.base_eligible_at_utc IS DISTINCT FROM source_attempt.base_eligible_at_utc
               OR source_slot.expires_at_utc IS DISTINCT FROM source_attempt.expires_at_utc
               OR source_slot.feature_bundle_policy_version IS DISTINCT FROM source_attempt.feature_bundle_policy_version
               OR source_slot.feature_bundle_sha256 IS DISTINCT FROM source_attempt.feature_bundle_sha256
               OR source_slot.source_timestamps IS DISTINCT FROM source_attempt.source_timestamps
               OR source_slot.source_provenance IS DISTINCT FROM source_attempt.source_provenance
               OR source_slot.frozen_inputs IS DISTINCT FROM source_attempt.frozen_inputs THEN
                reasons := array_append(reasons, 'ANCHOR_SLOT_ATTEMPT_MISMATCH');
            END IF;
            IF source_attempt.interval_minutes IS DISTINCT FROM 30
               OR NOT isfinite(source_attempt.source_candle_open_utc)
               OR NOT isfinite(source_attempt.source_candle_close_utc)
               OR NOT isfinite(source_attempt.base_eligible_at_utc)
               OR NOT isfinite(source_attempt.expires_at_utc)
               OR NOT isfinite(source_attempt.decision_time_utc)
               OR date_trunc('minute',source_attempt.source_candle_open_utc)
                    IS DISTINCT FROM source_attempt.source_candle_open_utc
               OR extract(minute FROM source_attempt.source_candle_open_utc)::integer % 30 <> 0
               OR source_attempt.source_candle_close_utc IS DISTINCT FROM
                    source_attempt.source_candle_open_utc + interval '30 minutes'
               OR source_attempt.base_eligible_at_utc IS DISTINCT FROM
                    source_attempt.source_candle_open_utc + interval '32 minutes'
               OR source_attempt.expires_at_utc IS DISTINCT FROM
                    source_attempt.source_candle_open_utc + interval '62 minutes'
               OR source_attempt.decision_time_utc < source_attempt.base_eligible_at_utc
               OR source_attempt.decision_time_utc >= source_attempt.expires_at_utc
               OR source_attempt.checked_at_utc IS DISTINCT FROM source_attempt.decision_time_utc
               OR source_attempt.missing_sources IS DISTINCT FROM '[]'::jsonb
               OR source_slot.feature_bundle_policy_version IS DISTINCT FROM
                    'prospective-decision-feature-bundle-v1'
               OR COALESCE(btrim(source_slot.feature_bundle_sha256),'') !~ '^[0-9a-f]{64}$'
               OR source_slot.decision_feature_bundle->>'model_score_status' IS DISTINCT FROM 'ABSENT' THEN
                reasons := array_append(reasons,'ANCHOR_DECISION_INTERVAL_OR_FEATURE_INVALID');
            END IF;
            selected_event_id := CASE direction
                WHEN 'LONG' THEN source_slot.long_event_id
                WHEN 'SHORT' THEN source_slot.short_event_id
                ELSE NULL END;
            SELECT e.* INTO source_event
            FROM research_events AS e
            WHERE e.event_id = selected_event_id;
            event_found := FOUND;
            IF NOT event_found THEN
                reasons := array_append(reasons, 'DIRECTION_EVENT_MISSING');
            ELSIF source_event.symbol IS DISTINCT FROM source_attempt.symbol
               OR source_event.direction IS DISTINCT FROM direction
               OR source_event.alert_time_utc
                    IS DISTINCT FROM source_attempt.decision_time_utc
               OR source_event.event_fingerprint !~ '^[0-9a-f]{64}$' THEN
                reasons := array_append(reasons, 'DIRECTION_EVENT_IDENTITY_MISMATCH');
            END IF;
        END IF;

        IF event_found THEN
            -- The legacy event->movement membership is intentionally not an
            -- authority input here: that worker only emits some rows after an
            -- outcome cell exists, so presence/absence would leak labels into
            -- structural selection.  Reconstruct membership causally from the
            -- last closed BTC minute available at the decision and the parent
            -- interval that contains that decision.
            SELECT b.* INTO source_bar
            FROM research_btc_price_bars AS b
            WHERE b.close_time_utc <= source_event.alert_time_utc
            ORDER BY b.close_time_utc DESC
            LIMIT 1;
            bar_found := FOUND;
            SELECT p.* INTO source_parent
            FROM research_btc_parent_movements AS p
            WHERE p.episode_policy_version = registry.parent_policy_version
              AND p.start_time_utc <= source_event.alert_time_utc
              AND (p.end_time_utc IS NULL
                   OR source_event.alert_time_utc < p.end_time_utc)
            ORDER BY p.start_time_utc DESC, p.btc_parent_movement_id COLLATE "C"
            LIMIT 1;
            parent_found := FOUND;
            IF NOT parent_found OR NOT bar_found THEN
                reasons := array_append(reasons, 'PARENT_SOURCE_GRAPH_INCOMPLETE');
            ELSIF source_event.alert_time_utc < source_parent.start_time_utc
               OR (source_parent.end_time_utc IS NOT NULL
                   AND source_event.alert_time_utc >= source_parent.end_time_utc)
               OR source_bar.close_time_utc < source_parent.start_time_utc
               OR source_bar.close_time_utc > source_event.alert_time_utc
               OR source_event.alert_time_utc - source_bar.close_time_utc
                    >= interval '1 minute'
               OR date_trunc('minute', source_bar.open_time_utc)
                    IS DISTINCT FROM source_bar.open_time_utc
               OR source_bar.close_time_utc IS DISTINCT FROM
                    source_bar.open_time_utc + interval '1 minute'
                        - interval '1 millisecond'
               OR least(source_bar.open, source_bar.high, source_bar.low,
                        source_bar.close) <= 0
               OR source_bar.high < greatest(
                    source_bar.open, source_bar.close, source_bar.low)
               OR source_bar.low > least(
                    source_bar.open, source_bar.close, source_bar.high)
               OR source_parent.price_source
                        IS DISTINCT FROM 'BINANCE_SPOT_BTCUSDT_1M'
               OR source_bar.price_source
                        IS DISTINCT FROM 'BINANCE_SPOT_BTCUSDT_1M'
               OR source_parent.btc_parent_movement_id IS DISTINCT FROM
                    encode(sha256(convert_to(
                        source_parent.episode_policy_version || '|'
                        || 'BINANCE_SPOT_BTCUSDT_1M' || '|'
                        || to_char(source_parent.start_time_utc AT TIME ZONE 'UTC',
                                   'YYYY-MM-DD"T"HH24:MI:SS')
                        || CASE WHEN extract(microseconds FROM
                                      source_parent.start_time_utc)::integer
                                      % 1000000 = 0
                                THEN '' ELSE '.' || to_char(
                                    source_parent.start_time_utc AT TIME ZONE 'UTC',
                                    'US') END
                        || '+00:00', 'UTF8')), 'hex')
               OR source_parent.state_json->'reversal_bps'
                        IS DISTINCT FROM '200'::jsonb
               OR source_parent.observed_through_utc < source_bar.close_time_utc THEN
                reasons := array_append(reasons, 'PARENT_SOURCE_GRAPH_INVALID');
            ELSIF source_parent.evidence_eligible IS TRUE
               AND source_parent.confirmed_at_utc
                        IS NOT DISTINCT FROM source_parent.start_time_utc
               AND source_parent.confirmed_at_utc <= source_event.alert_time_utc
               AND source_parent.boundary_reason = 'CAUSAL_CLOSE_REVERSAL'
               AND source_parent.direction IN ('UP','DOWN') THEN
                parent_class := 'LIVE';
            ELSIF source_parent.evidence_eligible IS FALSE
               AND source_parent.confirmed_at_utc IS NULL
               AND source_parent.boundary_reason IN (
                    'LEFT_BOUNDARY_UNVERIFIED','BTC_DATA_GAP'
               ) THEN
                parent_class := 'PROVEN_NOT_EVIDENCE_ELIGIBLE';
            ELSE
                reasons := array_append(reasons, 'PARENT_AUTHORITY_CLASS_UNKNOWN');
            END IF;
        END IF;

        IF source_attempt.decision_time_utc IS NOT NULL THEN
            IF replay_exact_snapshot THEN
                -- Revalidate the exact server-owned capture persisted with
                -- the fact.  A later archive insertion (even one carrying a
                -- backdated source timestamp) cannot silently replace it.
                IF persisted_snapshot_set_id IS NOT NULL THEN
                    SELECT w.* INTO source_snapshot
                    FROM research_max_pain_snapshot_sets AS w
                    WHERE w.snapshot_set_id = persisted_snapshot_set_id
                      AND w.source = 'WATCH_SHARED'
                      AND w.available_at_utc IS NOT NULL
                      AND w.created_at_utc IS NOT NULL
                      AND greatest(w.available_at_utc, w.created_at_utc)
                            <= source_attempt.decision_time_utc
                      AND greatest(w.available_at_utc, w.created_at_utc)
                            >= source_attempt.decision_time_utc
                               - interval '300 seconds';
                    snapshot_found := FOUND;
                END IF;
            ELSE
                IF persisted_snapshot_set_id IS NOT NULL THEN
                    SELECT w.* INTO source_snapshot
                    FROM research_max_pain_snapshot_sets AS w
                    WHERE w.source = 'WATCH_SHARED'
                      AND w.snapshot_set_id <= persisted_snapshot_set_id
                      AND w.available_at_utc IS NOT NULL
                      AND w.created_at_utc IS NOT NULL
                      AND greatest(w.available_at_utc, w.created_at_utc)
                            <= source_attempt.decision_time_utc
                      AND greatest(w.available_at_utc, w.created_at_utc)
                            >= source_attempt.decision_time_utc
                               - interval '300 seconds'
                    ORDER BY greatest(w.available_at_utc, w.created_at_utc) DESC,
                             w.snapshot_set_id DESC
                    LIMIT 1;
                    snapshot_found := FOUND;
                END IF;
            END IF;
        END IF;
        IF NOT snapshot_found THEN
            reasons := array_append(reasons, 'WATCH_CAUSAL_CAPTURE_MISSING');
        ELSE
            durable_at := greatest(
                source_snapshot.available_at_utc, source_snapshot.created_at_utc
            );
            operational := source_snapshot.source_metadata
                ->'capture_metadata'->'operational_scores';
            coin := operational->'coins'->source_attempt.symbol;
            model := coin->'models'->model_name;
            model_source := coin->'sources'->source_path;
            -- The outer archive hash is reference-only, but the complete
            -- selected-coin inner capture must pass the frozen source audit.
            IF source_snapshot.snapshot_set_id <= 0
               OR COALESCE(btrim(source_snapshot.snapshot_key::text),'') !~ '^[0-9a-f]{64}$'
               OR COALESCE(btrim(source_snapshot.payload_sha256::text),'') !~ '^[0-9a-f]{64}$' THEN
                reasons := array_append(reasons,'WATCH_OUTER_REFERENCE_INVALID');
            END IF;
            reasons := reasons || research_stage8_watch_capture_errors_v1(
                operational, source_attempt.symbol,
                registry.expected_watch_code_manifest, source_snapshot.cycle_id,
                source_attempt.decision_time_utc,
                source_snapshot.available_at_utc, source_snapshot.created_at_utc);
            computed_at := research_stage8_watch_timestamp_v1(
                operational->'computed_at_utc');
            -- Candidate-specific admission is stricter than capture storage:
            -- the chosen model must actually be available and retain its
            -- frozen operational direction. Unavailable zero stays UNKNOWN.
            IF jsonb_typeof(model) IS DISTINCT FROM 'object'
               OR model->'available' IS DISTINCT FROM 'true'::jsonb
               OR model->>'capture_status' IS DISTINCT FROM 'AVAILABLE'
               OR model->>'family' IS DISTINCT FROM expected_family
               OR NOT research_stage8_watch_finite_number_v1(model->'score')
               OR COALESCE(model->>'direction','') NOT IN ('BULLISH','BEARISH','NEUTRAL') THEN
                reasons := array_append(reasons,'WATCH_MODEL_AUTHORITY_INVALID');
            ELSE
                raw_score := (model->>'score')::numeric;
                IF raw_score < -100 OR raw_score > 100
                   OR model->>'direction' IS DISTINCT FROM (CASE
                        WHEN raw_score >= 12 THEN 'BULLISH'
                        WHEN raw_score <= -12 THEN 'BEARISH'
                        ELSE 'NEUTRAL' END) THEN
                    reasons := array_append(reasons,'WATCH_MODEL_SCORE_INVALID');
                ELSE
                    aligned_score := raw_score * multiplier;
                    derived_match := aligned_score >= candidate_threshold;
                END IF;
            END IF;
        END IF;
        -- Parent eligibility is a separate independence axis.  A missing or
        -- boundary-unverified parent cannot erase an otherwise known Watch
        -- predicate; conversely, a source-valid UNKNOWN is still an exact
        -- server derivation rather than an attestation failure.
        IF NOT EXISTS (
            SELECT 1 FROM unnest(reasons) AS reason(value)
            WHERE reason.value NOT LIKE 'PARENT_%'
        ) THEN
            derived_knowledge_status := 'KNOWN';
        ELSE
            raw_score := NULL;
            aligned_score := NULL;
            derived_match := NULL;
        END IF;
        status_value := 'VERIFIED';
    END IF;

    semantics := jsonb_build_object(
        'version', 'stage8-db-derived-projection-semantics-v1',
        'exact_binding_sha256', registry.exact_binding_sha256,
        'attempt_id', source_attempt.attempt_id,
        'attempt_fingerprint', btrim(source_attempt.attempt_fingerprint),
        'sampler_version', source_attempt.sampler_version,
        'source_candle_open_utc', research_stage8_utc_text_v1(
            source_attempt.source_candle_open_utc),
        'source_attempt_evaluation_status', source_attempt.evaluation_status,
        'anchor_slot_id', CASE WHEN slot_found THEN source_slot.anchor_slot_id END,
        'event_id', CASE WHEN event_found THEN source_event.event_id END,
        'event_fingerprint', CASE WHEN event_found
            THEN btrim(source_event.event_fingerprint) END,
        'symbol', source_attempt.symbol,
        'direction', direction,
        'decision_time_utc', CASE WHEN source_attempt.decision_time_utc IS NULL
            THEN NULL ELSE research_stage8_utc_text_v1(
                source_attempt.decision_time_utc) END,
        'selected_snapshot_set_id', CASE WHEN snapshot_found
            THEN source_snapshot.snapshot_set_id END,
        'selected_snapshot_key', CASE WHEN snapshot_found
            THEN btrim(source_snapshot.snapshot_key) END,
        'selected_payload_sha256', CASE WHEN snapshot_found
            THEN btrim(source_snapshot.payload_sha256) END,
        'selected_durably_available_at_utc', CASE WHEN snapshot_found
            THEN research_stage8_utc_text_v1(durable_at) END,
        'operational_scores_sha256', CASE WHEN snapshot_found
            THEN operational->>'payload_sha256' END,
        'watch_code_manifest_sha256', CASE WHEN snapshot_found
            THEN research_stage8_json_sha256_v1(operational->'code_sha256') END,
        'model', model_name,
        'model_observation_sha256', CASE WHEN snapshot_found
            THEN research_stage8_watch_json_sha256_v1(model) END,
        'model_source_sha256', CASE WHEN snapshot_found
            THEN research_stage8_watch_json_sha256_v1(model_source) END,
        'derivatives_snapshot_sha256', CASE WHEN snapshot_found
            THEN coin->'sources'->>'derivatives_snapshot_sha256' END,
        'raw_score', raw_score,
        'source_direction', CASE WHEN snapshot_found THEN model->>'direction' END,
        'direction_multiplier', multiplier,
        'aligned_score', aligned_score,
        'candidate_operator', candidate->>'operator',
        'candidate_threshold', candidate_threshold,
        'candidate_match', derived_match,
        'knowledge_status', derived_knowledge_status,
        'parent_authority_class', parent_class,
        'btc_parent_movement_id', CASE WHEN parent_found AND parent_class IN (
            'LIVE','PROVEN_NOT_EVIDENCE_ELIGIBLE')
            THEN source_parent.btc_parent_movement_id END,
        'parent_start_time_utc', CASE WHEN parent_found AND parent_class IN (
            'LIVE','PROVEN_NOT_EVIDENCE_ELIGIBLE')
            THEN research_stage8_utc_text_v1(source_parent.start_time_utc) END,
        'parent_policy_version', CASE WHEN parent_class IN (
            'LIVE','PROVEN_NOT_EVIDENCE_ELIGIBLE')
            THEN registry.parent_policy_version END,
        'membership_status', CASE parent_class
            WHEN 'LIVE' THEN 'LIVE'
            WHEN 'PROVEN_NOT_EVIDENCE_ELIGIBLE' THEN 'BOUNDARY_UNVERIFIED'
            ELSE NULL END,
        'parent_evidence_eligible', CASE WHEN parent_found AND parent_class IN (
            'LIVE','PROVEN_NOT_EVIDENCE_ELIGIBLE')
            THEN source_parent.evidence_eligible END
    );
    -- Hash only causal semantics.  Delivery fields and the parent's evolving
    -- end/observed-through/state fields may legitimately change after a
    -- decision and therefore must never invalidate a later outcome replay.
    source_rows := jsonb_build_object(
        'attempt_causal_sha256', research_stage8_json_sha256_v1(
            jsonb_build_object(
                'attempt_id', source_attempt.attempt_id,
                'attempt_fingerprint', btrim(source_attempt.attempt_fingerprint),
                'input_fingerprint', btrim(source_attempt.input_fingerprint),
                'sampler_version', source_attempt.sampler_version,
                'symbol', source_attempt.symbol,
                'source_candle_open_utc', research_stage8_utc_text_v1(
                    source_attempt.source_candle_open_utc),
                'evaluation_status', source_attempt.evaluation_status,
                'decision_time_utc', CASE
                    WHEN source_attempt.decision_time_utc IS NULL THEN NULL
                    ELSE research_stage8_utc_text_v1(
                        source_attempt.decision_time_utc) END
            )),
        'slot_causal_sha256', CASE WHEN slot_found THEN
            research_stage8_json_sha256_v1(jsonb_build_object(
                'anchor_slot_id', source_slot.anchor_slot_id,
                'sampler_version', source_slot.sampler_version,
                'symbol', source_slot.symbol,
                'source_candle_open_utc', research_stage8_utc_text_v1(
                    source_slot.source_candle_open_utc),
                'decision_time_utc', research_stage8_utc_text_v1(
                    source_slot.decision_time_utc),
                'input_fingerprint', btrim(source_slot.input_fingerprint),
                'long_event_id', source_slot.long_event_id,
                'short_event_id', source_slot.short_event_id
            )) END,
        'event_causal_sha256', CASE WHEN event_found THEN
            research_stage8_json_sha256_v1(jsonb_build_object(
                'event_id', source_event.event_id,
                'event_fingerprint', btrim(source_event.event_fingerprint),
                'symbol', source_event.symbol,
                'direction', source_event.direction,
                'alert_time_utc', research_stage8_utc_text_v1(
                    source_event.alert_time_utc),
                'current_price', source_event.current_price
            )) END,
        'watch_snapshot_causal_sha256', CASE WHEN snapshot_found THEN
            research_stage8_json_sha256_v1(jsonb_build_object(
                'snapshot_set_id', source_snapshot.snapshot_set_id,
                'snapshot_key', btrim(source_snapshot.snapshot_key),
                'payload_sha256', btrim(source_snapshot.payload_sha256),
                'source', source_snapshot.source,
                'cycle_id', source_snapshot.cycle_id,
                'available_at_utc', research_stage8_utc_text_v1(
                    source_snapshot.available_at_utc),
                'created_at_utc', research_stage8_utc_text_v1(
                    source_snapshot.created_at_utc),
                'source_metadata_sha256', research_stage8_json_sha256_v1(
                    source_snapshot.source_metadata)
            )) END,
        'membership_causal_sha256', NULL,
        'parent_causal_sha256', CASE WHEN parent_found THEN
            research_stage8_json_sha256_v1(jsonb_build_object(
                'btc_parent_movement_id',
                    source_parent.btc_parent_movement_id,
                'episode_policy_version', source_parent.episode_policy_version,
                'start_time_utc', research_stage8_utc_text_v1(
                    source_parent.start_time_utc),
                'confirmed_at_utc', CASE
                    WHEN source_parent.confirmed_at_utc IS NULL THEN NULL
                    ELSE research_stage8_utc_text_v1(
                        source_parent.confirmed_at_utc) END,
                'direction', CASE WHEN parent_class = 'LIVE'
                    THEN source_parent.direction ELSE NULL END,
                'evidence_eligible', source_parent.evidence_eligible,
                'boundary_reason', source_parent.boundary_reason,
                'price_source', source_parent.price_source
            )) END,
        'btc_bar_causal_sha256', CASE WHEN bar_found THEN
            research_stage8_json_sha256_v1(jsonb_build_object(
                'open_time_utc', research_stage8_utc_text_v1(
                    source_bar.open_time_utc),
                'close_time_utc', research_stage8_utc_text_v1(
                    source_bar.close_time_utc),
                'open', source_bar.open,
                'high', source_bar.high,
                'low', source_bar.low,
                'close', source_bar.close,
                'price_source', source_bar.price_source
            )) END
    );
    RETURN jsonb_build_object(
        'version', 'stage8-db-derived-projection-attestation-v1',
        'status', status_value,
        'reasons', to_jsonb(reasons),
        'projection_semantics', semantics,
        'projection_semantics_sha256', research_stage8_json_sha256_v1(semantics),
        'source_rows', source_rows,
        'source_rows_sha256', research_stage8_json_sha256_v1(source_rows)
    );
END;
$$;

-- Harden the earlier archive trigger as well: an invoker's temporary slot
-- relation must not hide that an event belongs to an immutable anchor. Keep
-- its original UPDATE/DELETE behavior and do not add source-write authority.
CREATE OR REPLACE FUNCTION prevent_prospective_anchor_event_mutation()
RETURNS TRIGGER LANGUAGE plpgsql SECURITY INVOKER AS $$
DECLARE anchor_owned BOOLEAN;
BEGIN
    PERFORM pg_catalog.set_config('search_path',
        pg_catalog.quote_ident(TG_TABLE_SCHEMA) || ',pg_catalog,pg_temp', true);
    EXECUTE pg_catalog.format(
        'SELECT EXISTS (SELECT 1 FROM %I.research_prospective_anchor_slots AS slot '
        || 'WHERE slot.long_event_id = $1 OR slot.short_event_id = $1)', TG_TABLE_SCHEMA)
    INTO anchor_owned USING OLD.event_id;
    IF anchor_owned THEN
        RAISE EXCEPTION 'Research Event % belongs to an immutable prospective anchor', OLD.event_id;
    END IF;
    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION research_stage8_fact_insert_guard_v1()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    batch research_stage8_projection_fact_batches%ROWTYPE;
    registry research_stage8_binding_registry%ROWTYPE;
    source_attempt RECORD;
    source_slot RECORD;
    source_slot_found BOOLEAN := FALSE;
    server_projection JSONB;
    server_semantics JSONB;
    identity JSONB;
    persisted_text TEXT;
BEGIN
    PERFORM pg_catalog.set_config(
        'search_path',
        pg_catalog.quote_ident(TG_TABLE_SCHEMA) || ',pg_catalog,pg_temp',
        true
    );
    IF NEW.fact_record_sha256 IS NOT NULL OR NEW.persisted_at_utc IS NOT NULL
       OR NEW.fact_record IS NOT NULL OR NEW.persisted_by IS NOT NULL
       OR NEW.selection_fact_identity IS NOT NULL
       OR NEW.selection_fact_identity_sha256 IS NOT NULL
       OR NEW.server_projection_attestation IS NOT NULL
       OR NEW.server_projection_attestation_sha256 IS NOT NULL
       OR NEW.server_projection_status IS NOT NULL THEN
        RAISE EXCEPTION 'Stage-8 fact server-owned fields cannot be supplied';
    END IF;
    IF EXISTS (SELECT 1 FROM research_stage8_projection_fact_batch_seals
               WHERE fact_batch_record_sha256 = NEW.fact_batch_record_sha256) THEN
        RAISE EXCEPTION 'Stage-8 sealed fact batch cannot accept more facts';
    END IF;
    SELECT * INTO STRICT batch FROM research_stage8_projection_fact_batches
    WHERE fact_batch_record_sha256 = NEW.fact_batch_record_sha256;
    SELECT * INTO STRICT registry FROM research_stage8_binding_registry
    WHERE exact_binding_sha256 = NEW.exact_binding_sha256;
    SELECT attempt_fingerprint, input_fingerprint, sampler_version, symbol,
           source_candle_open_utc, evaluation_status, decision_time_utc
    INTO STRICT source_attempt
    FROM research_prospective_anchor_attempts WHERE attempt_id = NEW.attempt_id;
    identity := NEW.fact->'identity';
    IF jsonb_typeof(NEW.fact) IS DISTINCT FROM 'object'
       OR (SELECT array_agg(key ORDER BY key)
           FROM jsonb_object_keys(NEW.fact) AS keys(key))
            IS DISTINCT FROM ARRAY[
                'applicability_status','binding','candidate_match','fact_sha256',
                'feature','identity','knowledge_status','manifest_sha256','reasons',
                'source','source_reasons','version'
            ]::TEXT[]
       OR jsonb_typeof(identity) IS DISTINCT FROM 'object'
       OR (SELECT array_agg(key ORDER BY key)
           FROM jsonb_object_keys(identity) AS keys(key))
            IS DISTINCT FROM ARRAY[
                'anchor_slot_id','attempt_fingerprint','attempt_id','candidate_id',
                'decision_time_utc','direction','event_fingerprint','event_id',
                'model','sampler_version','scope_id','source_candle_open_utc',
                'symbol','threshold_bps','window_minutes'
            ]::TEXT[]
       OR jsonb_typeof(NEW.fact->'feature') IS DISTINCT FROM 'object'
       OR (SELECT array_agg(key ORDER BY key)
           FROM jsonb_object_keys(NEW.fact->'feature') AS keys(key))
            IS DISTINCT FROM ARRAY[
                'aligned_score','candidate_direction','direction_multiplier',
                'model','name','operator','raw_score','source_direction','value'
            ]::TEXT[]
       OR jsonb_typeof(NEW.fact->'source') IS DISTINCT FROM 'object'
       OR (SELECT array_agg(key ORDER BY key)
           FROM jsonb_object_keys(NEW.fact->'source') AS keys(key))
            IS DISTINCT FROM ARRAY[
                'anchor_model_score_status','derivatives_snapshot_sha256',
                'model_observation_sha256','model_source_sha256',
                'outer_archive_hash_verified','price_provenance',
                'score_generation_status','watch_code_manifest_sha256',
                'watch_code_sha256','watch_inner_payload_sha256',
                'watch_selection_attestation',
                'watch_selection_attestation_sha256',
                'watch_selection_query_binding_sha256','watch_selection_status',
                'watch_snapshot','watch_version'
            ]::TEXT[]
       OR jsonb_typeof(NEW.fact->'source_reasons') IS DISTINCT FROM 'object'
       OR (SELECT array_agg(key ORDER BY key)
           FROM jsonb_object_keys(NEW.fact->'source_reasons') AS keys(key))
            IS DISTINCT FROM ARRAY['anchor','capture','selection']::TEXT[]
       OR jsonb_typeof(NEW.fact_authority) IS DISTINCT FROM 'object'
       OR (SELECT array_agg(key ORDER BY key)
           FROM jsonb_object_keys(NEW.fact_authority) AS keys(key))
            IS DISTINCT FROM ARRAY[
                'anchor_slot_id','attempt_fingerprint','attempt_id','candidate_match',
                'decision_time_utc','direction','event_fingerprint','event_id',
                'exact_binding_sha256','expected_fact_sha256',
                'expected_noneligibility_proof_sha256',
                'expected_parent_membership_evidence_sha256',
                'expected_watch_code_manifest_sha256',
                'expected_watch_selection_attestation_sha256',
                'fact_authority_sha256','knowledge_status',
                'observed_fact_code_manifest_sha256',
                'observed_fact_selection_attestation_sha256','projection_version',
                'source_audit_version','symbol','watch_code_manifest_sha256',
                'watch_selection_attestation_sha256',
                'watch_selection_observation_sha256'
            ]::TEXT[]
       OR research_stage8_has_forbidden_evidence_key_v1(jsonb_build_array(
            NEW.fact, NEW.fact_authority, NEW.watch_selection_attestation,
            NEW.parent_membership_evidence, NEW.noneligibility_proof
       ))
       OR NEW.fact->>'version' IS DISTINCT FROM registry.projection_version
       OR NEW.fact->>'manifest_sha256' IS DISTINCT FROM registry.manifest_sha256
       OR NEW.fact->'binding' IS DISTINCT FROM registry.exact_binding
       OR NEW.fact_authority->>'projection_version'
            IS DISTINCT FROM registry.projection_version
       OR NEW.fact_authority->>'source_audit_version'
            IS DISTINCT FROM registry.source_audit_version
       OR batch.exact_binding_sha256 IS DISTINCT FROM NEW.exact_binding_sha256
       OR NOT batch.attempt_ids @> to_jsonb(ARRAY[NEW.attempt_id])
       OR source_attempt.attempt_fingerprint IS DISTINCT FROM NEW.attempt_fingerprint
       OR source_attempt.sampler_version IS DISTINCT FROM identity->>'sampler_version'
       OR source_attempt.symbol IS DISTINCT FROM NEW.symbol
       OR identity->>'source_candle_open_utc' IS DISTINCT FROM
            research_stage8_utc_text_v1(source_attempt.source_candle_open_utc)
       OR identity->>'scope_id' IS DISTINCT FROM registry.scope_id
       OR identity->>'candidate_id' IS DISTINCT FROM registry.candidate_id
       OR identity->>'model' IS DISTINCT FROM
            registry.exact_binding->'binding'->'candidate'->>'model'
       OR identity->>'direction' IS DISTINCT FROM
            registry.exact_binding->'binding'->'candidate'->>'direction'
       OR NULLIF(identity->>'window_minutes','')::integer
            IS DISTINCT FROM registry.window_minutes
       OR NULLIF(identity->>'threshold_bps','')::integer
            IS DISTINCT FROM registry.threshold_bps
       OR NEW.fact_sha256 IS DISTINCT FROM NEW.fact->>'fact_sha256'
       OR NEW.fact_sha256 IS DISTINCT FROM research_stage8_json_sha256_v1(NEW.fact - 'fact_sha256')
       OR NEW.fact_authority_sha256 IS DISTINCT FROM NEW.fact_authority->>'fact_authority_sha256'
       OR NEW.fact_authority_sha256
            IS DISTINCT FROM research_stage8_json_sha256_v1(NEW.fact_authority - 'fact_authority_sha256')
       OR NEW.fact->'binding'->>'binding_sha256' IS DISTINCT FROM NEW.exact_binding_sha256
       OR NEW.fact_authority->>'exact_binding_sha256' IS DISTINCT FROM NEW.exact_binding_sha256
       OR NULLIF(identity->>'attempt_id','')::bigint IS DISTINCT FROM NEW.attempt_id
       OR identity->>'attempt_fingerprint' IS DISTINCT FROM NEW.attempt_fingerprint
       OR NULLIF(identity->>'anchor_slot_id','')::bigint IS DISTINCT FROM NEW.anchor_slot_id
       OR NULLIF(identity->>'event_id','')::bigint IS DISTINCT FROM NEW.event_id
       OR identity->>'event_fingerprint' IS DISTINCT FROM NEW.event_fingerprint
       OR identity->>'symbol' IS DISTINCT FROM NEW.symbol
       OR identity->>'direction' IS DISTINCT FROM NEW.direction
       OR NULLIF(identity->>'decision_time_utc','')::timestamptz IS DISTINCT FROM NEW.decision_time_utc
       OR NEW.fact->>'knowledge_status' IS DISTINCT FROM NEW.knowledge_status
       OR (NEW.fact->>'candidate_match')::boolean IS DISTINCT FROM NEW.candidate_match THEN
        RAISE EXCEPTION 'Stage-8 projected fact identity or hash is invalid';
    END IF;
    IF source_attempt.evaluation_status = 'EVALUABLE' THEN
        SELECT slot.anchor_slot_id,
               CASE
                   WHEN registry.exact_binding->'binding'->'candidate'->>'direction'
                            = 'LONG' THEN slot.long_event_id
                   ELSE slot.short_event_id
               END AS event_id,
               event_row.event_fingerprint,
               event_row.symbol,
               event_row.direction,
               event_row.alert_time_utc AS decision_time_utc
        INTO source_slot
        FROM research_prospective_anchor_slots AS slot
        JOIN research_events AS event_row
          ON event_row.event_id = CASE
              WHEN registry.exact_binding->'binding'->'candidate'->>'direction'
                        = 'LONG' THEN slot.long_event_id
              ELSE slot.short_event_id
          END
        WHERE slot.sampler_version = source_attempt.sampler_version
          AND slot.symbol = source_attempt.symbol
          AND slot.source_candle_open_utc = source_attempt.source_candle_open_utc
          AND slot.input_fingerprint = source_attempt.input_fingerprint
          AND slot.decision_time_utc = source_attempt.decision_time_utc;
        source_slot_found := FOUND;
        IF source_slot_found THEN
            IF NEW.anchor_slot_id IS DISTINCT FROM source_slot.anchor_slot_id
               OR NEW.event_id IS DISTINCT FROM source_slot.event_id
               OR NEW.event_fingerprint IS DISTINCT FROM source_slot.event_fingerprint
               OR NEW.symbol IS DISTINCT FROM source_slot.symbol
               OR NEW.direction IS DISTINCT FROM source_slot.direction
               OR NEW.decision_time_utc IS DISTINCT FROM source_slot.decision_time_utc
               OR source_slot.direction IS DISTINCT FROM
                    registry.exact_binding->'binding'->'candidate'->>'direction' THEN
                RAISE EXCEPTION 'Stage-8 fact event is not the exact source attempt slot event';
            END IF;
        ELSIF NEW.anchor_slot_id IS NOT NULL OR NEW.event_id IS NOT NULL
           OR NEW.event_fingerprint IS NOT NULL
           OR NEW.decision_time_utc IS DISTINCT FROM source_attempt.decision_time_utc
           OR NEW.knowledge_status IS DISTINCT FROM 'UNKNOWN'
           OR NEW.candidate_match IS NOT NULL
           OR NEW.parent_membership_evidence->>'validation_status'
                IS DISTINCT FROM 'UNKNOWN' THEN
            RAISE EXCEPTION 'Stage-8 missing slot must remain an exact UNKNOWN source state';
        END IF;
    ELSE
        IF NEW.anchor_slot_id IS NOT NULL OR NEW.event_id IS NOT NULL
           OR NEW.event_fingerprint IS NOT NULL OR NEW.decision_time_utc IS NOT NULL
           OR identity->'anchor_slot_id' IS DISTINCT FROM 'null'::jsonb
           OR identity->'event_id' IS DISTINCT FROM 'null'::jsonb
           OR identity->'event_fingerprint' IS DISTINCT FROM 'null'::jsonb
           OR identity->'decision_time_utc' IS DISTINCT FROM 'null'::jsonb THEN
            RAISE EXCEPTION 'Stage-8 non-evaluable fact must preserve null source identity';
        END IF;
    END IF;
    IF (NEW.parent_membership_evidence IS NULL) = (NEW.noneligibility_proof IS NULL)
       OR (NEW.parent_membership_evidence IS NULL)
            <> (NEW.parent_membership_evidence_sha256 IS NULL)
       OR (NEW.noneligibility_proof IS NULL)
            <> (NEW.noneligibility_proof_sha256 IS NULL)
       OR (NEW.parent_membership_evidence IS NOT NULL AND
           NEW.parent_membership_evidence_sha256
             IS DISTINCT FROM research_stage8_json_sha256_v1(NEW.parent_membership_evidence))
       OR (NEW.noneligibility_proof IS NOT NULL AND
           NEW.noneligibility_proof_sha256
             IS DISTINCT FROM NEW.noneligibility_proof->>'proof_sha256')
       OR (NEW.noneligibility_proof IS NOT NULL AND
           NEW.noneligibility_proof_sha256
             IS DISTINCT FROM research_stage8_json_sha256_v1(
                    NEW.noneligibility_proof - 'proof_sha256')) THEN
        RAISE EXCEPTION 'Stage-8 fact requires one independently hashed membership state';
    END IF;
    IF NEW.noneligibility_proof IS NOT NULL AND (
           (SELECT array_agg(key ORDER BY key)
            FROM jsonb_object_keys(NEW.noneligibility_proof) AS keys(key))
                IS DISTINCT FROM ARRAY[
                    'attempt_fingerprint','attempt_id','decision_time_utc',
                    'evaluation_status','proof_sha256','reason','sampler_version',
                    'symbol','version'
                ]::TEXT[]
           OR NEW.noneligibility_proof->>'version'
                IS DISTINCT FROM 'stage8-attempt-noneligibility-proof-v1'
           OR NULLIF(NEW.noneligibility_proof->>'attempt_id','')::bigint
                IS DISTINCT FROM NEW.attempt_id
           OR NEW.noneligibility_proof->>'attempt_fingerprint'
                IS DISTINCT FROM source_attempt.attempt_fingerprint
           OR NEW.noneligibility_proof->>'sampler_version'
                IS DISTINCT FROM source_attempt.sampler_version
           OR NEW.noneligibility_proof->>'symbol'
                IS DISTINCT FROM source_attempt.symbol
           OR NEW.noneligibility_proof->>'evaluation_status'
                IS DISTINCT FROM source_attempt.evaluation_status
           OR (source_attempt.evaluation_status IS DISTINCT FROM 'UNEVALUABLE'
               AND source_attempt.evaluation_status
                    IS DISTINCT FROM 'COVERAGE_EXCLUDED')
           OR NEW.noneligibility_proof->'decision_time_utc' IS DISTINCT FROM 'null'::jsonb
           OR source_attempt.decision_time_utc IS NOT NULL
           OR NEW.noneligibility_proof->>'reason' IS DISTINCT FROM CASE
                WHEN source_attempt.evaluation_status = 'UNEVALUABLE'
                    THEN 'ATTEMPT_UNEVALUABLE_NO_DECISION'
                WHEN source_attempt.evaluation_status = 'COVERAGE_EXCLUDED'
                    THEN 'ATTEMPT_COVERAGE_EXCLUDED_NO_DECISION'
                ELSE NULL
              END
       ) THEN
        RAISE EXCEPTION 'Stage-8 noneligibility proof does not match the source attempt';
    END IF;
    IF NEW.parent_membership_evidence IS NOT NULL AND (
           source_attempt.evaluation_status IS DISTINCT FROM 'EVALUABLE'
           OR source_attempt.decision_time_utc IS DISTINCT FROM NEW.decision_time_utc
       ) THEN
        RAISE EXCEPTION 'Stage-8 parent evidence requires the exact EVALUABLE source attempt';
    END IF;
    IF NEW.parent_membership_evidence IS NOT NULL THEN
        IF NEW.parent_membership_evidence->>'validation_status'
                IN ('VALID','PROVEN_NOT_EVIDENCE_ELIGIBLE') THEN
            IF (SELECT array_agg(key ORDER BY key)
                FROM jsonb_object_keys(NEW.parent_membership_evidence) AS keys(key))
                   IS DISTINCT FROM ARRAY[
                       'btc_bar','btc_observed_close_utc','btc_parent_movement_id',
                       'decision_time_utc','direction','event_fingerprint','event_id',
                       'membership_status','parent_boundary_reason',
                       'parent_confirmed_at_utc','parent_direction',
                       'parent_end_time_utc','parent_evidence_eligible',
                       'parent_observed_through_utc','parent_policy_version',
                       'parent_price_source','parent_start_time_utc','symbol',
                       'validation_status','version'
                   ]::TEXT[]
               OR NEW.parent_membership_evidence->>'version'
                    IS DISTINCT FROM 'stage8-canonical-parent-membership-evidence-v1'
               OR jsonb_typeof(NEW.parent_membership_evidence->'btc_bar')
                    IS DISTINCT FROM 'object'
               OR (SELECT array_agg(key ORDER BY key)
                   FROM jsonb_object_keys(
                       NEW.parent_membership_evidence->'btc_bar'
                   ) AS keys(key))
                    IS DISTINCT FROM ARRAY[
                        'close','close_time_utc','high','low','open',
                        'open_time_utc','price_source'
                    ]::TEXT[]
               OR NOT EXISTS (
                    SELECT 1
                    FROM research_events AS event_row
                    CROSS JOIN LATERAL (
                        SELECT b.* FROM research_btc_price_bars AS b
                        WHERE b.close_time_utc <= event_row.alert_time_utc
                        ORDER BY b.close_time_utc DESC LIMIT 1
                    ) AS btc_bar
                    CROSS JOIN LATERAL (
                        SELECT p.* FROM research_btc_parent_movements AS p
                        WHERE p.episode_policy_version = registry.parent_policy_version
                          AND p.start_time_utc <= event_row.alert_time_utc
                          AND (p.end_time_utc IS NULL
                               OR event_row.alert_time_utc < p.end_time_utc)
                        ORDER BY p.start_time_utc DESC,
                                 p.btc_parent_movement_id COLLATE "C" LIMIT 1
                    ) AS parent
                    WHERE event_row.event_id = NEW.event_id
                      AND event_row.event_fingerprint
                            IS NOT DISTINCT FROM NEW.event_fingerprint
                      AND event_row.symbol IS NOT DISTINCT FROM NEW.symbol
                      AND event_row.direction IS NOT DISTINCT FROM NEW.direction
                      AND event_row.alert_time_utc
                            IS NOT DISTINCT FROM NEW.decision_time_utc
                      AND NEW.parent_membership_evidence->>'event_id'
                            IS NOT DISTINCT FROM event_row.event_id::text
                      AND NEW.parent_membership_evidence->>'event_fingerprint'
                            IS NOT DISTINCT FROM event_row.event_fingerprint
                      AND NEW.parent_membership_evidence->>'symbol'
                            IS NOT DISTINCT FROM event_row.symbol
                      AND NEW.parent_membership_evidence->>'direction'
                            IS NOT DISTINCT FROM event_row.direction
                      AND NEW.parent_membership_evidence->>'decision_time_utc'
                            IS NOT DISTINCT FROM research_stage8_utc_text_v1(
                                event_row.alert_time_utc)
                      AND NEW.parent_membership_evidence->>'parent_policy_version'
                            IS NOT DISTINCT FROM registry.parent_policy_version
                      AND NEW.parent_membership_evidence->>'membership_status'
                            IS NOT DISTINCT FROM CASE
                                WHEN parent.evidence_eligible IS TRUE
                                 AND parent.confirmed_at_utc IS NOT NULL
                                 AND parent.confirmed_at_utc <= event_row.alert_time_utc
                                    THEN 'LIVE'
                                ELSE 'BOUNDARY_UNVERIFIED' END
                      AND NEW.parent_membership_evidence->>'btc_parent_movement_id'
                            IS NOT DISTINCT FROM parent.btc_parent_movement_id
                      AND NEW.parent_membership_evidence->>'btc_observed_close_utc'
                            IS NOT DISTINCT FROM research_stage8_utc_text_v1(
                                btc_bar.close_time_utc)
                      AND NEW.parent_membership_evidence->>'parent_start_time_utc'
                            IS NOT DISTINCT FROM research_stage8_utc_text_v1(
                                parent.start_time_utc)
                      AND NEW.parent_membership_evidence->'parent_end_time_utc'
                            IS NOT DISTINCT FROM CASE
                                WHEN parent.end_time_utc IS NULL THEN 'null'::jsonb
                                ELSE to_jsonb(research_stage8_utc_text_v1(
                                    parent.end_time_utc))
                            END
                      AND NEW.parent_membership_evidence->'parent_confirmed_at_utc'
                            IS NOT DISTINCT FROM CASE
                                WHEN parent.confirmed_at_utc IS NULL THEN 'null'::jsonb
                                ELSE to_jsonb(research_stage8_utc_text_v1(
                                    parent.confirmed_at_utc))
                            END
                      AND NEW.parent_membership_evidence->>'parent_direction'
                            IS NOT DISTINCT FROM parent.direction
                      AND NEW.parent_membership_evidence->'parent_evidence_eligible'
                            IS NOT DISTINCT FROM to_jsonb(parent.evidence_eligible)
                      AND NEW.parent_membership_evidence->>'parent_boundary_reason'
                            IS NOT DISTINCT FROM parent.boundary_reason
                      AND NEW.parent_membership_evidence->>'parent_observed_through_utc'
                            IS NOT DISTINCT FROM research_stage8_utc_text_v1(
                                parent.observed_through_utc)
                      AND NEW.parent_membership_evidence->>'parent_price_source'
                            IS NOT DISTINCT FROM parent.price_source
                      AND (parent.state_json->>'reversal_bps')::integer = 200
                      AND parent.observed_through_utc >= btc_bar.close_time_utc
                      AND event_row.alert_time_utc - btc_bar.close_time_utc
                            >= interval '0 seconds'
                      AND event_row.alert_time_utc - btc_bar.close_time_utc
                            < interval '1 minute'
                      AND btc_bar.close_time_utc >= parent.start_time_utc
                      AND (parent.end_time_utc IS NULL
                           OR parent.end_time_utc > parent.start_time_utc)
                      AND date_trunc('minute', btc_bar.open_time_utc)
                            = btc_bar.open_time_utc
                      AND btc_bar.close_time_utc = btc_bar.open_time_utc
                            + interval '1 minute' - interval '1 millisecond'
                      AND least(btc_bar.open, btc_bar.high, btc_bar.low,
                                btc_bar.close) > 0
                      AND btc_bar.high >= greatest(
                            btc_bar.open, btc_bar.close, btc_bar.low)
                      AND btc_bar.low <= least(
                            btc_bar.open, btc_bar.close, btc_bar.high)
                      AND parent.price_source = 'BINANCE_SPOT_BTCUSDT_1M'
                      AND btc_bar.price_source = 'BINANCE_SPOT_BTCUSDT_1M'
                      AND parent.btc_parent_movement_id = encode(sha256(convert_to(
                            parent.episode_policy_version || '|'
                            || 'BINANCE_SPOT_BTCUSDT_1M' || '|'
                            || to_char(parent.start_time_utc AT TIME ZONE 'UTC',
                                       'YYYY-MM-DD"T"HH24:MI:SS')
                            || CASE WHEN extract(microseconds FROM
                                          parent.start_time_utc)::integer
                                          % 1000000 = 0
                                    THEN '' ELSE '.' || to_char(
                                        parent.start_time_utc AT TIME ZONE 'UTC',
                                        'US') END
                            || '+00:00', 'UTF8')), 'hex')
                      AND NEW.parent_membership_evidence->'btc_bar'->>'open_time_utc'
                            IS NOT DISTINCT FROM research_stage8_utc_text_v1(
                                btc_bar.open_time_utc)
                      AND NEW.parent_membership_evidence->'btc_bar'->>'close_time_utc'
                            IS NOT DISTINCT FROM research_stage8_utc_text_v1(
                                btc_bar.close_time_utc)
                      AND (NEW.parent_membership_evidence->'btc_bar'->>'open')::double precision
                            IS NOT DISTINCT FROM btc_bar.open
                      AND (NEW.parent_membership_evidence->'btc_bar'->>'high')::double precision
                            IS NOT DISTINCT FROM btc_bar.high
                      AND (NEW.parent_membership_evidence->'btc_bar'->>'low')::double precision
                            IS NOT DISTINCT FROM btc_bar.low
                      AND (NEW.parent_membership_evidence->'btc_bar'->>'close')::double precision
                            IS NOT DISTINCT FROM btc_bar.close
                      AND NEW.parent_membership_evidence->'btc_bar'->>'price_source'
                            IS NOT DISTINCT FROM btc_bar.price_source
                      AND (
                          (NEW.parent_membership_evidence->>'validation_status' = 'VALID'
                           AND parent.confirmed_at_utc <= event_row.alert_time_utc
                           AND parent.evidence_eligible IS TRUE)
                          OR
                          (NEW.parent_membership_evidence->>'validation_status'
                                = 'PROVEN_NOT_EVIDENCE_ELIGIBLE'
                           AND parent.evidence_eligible IS FALSE
                           AND parent.boundary_reason IN (
                               'LEFT_BOUNDARY_UNVERIFIED','BTC_DATA_GAP'
                           ))
                      )
               ) THEN
                RAISE EXCEPTION 'Stage-8 parent evidence is not source-backed';
            END IF;
        ELSIF NEW.parent_membership_evidence->>'validation_status' = 'UNKNOWN' THEN
            IF (SELECT array_agg(key ORDER BY key)
                FROM jsonb_object_keys(NEW.parent_membership_evidence) AS keys(key))
                   NOT IN (
                       ARRAY[
                           'anchor_slot_id','attempt_fingerprint','attempt_id',
                           'decision_time_utc','direction','event_fingerprint',
                           'event_id','reason','source_presence','symbol',
                           'validation_status','version'
                       ]::TEXT[],
                       ARRAY[
                           'anchor_slot_id','attempt_fingerprint','attempt_id',
                           'decision_time_utc','direction','event_fingerprint',
                           'event_id','reason','source_presence','source_reason_codes',
                           'symbol','validation_status','version'
                       ]::TEXT[]
                   )
               OR NEW.parent_membership_evidence->>'version'
                    IS DISTINCT FROM 'stage8-canonical-parent-membership-evidence-v1'
               OR NULLIF(NEW.parent_membership_evidence->>'attempt_id','')::bigint
                    IS DISTINCT FROM NEW.attempt_id
               OR NEW.parent_membership_evidence->>'attempt_fingerprint'
                    IS DISTINCT FROM NEW.attempt_fingerprint
               OR NULLIF(NEW.parent_membership_evidence->>'anchor_slot_id','')::bigint
                    IS DISTINCT FROM NEW.anchor_slot_id
               OR NULLIF(NEW.parent_membership_evidence->>'event_id','')::bigint
                    IS DISTINCT FROM NEW.event_id
               OR NEW.parent_membership_evidence->>'event_fingerprint'
                    IS DISTINCT FROM NEW.event_fingerprint
               OR NEW.parent_membership_evidence->>'symbol' IS DISTINCT FROM NEW.symbol
               OR NEW.parent_membership_evidence->>'direction'
                    IS DISTINCT FROM NEW.direction
               OR NEW.parent_membership_evidence->>'decision_time_utc'
                    IS DISTINCT FROM NEW.fact->'identity'->>'decision_time_utc' THEN
                RAISE EXCEPTION 'Stage-8 UNKNOWN parent evidence is malformed';
            END IF;
        ELSE
            RAISE EXCEPTION 'Stage-8 parent evidence validation status is unknown';
        END IF;
    END IF;
    IF NEW.fact_authority->>'expected_fact_sha256' IS DISTINCT FROM NEW.fact_sha256
       OR (NEW.fact_authority->>'attempt_id')::bigint IS DISTINCT FROM NEW.attempt_id
       OR NEW.fact_authority->>'attempt_fingerprint' IS DISTINCT FROM NEW.attempt_fingerprint
       OR NULLIF(NEW.fact_authority->>'anchor_slot_id','')::bigint
            IS DISTINCT FROM NEW.anchor_slot_id
       OR NULLIF(NEW.fact_authority->>'event_id','')::bigint
            IS DISTINCT FROM NEW.event_id
       OR NEW.fact_authority->>'event_fingerprint' IS DISTINCT FROM NEW.event_fingerprint
       OR NEW.fact_authority->>'symbol' IS DISTINCT FROM NEW.symbol
       OR NEW.fact_authority->>'direction' IS DISTINCT FROM NEW.direction
       OR NULLIF(NEW.fact_authority->>'decision_time_utc','')::timestamptz
            IS DISTINCT FROM NEW.decision_time_utc
       OR NEW.fact_authority->>'knowledge_status' IS DISTINCT FROM NEW.knowledge_status
       OR NULLIF(NEW.fact_authority->>'candidate_match','')::boolean
            IS DISTINCT FROM NEW.candidate_match
       OR NEW.fact_authority->>'expected_watch_selection_attestation_sha256'
            IS DISTINCT FROM NEW.watch_selection_attestation_sha256
       OR NEW.fact_authority->>'expected_watch_code_manifest_sha256'
            IS DISTINCT FROM NEW.observed_watch_code_manifest_sha256
       OR NEW.fact_authority->>'expected_parent_membership_evidence_sha256'
            IS DISTINCT FROM NEW.parent_membership_evidence_sha256
       OR NEW.fact_authority->>'expected_noneligibility_proof_sha256'
            IS DISTINCT FROM NEW.noneligibility_proof_sha256 THEN
        RAISE EXCEPTION 'Stage-8 fact authority does not bind the persisted fact state';
    END IF;
    IF NEW.watch_selection_attestation IS NULL THEN
        IF NEW.watch_selection_attestation_sha256 IS NOT NULL
           OR NEW.observed_watch_code_manifest_sha256 IS NOT NULL
           OR NEW.knowledge_status = 'KNOWN' THEN
            RAISE EXCEPTION 'Known Stage-8 fact requires Watch selection authority';
        END IF;
    ELSIF jsonb_typeof(NEW.watch_selection_attestation) IS DISTINCT FROM 'object'
       OR COALESCE(NEW.watch_selection_attestation_sha256, '')
                !~ '^[0-9a-f]{64}$'
       OR COALESCE(NEW.observed_watch_code_manifest_sha256, '')
                !~ '^[0-9a-f]{64}$'
       OR NEW.watch_selection_attestation_sha256
                IS DISTINCT FROM NEW.watch_selection_attestation->>'attestation_sha256'
       OR NEW.watch_selection_attestation_sha256
                IS DISTINCT FROM research_stage8_json_sha256_v1(
                    NEW.watch_selection_attestation - 'attestation_sha256')
       OR NEW.fact_authority->>'watch_selection_attestation_sha256'
                IS DISTINCT FROM NEW.watch_selection_attestation_sha256
       OR NEW.observed_watch_code_manifest_sha256
                IS DISTINCT FROM NEW.fact_authority->>'watch_code_manifest_sha256'
       OR (NEW.knowledge_status = 'KNOWN' AND
           NEW.observed_watch_code_manifest_sha256
                IS DISTINCT FROM registry.expected_watch_code_manifest_sha256) THEN
        RAISE EXCEPTION 'Stage-8 Watch fact authority is invalid';
    END IF;
    server_projection := research_stage8_derive_projection_attestation_v1(
        TG_TABLE_SCHEMA, NEW.exact_binding_sha256, NEW.attempt_id,
        batch.watch_archive_high_water_snapshot_set_id, FALSE
    );
    server_semantics := server_projection->'projection_semantics';
    NEW.server_projection_attestation := server_projection;
    NEW.server_projection_attestation_sha256 :=
        research_stage8_json_sha256_v1(server_projection);
    NEW.server_projection_status := server_projection->>'status';
    IF NEW.server_projection_status = 'VERIFIED' THEN
        IF server_semantics->>'knowledge_status' = 'KNOWN' AND (
              NEW.knowledge_status IS DISTINCT FROM 'KNOWN'
           OR NEW.candidate_match IS DISTINCT FROM
                (server_semantics->>'candidate_match')::boolean
           OR NEW.anchor_slot_id IS DISTINCT FROM
                (server_semantics->>'anchor_slot_id')::bigint
           OR NEW.event_id IS DISTINCT FROM
                (server_semantics->>'event_id')::bigint
           OR NEW.event_fingerprint IS DISTINCT FROM
                server_semantics->>'event_fingerprint'
           OR NEW.symbol IS DISTINCT FROM server_semantics->>'symbol'
           OR NEW.direction IS DISTINCT FROM server_semantics->>'direction'
           OR NEW.decision_time_utc IS DISTINCT FROM
                (server_semantics->>'decision_time_utc')::timestamptz
           OR NEW.fact->'feature'->>'name' IS DISTINCT FROM
                registry.exact_binding->'binding'->'candidate'->>'feature'
           OR NEW.fact->'feature'->>'model' IS DISTINCT FROM
                server_semantics->>'model'
           OR NEW.fact->'feature'->>'candidate_direction' IS DISTINCT FROM
                server_semantics->>'direction'
           OR NEW.fact->'feature'->>'operator' IS DISTINCT FROM
                server_semantics->>'candidate_operator'
           OR NEW.fact->'feature'->'value' IS DISTINCT FROM
                server_semantics->'candidate_threshold'
           OR NEW.fact->'feature'->'direction_multiplier' IS DISTINCT FROM
                server_semantics->'direction_multiplier'
           OR NEW.fact->'feature'->'raw_score' IS DISTINCT FROM
                server_semantics->'raw_score'
           OR NEW.fact->'feature'->>'source_direction' IS DISTINCT FROM
                server_semantics->>'source_direction'
           OR NEW.fact->'feature'->'aligned_score' IS DISTINCT FROM
                server_semantics->'aligned_score'
           OR NEW.fact->'source'->>'watch_inner_payload_sha256' IS DISTINCT FROM
                server_semantics->>'operational_scores_sha256'
           OR NEW.fact->'source'->>'watch_code_manifest_sha256' IS DISTINCT FROM
                server_semantics->>'watch_code_manifest_sha256'
           OR NEW.fact->'source'->>'model_observation_sha256' IS DISTINCT FROM
                server_semantics->>'model_observation_sha256'
           OR NEW.fact->'source'->>'model_source_sha256' IS DISTINCT FROM
                server_semantics->>'model_source_sha256'
           OR NEW.fact->'source'->>'derivatives_snapshot_sha256' IS DISTINCT FROM
                server_semantics->>'derivatives_snapshot_sha256'
           OR NEW.fact->'source'->'watch_snapshot'->>'snapshot_set_id'
                IS DISTINCT FROM server_semantics->>'selected_snapshot_set_id'
           OR NEW.fact->'source'->'watch_snapshot'->>'snapshot_key'
                IS DISTINCT FROM server_semantics->>'selected_snapshot_key'
           OR NEW.fact->'source'->'watch_snapshot'->>'payload_sha256'
                IS DISTINCT FROM server_semantics->>'selected_payload_sha256'
           OR NEW.fact->'source'->'watch_snapshot'->>'durably_available_at_utc'
                IS DISTINCT FROM
                    server_semantics->>'selected_durably_available_at_utc'
           OR NEW.watch_selection_attestation->>'selection_status'
                IS DISTINCT FROM 'SELECTED'
           OR NEW.watch_selection_attestation->>'selected_snapshot_set_id'
                IS DISTINCT FROM server_semantics->>'selected_snapshot_set_id'
           OR NEW.watch_selection_attestation->>'selected_snapshot_key'
                IS DISTINCT FROM server_semantics->>'selected_snapshot_key'
           OR NEW.watch_selection_attestation->>'selected_payload_sha256'
                IS DISTINCT FROM server_semantics->>'selected_payload_sha256'
           OR NEW.watch_selection_attestation->>'selected_durably_available_at_utc'
                IS DISTINCT FROM
                    server_semantics->>'selected_durably_available_at_utc'
           OR NEW.watch_selection_attestation->>'watch_code_manifest_sha256'
                IS DISTINCT FROM server_semantics->>'watch_code_manifest_sha256'
           OR (CASE server_semantics->>'parent_authority_class'
                WHEN 'LIVE' THEN
                    NEW.parent_membership_evidence->>'validation_status' = 'VALID'
                    AND NEW.parent_membership_evidence->>'btc_parent_movement_id'
                        = server_semantics->>'btc_parent_movement_id'
                    AND NEW.parent_membership_evidence->>'parent_start_time_utc'
                        = server_semantics->>'parent_start_time_utc'
                WHEN 'PROVEN_NOT_EVIDENCE_ELIGIBLE' THEN
                    NEW.parent_membership_evidence->>'validation_status'
                        = 'PROVEN_NOT_EVIDENCE_ELIGIBLE'
                    AND NEW.parent_membership_evidence->>'btc_parent_movement_id'
                        = server_semantics->>'btc_parent_movement_id'
                    AND NEW.parent_membership_evidence->>'parent_start_time_utc'
                        = server_semantics->>'parent_start_time_utc'
                WHEN 'UNKNOWN' THEN
                    NEW.parent_membership_evidence->>'validation_status'
                        = 'UNKNOWN'
                ELSE FALSE END) IS NOT TRUE) THEN
            RAISE EXCEPTION 'Stage-8 fact differs from server-derived projection authority';
        ELSIF server_semantics->>'knowledge_status' = 'UNKNOWN' THEN
            IF NEW.knowledge_status IS DISTINCT FROM 'UNKNOWN'
               OR NEW.candidate_match IS NOT NULL THEN
                RAISE EXCEPTION 'Stage-8 unknown projection became a known fact: %',
                    server_projection->'reasons';
            END IF;
        ELSIF server_semantics->>'knowledge_status' NOT IN ('KNOWN','UNKNOWN')
              OR server_semantics->>'knowledge_status' IS NULL THEN
            RAISE EXCEPTION 'Stage-8 derived knowledge status is invalid';
        END IF;
    ELSIF NEW.server_projection_status = 'PROVEN_NONELIGIBLE' THEN
        IF NEW.knowledge_status IS DISTINCT FROM 'UNKNOWN'
           OR NEW.candidate_match IS NOT NULL
           OR NEW.noneligibility_proof IS NULL THEN
            RAISE EXCEPTION 'Stage-8 noneligible fact differs from server authority';
        END IF;
    ELSIF NEW.server_projection_status = 'UNKNOWN' THEN
        IF NEW.knowledge_status IS DISTINCT FROM 'UNKNOWN'
           OR NEW.candidate_match IS NOT NULL THEN
            RAISE EXCEPTION 'Stage-8 unknown server projection cannot become known';
        END IF;
    ELSE
        RAISE EXCEPTION 'Stage-8 server projection status is invalid';
    END IF;
    -- Structural selection consumes only this server-derived, closed identity.
    -- Free-form fact/source/reason JSON and the audit fact hash are excluded.
    NEW.selection_fact_identity := jsonb_build_object(
        'version', 'stage8-selection-fact-identity-v1',
        'exact_binding_sha256', registry.exact_binding_sha256,
        'attempt_id', NEW.attempt_id,
        'attempt_fingerprint', source_attempt.attempt_fingerprint,
        'sampler_version', source_attempt.sampler_version,
        'source_candle_open_utc', research_stage8_utc_text_v1(
            source_attempt.source_candle_open_utc),
        'source_attempt_evaluation_status', source_attempt.evaluation_status,
        'anchor_slot_id', CASE WHEN source_slot_found
            THEN source_slot.anchor_slot_id ELSE NULL END,
        'event_id', CASE WHEN source_slot_found
            THEN source_slot.event_id ELSE NULL END,
        'event_fingerprint', CASE
            WHEN source_slot_found
                THEN source_slot.event_fingerprint ELSE NULL END,
        'symbol', source_attempt.symbol,
        'direction', registry.exact_binding->'binding'->'candidate'->>'direction',
        'decision_time_utc', CASE
            WHEN source_slot_found
                THEN research_stage8_utc_text_v1(source_slot.decision_time_utc)
            ELSE NULL END,
        'knowledge_status', COALESCE(
            server_semantics->>'knowledge_status', 'UNKNOWN'
        ),
        'candidate_match', server_semantics->'candidate_match',
        'parent_authority_class', server_semantics->>'parent_authority_class',
        'btc_parent_movement_id', server_semantics->>'btc_parent_movement_id',
        'parent_start_time_utc', server_semantics->>'parent_start_time_utc',
        'parent_policy_version', server_semantics->>'parent_policy_version',
        'membership_status', server_semantics->>'membership_status',
        'parent_evidence_eligible', server_semantics->'parent_evidence_eligible'
    );
    NEW.selection_fact_identity_sha256 := research_stage8_json_sha256_v1(
        NEW.selection_fact_identity);
    NEW.persisted_at_utc := clock_timestamp();
    NEW.persisted_by := current_user;
    persisted_text := research_stage8_utc_text_v1(NEW.persisted_at_utc);
    NEW.fact_record := jsonb_build_object(
        'version', 'stage8-durable-projected-fact-record-v1',
        'fact_batch_record_sha256', NEW.fact_batch_record_sha256,
        'exact_binding_sha256', NEW.exact_binding_sha256,
        'attempt_id', NEW.attempt_id,
        'attempt_fingerprint', NEW.attempt_fingerprint,
        'anchor_slot_id', NEW.anchor_slot_id,
        'event_id', NEW.event_id,
        'event_fingerprint', NEW.event_fingerprint,
        'symbol', NEW.symbol,
        'direction', NEW.direction,
        'decision_time_utc', CASE WHEN NEW.decision_time_utc IS NULL THEN NULL
            ELSE research_stage8_utc_text_v1(NEW.decision_time_utc) END,
        'knowledge_status', NEW.knowledge_status,
        'candidate_match', NEW.candidate_match,
        'fact_sha256', NEW.fact_sha256,
        'fact_authority_sha256', NEW.fact_authority_sha256,
        'watch_selection_attestation_sha256', NEW.watch_selection_attestation_sha256,
        'observed_watch_code_manifest_sha256', NEW.observed_watch_code_manifest_sha256,
        'parent_membership_evidence_sha256', NEW.parent_membership_evidence_sha256,
        'noneligibility_proof_sha256', NEW.noneligibility_proof_sha256,
        'selection_fact_identity_sha256', NEW.selection_fact_identity_sha256,
        'server_projection_attestation_sha256',
            NEW.server_projection_attestation_sha256,
        'server_projection_status', NEW.server_projection_status,
        'persisted_at_utc', persisted_text,
        'persisted_by', NEW.persisted_by
    );
    NEW.fact_record_sha256 := research_stage8_json_sha256_v1(NEW.fact_record);
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION research_stage8_fact_seal_insert_guard_v1()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    batch research_stage8_projection_fact_batches%ROWTYPE;
    stored_attempt_ids JSONB;
    fact_records JSONB;
    expected_records_sha256 TEXT;
    sealed_text TEXT;
BEGIN
    PERFORM pg_catalog.set_config(
        'search_path',
        pg_catalog.quote_ident(TG_TABLE_SCHEMA) || ',pg_catalog,pg_temp',
        true
    );
    IF NEW.sealed_at_utc IS NOT NULL OR NEW.seal_record IS NOT NULL
       OR NEW.seal_record_sha256 IS NOT NULL OR NEW.sealed_by IS NOT NULL THEN
        RAISE EXCEPTION 'Stage-8 fact-seal server-owned fields cannot be supplied';
    END IF;
    SELECT * INTO STRICT batch FROM research_stage8_projection_fact_batches
    WHERE fact_batch_record_sha256 = NEW.fact_batch_record_sha256;
    SELECT COALESCE(jsonb_agg(attempt_id ORDER BY attempt_id), '[]'::jsonb),
           COALESCE(jsonb_agg(jsonb_build_object(
               'attempt_id', attempt_id,
               'fact_record_sha256', fact_record_sha256
           ) ORDER BY attempt_id), '[]'::jsonb)
    INTO stored_attempt_ids, fact_records
    FROM research_stage8_projected_fact_ledger
    WHERE fact_batch_record_sha256 = NEW.fact_batch_record_sha256;
    expected_records_sha256 := research_stage8_json_sha256_v1(jsonb_build_object(
        'version', 'stage8-durable-fact-record-set-v1',
        'fact_batch_record_sha256', NEW.fact_batch_record_sha256,
        'facts', fact_records
    ));
    IF NEW.fact_count <> batch.attempt_count
       OR stored_attempt_ids <> batch.attempt_ids
       OR NEW.fact_records_sha256 <> expected_records_sha256 THEN
        RAISE EXCEPTION 'Stage-8 fact batch cannot be sealed before exact population completion';
    END IF;
    NEW.sealed_at_utc := clock_timestamp();
    NEW.sealed_by := current_user;
    sealed_text := research_stage8_utc_text_v1(NEW.sealed_at_utc);
    NEW.seal_record := jsonb_build_object(
        'version', 'stage8-durable-projection-fact-seal-v1',
        'fact_batch_record_sha256', NEW.fact_batch_record_sha256,
        'fact_count', NEW.fact_count,
        'fact_records_sha256', NEW.fact_records_sha256,
        'sealed_at_utc', sealed_text,
        'sealed_by', NEW.sealed_by
    );
    NEW.seal_record_sha256 := research_stage8_json_sha256_v1(NEW.seal_record);
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION research_stage8_selection_insert_guard_v1()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    registry research_stage8_binding_registry%ROWTYPE;
    fact_batch research_stage8_projection_fact_batches%ROWTYPE;
    fact_seal research_stage8_projection_fact_batch_seals%ROWTYPE;
    identity JSONB;
    parent_start TIMESTAMPTZ;
    source_authority_entries JSONB;
    source_authority_sha256 TEXT;
    excluded_pre_freeze_parent_ids JSONB;
    proven_noneligible_attempt_ids JSONB;
    expected_identities JSONB;
    expected_representative_count INTEGER;
    expected_representative_set_sha256 TEXT;
    representative_binding JSONB;
    persisted_text TEXT;
BEGIN
    PERFORM pg_catalog.set_config(
        'search_path',
        pg_catalog.quote_ident(TG_TABLE_SCHEMA) || ',pg_catalog,pg_temp',
        true
    );
    IF NEW.selection_record_sha256 IS NOT NULL
       OR NEW.representative_identities_sha256 IS NOT NULL
       OR NEW.selection_attestation_sha256 IS NOT NULL
       OR NEW.persisted_at_utc IS NOT NULL OR NEW.selection_record IS NOT NULL
       OR NEW.persisted_by IS NOT NULL THEN
        RAISE EXCEPTION 'Stage-8 selection server-owned fields cannot be supplied';
    END IF;
    SELECT * INTO STRICT registry FROM research_stage8_binding_registry
    WHERE exact_binding_sha256 = NEW.exact_binding_sha256;
    SELECT * INTO STRICT fact_batch FROM research_stage8_projection_fact_batches
    WHERE fact_batch_record_sha256 = NEW.fact_batch_record_sha256;
    SELECT * INTO STRICT fact_seal FROM research_stage8_projection_fact_batch_seals
    WHERE fact_batch_record_sha256 = NEW.fact_batch_record_sha256;
    IF NEW.freeze_id <> registry.freeze_id
       OR NEW.registry_record_sha256 <> registry.registry_record_sha256
       OR NEW.verifier_profile_sha256 <> registry.verifier_profile_sha256
       OR fact_batch.exact_binding_sha256 <> NEW.exact_binding_sha256
       OR fact_batch.freeze_id <> NEW.freeze_id
       OR fact_batch.registry_record_sha256 <> NEW.registry_record_sha256
       OR fact_batch.registry_verification_receipt_sha256
            <> NEW.registry_verification_receipt_sha256
       OR NEW.selector_version <> registry.implementation_artifacts->'files'->'selector'->>'version'
       OR NEW.observed_projection_source_sha256 <> registry.implementation_artifacts->'files'->'projection'->>'sha256'
       OR NEW.observed_selector_source_sha256 <> registry.implementation_artifacts->'files'->'selector'->>'sha256'
       OR NEW.observed_watch_code_manifest_sha256 <> registry.expected_watch_code_manifest_sha256
       OR NEW.cohort_query_sha256 <> fact_batch.coverage_query_sha256
       OR NEW.outcome_free_population_receipt_sha256
            <> fact_batch.outcome_free_population_receipt_sha256
       OR NEW.source_high_water_attempt_id <> fact_batch.coverage_source_high_water_attempt_id
       OR NEW.selection_attestation->>'attempt_population_sha256'
            IS DISTINCT FROM fact_batch.coverage_attempt_population_sha256
       OR NULLIF(NEW.selection_attestation->>'source_attempt_count','')::integer
            IS DISTINCT FROM fact_batch.attempt_count
       OR fact_seal.fact_count <> fact_batch.attempt_count THEN
        RAISE EXCEPTION 'Stage-8 selection does not match its durable verifier profile';
    END IF;
    IF (SELECT array_agg(key ORDER BY key)
        FROM jsonb_object_keys(NEW.selection_attestation) AS keys(key))
       IS DISTINCT FROM ARRAY[
           'attempt_population_sha256','blocked_parents',
           'candidate_match_coverage_complete','cohort_query_sha256',
           'database_verification_asserted_by_selector',
           'deduplicated_exact_anchor_event_count','exact_binding_sha256',
           'excluded_pre_freeze_parent_ids','expected_projection_source_sha256',
           'expected_selector_source_sha256','expected_watch_code_manifest_sha256',
           'freeze_id','frozen_at_utc','global_blockers','manifest_sha256',
           'outcome_blind_selection','outcome_free_population_receipt_sha256',
           'outcome_or_label_fields_accepted','population_coverage_complete',
           'proven_noneligible_attempt_ids','qualification_evaluated',
           'registry_record_sha256','registry_verification_receipt_sha256',
           'representative_count','representative_set_sha256','selector_version',
           'source_attempt_count','source_authority_ledger_count',
           'source_authority_ledger_sha256','source_high_water_attempt_id',
           'source_transaction_identity_sha256','status','structural_authority',
           'truncated','verifier_profile_sha256','version'
       ]::TEXT[]
       OR jsonb_array_length(NEW.representative_identities) <> NEW.representative_count
       OR NEW.selection_attestation->>'version'
            IS DISTINCT FROM 'stage8-outcome-blind-representative-batch-v1'
       OR NEW.selection_attestation->>'selector_version' IS DISTINCT FROM NEW.selector_version
       OR NEW.selection_attestation->>'structural_authority' IS DISTINCT FROM
            'PREDICTED_SELECTION_FACT_IDENTITIES_REQUIRE_DURABLE_DB_MATCH'
       OR NEW.selection_attestation->>'exact_binding_sha256' IS DISTINCT FROM NEW.exact_binding_sha256
       OR NEW.selection_attestation->>'manifest_sha256' IS DISTINCT FROM registry.manifest_sha256
       OR NEW.selection_attestation->>'freeze_id' IS DISTINCT FROM NEW.freeze_id
       OR NEW.selection_attestation->>'frozen_at_utc'
            IS DISTINCT FROM research_stage8_utc_text_v1(registry.frozen_at_utc)
       OR NEW.selection_attestation->>'registry_record_sha256' IS DISTINCT FROM NEW.registry_record_sha256
       OR NEW.selection_attestation->>'verifier_profile_sha256' IS DISTINCT FROM NEW.verifier_profile_sha256
       OR NEW.selection_attestation->>'expected_projection_source_sha256'
            IS DISTINCT FROM NEW.observed_projection_source_sha256
       OR NEW.selection_attestation->>'expected_selector_source_sha256'
            IS DISTINCT FROM NEW.observed_selector_source_sha256
       OR NEW.selection_attestation->>'expected_watch_code_manifest_sha256'
            IS DISTINCT FROM NEW.observed_watch_code_manifest_sha256
       OR NEW.selection_attestation->>'registry_verification_receipt_sha256' IS DISTINCT FROM NEW.registry_verification_receipt_sha256
       OR NEW.selection_attestation->>'cohort_query_sha256' IS DISTINCT FROM NEW.cohort_query_sha256
       OR NEW.selection_attestation->>'outcome_free_population_receipt_sha256'
            IS DISTINCT FROM NEW.outcome_free_population_receipt_sha256
       OR NULLIF(NEW.selection_attestation->>'source_high_water_attempt_id','')::bigint
            IS DISTINCT FROM NEW.source_high_water_attempt_id
       OR NULLIF(NEW.selection_attestation->>'representative_count','')::integer
            IS DISTINCT FROM NEW.representative_count
       OR NEW.selection_attestation->>'representative_set_sha256' IS DISTINCT FROM NEW.representative_set_sha256
       OR NEW.selection_attestation->>'status' IS DISTINCT FROM 'COMPLETE'
       OR NEW.selection_attestation->'population_coverage_complete' IS DISTINCT FROM 'true'::jsonb
       OR NEW.selection_attestation->'candidate_match_coverage_complete' IS DISTINCT FROM 'true'::jsonb
       OR NEW.selection_attestation->'outcome_blind_selection' IS DISTINCT FROM 'true'::jsonb
       OR NEW.selection_attestation->'outcome_or_label_fields_accepted' IS DISTINCT FROM 'false'::jsonb
       OR NEW.selection_attestation->'truncated' IS DISTINCT FROM 'false'::jsonb
       OR NEW.selection_attestation->'qualification_evaluated' IS DISTINCT FROM 'false'::jsonb
       OR NEW.selection_attestation->'database_verification_asserted_by_selector' IS DISTINCT FROM 'false'::jsonb
       OR NEW.selection_attestation->'global_blockers' IS DISTINCT FROM '[]'::jsonb
       OR NEW.selection_attestation->'blocked_parents' IS DISTINCT FROM '[]'::jsonb THEN
        RAISE EXCEPTION 'Stage-8 selection attestation is incomplete or mismatched';
    END IF;

    SELECT COALESCE(jsonb_agg(jsonb_build_object(
               'attempt_id', fact_row.attempt_id,
               'expected_selection_fact_identity_sha256',
                   fact_row.selection_fact_identity_sha256
           ) ORDER BY fact_row.attempt_id), '[]'::jsonb)
    INTO source_authority_entries
    FROM research_stage8_projected_fact_ledger AS fact_row
    WHERE fact_row.fact_batch_record_sha256 = NEW.fact_batch_record_sha256;
    source_authority_sha256 := research_stage8_json_sha256_v1(jsonb_build_object(
        'version', 'stage8-selector-source-authority-ledger-v1',
        'exact_binding_sha256', NEW.exact_binding_sha256,
        'attempt_population_sha256', fact_batch.coverage_attempt_population_sha256,
        'entries', source_authority_entries
    ));
    IF NULLIF(NEW.selection_attestation->>'source_authority_ledger_count','')::integer
            IS DISTINCT FROM fact_seal.fact_count
       OR NEW.selection_attestation->>'source_authority_ledger_sha256'
            IS DISTINCT FROM source_authority_sha256
       OR NEW.selection_attestation->>'source_transaction_identity_sha256'
            IS DISTINCT FROM fact_batch.adapter_population_receipt
                ->>'transaction_identity_sha256' THEN
        RAISE EXCEPTION 'Stage-8 selection source-authority ledger is not the sealed fact population';
    END IF;

    SELECT COALESCE(jsonb_agg(parent_id ORDER BY parent_id), '[]'::jsonb)
    INTO excluded_pre_freeze_parent_ids
    FROM (
        SELECT DISTINCT fact_row.parent_membership_evidence
                            ->>'btc_parent_movement_id' AS parent_id
        FROM research_stage8_projected_fact_ledger AS fact_row
        WHERE fact_row.fact_batch_record_sha256 = NEW.fact_batch_record_sha256
          AND fact_row.parent_membership_evidence->>'validation_status' = 'VALID'
          AND (fact_row.parent_membership_evidence->>'parent_start_time_utc')::timestamptz
                <= registry.frozen_at_utc
    ) AS excluded;
    SELECT COALESCE(jsonb_agg(attempt_id ORDER BY attempt_id), '[]'::jsonb)
    INTO proven_noneligible_attempt_ids
    FROM (
        SELECT fact_row.attempt_id
        FROM research_stage8_projected_fact_ledger AS fact_row
        WHERE fact_row.fact_batch_record_sha256 = NEW.fact_batch_record_sha256
          AND (fact_row.noneligibility_proof IS NOT NULL
               OR fact_row.parent_membership_evidence->>'validation_status'
                    = 'PROVEN_NOT_EVIDENCE_ELIGIBLE')
    ) AS noneligible;
    IF NEW.selection_attestation->'excluded_pre_freeze_parent_ids'
            IS DISTINCT FROM excluded_pre_freeze_parent_ids
       OR NEW.selection_attestation->'proven_noneligible_attempt_ids'
            IS DISTINCT FROM proven_noneligible_attempt_ids
       OR NEW.selection_attestation->'deduplicated_exact_anchor_event_count'
            IS DISTINCT FROM '0'::jsonb THEN
        RAISE EXCEPTION 'Stage-8 selection source summary differs from the sealed ledger';
    END IF;

    -- A COMPLETE selector result cannot hide an UNKNOWN parent classification.
    -- Those rows make the pure selector BLOCKED, even if no representative was
    -- chosen from them.
    IF EXISTS (
        SELECT 1 FROM research_stage8_projected_fact_ledger AS fact_row
        WHERE fact_row.fact_batch_record_sha256 = NEW.fact_batch_record_sha256
          AND fact_row.noneligibility_proof IS NULL
          AND fact_row.parent_membership_evidence->>'validation_status'
                IS DISTINCT FROM 'VALID'
          AND fact_row.parent_membership_evidence->>'validation_status'
                IS DISTINCT FROM 'PROVEN_NOT_EVIDENCE_ELIGIBLE'
    ) THEN
        RAISE EXCEPTION 'Stage-8 COMPLETE selection cannot omit unknown parent authority';
    END IF;
    IF EXISTS (
        SELECT 1 FROM research_stage8_projected_fact_ledger AS fact_row
        WHERE fact_row.fact_batch_record_sha256 = NEW.fact_batch_record_sha256
          AND fact_row.parent_membership_evidence->>'validation_status' = 'VALID'
          AND (
              COALESCE(fact_row.parent_membership_evidence->>'btc_parent_movement_id','')
                    !~ '^[0-9a-f]{64}$'
              OR fact_row.parent_membership_evidence->>'membership_status'
                    IS DISTINCT FROM 'LIVE'
              OR fact_row.parent_membership_evidence->'parent_evidence_eligible'
                    IS DISTINCT FROM 'true'::jsonb
              OR fact_row.parent_membership_evidence->>'parent_policy_version'
                    IS DISTINCT FROM registry.parent_policy_version
              OR fact_row.parent_membership_evidence->>'event_id'
                    IS DISTINCT FROM fact_row.event_id::text
              OR fact_row.parent_membership_evidence->>'event_fingerprint'
                    IS DISTINCT FROM fact_row.event_fingerprint
              OR fact_row.parent_membership_evidence->>'symbol'
                    IS DISTINCT FROM fact_row.symbol
              OR fact_row.parent_membership_evidence->>'direction'
                    IS DISTINCT FROM fact_row.direction
              OR fact_row.parent_membership_evidence->>'decision_time_utc'
                    IS DISTINCT FROM fact_row.fact->'identity'->>'decision_time_utc'
              OR fact_row.parent_membership_evidence_sha256
                    IS DISTINCT FROM fact_row.fact_authority
                        ->>'expected_parent_membership_evidence_sha256'
          )
    ) THEN
        RAISE EXCEPTION 'Stage-8 LIVE parent authority does not bind the fact identity';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM research_stage8_projected_fact_ledger AS fact_row
        WHERE fact_row.fact_batch_record_sha256 = NEW.fact_batch_record_sha256
          AND fact_row.parent_membership_evidence->>'validation_status' = 'VALID'
        GROUP BY fact_row.anchor_slot_id, fact_row.event_id
        HAVING count(*) > 1
    ) OR EXISTS (
        SELECT 1
        FROM research_stage8_projected_fact_ledger AS fact_row
        WHERE fact_row.fact_batch_record_sha256 = NEW.fact_batch_record_sha256
          AND fact_row.parent_membership_evidence->>'validation_status' = 'VALID'
        GROUP BY fact_row.anchor_slot_id HAVING count(DISTINCT fact_row.event_id) > 1
    ) OR EXISTS (
        SELECT 1
        FROM research_stage8_projected_fact_ledger AS fact_row
        WHERE fact_row.fact_batch_record_sha256 = NEW.fact_batch_record_sha256
          AND fact_row.parent_membership_evidence->>'validation_status' = 'VALID'
        GROUP BY fact_row.event_id HAVING count(DISTINCT fact_row.anchor_slot_id) > 1
    ) THEN
        RAISE EXCEPTION 'Stage-8 COMPLETE selection has conflicting exact anchor events';
    END IF;
    IF EXISTS (
        SELECT 1 FROM jsonb_array_elements(NEW.representative_identities) AS item
        WHERE jsonb_typeof(item) IS DISTINCT FROM 'object'
           OR (SELECT array_agg(key ORDER BY key)
               FROM jsonb_object_keys(item) AS keys(key))
                IS DISTINCT FROM ARRAY[
                    'anchor_slot_id','attempt_fingerprint','btc_parent_movement_id',
                    'candidate_match',
                    'candidate_match_knowledge_status','decision_time_utc','direction',
                    'event_fingerprint','event_id','exact_binding_sha256',
                    'expected_selection_fact_identity_sha256',
                    'parent_start_time_utc','symbol','version'
                ]::TEXT[]
           OR item->>'version' IS DISTINCT FROM 'stage8-outcome-free-representative-identity-v1'
           OR item->>'exact_binding_sha256' IS DISTINCT FROM NEW.exact_binding_sha256
           OR COALESCE(item->>'btc_parent_movement_id','') !~ '^[0-9a-f]{64}$'
           OR COALESCE(item->>'expected_selection_fact_identity_sha256','')
                !~ '^[0-9a-f]{64}$'
           OR COALESCE(item->>'attempt_fingerprint','') !~ '^[0-9a-f]{64}$'
           OR COALESCE(item->>'event_fingerprint','') !~ '^[0-9a-f]{64}$'
           OR item->>'candidate_match_knowledge_status' IS DISTINCT FROM 'KNOWN'
           OR item->'candidate_match' IS DISTINCT FROM 'true'::jsonb
           OR jsonb_typeof(item->'anchor_slot_id') IS DISTINCT FROM 'number'
           OR jsonb_typeof(item->'event_id') IS DISTINCT FROM 'number'
    ) OR EXISTS (
        SELECT 1 FROM jsonb_array_elements(NEW.representative_identities) AS item
        GROUP BY item->>'btc_parent_movement_id' HAVING count(*) > 1
    ) OR EXISTS (
        SELECT 1 FROM jsonb_array_elements(NEW.representative_identities) AS item
        GROUP BY item->>'attempt_fingerprint',
                 item->>'expected_selection_fact_identity_sha256'
        HAVING count(*) > 1
    ) OR EXISTS (
        SELECT 1 FROM jsonb_array_elements(NEW.representative_identities) AS item
        GROUP BY item->>'anchor_slot_id', item->>'event_id', item->>'event_fingerprint'
        HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'Stage-8 representative identities are invalid or duplicated';
    END IF;
    FOR identity IN SELECT value FROM jsonb_array_elements(NEW.representative_identities)
    LOOP
        BEGIN
            parent_start := (identity->>'parent_start_time_utc')::timestamptz;
        EXCEPTION WHEN OTHERS THEN
            RAISE EXCEPTION 'Stage-8 parent start time is invalid';
        END;
        IF parent_start <= registry.frozen_at_utc THEN
            RAISE EXCEPTION 'Stage-8 parent must start strictly after durable freeze';
        END IF;
        IF NOT EXISTS (
            SELECT 1 FROM research_stage8_projected_fact_ledger AS fact_row
            WHERE fact_row.fact_batch_record_sha256 = NEW.fact_batch_record_sha256
              AND fact_row.exact_binding_sha256 = NEW.exact_binding_sha256
              AND fact_row.attempt_fingerprint = identity->>'attempt_fingerprint'
              AND fact_row.selection_fact_identity_sha256
                    = identity->>'expected_selection_fact_identity_sha256'
              AND fact_row.anchor_slot_id = (identity->>'anchor_slot_id')::bigint
              AND fact_row.event_id = (identity->>'event_id')::bigint
              AND fact_row.event_fingerprint = identity->>'event_fingerprint'
              AND fact_row.symbol = identity->>'symbol'
              AND fact_row.direction = identity->>'direction'
              AND fact_row.knowledge_status = 'KNOWN'
              AND fact_row.candidate_match IS TRUE
              AND fact_row.watch_selection_attestation_sha256
                    = fact_row.fact_authority->>'watch_selection_attestation_sha256'
              AND fact_row.observed_watch_code_manifest_sha256
                    = registry.expected_watch_code_manifest_sha256
              AND fact_row.parent_membership_evidence_sha256
                    = fact_row.fact_authority->>'expected_parent_membership_evidence_sha256'
              AND fact_row.parent_membership_evidence->>'validation_status' = 'VALID'
              AND fact_row.parent_membership_evidence->>'membership_status' = 'LIVE'
              AND fact_row.parent_membership_evidence->>'parent_evidence_eligible' = 'true'
              AND fact_row.parent_membership_evidence->>'parent_policy_version'
                    = registry.parent_policy_version
              AND fact_row.parent_membership_evidence->>'btc_parent_movement_id'
                    = identity->>'btc_parent_movement_id'
              AND (fact_row.parent_membership_evidence->>'parent_start_time_utc')::timestamptz
                    = parent_start
              AND fact_row.parent_membership_evidence->>'event_id'
                    = identity->>'event_id'
              AND fact_row.parent_membership_evidence->>'event_fingerprint'
                    = identity->>'event_fingerprint'
              AND fact_row.parent_membership_evidence->>'symbol' = identity->>'symbol'
              AND fact_row.parent_membership_evidence->>'direction' = identity->>'direction'
              AND fact_row.parent_membership_evidence->>'decision_time_utc'
                    = identity->>'decision_time_utc'
        ) THEN
            RAISE EXCEPTION 'Stage-8 representative is not backed by the sealed fact ledger';
        END IF;
    END LOOP;

    -- Recompute the pure earliest-known-match-per-parent selection from the
    -- complete sealed ledger.  This makes the selector writer unable to omit,
    -- duplicate, or cherry-pick representatives, even with direct INSERT.
    IF EXISTS (
        WITH live AS (
            SELECT fact_row.*,
                   fact_row.parent_membership_evidence->>'btc_parent_movement_id' AS parent_id
            FROM research_stage8_projected_fact_ledger AS fact_row
            WHERE fact_row.fact_batch_record_sha256 = NEW.fact_batch_record_sha256
              AND fact_row.parent_membership_evidence->>'validation_status' = 'VALID'
              AND fact_row.parent_membership_evidence->>'membership_status' = 'LIVE'
              AND fact_row.parent_membership_evidence->>'parent_evidence_eligible' = 'true'
              AND fact_row.parent_membership_evidence->>'parent_policy_version'
                    = registry.parent_policy_version
              AND (fact_row.parent_membership_evidence->>'parent_start_time_utc')::timestamptz
                    > registry.frozen_at_utc
        )
        SELECT 1 FROM live AS unknown_row
        WHERE unknown_row.knowledge_status <> 'KNOWN'
          AND NOT EXISTS (
              SELECT 1 FROM live AS prior_match
              WHERE prior_match.parent_id = unknown_row.parent_id
                AND prior_match.knowledge_status = 'KNOWN'
                AND prior_match.candidate_match IS TRUE
                AND (prior_match.decision_time_utc,
                     prior_match.symbol COLLATE "C",
                     prior_match.anchor_slot_id, prior_match.event_id)
                    < (unknown_row.decision_time_utc,
                       unknown_row.symbol COLLATE "C",
                       unknown_row.anchor_slot_id, unknown_row.event_id)
          )
    ) THEN
        RAISE EXCEPTION 'Stage-8 COMPLETE selection has earlier UNKNOWN candidate eligibility';
    END IF;

    representative_binding := jsonb_build_object(
        'exact_binding_sha256', NEW.exact_binding_sha256,
        'manifest_sha256', registry.manifest_sha256,
        'contract_version', registry.contract_version,
        'source_version', registry.source_version,
        'projection_version', registry.projection_version,
        'label_version', registry.label_version,
        'independence_version', registry.independence_version,
        'acceptance_version', registry.acceptance_version,
        'scope_id', registry.scope_id,
        'scope_symbols', registry.exact_binding->'binding'->'scope'->'symbols',
        'scope_price_route', registry.exact_binding->'binding'->'scope'->>'price_route',
        'candidate_id', registry.candidate_id,
        'candidate_model', registry.exact_binding->'binding'->'candidate'->>'model',
        'direction', registry.exact_binding->'binding'->'candidate'->>'direction',
        'window_minutes', registry.window_minutes,
        'threshold_bps', registry.threshold_bps,
        'parent_policy_version', registry.parent_policy_version
    );
    WITH live AS (
        SELECT fact_row.*,
               fact_row.parent_membership_evidence->>'btc_parent_movement_id' AS parent_id,
               fact_row.parent_membership_evidence->>'parent_start_time_utc' AS parent_start_text
        FROM research_stage8_projected_fact_ledger AS fact_row
        WHERE fact_row.fact_batch_record_sha256 = NEW.fact_batch_record_sha256
          AND fact_row.parent_membership_evidence->>'validation_status' = 'VALID'
          AND fact_row.parent_membership_evidence->>'membership_status' = 'LIVE'
          AND fact_row.parent_membership_evidence->>'parent_evidence_eligible' = 'true'
          AND fact_row.parent_membership_evidence->>'parent_policy_version'
                = registry.parent_policy_version
          AND (fact_row.parent_membership_evidence->>'parent_start_time_utc')::timestamptz
                > registry.frozen_at_utc
    ), winners AS (
        SELECT * FROM (
            SELECT live.*, row_number() OVER (
                PARTITION BY parent_id
                ORDER BY decision_time_utc, symbol COLLATE "C",
                         anchor_slot_id, event_id
            ) AS match_rank
            FROM live
            WHERE knowledge_status = 'KNOWN' AND candidate_match IS TRUE
        ) AS ranked WHERE match_rank = 1
    ), projected AS (
        SELECT jsonb_build_object(
            'version', 'stage8-outcome-free-representative-identity-v1',
            'exact_binding_sha256', NEW.exact_binding_sha256,
            'btc_parent_movement_id', parent_id,
            'parent_start_time_utc', parent_start_text,
            'expected_selection_fact_identity_sha256',
                selection_fact_identity_sha256,
            'attempt_fingerprint', attempt_fingerprint,
            'anchor_slot_id', anchor_slot_id,
            'event_id', event_id,
            'event_fingerprint', event_fingerprint,
            'symbol', symbol,
            'direction', direction,
            'decision_time_utc', fact->'identity'->'decision_time_utc',
            'candidate_match_knowledge_status', 'KNOWN',
            'candidate_match', true
        ) AS identity,
        jsonb_build_object(
            'binding', representative_binding,
            'btc_parent_movement_id', parent_id,
            'parent_start_time_utc', parent_start_text,
            'representative_status', 'VALID',
            'parent_policy_version', registry.parent_policy_version,
            'membership_status', 'LIVE',
            'parent_evidence_eligible', true,
            'freeze_id', NEW.freeze_id,
            'registry_record_sha256', NEW.registry_record_sha256,
            'registry_verification_receipt_sha256', NEW.registry_verification_receipt_sha256,
            'representative', jsonb_build_object(
                'expected_selection_fact_identity_sha256',
                    selection_fact_identity_sha256,
                'attempt_fingerprint', attempt_fingerprint,
                'anchor_slot_id', anchor_slot_id,
                'event_id', event_id,
                'event_fingerprint', event_fingerprint,
                'symbol', symbol,
                'direction', direction,
                'decision_time_utc', fact->'identity'->'decision_time_utc',
                'candidate_match_knowledge_status', 'KNOWN',
                'candidate_match', true
            )
        ) AS representative
        FROM winners
    ), finalized AS (
        SELECT projected.identity,
               projected.representative || jsonb_build_object(
                   'representative_identity_sha256',
                   research_stage8_json_sha256_v1(projected.identity)
               ) AS representative
        FROM projected
    )
    SELECT COALESCE(jsonb_agg(finalized.identity ORDER BY
               research_stage8_canonical_json_v1(finalized.identity) COLLATE "C"), '[]'::jsonb),
           count(*)::integer,
           research_stage8_json_sha256_v1(jsonb_build_object(
               'version', 'stage8-outcome-blind-representative-set-v1',
               'exact_binding_sha256', NEW.exact_binding_sha256,
               'representatives', COALESCE(jsonb_agg(
                   finalized.representative ORDER BY
                       research_stage8_canonical_json_v1(finalized.representative) COLLATE "C"
               ), '[]'::jsonb)
           ))
    INTO expected_identities, expected_representative_count,
         expected_representative_set_sha256
    FROM finalized;
    IF NEW.representative_identities IS DISTINCT FROM expected_identities
       OR NEW.representative_count <> expected_representative_count
       OR NEW.representative_set_sha256 <> expected_representative_set_sha256 THEN
        RAISE EXCEPTION 'Stage-8 representatives differ from the sealed deterministic selection';
    END IF;
    NEW.representative_identities_sha256 := research_stage8_json_sha256_v1(jsonb_build_object(
        'version', 'stage8-durable-representative-identities-v1',
        'exact_binding_sha256', NEW.exact_binding_sha256,
        'representatives', NEW.representative_identities
    ));
    NEW.selection_attestation_sha256 := research_stage8_json_sha256_v1(NEW.selection_attestation);
    NEW.persisted_at_utc := clock_timestamp();
    NEW.persisted_by := current_user;
    persisted_text := research_stage8_utc_text_v1(NEW.persisted_at_utc);
    NEW.selection_record := jsonb_build_object(
        'version', 'stage8-durable-selection-record-v1',
        'fact_batch_record_sha256', NEW.fact_batch_record_sha256,
        'exact_binding_sha256', NEW.exact_binding_sha256,
        'freeze_id', NEW.freeze_id,
        'registry_record_sha256', NEW.registry_record_sha256,
        'verifier_profile_sha256', NEW.verifier_profile_sha256,
        'registry_verification_receipt_sha256', NEW.registry_verification_receipt_sha256,
        'selector_version', NEW.selector_version,
        'observed_projection_source_sha256', NEW.observed_projection_source_sha256,
        'observed_selector_source_sha256', NEW.observed_selector_source_sha256,
        'observed_watch_code_manifest_sha256', NEW.observed_watch_code_manifest_sha256,
        'cohort_query_sha256', NEW.cohort_query_sha256,
        'outcome_free_population_receipt_sha256', NEW.outcome_free_population_receipt_sha256,
        'source_high_water_attempt_id', NEW.source_high_water_attempt_id,
        'representative_count', NEW.representative_count,
        'representative_set_sha256', NEW.representative_set_sha256,
        'representative_identities_sha256', NEW.representative_identities_sha256,
        'selection_attestation_sha256', NEW.selection_attestation_sha256,
        'persisted_at_utc', persisted_text,
        'persisted_by', NEW.persisted_by
    );
    NEW.selection_record_sha256 := research_stage8_json_sha256_v1(NEW.selection_record);
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION research_stage8_evaluation_insert_guard_v1()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    registry research_stage8_binding_registry%ROWTYPE;
    selection_row research_stage8_selection_receipts%ROWTYPE;
    fact_batch research_stage8_projection_fact_batches%ROWTYPE;
    fact_seal research_stage8_projection_fact_batch_seals%ROWTYPE;
    replay JSONB;
    evidence JSONB;
    evaluation_value JSONB;
    expected_attempt_population_sha256 TEXT;
    common_pass BOOLEAN;
    probability_pass BOOLEAN;
    asymmetry_pass BOOLEAN;
    computed_atomic BOOLEAN;
    computed_qualified BOOLEAN;
    selected_parent_ids JSONB;
    probability_parent_ids JSONB;
    asymmetry_parent_ids JSONB;
    probability_count INTEGER;
    probability_successes INTEGER;
    probability_failures INTEGER;
    probability_hit_rate DOUBLE PRECISION;
    probability_wilson DOUBLE PRECISION;
    probability_status TEXT;
    asymmetry_count INTEGER;
    asymmetry_sum_mfe DOUBLE PRECISION;
    asymmetry_sum_mae DOUBLE PRECISION;
    asymmetry_ratio DOUBLE PRECISION;
    asymmetry_dominance DOUBLE PRECISION;
    asymmetry_median_edge DOUBLE PRECISION;
    asymmetry_status TEXT;
    asymmetry_state TEXT;
    server_replay_entries JSONB;
    server_replay_attestation JSONB;
    server_replay_verified BOOLEAN;
    server_selected_verified BOOLEAN;
    server_blockers JSONB;
    database_clock TIMESTAMPTZ;
    representative_horizons_elapsed BOOLEAN;
    receipt_binding JSONB;
    receipt_unsigned JSONB;
    expected_receipt_representative_count INTEGER;
    expected_receipt_representative_set_sha256 TEXT;
    expected_receipt_attestation_sha256 TEXT;
    caller_evaluation_sha256 TEXT;
    persisted_text TEXT;
BEGIN
    PERFORM pg_catalog.set_config(
        'search_path',
        pg_catalog.quote_ident(TG_TABLE_SCHEMA) || ',pg_catalog,pg_temp',
        true
    );
    -- Whole-row JSON hashes contain timestamptz values. Normalize their JSON
    -- spelling so reader/evaluator sessions do not drift by connection zone.
    PERFORM pg_catalog.set_config('TimeZone', 'UTC', true);
    database_clock := pg_catalog.clock_timestamp();
    IF NEW.evaluation_record_sha256 IS NOT NULL
       OR NEW.outcome_adapter_version IS NOT NULL
       OR NEW.observed_outcome_adapter_source_sha256 IS NOT NULL
       OR NEW.outcome_source_manifest IS NOT NULL
       OR NEW.outcome_source_manifest_sha256 IS NOT NULL
       OR NEW.transaction_identity_sha256 IS NOT NULL
       OR NEW.fact_replay_receipt IS NOT NULL
       OR NEW.fact_replay_receipt_sha256 IS NOT NULL
       OR NEW.evidence_receipt IS NOT NULL
       OR NEW.evidence_receipt_sha256 IS NOT NULL
       OR NEW.evaluation IS NOT NULL OR NEW.evaluation_sha256 IS NOT NULL
       OR NEW.persistence_payload_sha256 IS NOT NULL
       OR NEW.server_replay_attestation IS NOT NULL
       OR NEW.server_replay_attestation_sha256 IS NOT NULL
       OR NEW.server_replay_verified IS NOT NULL
       OR NEW.atomic_gate_passed IS NOT NULL
       OR NEW.research_qualified IS NOT NULL
       OR NEW.result_scope IS NOT NULL OR NEW.live_authorized IS NOT NULL
       OR NEW.telegram_authorized IS NOT NULL OR NEW.trade_authorized IS NOT NULL
       OR NEW.persisted_at_utc IS NOT NULL OR NEW.evaluation_record IS NOT NULL
       OR NEW.persisted_by IS NOT NULL THEN
        RAISE EXCEPTION 'Stage-8 evaluation server-owned fields cannot be supplied';
    END IF;
    SELECT * INTO STRICT registry FROM research_stage8_binding_registry
    WHERE exact_binding_sha256 = NEW.exact_binding_sha256;
    SELECT * INTO STRICT selection_row FROM research_stage8_selection_receipts
    WHERE selection_record_sha256 = NEW.selection_record_sha256;
    SELECT * INTO STRICT fact_batch FROM research_stage8_projection_fact_batches
    WHERE fact_batch_record_sha256 = selection_row.fact_batch_record_sha256;
    SELECT * INTO STRICT fact_seal FROM research_stage8_projection_fact_batch_seals
    WHERE fact_batch_record_sha256 = selection_row.fact_batch_record_sha256;
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
               'attempt_id', fact.attempt_id,
               'persisted_projection_attestation_sha256',
                    fact.server_projection_attestation_sha256,
               'recomputed_projection_attestation_sha256',
                    research_stage8_json_sha256_v1(derived.value),
               'projection_semantics_sha256',
                    derived.value->>'projection_semantics_sha256',
               'derived_knowledge_status',
                    derived.value->'projection_semantics'->>'knowledge_status',
               'derived_candidate_match',
                    derived.value->'projection_semantics'->'candidate_match',
               'derived_parent_authority_class',
                    derived.value->'projection_semantics'
                        ->>'parent_authority_class',
               'status', CASE
                    WHEN fact.server_projection_attestation_sha256
                            = research_stage8_json_sha256_v1(derived.value)
                     AND fact.server_projection_attestation = derived.value
                     AND derived.value->>'status'
                            IN ('VERIFIED','PROVEN_NONELIGIBLE')
                        THEN 'VERIFIED' ELSE 'UNKNOWN' END
           ) ORDER BY fact.attempt_id), '[]'::jsonb),
           COALESCE(bool_and(
               fact.server_projection_attestation_sha256
                    = research_stage8_json_sha256_v1(derived.value)
               AND fact.server_projection_attestation = derived.value
               AND derived.value->>'status'
                    IN ('VERIFIED','PROVEN_NONELIGIBLE')
           ), FALSE)
    INTO server_replay_entries, server_replay_verified
    FROM research_stage8_projected_fact_ledger AS fact
    CROSS JOIN LATERAL (
        SELECT research_stage8_derive_projection_attestation_v1(
            TG_TABLE_SCHEMA, NEW.exact_binding_sha256, fact.attempt_id,
            NULLIF(fact.server_projection_attestation->'projection_semantics'
                        ->>'selected_snapshot_set_id', '')::bigint,
            TRUE
        ) AS value
    ) AS derived
    WHERE fact.fact_batch_record_sha256 = fact_batch.fact_batch_record_sha256;
    server_replay_verified := server_replay_verified
        AND jsonb_array_length(server_replay_entries) = fact_batch.attempt_count
        AND fact_seal.fact_count = fact_batch.attempt_count;
    SELECT NOT EXISTS (
        SELECT 1
        FROM jsonb_array_elements(selection_row.representative_identities)
                AS selected(identity)
        LEFT JOIN research_stage8_projected_fact_ledger AS fact
          ON fact.fact_batch_record_sha256 = fact_batch.fact_batch_record_sha256
         AND fact.selection_fact_identity_sha256
                = selected.identity->>'expected_selection_fact_identity_sha256'
         AND fact.attempt_fingerprint = selected.identity->>'attempt_fingerprint'
         AND fact.anchor_slot_id = NULLIF(selected.identity->>'anchor_slot_id','')::bigint
         AND fact.event_id = NULLIF(selected.identity->>'event_id','')::bigint
         AND fact.event_fingerprint = selected.identity->>'event_fingerprint'
         AND fact.symbol = selected.identity->>'symbol'
         AND fact.direction = selected.identity->>'direction'
         AND fact.decision_time_utc =
                NULLIF(selected.identity->>'decision_time_utc','')::timestamptz
        WHERE fact.attempt_id IS NULL
           OR fact.server_projection_status <> 'VERIFIED'
           OR fact.server_projection_attestation->'projection_semantics'
                    ->>'knowledge_status' <> 'KNOWN'
           OR fact.server_projection_attestation->'projection_semantics'
                    ->'candidate_match' IS DISTINCT FROM 'true'::jsonb
           OR fact.server_projection_attestation->'projection_semantics'
                    ->>'parent_authority_class' <> 'LIVE'
    ) INTO server_selected_verified;
    server_replay_verified := server_replay_verified
        AND server_selected_verified;
    server_replay_attestation := jsonb_build_object(
        'version', 'stage8-db-owned-projection-replay-attestation-v1',
        'exact_binding_sha256', NEW.exact_binding_sha256,
        'fact_batch_record_sha256', fact_batch.fact_batch_record_sha256,
        'selection_record_sha256', selection_row.selection_record_sha256,
        'attempt_count', fact_batch.attempt_count,
        'entries', server_replay_entries,
        'selected_representatives_verified', server_selected_verified,
        'all_projection_semantics_verified', server_replay_verified
    );
    IF jsonb_typeof(NEW.persistence_payload) IS DISTINCT FROM 'object'
       OR (SELECT array_agg(key ORDER BY key COLLATE "C")
           FROM jsonb_object_keys(NEW.persistence_payload) AS keys(key))
            IS DISTINCT FROM ARRAY[
                'evaluation','evaluation_sha256','evidence_receipt',
                'evidence_receipt_sha256','exact_binding_sha256',
                'fact_replay_receipt','fact_replay_receipt_sha256',
                'live_authorized','manifest_sha256',
                'outcome_adapter_source_sha256','outcome_adapter_version',
                'outcome_source_manifest','outcome_source_manifest_sha256',
                'persistence_payload_sha256','result_scope',
                'selection_record_sha256','telegram_authorized',
                'trade_authorized','transaction_identity_sha256','version'
            ]::TEXT[] THEN
        RAISE EXCEPTION 'Stage-8 evaluation persistence payload is not closed';
    END IF;
    replay := NEW.persistence_payload->'fact_replay_receipt';
    evidence := NEW.persistence_payload->'evidence_receipt';
    evaluation_value := NEW.persistence_payload->'evaluation';
    IF selection_row.exact_binding_sha256 IS DISTINCT FROM NEW.exact_binding_sha256
       OR NEW.persistence_payload->>'version' IS DISTINCT FROM
            'stage8-authoritative-outcome-evaluation-persistence-v1'
       OR NEW.persistence_payload->>'manifest_sha256' IS DISTINCT FROM registry.manifest_sha256
       OR NEW.persistence_payload->>'exact_binding_sha256' IS DISTINCT FROM NEW.exact_binding_sha256
       OR NEW.persistence_payload->>'selection_record_sha256' IS DISTINCT FROM NEW.selection_record_sha256
       OR NEW.persistence_payload->>'outcome_adapter_version' IS DISTINCT FROM
            registry.implementation_artifacts->'files'->'outcome_db_adapter'->>'version'
       OR NEW.persistence_payload->>'outcome_adapter_source_sha256' IS DISTINCT FROM
            registry.implementation_artifacts->'files'->'outcome_db_adapter'->>'sha256'
       OR NEW.persistence_payload->>'result_scope' IS DISTINCT FROM 'EXPERIMENTAL_RESEARCH_ONLY'
       OR NEW.persistence_payload->'live_authorized' IS DISTINCT FROM 'false'::jsonb
       OR NEW.persistence_payload->'telegram_authorized' IS DISTINCT FROM 'false'::jsonb
       OR NEW.persistence_payload->'trade_authorized' IS DISTINCT FROM 'false'::jsonb
       OR NEW.persistence_payload->>'persistence_payload_sha256' IS DISTINCT FROM
            research_stage8_json_sha256_v1(
                NEW.persistence_payload - 'persistence_payload_sha256')
       OR NEW.persistence_payload->>'outcome_source_manifest_sha256' IS DISTINCT FROM
            research_stage8_json_sha256_v1(
                NEW.persistence_payload->'outcome_source_manifest')
       OR jsonb_typeof(NEW.persistence_payload->'outcome_source_manifest')
            IS DISTINCT FROM 'object'
       OR (SELECT array_agg(key ORDER BY key COLLATE "C")
           FROM jsonb_object_keys(
               NEW.persistence_payload->'outcome_source_manifest') AS keys(key))
            IS DISTINCT FROM ARRAY['files','version']::TEXT[]
       OR NEW.persistence_payload->'outcome_source_manifest'->>'version'
            IS DISTINCT FROM 'stage8-outcome-reader-source-manifest-v1'
       OR (SELECT array_agg(key ORDER BY key COLLATE "C")
           FROM jsonb_object_keys(
               NEW.persistence_payload->'outcome_source_manifest'->'files') AS keys(key))
            IS DISTINCT FROM ARRAY[
                'canonical_price_path.py','research_common_window_metrics.py',
                'research_operational_score_source_audit.py',
                'research_stage8_acceptance.py','research_stage8_contract.py',
                'research_stage8_feature_projection.py',
                'research_stage8_outcome_db_adapter.py',
                'research_stage8_projection_db_adapter.py',
                'research_stage8_registry.py',
                'research_stage8_representative_selector.py'
            ]::TEXT[]
       OR EXISTS (
            SELECT 1 FROM jsonb_each_text(
                NEW.persistence_payload->'outcome_source_manifest'->'files') AS file
            WHERE file.value !~ '^[0-9a-f]{64}$'
       )
       OR NEW.persistence_payload->'outcome_source_manifest'->'files'
            ->>'canonical_price_path.py' IS DISTINCT FROM
            registry.implementation_artifacts->'files'->'canonical_price_path'->>'sha256'
       OR NEW.persistence_payload->'outcome_source_manifest'->'files'
            ->>'research_common_window_metrics.py' IS DISTINCT FROM
            registry.implementation_artifacts->'files'->'common_window_metrics'->>'sha256'
       OR NEW.persistence_payload->'outcome_source_manifest'->'files'
            ->>'research_operational_score_source_audit.py' IS DISTINCT FROM
            registry.implementation_artifacts->'files'->'source_audit'->>'sha256'
       OR NEW.persistence_payload->'outcome_source_manifest'->'files'
            ->>'research_stage8_outcome_db_adapter.py' IS DISTINCT FROM
            registry.implementation_artifacts->'files'->'outcome_db_adapter'->>'sha256'
       OR NEW.persistence_payload->'outcome_source_manifest'->'files'
            ->>'research_stage8_acceptance.py' IS DISTINCT FROM
            registry.implementation_artifacts->'files'->'acceptance'->>'sha256'
       OR NEW.persistence_payload->'outcome_source_manifest'->'files'
            ->>'research_stage8_contract.py' IS DISTINCT FROM
            registry.implementation_artifacts->'files'->'contract'->>'sha256'
       OR NEW.persistence_payload->'outcome_source_manifest'->'files'
            ->>'research_stage8_feature_projection.py' IS DISTINCT FROM
            registry.implementation_artifacts->'files'->'projection'->>'sha256'
       OR NEW.persistence_payload->'outcome_source_manifest'->'files'
            ->>'research_stage8_projection_db_adapter.py' IS DISTINCT FROM
            registry.implementation_artifacts->'files'->'projection_db_adapter'->>'sha256'
       OR NEW.persistence_payload->'outcome_source_manifest'->'files'
            ->>'research_stage8_registry.py' IS DISTINCT FROM
            registry.implementation_artifacts->'files'->'registry_adapter'->>'sha256'
       OR NEW.persistence_payload->'outcome_source_manifest'->'files'
            ->>'research_stage8_representative_selector.py' IS DISTINCT FROM
            registry.implementation_artifacts->'files'->'selector'->>'sha256' THEN
        RAISE EXCEPTION 'Stage-8 evaluation source manifest is invalid';
    END IF;
    IF jsonb_typeof(replay) IS DISTINCT FROM 'object'
       OR (SELECT array_agg(key ORDER BY key COLLATE "C")
           FROM jsonb_object_keys(replay) AS keys(key)) IS DISTINCT FROM ARRAY[
            'all_causal_fact_semantics_verified','attempt_count','attempt_ids',
            'attempt_population_sha256','comparisons','complete',
            'durable_selection_server_recomputed','exact_binding_sha256',
            'fact_batch_record_sha256','fact_replay_receipt_sha256',
            'manifest_sha256','outcome_free','projection_adapter_version',
            'projection_authority_receipt_sha256',
            'projection_population_receipt_sha256','projection_result_sha256',
            'projection_source_manifest_sha256','regenerated_fact_count',
            'selection_record_sha256','status','transaction_identity_sha256',
            'truncated','version'
       ]::TEXT[]
       OR replay->>'version' IS DISTINCT FROM 'stage8-authoritative-fact-replay-receipt-v1'
       OR replay->>'manifest_sha256' IS DISTINCT FROM registry.manifest_sha256
       OR replay->>'exact_binding_sha256' IS DISTINCT FROM NEW.exact_binding_sha256
       OR replay->>'selection_record_sha256' IS DISTINCT FROM NEW.selection_record_sha256
       OR replay->>'fact_batch_record_sha256' IS DISTINCT FROM fact_batch.fact_batch_record_sha256
       OR replay->>'transaction_identity_sha256' IS DISTINCT FROM
            NEW.persistence_payload->>'transaction_identity_sha256'
       OR replay->>'projection_adapter_version' IS DISTINCT FROM
            registry.implementation_artifacts->'files'->'projection_db_adapter'->>'version'
       OR replay->>'fact_replay_receipt_sha256' IS DISTINCT FROM
            research_stage8_json_sha256_v1(replay - 'fact_replay_receipt_sha256')
       OR replay->'attempt_ids' IS DISTINCT FROM fact_batch.attempt_ids
       OR NULLIF(replay->>'attempt_count','')::integer IS DISTINCT FROM fact_batch.attempt_count
       OR NULLIF(replay->>'regenerated_fact_count','')::integer IS DISTINCT FROM fact_batch.attempt_count
       OR jsonb_typeof(replay->'comparisons') IS DISTINCT FROM 'array'
       OR jsonb_array_length(replay->'comparisons') IS DISTINCT FROM fact_batch.attempt_count
       OR replay->'outcome_free' IS DISTINCT FROM 'true'::jsonb
       OR replay->'truncated' IS DISTINCT FROM 'false'::jsonb THEN
        RAISE EXCEPTION 'Stage-8 authoritative fact replay receipt is invalid';
    END IF;
    expected_attempt_population_sha256 := research_stage8_json_sha256_v1(
        jsonb_build_object(
            'version', 'stage8-authoritative-fact-replay-receipt-v1',
            'exact_binding_sha256', NEW.exact_binding_sha256,
            'attempt_ids', fact_batch.attempt_ids
        ));
    IF replay->>'attempt_population_sha256'
            IS DISTINCT FROM expected_attempt_population_sha256 THEN
        RAISE EXCEPTION 'Stage-8 fact replay population hash is invalid';
    END IF;
    IF jsonb_typeof(evidence) IS DISTINCT FROM 'object'
       OR (SELECT array_agg(key ORDER BY key COLLATE "C")
           FROM jsonb_object_keys(evidence) AS keys(key)) IS DISTINCT FROM ARRAY[
            'adapter_version','all_representative_facts_valid',
            'asymmetry_evidence_valid_count','btc_parent_movement_ids','complete',
            'evidence_receipt_sha256','exact_binding_sha256','fact_authority',
            'fact_batch_record_sha256','fact_seal_record_sha256','manifest_sha256',
            'outcome_authority','outcome_query_count','parent_count',
            'population_read_complete','probability_evidence_valid_count',
            'projection_replay_query_count','query_count','raw_source_hashes',
            'read_finished_at_utc','read_started_at_utc','representatives',
            'selection_population_read_complete','selection_record_sha256',
            'source_manifest_sha256','transaction_identity_sha256','truncated','version'
       ]::TEXT[]
       OR evidence->>'version' IS DISTINCT FROM 'stage8-durable-outcome-evidence-receipt-v1'
       OR evidence->>'adapter_version' IS DISTINCT FROM
            registry.implementation_artifacts->'files'->'outcome_db_adapter'->>'version'
       OR evidence->>'manifest_sha256' IS DISTINCT FROM registry.manifest_sha256
       OR evidence->>'exact_binding_sha256' IS DISTINCT FROM NEW.exact_binding_sha256
       OR evidence->>'selection_record_sha256' IS DISTINCT FROM NEW.selection_record_sha256
       OR evidence->>'fact_batch_record_sha256' IS DISTINCT FROM fact_batch.fact_batch_record_sha256
       OR evidence->>'fact_seal_record_sha256' IS DISTINCT FROM fact_seal.seal_record_sha256
       OR evidence->>'transaction_identity_sha256' IS DISTINCT FROM
            NEW.persistence_payload->>'transaction_identity_sha256'
       OR evidence->>'source_manifest_sha256' IS DISTINCT FROM
            NEW.persistence_payload->>'outcome_source_manifest_sha256'
       OR evidence->>'evidence_receipt_sha256' IS DISTINCT FROM
            research_stage8_json_sha256_v1(evidence - 'evidence_receipt_sha256')
       OR NULLIF(evidence->>'query_count','')::integer IS DISTINCT FROM 15
       OR NULLIF(evidence->>'outcome_query_count','')::integer IS DISTINCT FROM 7
       OR NULLIF(evidence->>'projection_replay_query_count','')::integer IS DISTINCT FROM 8
       OR jsonb_typeof(evidence->'raw_source_hashes') IS DISTINCT FROM 'object'
       OR (SELECT array_agg(key ORDER BY key COLLATE "C")
           FROM jsonb_object_keys(evidence->'raw_source_hashes') AS keys(key))
            IS DISTINCT FROM ARRAY[
                'fact_batch_row_sha256','fact_seal_row_sha256',
                'registry_row_sha256','sealed_fact_population_sha256',
                'selection_row_sha256'
            ]::text[]
       OR EXISTS (
            SELECT 1 FROM jsonb_each_text(evidence->'raw_source_hashes') AS item
            WHERE item.value !~ '^[0-9a-f]{64}$'
       )
       OR evidence->'raw_source_hashes'->>'registry_row_sha256'
            IS DISTINCT FROM (
                SELECT research_stage8_json_sha256_v1(
                    to_jsonb(read_row) || jsonb_build_object(
                        'frozen_at_utc',
                        research_stage8_utc_text_v1(read_row.frozen_at_utc)))
                FROM research_stage8_registry_read_v1 AS read_row
                WHERE read_row.exact_binding_sha256 = NEW.exact_binding_sha256)
       OR evidence->'raw_source_hashes'->>'selection_row_sha256'
            IS DISTINCT FROM (
                SELECT research_stage8_json_sha256_v1(to_jsonb(read_row))
                FROM research_stage8_selection_read_v1 AS read_row
                WHERE read_row.selection_record_sha256 = NEW.selection_record_sha256)
       OR evidence->'raw_source_hashes'->>'fact_batch_row_sha256'
            IS DISTINCT FROM (
                SELECT research_stage8_json_sha256_v1(to_jsonb(read_row))
                FROM research_stage8_fact_batch_read_v1 AS read_row
                WHERE read_row.fact_batch_record_sha256
                        = fact_batch.fact_batch_record_sha256)
       OR evidence->'raw_source_hashes'->>'fact_seal_row_sha256'
            IS DISTINCT FROM (
                SELECT research_stage8_json_sha256_v1(to_jsonb(read_row))
                FROM research_stage8_fact_seal_read_v1 AS read_row
                WHERE read_row.fact_batch_record_sha256
                        = fact_batch.fact_batch_record_sha256)
       OR evidence->'raw_source_hashes'->>'sealed_fact_population_sha256'
            IS DISTINCT FROM (
                SELECT research_stage8_json_sha256_v1(COALESCE(
                    jsonb_agg(research_stage8_json_sha256_v1(to_jsonb(read_row))
                              ORDER BY read_row.attempt_id), '[]'::jsonb))
                FROM research_stage8_fact_read_v1 AS read_row
                WHERE read_row.fact_batch_record_sha256
                        = fact_batch.fact_batch_record_sha256)
       OR jsonb_typeof(evidence->'read_started_at_utc') IS DISTINCT FROM 'string'
       OR jsonb_typeof(evidence->'read_finished_at_utc') IS DISTINCT FROM 'string'
       OR research_stage8_watch_timestamp_v1(evidence->'read_started_at_utc') IS NULL
       OR research_stage8_watch_timestamp_v1(evidence->'read_finished_at_utc') IS NULL
       OR research_stage8_watch_timestamp_v1(evidence->'read_started_at_utc') > database_clock
       OR research_stage8_watch_timestamp_v1(evidence->'read_finished_at_utc') > database_clock
       OR (evidence->>'read_finished_at_utc')::timestamptz
            < (evidence->>'read_started_at_utc')::timestamptz
       OR evidence->>'outcome_authority' IS DISTINCT FROM
            'DURABLE_ROWS_READ_IN_CALLER_OWNED_RO_RR_SNAPSHOT'
       OR evidence->>'fact_authority' IS DISTINCT FROM (CASE
            WHEN replay->'all_causal_fact_semantics_verified' = 'true'::jsonb
                THEN 'SAME_SNAPSHOT_CAUSAL_SOURCE_REPLAY_VERIFIED'
            ELSE 'SAME_SNAPSHOT_CAUSAL_SOURCE_REPLAY_UNKNOWN' END)
       OR NULLIF(evidence->>'parent_count','')::integer
            IS DISTINCT FROM selection_row.representative_count
       OR jsonb_typeof(evidence->'btc_parent_movement_ids') IS DISTINCT FROM 'array'
       OR jsonb_array_length(evidence->'btc_parent_movement_ids')
            IS DISTINCT FROM selection_row.representative_count
       OR jsonb_typeof(evidence->'representatives') IS DISTINCT FROM 'array'
       OR jsonb_array_length(evidence->'representatives')
            IS DISTINCT FROM selection_row.representative_count
       OR evidence->'selection_population_read_complete' IS DISTINCT FROM 'true'::jsonb
       OR evidence->'population_read_complete' IS DISTINCT FROM 'true'::jsonb
       OR evidence->'complete' IS DISTINCT FROM 'true'::jsonb
       OR evidence->'truncated' IS DISTINCT FROM 'false'::jsonb THEN
        RAISE EXCEPTION 'Stage-8 durable outcome evidence receipt is invalid';
    END IF;

    -- The evaluator role may only persist the adapter's closed receipt.  Bind
    -- every evidence row, in order, to the server-recomputed durable selection
    -- and to the exact sealed fact row.  In particular, route membership is
    -- never accepted from the evaluation's declarative parent-id lists.
    SELECT COALESCE(jsonb_agg(item.value->'btc_parent_movement_id'
                              ORDER BY item.ordinality), '[]'::jsonb)
    INTO selected_parent_ids
    FROM jsonb_array_elements(selection_row.representative_identities)
         WITH ORDINALITY AS item(value, ordinality);
    IF evidence->'btc_parent_movement_ids' IS DISTINCT FROM selected_parent_ids
       OR EXISTS (
            SELECT 1
            FROM jsonb_array_elements(evidence->'btc_parent_movement_ids')
                 AS parent(value)
            WHERE jsonb_typeof(parent.value) IS DISTINCT FROM 'string'
               OR trim(both '"' from parent.value::text) !~ '^[0-9a-f]{64}$'
       ) OR EXISTS (
            SELECT 1
            FROM jsonb_array_elements(evidence->'btc_parent_movement_ids')
                 AS parent(value)
            GROUP BY parent.value HAVING count(*) > 1
       ) THEN
        RAISE EXCEPTION 'Stage-8 evidence parent population differs from durable selection';
    END IF;
    IF EXISTS (
        WITH selected AS (
            SELECT item.value, item.ordinality
            FROM jsonb_array_elements(selection_row.representative_identities)
                 WITH ORDINALITY AS item(value, ordinality)
        ), observed AS (
            SELECT item.value, item.ordinality
            FROM jsonb_array_elements(evidence->'representatives')
                 WITH ORDINALITY AS item(value, ordinality)
        )
        SELECT 1
        FROM observed
        FULL JOIN selected USING (ordinality)
        WHERE observed.value IS NULL OR selected.value IS NULL
           OR jsonb_typeof(observed.value) IS DISTINCT FROM 'object'
           OR (SELECT array_agg(key ORDER BY key COLLATE "C")
               FROM jsonb_object_keys(observed.value) AS keys(key))
                IS DISTINCT FROM ARRAY[
                    'asymmetry','btc_parent_movement_id',
                    'durable_representative_identity_sha256','event_fingerprint',
                    'event_id','expected_selection_fact_identity_sha256','fact',
                    'parent_start_time_utc','probability',
                    'representative_identity_sha256',
                    'selection_fact_identity_sha256'
                ]::TEXT[]
           OR observed.value->'btc_parent_movement_id'
                IS DISTINCT FROM selected.value->'btc_parent_movement_id'
           OR observed.value->'parent_start_time_utc'
                IS DISTINCT FROM selected.value->'parent_start_time_utc'
           OR observed.value->'event_id'
                IS DISTINCT FROM selected.value->'event_id'
           OR observed.value->'event_fingerprint'
                IS DISTINCT FROM selected.value->'event_fingerprint'
           OR observed.value->'expected_selection_fact_identity_sha256'
                IS DISTINCT FROM selected.value
                    ->'expected_selection_fact_identity_sha256'
           OR observed.value->'selection_fact_identity_sha256'
                IS DISTINCT FROM selected.value
                    ->'expected_selection_fact_identity_sha256'
           OR COALESCE(observed.value->>'representative_identity_sha256','')
                !~ '^[0-9a-f]{64}$'
           OR observed.value->>'representative_identity_sha256'
                IS DISTINCT FROM research_stage8_json_sha256_v1(
                    jsonb_build_object(
                        'version',
                            'stage8-outcome-free-representative-identity-v1',
                        'exact_binding_sha256', NEW.exact_binding_sha256,
                        'btc_parent_movement_id',
                            selected.value->'btc_parent_movement_id',
                        'parent_start_time_utc',
                            selected.value->'parent_start_time_utc',
                        'selection_fact_identity_sha256',
                            selected.value
                                ->'expected_selection_fact_identity_sha256',
                        'attempt_fingerprint',
                            selected.value->'attempt_fingerprint',
                        'anchor_slot_id', selected.value->'anchor_slot_id',
                        'event_id', selected.value->'event_id',
                        'event_fingerprint', selected.value->'event_fingerprint',
                        'symbol', selected.value->'symbol',
                        'direction', selected.value->'direction',
                        'decision_time_utc', selected.value->'decision_time_utc',
                        'candidate_match_knowledge_status', 'KNOWN',
                        'candidate_match', true
                    ))
           OR observed.value->>'durable_representative_identity_sha256'
                IS DISTINCT FROM research_stage8_json_sha256_v1(selected.value)
           OR jsonb_typeof(observed.value->'fact') IS DISTINCT FROM 'object'
           OR (SELECT array_agg(key ORDER BY key COLLATE "C")
               FROM jsonb_object_keys(observed.value->'fact') AS keys(key))
                IS DISTINCT FROM ARRAY[
                    'event_row_sha256','expected_selection_fact_identity_sha256',
                    'fact_record_sha256',
                    'fact_row_sha256','fact_sha256','reasons',
                    'selection_fact_identity_sha256','validation_status'
                ]::TEXT[]
           OR observed.value->'fact'->>'expected_selection_fact_identity_sha256'
                IS DISTINCT FROM selected.value
                    ->>'expected_selection_fact_identity_sha256'
           OR observed.value->'fact'->>'selection_fact_identity_sha256'
                IS DISTINCT FROM selected.value
                    ->>'expected_selection_fact_identity_sha256'
           OR (observed.value->'fact'->>'validation_status'
                    IS DISTINCT FROM 'VALID'
               AND observed.value->'fact'->>'validation_status'
                    IS DISTINCT FROM 'UNKNOWN')
           OR jsonb_typeof(observed.value->'fact'->'reasons')
                IS DISTINCT FROM 'array'
           OR (observed.value->'fact'->>'validation_status' = 'VALID' AND (
                observed.value->'fact'->'reasons' IS DISTINCT FROM '[]'::jsonb
                OR COALESCE(observed.value->'fact'->>'fact_record_sha256','')
                    !~ '^[0-9a-f]{64}$'
                OR COALESCE(observed.value->'fact'->>'fact_sha256','')
                    !~ '^[0-9a-f]{64}$'
                OR COALESCE(observed.value->'fact'->>'fact_row_sha256','')
                    !~ '^[0-9a-f]{64}$'
                OR COALESCE(observed.value->'fact'->>'event_row_sha256','')
                    !~ '^[0-9a-f]{64}$'
           ))
           OR NOT EXISTS (
                SELECT 1
                FROM research_stage8_projected_fact_ledger AS stored_fact
                WHERE stored_fact.fact_batch_record_sha256
                        = fact_batch.fact_batch_record_sha256
                  AND stored_fact.event_id = (selected.value->>'event_id')::bigint
                  AND stored_fact.selection_fact_identity_sha256
                        = selected.value
                            ->>'expected_selection_fact_identity_sha256'
                  AND stored_fact.fact_record_sha256
                        = observed.value->'fact'->>'fact_record_sha256'
                  AND stored_fact.fact_sha256
                        = observed.value->'fact'->>'fact_sha256'
           )
           OR (observed.value->'fact'->>'validation_status' = 'VALID'
               AND NOT EXISTS (
                    SELECT 1
                    FROM research_events AS source_event
                    WHERE source_event.event_id
                            = (selected.value->>'event_id')::bigint
                      AND btrim(source_event.event_fingerprint::text)
                            = selected.value->>'event_fingerprint'
                      AND source_event.symbol = selected.value->>'symbol'
                      AND source_event.direction = selected.value->>'direction'
                      AND source_event.alert_time_utc
                            = (selected.value->>'decision_time_utc')::timestamptz
                      AND source_event.current_price > 0
                      AND observed.value->'fact'->>'event_row_sha256'
                            = research_stage8_json_sha256_v1(to_jsonb(source_event))
               ))
           OR jsonb_typeof(observed.value->'probability') IS DISTINCT FROM 'object'
           OR (SELECT array_agg(key ORDER BY key COLLATE "C")
               FROM jsonb_object_keys(observed.value->'probability') AS keys(key))
                IS DISTINCT FROM ARRAY[
                    'reasons','reported_status','source_row_sha256',
                    'source_status','validation_status'
                ]::TEXT[]
           OR (observed.value->'probability'->>'validation_status'
                    IS DISTINCT FROM 'VALID'
               AND observed.value->'probability'->>'validation_status'
                    IS DISTINCT FROM 'UNKNOWN')
           -- UNKNOWN is fail-closed only when the exact cell is genuinely
           -- absent/nonterminal.  A caller cannot hide a terminal loss (or a
           -- corrupt terminal row) from the route denominator.
           OR (observed.value->'probability'->>'validation_status' = 'UNKNOWN'
               AND EXISTS (
                    SELECT 1
                    FROM research_ordered_first_touch_outcomes AS terminal_cell
                    WHERE terminal_cell.event_id
                            = (selected.value->>'event_id')::bigint
                      AND terminal_cell.window_minutes = registry.window_minutes
                      AND terminal_cell.threshold_bps = registry.threshold_bps
                      AND terminal_cell.method_version = 'ordered-first-touch-v7'
                      AND terminal_cell.direction = selected.value->>'direction'
                      AND terminal_cell.measurement_start_utc
                            = (selected.value->>'decision_time_utc')::timestamptz
                      AND terminal_cell.status IN ('SUCCESS','FAILURE')
               ))
           OR jsonb_typeof(observed.value->'probability'->'reasons')
                IS DISTINCT FROM 'array'
           OR (observed.value->'probability'->>'validation_status' = 'VALID' AND (
                (observed.value->'probability'->>'reported_status'
                    IS DISTINCT FROM 'SUCCESS'
                 AND observed.value->'probability'->>'reported_status'
                    IS DISTINCT FROM 'FAILURE')
                OR observed.value->'probability'->'reasons'
                    IS DISTINCT FROM '[]'::jsonb
                OR COALESCE(observed.value->'probability'->>'source_row_sha256','')
                    !~ '^[0-9a-f]{64}$'
           ))
           OR (observed.value->'probability'->>'validation_status' = 'VALID'
               AND NOT EXISTS (
                    SELECT 1
                    FROM research_ordered_first_touch_outcomes AS source_outcome
                    JOIN research_events AS source_event
                      ON source_event.event_id = source_outcome.event_id
                    WHERE source_outcome.event_id
                            = (selected.value->>'event_id')::bigint
                      AND source_outcome.window_minutes = registry.window_minutes
                      AND source_outcome.threshold_bps = registry.threshold_bps
                      AND source_outcome.method_version = 'ordered-first-touch-v7'
                      AND source_outcome.direction = selected.value->>'direction'
                      AND source_outcome.measurement_start_utc
                            = (selected.value->>'decision_time_utc')::timestamptz
                      AND source_outcome.status IN ('SUCCESS','FAILURE')
                      AND source_outcome.status
                            = observed.value->'probability'->>'source_status'
                      AND source_outcome.status
                            = observed.value->'probability'->>'reported_status'
                      AND ((source_outcome.status = 'SUCCESS'
                            AND source_outcome.success IS TRUE
                            AND source_outcome.first_touch_side = 'FAVORABLE'
                            AND source_outcome.terminal_reason = 'FAVORABLE_FIRST')
                           OR (source_outcome.status = 'FAILURE'
                            AND source_outcome.success IS FALSE
                            AND source_outcome.first_touch_side = 'ADVERSE'
                            AND source_outcome.terminal_reason = 'ADVERSE_FIRST'))
                      AND source_outcome.input_path_complete IS TRUE
                      AND source_outcome.path_complete IS TRUE
                      AND source_outcome.candle_interval_seconds = 60
                      AND source_outcome.path_samples > 0
                      AND source_outcome.path_samples
                            <= source_outcome.window_minutes
                      AND source_outcome.reference_price > 0
                      AND abs(source_outcome.reference_price
                              - source_event.current_price)
                            <= 1e-12 * greatest(
                                1.0, abs(source_event.current_price))
                      AND source_outcome.observed_through_utc
                            >= source_outcome.measurement_start_utc
                      AND source_outcome.decision_time_utc IS NOT NULL
                      AND source_outcome.decision_time_utc
                            <= source_outcome.measurement_start_utc
                               + source_outcome.window_minutes * interval '1 minute'
                      AND source_outcome.created_at_utc
                            <= source_outcome.updated_at_utc
                      AND source_outcome.created_at_utc
                            >= source_outcome.measurement_start_utc
                      AND source_outcome.created_at_utc
                            >= source_event.alert_time_utc
                      AND source_outcome.measurement_start_utc
                            <= source_outcome.updated_at_utc
                      AND source_outcome.observed_through_utc
                            <= source_outcome.updated_at_utc
                      AND source_outcome.decision_time_utc
                            <= source_outcome.updated_at_utc
                      AND source_outcome.updated_at_utc
                            <= (evidence->>'read_started_at_utc')::timestamptz
                      AND btrim(source_event.event_fingerprint::text)
                            = selected.value->>'event_fingerprint'
                      AND source_event.symbol = selected.value->>'symbol'
                      AND source_event.direction = selected.value->>'direction'
                      AND source_event.alert_time_utc
                            = (selected.value->>'decision_time_utc')::timestamptz
                      AND observed.value->'probability'->>'source_row_sha256'
                            = research_stage8_json_sha256_v1(
                                to_jsonb(source_outcome))
                      AND length(source_outcome.price_source)
                            - length(replace(source_outcome.price_source,'|','')) = 2
                      AND ((source_event.symbol = 'HYPE'
                            AND source_outcome.market_pair = 'HYPE/USDT'
                            AND source_outcome.data_quality_status
                                = 'VERIFIED_HYPERLIQUID_SPOT_1M_CLOSED_CANDLES'
                            AND split_part(source_outcome.price_source,'|',1)
                                = 'reference=hyperliquid_spot_@107'
                            AND split_part(source_outcome.price_source,'|',2)
                                = 'path=hyperliquid_spot:HYPE/USDT:1m'
                            AND split_part(source_outcome.price_source,'|',3)
                                LIKE 'provenance=_%'
                            AND split_part(source_outcome.price_source,'|',4) = ''
                            AND source_outcome.calculation_audit
                                  ->'price_provenance'->>'instrument' = '@107')
                           OR (source_event.symbol <> 'HYPE'
                            AND source_outcome.market_pair
                                = source_event.symbol || 'USDT'
                            AND source_outcome.data_quality_status
                                = 'VERIFIED_BINANCE_SPOT_1M_CLOSED_CANDLES'
                            AND split_part(source_outcome.price_source,'|',1)
                                IN ('reference=binance_spot',
                                    'reference=binance_spot:'
                                      || source_outcome.market_pair)
                            AND split_part(source_outcome.price_source,'|',2)
                                = 'path=binance_spot:'
                                  || source_outcome.market_pair || ':1m'
                            AND split_part(source_outcome.price_source,'|',3)
                                LIKE 'provenance=_%'
                            AND split_part(source_outcome.price_source,'|',4) = ''))
               ))
           OR jsonb_typeof(observed.value->'asymmetry') IS DISTINCT FROM 'object'
           OR (SELECT array_agg(key ORDER BY key COLLATE "C")
               FROM jsonb_object_keys(observed.value->'asymmetry') AS keys(key))
                IS DISTINCT FROM ARRAY[
                    'mae_pct','mfe_pct','reasons','source_row_sha256',
                    'source_status','validation_status','zero_denominator'
                ]::TEXT[]
           OR (observed.value->'asymmetry'->>'validation_status'
                    IS DISTINCT FROM 'VALID'
               AND observed.value->'asymmetry'->>'validation_status'
                    IS DISTINCT FROM 'UNKNOWN')
           -- READY cells likewise cannot be relabelled UNKNOWN to cherry-pick
           -- favorable common-window measurements.
           OR (observed.value->'asymmetry'->>'validation_status' = 'UNKNOWN'
               AND EXISTS (
                    SELECT 1
                    FROM research_common_window_metrics AS ready_cell
                    WHERE ready_cell.event_id
                            = (selected.value->>'event_id')::bigint
                      AND ready_cell.window_minutes = registry.window_minutes
                      AND ready_cell.method_version = 'common-window-spot-1m-v1'
                      AND ready_cell.measurement_start_utc
                            = (selected.value->>'decision_time_utc')::timestamptz
                      AND ready_cell.status = 'READY'
               ))
           OR jsonb_typeof(observed.value->'asymmetry'->'reasons')
                IS DISTINCT FROM 'array'
           OR (observed.value->'asymmetry'->>'validation_status' = 'VALID' AND (
                observed.value->'asymmetry'->'reasons' IS DISTINCT FROM '[]'::jsonb
                OR jsonb_typeof(observed.value->'asymmetry'->'mfe_pct')
                    IS DISTINCT FROM 'number'
                OR jsonb_typeof(observed.value->'asymmetry'->'mae_pct')
                    IS DISTINCT FROM 'number'
                OR (observed.value->'asymmetry'->>'mfe_pct')::double precision < 0
                OR (observed.value->'asymmetry'->>'mae_pct')::double precision < 0
                OR observed.value->'asymmetry'->'zero_denominator'
                    IS DISTINCT FROM to_jsonb(
                        (observed.value->'asymmetry'->>'mae_pct')::double precision = 0
                    )
                OR COALESCE(observed.value->'asymmetry'->>'source_row_sha256','')
                    !~ '^[0-9a-f]{64}$'
           ))
           OR (observed.value->'asymmetry'->>'validation_status' = 'VALID'
               AND NOT EXISTS (
                    SELECT 1
                    FROM research_common_window_metrics AS source_metric
                    JOIN research_events AS source_event
                      ON source_event.event_id = source_metric.event_id
                    WHERE source_metric.event_id
                            = (selected.value->>'event_id')::bigint
                      AND source_metric.window_minutes = registry.window_minutes
                      AND source_metric.method_version
                            = 'common-window-spot-1m-v1'
                      AND source_metric.status = 'READY'
                      AND source_metric.measurement_start_utc
                            = (selected.value->>'decision_time_utc')::timestamptz
                      AND source_metric.window_end_utc
                            = source_metric.measurement_start_utc
                               + source_metric.window_minutes * interval '1 minute'
                      AND source_metric.created_at_utc
                            <= source_metric.updated_at_utc
                      AND source_metric.updated_at_utc
                            <= (evidence->>'read_started_at_utc')::timestamptz
                      AND btrim(source_event.event_fingerprint::text)
                            = selected.value->>'event_fingerprint'
                      AND source_event.symbol = selected.value->>'symbol'
                      AND source_event.direction = selected.value->>'direction'
                      AND source_event.alert_time_utc
                            = (selected.value->>'decision_time_utc')::timestamptz
                      AND observed.value->'asymmetry'->>'source_status' = 'READY'
                      AND observed.value->'asymmetry'->>'source_row_sha256'
                            = research_stage8_json_sha256_v1(
                                to_jsonb(source_metric))
                      AND jsonb_typeof(source_metric.result) = 'object'
                      AND (SELECT array_agg(key ORDER BY key COLLATE "C")
                           FROM jsonb_object_keys(source_metric.result)
                                AS result_keys(key)) IS NOT DISTINCT FROM ARRAY[
                            'asymmetry_method','asymmetry_ratio','asymmetry_status',
                            'boundary_policy','candle_interval_seconds',
                            'data_quality_status','direction','expected_candles',
                            'initial_gap_seconds','mae_pct','max_adverse_price',
                            'max_favorable_price','measurement_kind',
                            'measurement_start_utc','method_version','mfe_pct',
                            'missing_reason','observation_closed','observed_at_utc',
                            'observed_from_utc','observed_prefix_complete',
                            'observed_through_utc','path_complete','path_samples',
                            'path_sha256','reference_price','source','status','symbol',
                            'trailing_partial_minute_seconds','window_end_utc',
                            'window_minutes'
                      ]::text[]
                      AND source_metric.result->>'method_version'
                            = source_metric.method_version
                      AND source_metric.result->>'measurement_kind' = 'FIXED_WINDOW'
                      AND NULLIF(source_metric.result->>'window_minutes','')::integer
                            = source_metric.window_minutes
                      AND source_metric.result->>'symbol' = source_event.symbol
                      AND source_metric.result->>'direction' = source_event.direction
                      AND (source_metric.result->>'measurement_start_utc')::timestamptz
                            = source_metric.measurement_start_utc
                      AND (source_metric.result->>'window_end_utc')::timestamptz
                            = source_metric.window_end_utc
                      AND source_metric.result->>'status' = 'READY'
                      AND source_metric.result->'observation_closed' = 'true'::jsonb
                      AND source_metric.result->'path_complete' = 'true'::jsonb
                      AND source_metric.result->'observed_prefix_complete' = 'true'::jsonb
                      AND source_metric.result->'missing_reason' = 'null'::jsonb
                      AND source_metric.result->>'boundary_policy'
                            = 'EXCLUDE_PARTIAL_MINUTES_USE_IMMUTABLE_ALERT_PRICE'
                      AND NULLIF(source_metric.result->>'candle_interval_seconds','')::integer = 60
                      AND NULLIF(source_metric.result->>'expected_candles','')::integer > 0
                      AND NULLIF(source_metric.result->>'expected_candles','')::integer
                            <= source_metric.window_minutes
                      AND source_metric.result->'path_samples'
                            = source_metric.result->'expected_candles'
                      AND (source_metric.result->>'observed_at_utc')::timestamptz
                            >= source_metric.window_end_utc
                      AND (source_metric.result->>'observed_at_utc')::timestamptz
                            <= (evidence->>'read_started_at_utc')::timestamptz
                      AND source_metric.result->>'asymmetry_method'
                            = 'sum_mfe_pct_over_sum_mae_pct_same_full_window_v1'
                      AND source_metric.result->>'data_quality_status' IN (
                            'VERIFIED_BINANCE_SPOT_1M_CLOSED_CANDLES',
                            'VERIFIED_HYPERLIQUID_SPOT_1M_CLOSED_CANDLES')
                      AND COALESCE(source_metric.result->>'path_sha256','')
                            ~ '^[0-9a-f]{64}$'
                      AND jsonb_typeof(source_metric.result->'reference_price') = 'number'
                      AND (source_metric.result->>'reference_price')::double precision > 0
                      AND abs((source_metric.result->>'reference_price')::double precision
                              - source_event.current_price)
                            <= 1e-12 * greatest(1.0, abs(source_event.current_price))
                      AND jsonb_typeof(source_metric.result->'mfe_pct') = 'number'
                      AND jsonb_typeof(source_metric.result->'mae_pct') = 'number'
                      AND (source_metric.result->>'mfe_pct')::double precision >= 0
                      AND (source_metric.result->>'mae_pct')::double precision >= 0
                      AND source_metric.result->'mfe_pct'
                            = observed.value->'asymmetry'->'mfe_pct'
                      AND source_metric.result->'mae_pct'
                            = observed.value->'asymmetry'->'mae_pct'
                      AND observed.value->'asymmetry'->'zero_denominator'
                            = to_jsonb((source_metric.result->>'mae_pct')::double precision = 0)
                      AND ((source_metric.result->>'mae_pct')::double precision > 0
                           AND source_metric.result->>'asymmetry_status' = 'DEFINED'
                           AND jsonb_typeof(source_metric.result->'asymmetry_ratio') = 'number'
                           AND abs((source_metric.result->>'asymmetry_ratio')::double precision
                                   - (source_metric.result->>'mfe_pct')::double precision
                                     / (source_metric.result->>'mae_pct')::double precision)
                                <= 1e-12 * greatest(1.0, abs(
                                    (source_metric.result->>'mfe_pct')::double precision
                                      / (source_metric.result->>'mae_pct')::double precision))
                           OR ((source_metric.result->>'mae_pct')::double precision = 0
                               AND source_metric.result->'asymmetry_ratio' = 'null'::jsonb
                               AND source_metric.result->>'asymmetry_status'
                                    = 'UNDEFINED_ZERO_MAE'))
                      AND jsonb_typeof(source_metric.result->'source') = 'object'
                      AND (SELECT array_agg(key ORDER BY key COLLATE "C")
                           FROM jsonb_object_keys(source_metric.result->'source')
                                AS source_keys(key)) IS NOT DISTINCT FROM ARRAY[
                            'exchange','instrument','interval','interval_seconds',
                            'market','method_version','pair','provenance_version',
                            'provider_provenance','symbol'
                      ]::text[]
                      AND source_metric.result->'source'->>'provenance_version'
                            = 'canonical-spot-reference-provenance-v1'
                      AND source_metric.result->'source'->>'method_version'
                            = 'canonical-spot-1m-ohlc-path-v3'
                      AND source_metric.result->'source'->>'symbol' = source_event.symbol
                      AND source_metric.result->'source'->>'market' = 'spot'
                      AND source_metric.result->'source'->>'interval' = '1m'
                      AND NULLIF(source_metric.result->'source'
                            ->>'interval_seconds','')::integer = 60
                      AND btrim(COALESCE(source_metric.result->'source'
                            ->>'provider_provenance','')) <> ''
                      AND ((source_event.symbol = 'HYPE'
                            AND source_metric.result->'source'->>'exchange'
                                = 'hyperliquid'
                            AND source_metric.result->'source'->>'pair' = 'HYPE/USDT'
                            AND source_metric.result->'source'->>'instrument' = '@107'
                            AND source_metric.result->>'data_quality_status'
                                = 'VERIFIED_HYPERLIQUID_SPOT_1M_CLOSED_CANDLES')
                           OR (source_event.symbol <> 'HYPE'
                            AND source_metric.result->'source'->>'exchange' = 'binance'
                            AND source_metric.result->'source'->>'pair'
                                = source_event.symbol || 'USDT'
                            AND source_metric.result->'source'->'instrument' = 'null'::jsonb
                            AND source_metric.result->>'data_quality_status'
                                = 'VERIFIED_BINANCE_SPOT_1M_CLOSED_CANDLES'))
               ))
    ) THEN
        RAISE EXCEPTION 'Stage-8 evidence representative does not bind durable selection/facts';
    END IF;
    IF evidence->'all_representative_facts_valid' IS DISTINCT FROM to_jsonb(
           NOT EXISTS (
               SELECT 1 FROM jsonb_array_elements(evidence->'representatives') AS item
               WHERE item->'fact'->>'validation_status' IS DISTINCT FROM 'VALID'
           )
       ) OR NULLIF(evidence->>'probability_evidence_valid_count','')::integer
            IS DISTINCT FROM (
                SELECT count(*)::integer
                FROM jsonb_array_elements(evidence->'representatives') AS item
                WHERE item->'probability'->>'validation_status' = 'VALID'
            )
       OR NULLIF(evidence->>'asymmetry_evidence_valid_count','')::integer
            IS DISTINCT FROM (
                SELECT count(*)::integer
                FROM jsonb_array_elements(evidence->'representatives') AS item
                WHERE item->'asymmetry'->>'validation_status' = 'VALID'
            ) THEN
        RAISE EXCEPTION 'Stage-8 evidence validity summaries differ from representatives';
    END IF;

    -- A VERIFIED replay must enumerate the sealed attempt population exactly
    -- and prove equal causal/selection semantics for every attempt.  A caller
    -- cannot promote an UNKNOWN comparison by only flipping receipt booleans.
    IF EXISTS (
        WITH expected AS (
            SELECT item.value, item.ordinality
            FROM jsonb_array_elements(fact_batch.attempt_ids)
                 WITH ORDINALITY AS item(value, ordinality)
        ), observed AS (
            SELECT item.value, item.ordinality
            FROM jsonb_array_elements(replay->'comparisons')
                 WITH ORDINALITY AS item(value, ordinality)
        )
        SELECT 1
        FROM observed
        FULL JOIN expected USING (ordinality)
        WHERE observed.value IS NULL OR expected.value IS NULL
           OR jsonb_typeof(observed.value) IS DISTINCT FROM 'object'
           OR (SELECT array_agg(key ORDER BY key COLLATE "C")
               FROM jsonb_object_keys(observed.value) AS keys(key))
                IS DISTINCT FROM ARRAY[
                    'attempt_id','durable_fact_record_sha256','durable_fact_semantic_sha256',
                    'durable_fact_sha256','durable_parent_class',
                    'durable_parent_semantic_sha256',
                    'durable_selection_fact_identity_sha256','reasons',
                    'replayed_fact_semantic_sha256','replayed_fact_sha256',
                    'replayed_parent_class','replayed_parent_semantic_sha256',
                    'replayed_selection_fact_identity_sha256','status'
                ]::TEXT[]
           OR observed.value->'attempt_id' IS DISTINCT FROM expected.value
           OR (observed.value->>'status' IS DISTINCT FROM 'VERIFIED'
               AND observed.value->>'status' IS DISTINCT FROM 'UNKNOWN')
           OR jsonb_typeof(observed.value->'reasons') IS DISTINCT FROM 'array'
           OR NOT EXISTS (
                SELECT 1 FROM research_stage8_projected_fact_ledger AS stored_fact
                WHERE stored_fact.fact_batch_record_sha256
                        = fact_batch.fact_batch_record_sha256
                  AND stored_fact.attempt_id
                        = (observed.value->>'attempt_id')::bigint
                  AND stored_fact.fact_record_sha256
                        = observed.value->>'durable_fact_record_sha256'
                  AND stored_fact.fact_sha256
                        = observed.value->>'durable_fact_sha256'
                  AND stored_fact.selection_fact_identity_sha256
                        = observed.value
                            ->>'durable_selection_fact_identity_sha256'
           )
           OR (observed.value->>'status' = 'VERIFIED' AND (
                observed.value->'reasons' IS DISTINCT FROM '[]'::jsonb
                OR COALESCE(observed.value->>'durable_fact_semantic_sha256','')
                    !~ '^[0-9a-f]{64}$'
                OR observed.value->>'durable_fact_semantic_sha256'
                    IS DISTINCT FROM observed.value->>'replayed_fact_semantic_sha256'
                OR observed.value->>'durable_parent_class'
                    IS DISTINCT FROM observed.value->>'replayed_parent_class'
                OR COALESCE(observed.value->>'durable_parent_semantic_sha256','')
                    !~ '^[0-9a-f]{64}$'
                OR observed.value->>'durable_parent_semantic_sha256'
                    IS DISTINCT FROM observed.value->>'replayed_parent_semantic_sha256'
                OR COALESCE(observed.value
                        ->>'durable_selection_fact_identity_sha256','')
                    !~ '^[0-9a-f]{64}$'
                OR observed.value->>'durable_selection_fact_identity_sha256'
                    IS DISTINCT FROM observed.value
                        ->>'replayed_selection_fact_identity_sha256'
           ))
    ) THEN
        RAISE EXCEPTION 'Stage-8 fact replay comparisons are invalid';
    END IF;
    IF replay->'all_causal_fact_semantics_verified' IS DISTINCT FROM to_jsonb(
           NOT EXISTS (
               SELECT 1 FROM jsonb_array_elements(replay->'comparisons') AS item
               WHERE item->>'status' IS DISTINCT FROM 'VERIFIED'
           )
       ) OR replay->'durable_selection_server_recomputed'
            IS DISTINCT FROM 'false'::jsonb
       OR replay->'complete' IS DISTINCT FROM replay->'all_causal_fact_semantics_verified'
       OR replay->>'status' IS DISTINCT FROM (CASE
            WHEN replay->'all_causal_fact_semantics_verified' = 'true'::jsonb
                THEN 'VERIFIED' ELSE 'UNKNOWN' END) THEN
        RAISE EXCEPTION 'Stage-8 fact replay summary differs from comparisons';
    END IF;
    IF jsonb_typeof(evaluation_value) IS DISTINCT FROM 'object'
       OR (SELECT array_agg(key ORDER BY key COLLATE "C")
           FROM jsonb_object_keys(evaluation_value) AS keys(key)) IS DISTINCT FROM ARRAY[
            'acceptance_policy_sha256','acceptance_policy_version','atomic_expression',
            'atomic_gate_passed','authoritative_fact_replay_verified','common',
            'delivery_status_required','durable_fact_source_authority_verified',
            'durable_outcome_atomic_gate_evidence_verified',
            'durable_outcome_source_read_verified','durable_registry_persistence_verified',
            'evaluator_version','evidence_receipt_sha256','exact_binding_sha256',
            'excluded_representatives','fact_replay_receipt_sha256',
            'fresh_three_parent_route','live_authorized','live_effect','manifest_sha256',
            'maximum_result','qualification_blockers',
            'registry_selection_persistence_verified','registry_selection_receipt',
            'representative_rows_received','research_qualified','result_scope','routes',
            'selection_record_sha256','status','structurally_eligible',
            'telegram_authorized','trade_authorized','trade_execution_effect'
       ]::TEXT[]
       OR evaluation_value->>'manifest_sha256' IS DISTINCT FROM registry.manifest_sha256
       OR evaluation_value->>'exact_binding_sha256' IS DISTINCT FROM NEW.exact_binding_sha256
       OR evaluation_value->>'selection_record_sha256' IS DISTINCT FROM NEW.selection_record_sha256
       OR evaluation_value->>'evaluator_version' IS DISTINCT FROM
            registry.implementation_artifacts->'files'->'acceptance'->>'version'
       OR evaluation_value->>'fact_replay_receipt_sha256'
            IS DISTINCT FROM replay->>'fact_replay_receipt_sha256'
       OR evaluation_value->>'evidence_receipt_sha256'
            IS DISTINCT FROM evidence->>'evidence_receipt_sha256'
       OR evaluation_value->>'result_scope' IS DISTINCT FROM 'EXPERIMENTAL_RESEARCH_ONLY'
       OR evaluation_value->'live_authorized' IS DISTINCT FROM 'false'::jsonb
       OR evaluation_value->'telegram_authorized' IS DISTINCT FROM 'false'::jsonb
       OR evaluation_value->'trade_authorized' IS DISTINCT FROM 'false'::jsonb
       OR evaluation_value->'delivery_status_required' IS DISTINCT FROM 'false'::jsonb
       OR evaluation_value->>'live_effect' IS DISTINCT FROM 'NONE'
       OR evaluation_value->>'trade_execution_effect' IS DISTINCT FROM 'NONE'
       OR evaluation_value->'durable_registry_persistence_verified'
            IS DISTINCT FROM 'true'::jsonb
       OR evaluation_value->'registry_selection_persistence_verified'
            IS DISTINCT FROM 'true'::jsonb
       OR evaluation_value->'durable_outcome_source_read_verified'
            IS DISTINCT FROM 'true'::jsonb
       OR evaluation_value->'authoritative_fact_replay_verified'
            IS DISTINCT FROM 'false'::jsonb
       OR evaluation_value->'durable_fact_source_authority_verified'
            IS DISTINCT FROM 'false'::jsonb
       OR evaluation_value->'durable_outcome_atomic_gate_evidence_verified'
            IS DISTINCT FROM 'false'::jsonb
       OR evaluation_value->'research_qualified' IS DISTINCT FROM 'false'::jsonb
       OR jsonb_typeof(evaluation_value->'qualification_blockers')
            IS DISTINCT FROM 'array'
       OR NOT (evaluation_value->'qualification_blockers'
                ? 'SERVER_DB_REPLAY_ATTESTATION_REQUIRED')
       OR NEW.persistence_payload->>'evaluation_sha256' IS DISTINCT FROM
            research_stage8_json_sha256_v1(evaluation_value) THEN
        RAISE EXCEPTION 'Stage-8 authoritative evaluation is invalid or unsafe';
    END IF;
    IF jsonb_typeof(evaluation_value->'common') IS DISTINCT FROM 'object'
       OR (SELECT array_agg(key ORDER BY key COLLATE "C")
           FROM jsonb_object_keys(evaluation_value->'common') AS keys(key))
            IS DISTINCT FROM ARRAY[
                'blockers','duplicate_btc_parent_movement_ids','passed',
                'selection_provenance_complete'
            ]::TEXT[]
       OR jsonb_typeof(evaluation_value->'routes') IS DISTINCT FROM 'object'
       OR (SELECT array_agg(key ORDER BY key COLLATE "C")
           FROM jsonb_object_keys(evaluation_value->'routes') AS keys(key))
            IS DISTINCT FROM ARRAY['ASYMMETRY','PROBABILITY']::TEXT[]
       OR (SELECT array_agg(key ORDER BY key COLLATE "C")
           FROM jsonb_object_keys(
               evaluation_value->'routes'->'PROBABILITY') AS keys(key))
            IS DISTINCT FROM ARRAY[
                'btc_parent_movement_ids','checks','distinct_parent_count',
                'exclusions','failures','hit_rate_pct','passed','status',
                'successes','wilson_95_lower_pct'
            ]::TEXT[]
       OR (SELECT array_agg(key ORDER BY key COLLATE "C")
           FROM jsonb_object_keys(
               evaluation_value->'routes'->'PROBABILITY'->'checks') AS keys(key))
            IS DISTINCT FROM ARRAY[
                'hit_rate_pct_gte_70','minimum_distinct_parents',
                'wilson_95_lower_pct_gte_40'
            ]::TEXT[]
       OR (SELECT array_agg(key ORDER BY key COLLATE "C")
           FROM jsonb_object_keys(
               evaluation_value->'routes'->'ASYMMETRY') AS keys(key))
            IS DISTINCT FROM ARRAY[
                'btc_parent_movement_ids','checks',
                'common_window_asymmetry_ratio',
                'common_window_asymmetry_state',
                'common_window_favorable_dominance_pct',
                'common_window_median_paired_edge_pct','distinct_parent_count',
                'exclusions','passed','status','sum_mae_pct','sum_mfe_pct'
            ]::TEXT[]
       OR (SELECT array_agg(key ORDER BY key COLLATE "C")
           FROM jsonb_object_keys(
               evaluation_value->'routes'->'ASYMMETRY'->'checks') AS keys(key))
            IS DISTINCT FROM ARRAY[
                'common_window_asymmetry_ratio_gte_1_5',
                'common_window_favorable_dominance_pct_gte_60',
                'common_window_median_paired_edge_pct_gt_0',
                'minimum_distinct_parents'
            ]::TEXT[] THEN
        RAISE EXCEPTION 'Stage-8 evaluation route schema is not closed';
    END IF;
    IF jsonb_typeof(evaluation_value->'routes'->'PROBABILITY'->'exclusions')
            IS DISTINCT FROM 'object'
       OR jsonb_typeof(evaluation_value->'routes'->'ASYMMETRY'->'exclusions')
            IS DISTINCT FROM 'object' THEN
        RAISE EXCEPTION 'Stage-8 route exclusions are not closed objects';
    END IF;
    -- Reconstruct the historical, outcome-blind acceptance receipt solely
    -- from the guarded durable registry and selector row.  The selector's
    -- transport identity intentionally names its digest
    -- expected_selection_fact_identity_sha256; the acceptance identity uses
    -- selection_fact_identity_sha256.  Consequently this hash must not be
    -- compared with selection_row.representative_set_sha256.
    receipt_binding := jsonb_build_object(
        'exact_binding_sha256', NEW.exact_binding_sha256,
        'manifest_sha256', registry.manifest_sha256,
        'contract_version', registry.contract_version,
        'source_version', registry.source_version,
        'projection_version', registry.projection_version,
        'label_version', registry.label_version,
        'independence_version', registry.independence_version,
        'acceptance_version', registry.acceptance_version,
        'scope_id', registry.scope_id,
        'scope_symbols', registry.exact_binding->'binding'->'scope'->'symbols',
        'scope_price_route',
            registry.exact_binding->'binding'->'scope'->>'price_route',
        'candidate_id', registry.candidate_id,
        'candidate_model',
            registry.exact_binding->'binding'->'candidate'->>'model',
        'direction',
            registry.exact_binding->'binding'->'candidate'->>'direction',
        'window_minutes', registry.window_minutes,
        'threshold_bps', registry.threshold_bps,
        'parent_policy_version', registry.parent_policy_version
    );
    WITH acceptance_identities AS (
        SELECT identity,
               (identity - 'expected_selection_fact_identity_sha256')
               || jsonb_build_object(
                    'selection_fact_identity_sha256',
                    identity->'expected_selection_fact_identity_sha256'
                  ) AS acceptance_identity
        FROM jsonb_array_elements(selection_row.representative_identities)
             AS selected(identity)
    ), acceptance_records AS (
        SELECT jsonb_build_object(
            'binding', receipt_binding,
            'btc_parent_movement_id', identity->'btc_parent_movement_id',
            'parent_start_time_utc', identity->'parent_start_time_utc',
            'representative_status', 'VALID',
            'parent_policy_version', registry.parent_policy_version,
            'membership_status', 'LIVE',
            'parent_evidence_eligible', true,
            'freeze_id', registry.freeze_id,
            'registry_record_sha256', registry.registry_record_sha256,
            'registry_verification_receipt_sha256',
                selection_row.registry_verification_receipt_sha256,
            'representative', acceptance_identity - ARRAY[
                'version','exact_binding_sha256','btc_parent_movement_id',
                'parent_start_time_utc'
            ]::text[],
            'representative_identity_sha256',
                research_stage8_json_sha256_v1(acceptance_identity)
        ) AS record
        FROM acceptance_identities
    )
    SELECT count(*)::integer,
           research_stage8_json_sha256_v1(jsonb_build_object(
               'version', 'stage8-outcome-blind-representative-set-v1',
               'exact_binding_sha256', NEW.exact_binding_sha256,
               'representatives', COALESCE(jsonb_agg(
                   record ORDER BY
                       research_stage8_canonical_json_v1(record) COLLATE "C"
               ), '[]'::jsonb)
           ))
    INTO expected_receipt_representative_count,
         expected_receipt_representative_set_sha256
    FROM acceptance_records;
    receipt_unsigned := jsonb_build_object(
        'registration_evidence',
            'CALLER_SUPPLIED_REGISTRY_REFERENCES_NOT_DB_VERIFIED',
        'freeze_id', registry.freeze_id,
        'frozen_at_utc', research_stage8_utc_text_v1(registry.frozen_at_utc),
        'registry_record_sha256', registry.registry_record_sha256,
        'registry_verification_receipt_sha256',
            selection_row.registry_verification_receipt_sha256,
        'status', 'COMPLETE',
        'exact_binding_sha256', NEW.exact_binding_sha256,
        'manifest_sha256', registry.manifest_sha256,
        'acceptance_policy_version', registry.acceptance_version,
        'acceptance_policy_sha256',
            '6f07e20e4a24c09ee8ffa8813a1e3a90888e4765ddab14db86eae0f3c0bb424a',
        'independence_version', registry.independence_version,
        'parent_policy_version', registry.parent_policy_version,
        'representative_policy',
            'EARLIEST_VALID_MATCH_BEFORE_INSPECTING_LABELS',
        'eligible_parent_rule',
            'PARENT_START_STRICTLY_AFTER_REAL_DURABLE_FREEZE',
        'prospective_clock_basis',
            'DURABLE_REGISTRY_FROZEN_AT_RECEIPT_NOT_LOCAL_CLOCK',
        'cohort_query_sha256', selection_row.cohort_query_sha256,
        'population_receipt_sha256',
            selection_row.outcome_free_population_receipt_sha256,
        'source_high_water_attempt_id',
            selection_row.source_high_water_attempt_id,
        'representative_count', expected_receipt_representative_count,
        'representative_set_sha256',
            expected_receipt_representative_set_sha256,
        'population_coverage_complete', true,
        'candidate_match_coverage_complete', true,
        'outcome_blind_selection', true,
        'truncated', false
    );
    expected_receipt_attestation_sha256 :=
        research_stage8_json_sha256_v1(receipt_unsigned);
    IF evaluation_value->>'acceptance_policy_version'
            IS DISTINCT FROM registry.acceptance_version
       OR evaluation_value->>'acceptance_policy_sha256' IS DISTINCT FROM
            '6f07e20e4a24c09ee8ffa8813a1e3a90888e4765ddab14db86eae0f3c0bb424a'
       OR evaluation_value->>'atomic_expression' IS DISTINCT FROM
            'N_ROUTE_DISTINCT_BTC_PARENTS >= 5 AND (PROBABILITY OR ASYMMETRY)'
       OR evaluation_value->>'maximum_result'
            IS DISTINCT FROM 'EXPERIMENTAL_RESEARCH_ONLY'
       OR evaluation_value->'fresh_three_parent_route' IS DISTINCT FROM 'false'::jsonb
       OR NULLIF(evaluation_value->>'representative_rows_received','')::integer
            IS DISTINCT FROM selection_row.representative_count
       OR jsonb_typeof(evaluation_value->'excluded_representatives')
            IS DISTINCT FROM 'array'
       OR jsonb_typeof(evaluation_value->'registry_selection_receipt')
            IS DISTINCT FROM 'object'
       OR (SELECT array_agg(key ORDER BY key COLLATE "C")
           FROM jsonb_object_keys(evaluation_value->'registry_selection_receipt')
                AS keys(key)) IS DISTINCT FROM ARRAY[
            'attestation_sha256','attestation_structurally_valid',
            'cohort_query_sha256','freeze_id','frozen_at_utc',
            'persistence_verified_by_evaluator','population_receipt_sha256',
            'registration_evidence','registry_record_sha256',
            'registry_verification_receipt_sha256','representative_count',
            'representative_set_sha256','source_high_water_attempt_id',
            'verification_boundary'
       ]::text[]
       OR evaluation_value->'registry_selection_receipt'
            ->>'registration_evidence' IS DISTINCT FROM
            'CALLER_SUPPLIED_REGISTRY_REFERENCES_NOT_DB_VERIFIED'
       OR evaluation_value->'registry_selection_receipt'->>'freeze_id'
            IS DISTINCT FROM registry.freeze_id
       OR evaluation_value->'registry_selection_receipt'->>'frozen_at_utc'
            IS DISTINCT FROM research_stage8_utc_text_v1(registry.frozen_at_utc)
       OR evaluation_value->'registry_selection_receipt'
            ->>'registry_record_sha256' IS DISTINCT FROM registry.registry_record_sha256
       OR evaluation_value->'registry_selection_receipt'
            ->>'registry_verification_receipt_sha256' IS DISTINCT FROM
            selection_row.registry_verification_receipt_sha256
       OR evaluation_value->'registry_selection_receipt'->>'cohort_query_sha256'
            IS DISTINCT FROM selection_row.cohort_query_sha256
       OR evaluation_value->'registry_selection_receipt'->>'population_receipt_sha256'
            IS DISTINCT FROM selection_row.outcome_free_population_receipt_sha256
       OR NULLIF(evaluation_value->'registry_selection_receipt'
                    ->>'source_high_water_attempt_id','')::bigint
            IS DISTINCT FROM selection_row.source_high_water_attempt_id
       OR NULLIF(evaluation_value->'registry_selection_receipt'
                    ->>'representative_count','')::integer
            IS DISTINCT FROM selection_row.representative_count
       OR expected_receipt_representative_count
            IS DISTINCT FROM selection_row.representative_count
       OR evaluation_value->'registry_selection_receipt'
                    ->>'representative_set_sha256'
            IS DISTINCT FROM expected_receipt_representative_set_sha256
       OR evaluation_value->'registry_selection_receipt'
                    ->>'attestation_sha256'
            IS DISTINCT FROM expected_receipt_attestation_sha256
       OR evaluation_value->'registry_selection_receipt'
                    ->'attestation_structurally_valid' IS DISTINCT FROM 'true'::jsonb
       OR evaluation_value->'registry_selection_receipt'
                    ->'persistence_verified_by_evaluator' IS DISTINCT FROM 'true'::jsonb
       OR evaluation_value->'registry_selection_receipt'->>'verification_boundary'
            IS DISTINCT FROM
            'OUTCOME_ADAPTER_VERIFIED_DURABLE_SELECTION_AND_SAME_SNAPSHOT_OUTCOMES' THEN
        RAISE EXCEPTION 'Stage-8 evaluation policy or durable provenance is invalid';
    END IF;
    common_pass := NOT EXISTS (
        SELECT 1 FROM jsonb_array_elements(evidence->'representatives') AS item
        WHERE item->'fact'->>'validation_status' IS DISTINCT FROM 'VALID'
    );
    IF evaluation_value->'common'->'passed' IS DISTINCT FROM to_jsonb(common_pass)
       OR evaluation_value->'common'->'selection_provenance_complete'
            IS DISTINCT FROM 'true'::jsonb
       OR evaluation_value->'common'->'duplicate_btc_parent_movement_ids'
            IS DISTINCT FROM '[]'::jsonb
       OR evaluation_value->'common'->'blockers' IS DISTINCT FROM (CASE
            WHEN common_pass THEN '[]'::jsonb
            ELSE '["SHARED_REPRESENTATIVE_PROVENANCE_INVALID"]'::jsonb END) THEN
        RAISE EXCEPTION 'Stage-8 common gate differs from durable provenance';
    END IF;

    -- Recompute the probability route solely from VALID durable evidence.
    SELECT COALESCE(jsonb_agg(item->'btc_parent_movement_id'
                              ORDER BY item->>'btc_parent_movement_id' COLLATE "C"),
                    '[]'::jsonb),
           count(*)::integer,
           count(*) FILTER (
               WHERE item->'probability'->>'reported_status' = 'SUCCESS'
           )::integer
    INTO probability_parent_ids, probability_count, probability_successes
    FROM jsonb_array_elements(evidence->'representatives') AS item
    WHERE item->'fact'->>'validation_status' = 'VALID'
      AND item->'probability'->>'validation_status' = 'VALID';
    probability_failures := probability_count - probability_successes;
    IF probability_count = 0 THEN
        probability_hit_rate := NULL;
        probability_wilson := NULL;
    ELSE
        probability_hit_rate := 100.0 * probability_successes / probability_count;
        probability_wilson := 100.0 * greatest(0.0,
            ((probability_successes::double precision / probability_count)
             + (1.959963984540054 * 1.959963984540054)
                / (2.0 * probability_count)
             - 1.959963984540054 * sqrt(
                (((probability_successes::double precision / probability_count)
                    * (1.0 - probability_successes::double precision
                                  / probability_count))
                 + (1.959963984540054 * 1.959963984540054)
                    / (4.0 * probability_count)) / probability_count
             ))
            / (1.0 + (1.959963984540054 * 1.959963984540054)
                        / probability_count));
    END IF;
    probability_pass := probability_count >= 5
        AND probability_hit_rate >= 70.0
        AND probability_wilson >= 40.0;
    probability_status := CASE
        WHEN probability_pass THEN 'PASS'
        WHEN probability_count = 0 THEN 'UNAVAILABLE'
        WHEN probability_count < 5 THEN 'INSUFFICIENT'
        ELSE 'FAIL'
    END;
    IF evaluation_value->'routes'->'PROBABILITY'->'btc_parent_movement_ids'
            IS DISTINCT FROM probability_parent_ids
       OR NULLIF(evaluation_value->'routes'->'PROBABILITY'
                    ->>'distinct_parent_count','')::integer
            IS DISTINCT FROM probability_count
       OR NULLIF(evaluation_value->'routes'->'PROBABILITY'
                    ->>'successes','')::integer
            IS DISTINCT FROM probability_successes
       OR NULLIF(evaluation_value->'routes'->'PROBABILITY'
                    ->>'failures','')::integer
            IS DISTINCT FROM probability_failures
       OR (CASE WHEN probability_hit_rate IS NULL THEN
                evaluation_value->'routes'->'PROBABILITY'->'hit_rate_pct'
                    IS DISTINCT FROM 'null'::jsonb
               ELSE jsonb_typeof(evaluation_value->'routes'->'PROBABILITY'
                                    ->'hit_rate_pct') IS DISTINCT FROM 'number'
                 OR abs((evaluation_value->'routes'->'PROBABILITY'
                            ->>'hit_rate_pct')::double precision
                        - probability_hit_rate)
                    > 1e-10 * greatest(1.0, abs(probability_hit_rate)) END)
       OR (CASE WHEN probability_wilson IS NULL THEN
                evaluation_value->'routes'->'PROBABILITY'->'wilson_95_lower_pct'
                    IS DISTINCT FROM 'null'::jsonb
               ELSE jsonb_typeof(evaluation_value->'routes'->'PROBABILITY'
                                    ->'wilson_95_lower_pct') IS DISTINCT FROM 'number'
                 OR abs((evaluation_value->'routes'->'PROBABILITY'
                            ->>'wilson_95_lower_pct')::double precision
                        - probability_wilson)
                    > 1e-10 * greatest(1.0, abs(probability_wilson)) END)
       OR evaluation_value->'routes'->'PROBABILITY'->'checks'
                ->'minimum_distinct_parents'
            IS DISTINCT FROM to_jsonb(probability_count >= 5)
       OR evaluation_value->'routes'->'PROBABILITY'->'checks'
                ->'hit_rate_pct_gte_70'
            IS DISTINCT FROM to_jsonb(
                probability_hit_rate IS NOT NULL AND probability_hit_rate >= 70.0)
       OR evaluation_value->'routes'->'PROBABILITY'->'checks'
                ->'wilson_95_lower_pct_gte_40'
            IS DISTINCT FROM to_jsonb(
                probability_wilson IS NOT NULL AND probability_wilson >= 40.0)
       OR evaluation_value->'routes'->'PROBABILITY'->>'status'
            IS DISTINCT FROM probability_status
       OR evaluation_value->'routes'->'PROBABILITY'->'passed'
            IS DISTINCT FROM to_jsonb(probability_pass) THEN
        RAISE EXCEPTION 'Stage-8 probability route differs from durable evidence';
    END IF;

    -- Recompute all common-window aggregates from the same exact evidence
    -- population.  Route identities cannot be borrowed across the OR gate.
    SELECT COALESCE(jsonb_agg(item->'btc_parent_movement_id'
                              ORDER BY item->>'btc_parent_movement_id' COLLATE "C"),
                    '[]'::jsonb),
           count(*)::integer,
           sum((item->'asymmetry'->>'mfe_pct')::double precision
               ORDER BY item->>'btc_parent_movement_id' COLLATE "C"),
           sum((item->'asymmetry'->>'mae_pct')::double precision
               ORDER BY item->>'btc_parent_movement_id' COLLATE "C"),
           100.0 * count(*) FILTER (
               WHERE (item->'asymmetry'->>'mfe_pct')::double precision
                   > (item->'asymmetry'->>'mae_pct')::double precision
           ) / NULLIF(count(*), 0),
           percentile_cont(0.5) WITHIN GROUP (ORDER BY
               (item->'asymmetry'->>'mfe_pct')::double precision
               - (item->'asymmetry'->>'mae_pct')::double precision)
    INTO asymmetry_parent_ids, asymmetry_count, asymmetry_sum_mfe,
         asymmetry_sum_mae, asymmetry_dominance, asymmetry_median_edge
    FROM jsonb_array_elements(evidence->'representatives') AS item
    WHERE item->'fact'->>'validation_status' = 'VALID'
      AND item->'asymmetry'->>'validation_status' = 'VALID';
    asymmetry_ratio := CASE
        WHEN asymmetry_count > 0 AND asymmetry_sum_mae > 0.0
            THEN asymmetry_sum_mfe / asymmetry_sum_mae
        ELSE NULL
    END;
    asymmetry_pass := COALESCE(asymmetry_count >= 5
        AND asymmetry_ratio >= 1.5
        AND asymmetry_dominance >= 60.0
        AND asymmetry_median_edge > 0.0, FALSE);
    asymmetry_status := CASE
        WHEN asymmetry_pass THEN 'PASS'
        WHEN asymmetry_count = 0 OR asymmetry_ratio IS NULL THEN 'UNAVAILABLE'
        WHEN asymmetry_count < 5 THEN 'INSUFFICIENT'
        ELSE 'FAIL'
    END;
    asymmetry_state := CASE
        WHEN asymmetry_ratio IS NOT NULL THEN 'FINITE'
        WHEN asymmetry_count > 0 AND asymmetry_sum_mae = 0.0
            THEN 'ZERO_DENOMINATOR'
        ELSE 'DATA_MISSING'
    END;
    IF evaluation_value->'routes'->'ASYMMETRY'->'btc_parent_movement_ids'
            IS DISTINCT FROM asymmetry_parent_ids
       OR NULLIF(evaluation_value->'routes'->'ASYMMETRY'
                    ->>'distinct_parent_count','')::integer
            IS DISTINCT FROM asymmetry_count
       OR (CASE WHEN asymmetry_sum_mfe IS NULL THEN
                evaluation_value->'routes'->'ASYMMETRY'->'sum_mfe_pct'
                    IS DISTINCT FROM 'null'::jsonb
               ELSE jsonb_typeof(evaluation_value->'routes'->'ASYMMETRY'
                                    ->'sum_mfe_pct') IS DISTINCT FROM 'number'
                 OR abs((evaluation_value->'routes'->'ASYMMETRY'
                            ->>'sum_mfe_pct')::double precision
                        - asymmetry_sum_mfe)
                    > 1e-10 * greatest(1.0, abs(asymmetry_sum_mfe)) END)
       OR (CASE WHEN asymmetry_sum_mae IS NULL THEN
                evaluation_value->'routes'->'ASYMMETRY'->'sum_mae_pct'
                    IS DISTINCT FROM 'null'::jsonb
               ELSE jsonb_typeof(evaluation_value->'routes'->'ASYMMETRY'
                                    ->'sum_mae_pct') IS DISTINCT FROM 'number'
                 OR abs((evaluation_value->'routes'->'ASYMMETRY'
                            ->>'sum_mae_pct')::double precision
                        - asymmetry_sum_mae)
                    > 1e-10 * greatest(1.0, abs(asymmetry_sum_mae)) END)
       OR (CASE WHEN asymmetry_ratio IS NULL THEN
                evaluation_value->'routes'->'ASYMMETRY'
                    ->'common_window_asymmetry_ratio' IS DISTINCT FROM 'null'::jsonb
               ELSE jsonb_typeof(evaluation_value->'routes'->'ASYMMETRY'
                    ->'common_window_asymmetry_ratio') IS DISTINCT FROM 'number'
                 OR abs((evaluation_value->'routes'->'ASYMMETRY'
                            ->>'common_window_asymmetry_ratio')::double precision
                        - asymmetry_ratio)
                    > 1e-10 * greatest(1.0, abs(asymmetry_ratio)) END)
       OR evaluation_value->'routes'->'ASYMMETRY'
                ->>'common_window_asymmetry_state'
            IS DISTINCT FROM asymmetry_state
       OR (CASE WHEN asymmetry_dominance IS NULL THEN
                evaluation_value->'routes'->'ASYMMETRY'
                    ->'common_window_favorable_dominance_pct'
                    IS DISTINCT FROM 'null'::jsonb
               ELSE jsonb_typeof(evaluation_value->'routes'->'ASYMMETRY'
                    ->'common_window_favorable_dominance_pct')
                    IS DISTINCT FROM 'number'
                 OR abs((evaluation_value->'routes'->'ASYMMETRY'
                            ->>'common_window_favorable_dominance_pct')
                            ::double precision - asymmetry_dominance)
                    > 1e-10 * greatest(1.0, abs(asymmetry_dominance)) END)
       OR (CASE WHEN asymmetry_median_edge IS NULL THEN
                evaluation_value->'routes'->'ASYMMETRY'
                    ->'common_window_median_paired_edge_pct'
                    IS DISTINCT FROM 'null'::jsonb
               ELSE jsonb_typeof(evaluation_value->'routes'->'ASYMMETRY'
                    ->'common_window_median_paired_edge_pct')
                    IS DISTINCT FROM 'number'
                 OR abs((evaluation_value->'routes'->'ASYMMETRY'
                            ->>'common_window_median_paired_edge_pct')
                            ::double precision - asymmetry_median_edge)
                    > 1e-10 * greatest(1.0, abs(asymmetry_median_edge)) END)
       OR evaluation_value->'routes'->'ASYMMETRY'->'checks'
                ->'minimum_distinct_parents'
            IS DISTINCT FROM to_jsonb(asymmetry_count >= 5)
       OR evaluation_value->'routes'->'ASYMMETRY'->'checks'
                ->'common_window_asymmetry_ratio_gte_1_5'
            IS DISTINCT FROM to_jsonb(
                asymmetry_ratio IS NOT NULL AND asymmetry_ratio >= 1.5)
       OR evaluation_value->'routes'->'ASYMMETRY'->'checks'
                ->'common_window_favorable_dominance_pct_gte_60'
            IS DISTINCT FROM to_jsonb(
                asymmetry_dominance IS NOT NULL AND asymmetry_dominance >= 60.0)
       OR evaluation_value->'routes'->'ASYMMETRY'->'checks'
                ->'common_window_median_paired_edge_pct_gt_0'
            IS DISTINCT FROM to_jsonb(
                asymmetry_median_edge IS NOT NULL AND asymmetry_median_edge > 0.0)
       OR evaluation_value->'routes'->'ASYMMETRY'->>'status'
            IS DISTINCT FROM asymmetry_status
       OR evaluation_value->'routes'->'ASYMMETRY'->'passed'
            IS DISTINCT FROM to_jsonb(asymmetry_pass) THEN
        RAISE EXCEPTION 'Stage-8 asymmetry route differs from durable evidence';
    END IF;
    computed_atomic := common_pass AND (probability_pass OR asymmetry_pass);
    SELECT COALESCE(bool_and(database_clock >=
        (identity->>'decision_time_utc')::timestamptz
            + make_interval(mins => registry.window_minutes)), FALSE)
    INTO representative_horizons_elapsed
    FROM jsonb_array_elements(selection_row.representative_identities) AS selected(identity);
    -- The caller replay remains diagnostic.  Qualification authority comes
    -- exclusively from the server rerunning the causal projection for every
    -- sealed attempt and comparing it to the trigger-owned fact attestation.
    computed_qualified := computed_atomic
        AND server_replay_verified
        AND selection_row.representative_count >= 5
        AND representative_horizons_elapsed;
    IF evaluation_value->'atomic_gate_passed'
            IS DISTINCT FROM to_jsonb(computed_atomic)
       OR evaluation_value->'structurally_eligible'
            IS DISTINCT FROM to_jsonb(computed_atomic)
       OR evaluation_value->'research_qualified' IS DISTINCT FROM 'false'::jsonb
       OR evaluation_value->>'status' = 'RESEARCH_QUALIFIED_EXPERIMENTAL_ONLY' THEN
        RAISE EXCEPTION 'Stage-8 caller evaluation must remain diagnostic';
    END IF;
    IF jsonb_typeof(evaluation_value->'research_qualified') IS DISTINCT FROM 'boolean'
       OR jsonb_typeof(evaluation_value->'atomic_gate_passed') IS DISTINCT FROM 'boolean'
       OR jsonb_typeof(evaluation_value->'qualification_blockers') IS DISTINCT FROM 'array'
       OR jsonb_array_length(evaluation_value->'qualification_blockers') = 0 THEN
        RAISE EXCEPTION 'Stage-8 evaluation qualification state is contradictory';
    END IF;
    caller_evaluation_sha256 := NEW.persistence_payload->>'evaluation_sha256';
    SELECT COALESCE(jsonb_agg(value ORDER BY (value #>> '{}') COLLATE "C"), '[]'::jsonb)
    INTO server_blockers
    FROM jsonb_array_elements(evaluation_value->'qualification_blockers') AS item(value)
    WHERE value #>> '{}' NOT IN (
        'SERVER_DB_REPLAY_ATTESTATION_REQUIRED',
        'AUTHORITATIVE_FACT_SOURCE_REPLAY_NOT_VERIFIED'
    );
    IF computed_qualified THEN
        server_blockers := '[]'::jsonb;
    ELSIF NOT server_replay_verified
          AND NOT (server_blockers ? 'SERVER_DB_REPLAY_ATTESTATION_REQUIRED') THEN
        server_blockers := server_blockers
            || '["SERVER_DB_REPLAY_ATTESTATION_REQUIRED"]'::jsonb;
    ELSIF jsonb_array_length(server_blockers) = 0 THEN
        server_blockers := '["BELOW_ACCEPTANCE_GATE"]'::jsonb;
    END IF;
    IF NOT representative_horizons_elapsed THEN
        server_blockers := server_blockers
            || '["REPRESENTATIVE_OUTCOME_HORIZON_NOT_ELAPSED"]'::jsonb;
    END IF;
    -- The stored evaluation is minted by this trigger.  The signed caller
    -- payload remains immutable input evidence, but it can never claim DB
    -- replay authority or qualification before persistence/readback.
    evaluation_value := evaluation_value || jsonb_build_object(
        'authoritative_fact_replay_verified', server_replay_verified,
        'durable_fact_source_authority_verified', server_replay_verified,
        'durable_outcome_atomic_gate_evidence_verified', computed_atomic,
        'research_qualified', computed_qualified,
        'qualification_blockers', server_blockers,
        'status', CASE WHEN computed_qualified
            THEN 'RESEARCH_QUALIFIED_EXPERIMENTAL_ONLY'
            ELSE evaluation_value->>'status' END
    );
    NEW.outcome_adapter_version := NEW.persistence_payload->>'outcome_adapter_version';
    NEW.observed_outcome_adapter_source_sha256 :=
        NEW.persistence_payload->>'outcome_adapter_source_sha256';
    NEW.outcome_source_manifest := NEW.persistence_payload->'outcome_source_manifest';
    NEW.outcome_source_manifest_sha256 :=
        NEW.persistence_payload->>'outcome_source_manifest_sha256';
    NEW.transaction_identity_sha256 :=
        NEW.persistence_payload->>'transaction_identity_sha256';
    NEW.fact_replay_receipt := replay;
    NEW.fact_replay_receipt_sha256 := replay->>'fact_replay_receipt_sha256';
    NEW.evidence_receipt := evidence;
    NEW.evidence_receipt_sha256 := evidence->>'evidence_receipt_sha256';
    NEW.evaluation := evaluation_value;
    NEW.evaluation_sha256 := research_stage8_json_sha256_v1(evaluation_value);
    NEW.persistence_payload_sha256 :=
        NEW.persistence_payload->>'persistence_payload_sha256';
    NEW.server_replay_attestation := server_replay_attestation;
    NEW.server_replay_attestation_sha256 :=
        research_stage8_json_sha256_v1(server_replay_attestation);
    NEW.server_replay_verified := server_replay_verified;
    NEW.atomic_gate_passed := computed_atomic;
    NEW.research_qualified := computed_qualified;
    NEW.result_scope := 'EXPERIMENTAL_RESEARCH_ONLY';
    NEW.live_authorized := FALSE;
    NEW.telegram_authorized := FALSE;
    NEW.trade_authorized := FALSE;
    NEW.persisted_at_utc := clock_timestamp();
    NEW.persisted_by := current_user;
    persisted_text := research_stage8_utc_text_v1(NEW.persisted_at_utc);
    NEW.evaluation_record := jsonb_build_object(
        'version', 'stage8-durable-evaluation-record-v1',
        'exact_binding_sha256', NEW.exact_binding_sha256,
        'selection_record_sha256', NEW.selection_record_sha256,
        'outcome_adapter_version', NEW.outcome_adapter_version,
        'observed_outcome_adapter_source_sha256',
            NEW.observed_outcome_adapter_source_sha256,
        'outcome_source_manifest_sha256', NEW.outcome_source_manifest_sha256,
        'transaction_identity_sha256', NEW.transaction_identity_sha256,
        'fact_replay_receipt_sha256', NEW.fact_replay_receipt_sha256,
        'evidence_receipt_sha256', NEW.evidence_receipt_sha256,
        'caller_evaluation_sha256', caller_evaluation_sha256,
        'evaluation_sha256', NEW.evaluation_sha256,
        'persistence_payload_sha256', NEW.persistence_payload_sha256,
        'server_replay_attestation_sha256',
            NEW.server_replay_attestation_sha256,
        'server_replay_verified', NEW.server_replay_verified,
        'atomic_gate_passed', NEW.atomic_gate_passed,
        'research_qualified', NEW.research_qualified,
        'result_scope', NEW.result_scope,
        'live_authorized', NEW.live_authorized,
        'telegram_authorized', NEW.telegram_authorized,
        'trade_authorized', NEW.trade_authorized,
        'persisted_at_utc', persisted_text,
        'persisted_by', NEW.persisted_by
    );
    NEW.evaluation_record_sha256 := research_stage8_json_sha256_v1(NEW.evaluation_record);
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION research_stage8_append_only_v1()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_catalog.set_config(
        'search_path',
        pg_catalog.quote_ident(TG_TABLE_SCHEMA) || ',pg_catalog,pg_temp',
        true
    );
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME;
END;
$$;

-- A fact batch is not a durable object until its exact fact population is
-- sealed.  Deferring this check to commit keeps the batch, every fact, and the
-- seal in the same actual REPEATABLE READ transaction.  In particular, a
-- preallocated-but-uncommitted Watch snapshot ID cannot appear between the
-- server-owned archive cutoff and later fact inserts in another transaction.
CREATE OR REPLACE FUNCTION research_stage8_fact_batch_complete_at_commit_v1()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_catalog.set_config(
        'search_path',
        pg_catalog.quote_ident(TG_TABLE_SCHEMA) || ',pg_catalog,pg_temp',
        true
    );
    IF NOT EXISTS (
        SELECT 1
        FROM research_stage8_projection_fact_batch_seals AS seal
        WHERE seal.fact_batch_record_sha256 = NEW.fact_batch_record_sha256
    ) THEN
        RAISE EXCEPTION
            'Stage-8 fact batch must be completed and sealed in its insert transaction';
    END IF;
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS trg_stage8_registry_insert_guard ON research_stage8_binding_registry;
CREATE TRIGGER trg_stage8_registry_insert_guard
BEFORE INSERT ON research_stage8_binding_registry
FOR EACH ROW EXECUTE FUNCTION research_stage8_registry_insert_guard_v1();
DROP TRIGGER IF EXISTS trg_stage8_registry_append_only ON research_stage8_binding_registry;
CREATE TRIGGER trg_stage8_registry_append_only
BEFORE UPDATE OR DELETE ON research_stage8_binding_registry
FOR EACH ROW EXECUTE FUNCTION research_stage8_append_only_v1();

DROP TRIGGER IF EXISTS trg_stage8_fact_batch_insert_guard ON research_stage8_projection_fact_batches;
CREATE TRIGGER trg_stage8_fact_batch_insert_guard
BEFORE INSERT ON research_stage8_projection_fact_batches
FOR EACH ROW EXECUTE FUNCTION research_stage8_fact_batch_insert_guard_v1();
DROP TRIGGER IF EXISTS trg_stage8_fact_batch_complete_at_commit
    ON research_stage8_projection_fact_batches;
CREATE CONSTRAINT TRIGGER trg_stage8_fact_batch_complete_at_commit
AFTER INSERT ON research_stage8_projection_fact_batches
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION research_stage8_fact_batch_complete_at_commit_v1();
DROP TRIGGER IF EXISTS trg_stage8_fact_batch_append_only ON research_stage8_projection_fact_batches;
CREATE TRIGGER trg_stage8_fact_batch_append_only
BEFORE UPDATE OR DELETE ON research_stage8_projection_fact_batches
FOR EACH ROW EXECUTE FUNCTION research_stage8_append_only_v1();

DROP TRIGGER IF EXISTS trg_stage8_fact_insert_guard ON research_stage8_projected_fact_ledger;
CREATE TRIGGER trg_stage8_fact_insert_guard
BEFORE INSERT ON research_stage8_projected_fact_ledger
FOR EACH ROW EXECUTE FUNCTION research_stage8_fact_insert_guard_v1();
DROP TRIGGER IF EXISTS trg_stage8_fact_append_only ON research_stage8_projected_fact_ledger;
CREATE TRIGGER trg_stage8_fact_append_only
BEFORE UPDATE OR DELETE ON research_stage8_projected_fact_ledger
FOR EACH ROW EXECUTE FUNCTION research_stage8_append_only_v1();

DROP TRIGGER IF EXISTS trg_stage8_fact_seal_insert_guard ON research_stage8_projection_fact_batch_seals;
CREATE TRIGGER trg_stage8_fact_seal_insert_guard
BEFORE INSERT ON research_stage8_projection_fact_batch_seals
FOR EACH ROW EXECUTE FUNCTION research_stage8_fact_seal_insert_guard_v1();
DROP TRIGGER IF EXISTS trg_stage8_fact_seal_append_only ON research_stage8_projection_fact_batch_seals;
CREATE TRIGGER trg_stage8_fact_seal_append_only
BEFORE UPDATE OR DELETE ON research_stage8_projection_fact_batch_seals
FOR EACH ROW EXECUTE FUNCTION research_stage8_append_only_v1();

DROP TRIGGER IF EXISTS trg_stage8_selection_insert_guard ON research_stage8_selection_receipts;
CREATE TRIGGER trg_stage8_selection_insert_guard
BEFORE INSERT ON research_stage8_selection_receipts
FOR EACH ROW EXECUTE FUNCTION research_stage8_selection_insert_guard_v1();
DROP TRIGGER IF EXISTS trg_stage8_selection_append_only ON research_stage8_selection_receipts;
CREATE TRIGGER trg_stage8_selection_append_only
BEFORE UPDATE OR DELETE ON research_stage8_selection_receipts
FOR EACH ROW EXECUTE FUNCTION research_stage8_append_only_v1();

DROP TRIGGER IF EXISTS trg_stage8_evaluation_insert_guard ON research_stage8_evaluation_receipts;
CREATE TRIGGER trg_stage8_evaluation_insert_guard
BEFORE INSERT ON research_stage8_evaluation_receipts
FOR EACH ROW EXECUTE FUNCTION research_stage8_evaluation_insert_guard_v1();
DROP TRIGGER IF EXISTS trg_stage8_evaluation_append_only ON research_stage8_evaluation_receipts;
CREATE TRIGGER trg_stage8_evaluation_append_only
BEFORE UPDATE OR DELETE ON research_stage8_evaluation_receipts
FOR EACH ROW EXECUTE FUNCTION research_stage8_append_only_v1();

CREATE OR REPLACE VIEW research_stage8_registry_read_v1
WITH (security_barrier = true)
AS SELECT exact_binding, exact_binding_sha256, manifest_sha256, contract_version,
    hash_version, source_version, source_audit_version, projection_version,
    candidate_version, label_version, independence_version, acceptance_version,
    parent_policy_version, scope_id, candidate_id, window_minutes, threshold_bps,
    implementation_artifacts, implementation_artifacts_sha256,
    expected_watch_code_manifest, expected_watch_code_manifest_sha256,
    verifier_profile, verifier_profile_sha256, frozen_at_utc, freeze_id,
    registry_record, registry_record_sha256
FROM research_stage8_binding_registry;

CREATE OR REPLACE VIEW research_stage8_fact_batch_read_v1
WITH (security_barrier = true)
AS SELECT fact_batch_record_sha256, exact_binding_sha256, freeze_id,
    registry_record_sha256, verifier_profile_sha256,
    registry_verification_receipt_sha256, projection_adapter_version,
    observed_projection_source_sha256,
    observed_projection_adapter_source_sha256,
    observed_registry_adapter_source_sha256,
    observed_registry_migration_sha256,
    projection_source_manifest, projection_source_manifest_sha256,
    adapter_query_binding_sha256, adapter_population_receipt,
    adapter_population_receipt_sha256, adapter_authority_receipt,
    adapter_authority_receipt_sha256, adapter_result_sha256,
    coverage_query_scope, coverage_query_sha256,
    outcome_free_population_receipt_sha256,
    coverage_attempt_population_sha256,
    coverage_source_high_water_attempt_id,
    watch_archive_high_water_snapshot_set_id, attempt_ids, attempt_count,
    persisted_at_utc, fact_batch_record
FROM research_stage8_projection_fact_batches;

CREATE OR REPLACE VIEW research_stage8_fact_read_v1
WITH (security_barrier = true)
AS SELECT fact_record_sha256, fact_batch_record_sha256,
    exact_binding_sha256, attempt_id, attempt_fingerprint, anchor_slot_id,
    event_id, event_fingerprint, symbol, direction, decision_time_utc,
    knowledge_status, candidate_match, fact, fact_sha256,
    fact_authority, fact_authority_sha256,
    watch_selection_attestation, watch_selection_attestation_sha256,
    observed_watch_code_manifest_sha256, parent_membership_evidence,
    parent_membership_evidence_sha256, noneligibility_proof,
    noneligibility_proof_sha256, selection_fact_identity,
    selection_fact_identity_sha256, persisted_at_utc, fact_record
FROM research_stage8_projected_fact_ledger;

CREATE OR REPLACE VIEW research_stage8_fact_seal_read_v1
WITH (security_barrier = true)
AS SELECT fact_batch_record_sha256, fact_count, fact_records_sha256,
    sealed_at_utc, seal_record, seal_record_sha256
FROM research_stage8_projection_fact_batch_seals;

CREATE OR REPLACE VIEW research_stage8_selection_read_v1
WITH (security_barrier = true)
AS SELECT selection_record_sha256, fact_batch_record_sha256,
    exact_binding_sha256, freeze_id,
    registry_record_sha256, verifier_profile_sha256,
    registry_verification_receipt_sha256, selector_version,
    observed_projection_source_sha256, observed_selector_source_sha256,
    observed_watch_code_manifest_sha256, cohort_query_sha256,
    outcome_free_population_receipt_sha256, source_high_water_attempt_id,
    representative_count, representative_set_sha256,
    representative_identities, representative_identities_sha256,
    selection_attestation, selection_attestation_sha256,
    persisted_at_utc, selection_record
FROM research_stage8_selection_receipts;

CREATE OR REPLACE VIEW research_stage8_evaluation_read_v1
WITH (security_barrier = true)
AS SELECT evaluation_record_sha256, exact_binding_sha256,
    selection_record_sha256, outcome_adapter_version,
    observed_outcome_adapter_source_sha256,
    outcome_source_manifest, outcome_source_manifest_sha256,
    transaction_identity_sha256, fact_replay_receipt,
    fact_replay_receipt_sha256, evidence_receipt,
    evidence_receipt_sha256, evaluation, evaluation_sha256,
    persistence_payload, persistence_payload_sha256,
    server_replay_attestation, server_replay_attestation_sha256,
    server_replay_verified, atomic_gate_passed, research_qualified, result_scope,
    live_authorized, telegram_authorized, trade_authorized,
    persisted_at_utc, evaluation_record
FROM research_stage8_evaluation_receipts;

REVOKE ALL ON research_stage8_binding_registry FROM PUBLIC;
REVOKE ALL ON research_stage8_projection_fact_batches FROM PUBLIC;
REVOKE ALL ON research_stage8_projected_fact_ledger FROM PUBLIC;
REVOKE ALL ON research_stage8_projection_fact_batch_seals FROM PUBLIC;
REVOKE ALL ON research_stage8_selection_receipts FROM PUBLIC;
REVOKE ALL ON research_stage8_evaluation_receipts FROM PUBLIC;
REVOKE ALL ON research_stage8_registry_read_v1 FROM PUBLIC;
REVOKE ALL ON research_stage8_fact_batch_read_v1 FROM PUBLIC;
REVOKE ALL ON research_stage8_fact_read_v1 FROM PUBLIC;
REVOKE ALL ON research_stage8_fact_seal_read_v1 FROM PUBLIC;
REVOKE ALL ON research_stage8_selection_read_v1 FROM PUBLIC;
REVOKE ALL ON research_stage8_evaluation_read_v1 FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION research_stage8_canonical_json_v1(JSONB) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION research_stage8_json_sha256_v1(JSONB) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION research_stage8_watch_canonical_json_v1(JSONB) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION research_stage8_watch_json_sha256_v1(JSONB) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION research_stage8_watch_round2_v1(DOUBLE PRECISION) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION research_stage8_watch_timestamp_v1(JSONB) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION research_stage8_watch_finite_number_v1(JSONB) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION research_stage8_watch_components_match_v1(JSONB, JSONB) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION research_stage8_watch_capture_errors_v1(
    JSONB, TEXT, JSONB, TEXT, TIMESTAMPTZ, TIMESTAMPTZ, TIMESTAMPTZ) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION research_stage8_has_forbidden_evidence_key_v1(JSONB)
    FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION research_stage8_utc_text_v1(TIMESTAMPTZ) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION
    research_stage8_anchor_canonical_json_v1(JSONB),
    research_stage8_anchor_strip_v1(TEXT),
    research_stage8_anchor_truthy_v1(JSONB),
    research_stage8_anchor_nonempty_v1(JSONB),
    research_stage8_anchor_reference_canonical_json_v1(JSONB),
    research_stage8_anchor_json_sha256_v1(JSONB),
    research_stage8_anchor_timestamp_v1(JSONB),
    research_stage8_anchor_aware_timestamp_v1(JSONB),
    research_stage8_anchor_finite_number_v1(JSONB),
    research_stage8_anchor_number_v1(JSONB),
    research_stage8_anchor_round6_v1(DOUBLE PRECISION),
    research_stage8_anchor_coverage_valid_v1(JSONB, TEXT, TIMESTAMPTZ),
    research_stage8_anchor_sources_valid_v1(JSONB, JSONB, JSONB, TEXT, TIMESTAMPTZ, TIMESTAMPTZ, TIMESTAMPTZ, TIMESTAMPTZ),
    research_stage8_anchor_session_ratios_v1(TIMESTAMPTZ, TIMESTAMPTZ),
    research_stage8_anchor_width_error_v1(JSONB, TEXT, TIMESTAMPTZ, INTEGER),
    research_stage8_anchor_width_valid_v1(JSONB, TEXT, TIMESTAMPTZ, INTEGER),
    research_stage8_anchor_feature_name_valid_v1(TEXT),
    research_stage8_anchor_forbidden_bundle_key_v1(JSONB),
    research_stage8_anchor_series_valid_v1(JSONB, TIMESTAMPTZ),
    research_stage8_anchor_bundle_valid_v1(JSONB, TEXT, TIMESTAMPTZ, TEXT),
    research_stage8_anchor_errors_v1(JSONB, JSONB, JSONB)
    FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION prevent_prospective_anchor_event_mutation() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION research_stage8_expected_scope_v1(TEXT) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION research_stage8_expected_candidate_v1(TEXT) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION research_stage8_derive_projection_attestation_v1(
    TEXT, TEXT, BIGINT, BIGINT, BOOLEAN) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION research_stage8_registry_insert_guard_v1() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION research_stage8_fact_batch_insert_guard_v1() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION research_stage8_fact_insert_guard_v1() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION research_stage8_fact_seal_insert_guard_v1() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION research_stage8_selection_insert_guard_v1() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION research_stage8_evaluation_insert_guard_v1() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION research_stage8_append_only_v1() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION research_stage8_fact_batch_complete_at_commit_v1()
    FROM PUBLIC;

DO $$
DECLARE
    target_schema TEXT := current_schema();
    target_role TEXT;
BEGIN
    -- Reapply converges stale local grants from earlier revisions to this
    -- migration's exact allow-list.  These five roles are Stage-8-exclusive.
    FOREACH target_role IN ARRAY ARRAY[
        'research_stage8_registrar_v1',
        'research_stage8_fact_writer_v1',
        'research_stage8_selector_writer_v1',
        'research_stage8_evaluator_writer_v1',
        'research_stage8_reader_v1'
    ] LOOP
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = target_role) THEN
            EXECUTE format(
                'REVOKE ALL PRIVILEGES ON SCHEMA %I FROM %I',
                target_schema, target_role
            );
            EXECUTE format(
                'REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA %I FROM %I',
                target_schema, target_role
            );
            EXECUTE format(
                'REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA %I FROM %I',
                target_schema, target_role
            );
            EXECUTE format(
                'REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA %I FROM %I',
                target_schema, target_role
            );
        END IF;
    END LOOP;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'research_stage8_registrar_v1') THEN
        EXECUTE format('GRANT USAGE ON SCHEMA %I TO research_stage8_registrar_v1', target_schema);
        GRANT INSERT ON research_stage8_binding_registry TO research_stage8_registrar_v1;
        -- INSERT .. ON CONFLICT DO NOTHING needs read access to its arbiter
        -- column; do not expose the rest of the base row to the registrar.
        GRANT SELECT (exact_binding_sha256)
            ON research_stage8_binding_registry TO research_stage8_registrar_v1;
        GRANT SELECT ON research_stage8_registry_read_v1 TO research_stage8_registrar_v1;
        GRANT EXECUTE ON FUNCTION research_stage8_canonical_json_v1(JSONB),
            research_stage8_json_sha256_v1(JSONB),
            research_stage8_utc_text_v1(TIMESTAMPTZ),
            research_stage8_expected_scope_v1(TEXT),
            research_stage8_expected_candidate_v1(TEXT)
            TO research_stage8_registrar_v1;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'research_stage8_fact_writer_v1') THEN
        EXECUTE format('GRANT USAGE ON SCHEMA %I TO research_stage8_fact_writer_v1', target_schema);
        GRANT INSERT ON research_stage8_projection_fact_batches,
            research_stage8_projected_fact_ledger,
            research_stage8_projection_fact_batch_seals
            TO research_stage8_fact_writer_v1;
        -- Trigger guards are SECURITY INVOKER.  These base reads are the exact
        -- durable rows already exposed through the corresponding safe views;
        -- only the two source identity columns are granted from the anchor table.
        GRANT SELECT ON research_stage8_binding_registry,
            research_stage8_projection_fact_batches,
            research_stage8_projected_fact_ledger,
            research_stage8_projection_fact_batch_seals
            TO research_stage8_fact_writer_v1;
        -- The projection authority helper materializes whole source rows into
        -- typed PL/pgSQL records, so PostgreSQL requires relation SELECT (not
        -- merely a subset of columns).  This role still receives no outcome
        -- tables and no DML on any source relation.
        GRANT SELECT ON research_prospective_anchor_attempts,
            research_prospective_anchor_slots, research_events,
            research_max_pain_snapshot_sets,
            research_btc_parent_movements, research_btc_price_bars
            TO research_stage8_fact_writer_v1;
        GRANT SELECT ON research_stage8_registry_read_v1,
            research_stage8_fact_batch_read_v1,
            research_stage8_fact_read_v1,
            research_stage8_fact_seal_read_v1
            TO research_stage8_fact_writer_v1;
        GRANT EXECUTE ON FUNCTION research_stage8_canonical_json_v1(JSONB),
            research_stage8_json_sha256_v1(JSONB),
            research_stage8_has_forbidden_evidence_key_v1(JSONB),
            research_stage8_utc_text_v1(TIMESTAMPTZ),
            research_stage8_derive_projection_attestation_v1(
                TEXT, TEXT, BIGINT, BIGINT, BOOLEAN)
            TO research_stage8_fact_writer_v1;
        GRANT EXECUTE ON FUNCTION research_stage8_watch_canonical_json_v1(JSONB),
            research_stage8_watch_json_sha256_v1(JSONB),
            research_stage8_watch_round2_v1(DOUBLE PRECISION),
            research_stage8_watch_timestamp_v1(JSONB),
            research_stage8_watch_finite_number_v1(JSONB),
            research_stage8_watch_components_match_v1(JSONB, JSONB),
            research_stage8_watch_capture_errors_v1(
                JSONB, TEXT, JSONB, TEXT, TIMESTAMPTZ, TIMESTAMPTZ, TIMESTAMPTZ)
            TO research_stage8_fact_writer_v1;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'research_stage8_fact_writer_v1') THEN
        GRANT EXECUTE ON FUNCTION
            research_stage8_anchor_canonical_json_v1(JSONB),
            research_stage8_anchor_strip_v1(TEXT),
            research_stage8_anchor_truthy_v1(JSONB),
            research_stage8_anchor_nonempty_v1(JSONB),
            research_stage8_anchor_reference_canonical_json_v1(JSONB),
            research_stage8_anchor_json_sha256_v1(JSONB),
            research_stage8_anchor_timestamp_v1(JSONB),
            research_stage8_anchor_aware_timestamp_v1(JSONB),
            research_stage8_anchor_finite_number_v1(JSONB),
            research_stage8_anchor_number_v1(JSONB),
            research_stage8_anchor_round6_v1(DOUBLE PRECISION),
            research_stage8_anchor_coverage_valid_v1(JSONB, TEXT, TIMESTAMPTZ),
            research_stage8_anchor_sources_valid_v1(JSONB, JSONB, JSONB, TEXT, TIMESTAMPTZ, TIMESTAMPTZ, TIMESTAMPTZ, TIMESTAMPTZ),
            research_stage8_anchor_session_ratios_v1(TIMESTAMPTZ, TIMESTAMPTZ),
            research_stage8_anchor_width_error_v1(JSONB, TEXT, TIMESTAMPTZ, INTEGER),
            research_stage8_anchor_width_valid_v1(JSONB, TEXT, TIMESTAMPTZ, INTEGER),
            research_stage8_anchor_feature_name_valid_v1(TEXT),
            research_stage8_anchor_forbidden_bundle_key_v1(JSONB),
            research_stage8_anchor_series_valid_v1(JSONB, TIMESTAMPTZ),
            research_stage8_anchor_bundle_valid_v1(JSONB, TEXT, TIMESTAMPTZ, TEXT),
            research_stage8_anchor_errors_v1(JSONB, JSONB, JSONB)
            TO research_stage8_fact_writer_v1;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'research_stage8_selector_writer_v1') THEN
        EXECUTE format('GRANT USAGE ON SCHEMA %I TO research_stage8_selector_writer_v1', target_schema);
        GRANT INSERT ON research_stage8_selection_receipts TO research_stage8_selector_writer_v1;
        GRANT SELECT ON research_stage8_binding_registry,
            research_stage8_projection_fact_batches,
            research_stage8_projected_fact_ledger,
            research_stage8_projection_fact_batch_seals,
            research_stage8_selection_receipts
            TO research_stage8_selector_writer_v1;
        GRANT SELECT ON research_stage8_registry_read_v1,
            research_stage8_fact_batch_read_v1,
            research_stage8_fact_read_v1,
            research_stage8_fact_seal_read_v1,
            research_stage8_selection_read_v1
            TO research_stage8_selector_writer_v1;
        GRANT EXECUTE ON FUNCTION research_stage8_canonical_json_v1(JSONB),
            research_stage8_json_sha256_v1(JSONB),
            research_stage8_utc_text_v1(TIMESTAMPTZ)
            TO research_stage8_selector_writer_v1;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'research_stage8_evaluator_writer_v1') THEN
        EXECUTE format('GRANT USAGE ON SCHEMA %I TO research_stage8_evaluator_writer_v1', target_schema);
        GRANT INSERT ON research_stage8_evaluation_receipts TO research_stage8_evaluator_writer_v1;
        GRANT SELECT ON research_stage8_binding_registry,
            research_stage8_projection_fact_batches,
            research_stage8_projection_fact_batch_seals,
            research_stage8_selection_receipts,
            research_stage8_evaluation_receipts
            TO research_stage8_evaluator_writer_v1;
        -- The invoker-rights evaluation guard cross-checks the closed evidence
        -- receipt against only these persisted fact identity columns.  Keep the
        -- evaluator's base-ledger read privilege column-scoped.
        GRANT SELECT (fact_batch_record_sha256, attempt_id,
                      attempt_fingerprint, anchor_slot_id, event_id,
                      event_fingerprint, symbol, direction, decision_time_utc,
                      selection_fact_identity_sha256,
                      fact_record_sha256, fact_sha256,
                      server_projection_attestation,
                      server_projection_attestation_sha256,
                      server_projection_status)
            ON research_stage8_projected_fact_ledger
            TO research_stage8_evaluator_writer_v1;
        GRANT SELECT ON research_stage8_registry_read_v1,
            research_stage8_fact_batch_read_v1,
            research_stage8_fact_read_v1,
            research_stage8_fact_seal_read_v1,
            research_stage8_selection_read_v1
            TO research_stage8_evaluator_writer_v1;
        GRANT SELECT ON research_stage8_evaluation_read_v1
            TO research_stage8_evaluator_writer_v1;
        -- The invoker-rights guard hashes and validates the complete immutable
        -- outcome/common/event source rows. Full SELECT is necessary for
        -- to_jsonb(row); no source DML is granted.
        GRANT SELECT ON research_events,
            research_ordered_first_touch_outcomes,
            research_common_window_metrics,
            research_prospective_anchor_attempts,
            research_prospective_anchor_slots,
            research_max_pain_snapshot_sets,
            research_btc_parent_movements,
            research_btc_price_bars
            TO research_stage8_evaluator_writer_v1;
        GRANT EXECUTE ON FUNCTION research_stage8_canonical_json_v1(JSONB),
            research_stage8_json_sha256_v1(JSONB),
            research_stage8_utc_text_v1(TIMESTAMPTZ),
            research_stage8_derive_projection_attestation_v1(
                TEXT, TEXT, BIGINT, BIGINT, BOOLEAN)
            TO research_stage8_evaluator_writer_v1;
        GRANT EXECUTE ON FUNCTION research_stage8_watch_canonical_json_v1(JSONB),
            research_stage8_watch_json_sha256_v1(JSONB),
            research_stage8_watch_round2_v1(DOUBLE PRECISION),
            research_stage8_watch_timestamp_v1(JSONB),
            research_stage8_watch_finite_number_v1(JSONB),
            research_stage8_watch_components_match_v1(JSONB, JSONB),
            research_stage8_watch_capture_errors_v1(
                JSONB, TEXT, JSONB, TEXT, TIMESTAMPTZ, TIMESTAMPTZ, TIMESTAMPTZ)
            TO research_stage8_evaluator_writer_v1;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'research_stage8_evaluator_writer_v1') THEN
        GRANT EXECUTE ON FUNCTION
            research_stage8_anchor_canonical_json_v1(JSONB),
            research_stage8_anchor_strip_v1(TEXT),
            research_stage8_anchor_truthy_v1(JSONB),
            research_stage8_anchor_nonempty_v1(JSONB),
            research_stage8_anchor_reference_canonical_json_v1(JSONB),
            research_stage8_anchor_json_sha256_v1(JSONB),
            research_stage8_anchor_timestamp_v1(JSONB),
            research_stage8_anchor_aware_timestamp_v1(JSONB),
            research_stage8_anchor_finite_number_v1(JSONB),
            research_stage8_anchor_number_v1(JSONB),
            research_stage8_anchor_round6_v1(DOUBLE PRECISION),
            research_stage8_anchor_coverage_valid_v1(JSONB, TEXT, TIMESTAMPTZ),
            research_stage8_anchor_sources_valid_v1(JSONB, JSONB, JSONB, TEXT, TIMESTAMPTZ, TIMESTAMPTZ, TIMESTAMPTZ, TIMESTAMPTZ),
            research_stage8_anchor_session_ratios_v1(TIMESTAMPTZ, TIMESTAMPTZ),
            research_stage8_anchor_width_error_v1(JSONB, TEXT, TIMESTAMPTZ, INTEGER),
            research_stage8_anchor_width_valid_v1(JSONB, TEXT, TIMESTAMPTZ, INTEGER),
            research_stage8_anchor_feature_name_valid_v1(TEXT),
            research_stage8_anchor_forbidden_bundle_key_v1(JSONB),
            research_stage8_anchor_series_valid_v1(JSONB, TIMESTAMPTZ),
            research_stage8_anchor_bundle_valid_v1(JSONB, TEXT, TIMESTAMPTZ, TEXT),
            research_stage8_anchor_errors_v1(JSONB, JSONB, JSONB)
            TO research_stage8_evaluator_writer_v1;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'research_stage8_reader_v1') THEN
        EXECUTE format('GRANT USAGE ON SCHEMA %I TO research_stage8_reader_v1', target_schema);
        GRANT SELECT ON research_stage8_registry_read_v1,
            research_stage8_fact_batch_read_v1, research_stage8_fact_read_v1,
            research_stage8_fact_seal_read_v1, research_stage8_selection_read_v1,
            research_stage8_evaluation_read_v1
            TO research_stage8_reader_v1;
        -- The trusted same-snapshot outcome adapter serializes complete source
        -- rows with to_jsonb(), so column-level grants would be insufficient.
        -- This role remains SELECT-only on the outcome and projection-replay
        -- sources used by its fixed 15-query plan.
        GRANT SELECT ON research_events,
            research_ordered_first_touch_outcomes,
            research_common_window_metrics,
            research_max_pain_snapshot_sets,
            research_prospective_anchor_attempts,
            research_prospective_anchor_slots,
            research_btc_parent_movements,
            research_btc_price_bars
            TO research_stage8_reader_v1;
        -- Compatibility read for the versioned coverage/source-audit pass.
        -- The outcome-free population identity excludes this relation, and
        -- neither projection derivation nor qualification consumes it.
        GRANT SELECT ON research_event_btc_movements
            TO research_stage8_reader_v1;
    END IF;
END
$$;

COMMENT ON TABLE research_stage8_binding_registry IS
    'Immutable exact Stage-8 research bindings. frozen_at_utc is assigned only by the DB clock; a row starts prospective eligibility and grants no delivery or trading authority.';
COMMENT ON TABLE research_stage8_selection_receipts IS
    'Append-only outcome-blind representative selections bound to one durable Stage-8 registry row and its independently frozen verifier profile.';
COMMENT ON TABLE research_stage8_evaluation_receipts IS
    'Append-only experimental research evaluations. These records never authorize Telegram, LIVE, deployment, or trading.';
COMMENT ON TABLE research_stage8_projection_fact_batches IS
    'Append-only exact attempt populations emitted by the frozen read-only projection adapter; a batch is unusable until separately sealed.';
COMMENT ON TABLE research_stage8_projected_fact_ledger IS
    'One append-only exact-binding fact state for every attempt in its batch, including UNKNOWN and false states; no outcomes are stored.';
COMMENT ON TABLE research_stage8_projection_fact_batch_seals IS
    'Immutable completeness seal over the exact fact rows and source-backed attempt-ID population.';
