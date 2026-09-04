-- Silent signal-snapshot freeze v1 (Stage 4)
--
-- Additive, database-enforced boundary for the four event types emitted by
-- research_signal_snapshot.py.  This migration does not grant Formula,
-- outcome, Telegram or trading authority.  It validates existing reserved
-- rows before installing the guards, then makes valid Stage-4 rows immutable.
--
-- Apply only to the approved research database after migrations 001 and 007.
-- The application remains responsible for enforcing its pre-JSONB 32,000-byte
-- canonical serialization limit; PostgreSQL JSONB does not retain the original
-- wire serialization, so that exact byte boundary cannot be reconstructed here.

-- Engine truth and the full event-set commitment are computed by one fixed,
-- unprivileged LOGIN identity.  Roles/passwords are provisioned out of band.
SET LOCAL search_path = pg_catalog;

DO $roles$
DECLARE
    writer_row RECORD;
BEGIN
    SELECT * INTO writer_row
    FROM pg_catalog.pg_roles
    WHERE rolname = 'research_signal_snapshot_writer_v1';
    IF NOT FOUND THEN
        RAISE EXCEPTION
            'Required LOGIN role research_signal_snapshot_writer_v1 is missing';
    END IF;
    IF NOT writer_row.rolcanlogin
       OR writer_row.rolinherit
       OR writer_row.rolsuper
       OR writer_row.rolcreatedb
       OR writer_row.rolcreaterole
       OR writer_row.rolreplication
       OR writer_row.rolbypassrls
    THEN
        RAISE EXCEPTION
            'research_signal_snapshot_writer_v1 must be an unprivileged NOINHERIT LOGIN role';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM pg_catalog.pg_auth_members membership
        WHERE membership.member = writer_row.oid
           OR membership.roleid = writer_row.oid
    ) THEN
        RAISE EXCEPTION
            'Stage-4 writer must not participate in role membership';
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_database database_row
        WHERE database_row.datname = pg_catalog.current_database()
          AND database_row.datdba = writer_row.oid
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_namespace namespace_row
        WHERE namespace_row.nspname = 'public'
          AND namespace_row.nspowner = writer_row.oid
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_class class_row
        WHERE class_row.relnamespace = 'public'::regnamespace
          AND class_row.relname IN (
              'research_events', 'research_events_event_id_seq',
              'research_max_pain_snapshot_sets',
              'research_max_pain_snapshot_sets_snapshot_set_id_seq',
              'research_max_pain_snapshot_symbols',
              'research_max_pain_snapshot_rows',
              'research_max_pain_snapshot_rows_snapshot_row_id_seq'
          )
          AND class_row.relowner = writer_row.oid
    ) THEN
        RAISE EXCEPTION
            'Stage-4 writer cannot own its database, schema or source relations';
    END IF;
    IF pg_catalog.has_schema_privilege(
        'research_signal_snapshot_writer_v1', 'public', 'CREATE'
    ) THEN
        RAISE EXCEPTION
            'Stage-4 writer cannot create objects in schema public';
    END IF;
END;
$roles$;

-- Close the preflight/write race before examining any reserved rows.  The
-- schema-admin applies this file in one transaction, so the lock remains held
-- through validation, trigger replacement and ACL installation.
LOCK TABLE public.research_events IN SHARE ROW EXCLUSIVE MODE;

-- Existing Stage-4 rows are acceptable only when they were protected by the
-- exact final guards from a prior application of this migration.  A trigger
-- that merely reuses one of our names is not evidence of trusted origin.
DO $prior_install$
DECLARE
    has_stage4_rows BOOLEAN;
    trusted_owner OID;
    trusted_trigger_count INTEGER;
BEGIN
    SELECT relation_row.relowner
    INTO trusted_owner
    FROM pg_catalog.pg_class relation_row
    WHERE relation_row.oid = 'public.research_events'::REGCLASS;

    SELECT EXISTS (
        SELECT 1
        FROM public.research_events event_row
        WHERE event_row.capture_stage = 'SILENT_SIGNAL_SNAPSHOT'
           OR event_row.event_type IN (
                'MAX_PAIN_CONFIRMATION_STATE',
                'MAGNET_CONFIRMATION_STATE',
                'SILENT_COMBINED_CONFIRMATION_SNAPSHOT',
                'SIGNAL_SNAPSHOT_PROJECTION'
           )
    ) INTO has_stage4_rows;

    IF has_stage4_rows THEN
        SELECT COUNT(*)
        INTO trusted_trigger_count
        FROM pg_catalog.pg_trigger trigger_row
        JOIN pg_catalog.pg_proc function_row
          ON function_row.oid = trigger_row.tgfoid
        JOIN pg_catalog.pg_namespace function_namespace
          ON function_namespace.oid = function_row.pronamespace
        WHERE trigger_row.tgrelid = 'public.research_events'::REGCLASS
          AND NOT trigger_row.tgisinternal
          AND trigger_row.tgenabled = 'A'
          AND function_namespace.nspname = 'public'
          AND function_row.proowner = trusted_owner
          AND NOT function_row.prosecdef
          AND function_row.proconfig = ARRAY[
                'search_path=pg_catalog, public'
              ]::TEXT[]
          AND trigger_row.tgqual IS NULL
          AND (
                (
                    trigger_row.tgname =
                        'trg_research_signal_snapshot_v1_writer'
                    AND trigger_row.tgtype::INTEGER = 23
                    AND NOT trigger_row.tgdeferrable
                    AND NOT trigger_row.tginitdeferred
                    AND function_row.proname =
                        'assert_research_signal_snapshot_v1_writer'
                )
                OR (
                    trigger_row.tgname =
                        'trg_research_signal_snapshot_v1_envelope'
                    AND trigger_row.tgtype::INTEGER = 23
                    AND NOT trigger_row.tgdeferrable
                    AND NOT trigger_row.tginitdeferred
                    AND function_row.proname =
                        'validate_research_signal_snapshot_v1_envelope'
                )
                OR (
                    trigger_row.tgname =
                        'trg_research_signal_snapshot_v1_set_complete'
                    AND trigger_row.tgtype::INTEGER = 5
                    AND trigger_row.tgdeferrable
                    AND trigger_row.tginitdeferred
                    AND trigger_row.tgconstraint <> 0
                    AND function_row.proname =
                        'validate_research_signal_snapshot_v1_set_complete'
                )
          );
        IF trusted_trigger_count <> 3 OR EXISTS (
            SELECT 1
            FROM pg_catalog.pg_trigger trigger_row
            WHERE trigger_row.tgrelid = 'public.research_events'::REGCLASS
              AND NOT trigger_row.tgisinternal
              AND (trigger_row.tgtype::INTEGER & 4) = 4
              AND trigger_row.tgname NOT IN (
                    'trg_research_signal_snapshot_v1_writer',
                    'trg_research_signal_snapshot_v1_envelope',
                    'trg_research_signal_snapshot_v1_set_complete'
              )
        ) THEN
            RAISE EXCEPTION
                'Untrusted pre-migration Stage-4 rows require manual quarantine';
        END IF;
    END IF;
END;
$prior_install$;

DO $row_visibility$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_catalog.pg_class relation_row
        WHERE relation_row.oid IN (
            'public.research_events'::REGCLASS,
            'public.research_max_pain_snapshot_sets'::REGCLASS,
            'public.research_max_pain_snapshot_symbols'::REGCLASS,
            'public.research_max_pain_snapshot_rows'::REGCLASS
        )
          AND (relation_row.relrowsecurity OR relation_row.relforcerowsecurity)
    ) OR EXISTS (
        SELECT 1
        FROM pg_catalog.pg_policy policy_row
        WHERE policy_row.polrelid IN (
            'public.research_events'::REGCLASS,
            'public.research_max_pain_snapshot_sets'::REGCLASS,
            'public.research_max_pain_snapshot_symbols'::REGCLASS,
            'public.research_max_pain_snapshot_rows'::REGCLASS
        )
    ) OR EXISTS (
        SELECT 1
        FROM pg_catalog.pg_rewrite rule_row
        WHERE rule_row.ev_class IN (
            'public.research_events'::REGCLASS,
            'public.research_max_pain_snapshot_sets'::REGCLASS,
            'public.research_max_pain_snapshot_symbols'::REGCLASS,
            'public.research_max_pain_snapshot_rows'::REGCLASS
        )
          AND rule_row.rulename <> '_RETURN'
    ) THEN
        RAISE EXCEPTION
            'Stage-4 source and event tables cannot use RLS, policies or rules';
    END IF;
END;
$row_visibility$;

SET LOCAL search_path = public;

CREATE OR REPLACE FUNCTION public.research_signal_snapshot_v1_reserved_type(
    candidate_type TEXT
)
RETURNS BOOLEAN
LANGUAGE SQL
IMMUTABLE
STRICT
PARALLEL SAFE
SET search_path = pg_catalog, public
AS $function$
    SELECT candidate_type = ANY (
        ARRAY[
            'MAX_PAIN_CONFIRMATION_STATE',
            'MAGNET_CONFIRMATION_STATE',
            'SILENT_COMBINED_CONFIRMATION_SNAPSHOT',
            'SIGNAL_SNAPSHOT_PROJECTION'
        ]::TEXT[]
    );
$function$;

CREATE OR REPLACE FUNCTION public.assert_research_signal_snapshot_v1_writer()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $function$
DECLARE
    stage4_row BOOLEAN := (
        NEW.capture_stage = 'SILENT_SIGNAL_SNAPSHOT'
        OR public.research_signal_snapshot_v1_reserved_type(NEW.event_type)
    );
BEGIN
    IF stage4_row AND (
        SESSION_USER <> 'research_signal_snapshot_writer_v1'
        OR CURRENT_USER <> 'research_signal_snapshot_writer_v1'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '42501',
            MESSAGE = 'Stage-4 signal snapshot requires its dedicated writer';
    END IF;
    IF SESSION_USER = 'research_signal_snapshot_writer_v1'
       AND NOT stage4_row
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '42501',
            MESSAGE = 'Stage-4 writer cannot create non-Stage-4 research events';
    END IF;
    RETURN NEW;
END;
$function$;

CREATE OR REPLACE FUNCTION public.research_signal_snapshot_v1_sha256(
    candidate_hash TEXT
)
RETURNS BOOLEAN
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
SET search_path = pg_catalog, public
AS $function$
    SELECT COALESCE(BTRIM(candidate_hash) ~ '^[0-9a-f]{64}$', FALSE);
$function$;

CREATE OR REPLACE FUNCTION public.research_signal_snapshot_v1_text_sha256(
    candidate_text TEXT
)
RETURNS TEXT
LANGUAGE SQL
IMMUTABLE
STRICT
PARALLEL SAFE
SET search_path = pg_catalog, public
AS $function$
    SELECT ENCODE(SHA256(CONVERT_TO(candidate_text, 'UTF8')), 'hex');
$function$;

CREATE OR REPLACE FUNCTION public.research_signal_snapshot_v1_commitment_canonical(
    candidate JSONB
)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
SET search_path = pg_catalog, public
AS $function$
DECLARE
    kind TEXT;
    raw TEXT;
    body TEXT;
    item_count BIGINT;
BEGIN
    kind := JSONB_TYPEOF(candidate);
    IF kind = 'null' THEN
        RETURN 'n';
    ELSIF kind = 'boolean' THEN
        RETURN CASE WHEN candidate = 'true'::JSONB THEN 'b1' ELSE 'b0' END;
    ELSIF kind = 'number' THEN
        raw := (candidate #>> '{}')::NUMERIC::TEXT;
        IF STRPOS(raw, '.') > 0 THEN
            raw := RTRIM(RTRIM(raw, '0'), '.');
        END IF;
        IF raw = '-0' THEN
            raw := '0';
        END IF;
        RETURN 'd' || OCTET_LENGTH(CONVERT_TO(raw, 'UTF8'))::TEXT || ':' || raw;
    ELSIF kind = 'string' THEN
        raw := candidate #>> '{}';
        RETURN 's' || OCTET_LENGTH(CONVERT_TO(raw, 'UTF8'))::TEXT || ':' || raw;
    ELSIF kind = 'array' THEN
        SELECT COUNT(*), STRING_AGG(
            public.research_signal_snapshot_v1_commitment_canonical(value),
            '' ORDER BY ordinal
        )
        INTO item_count, body
        FROM JSONB_ARRAY_ELEMENTS(candidate)
             WITH ORDINALITY AS item(value, ordinal);
        RETURN 'a' || item_count::TEXT || ':' || COALESCE(body, '');
    ELSIF kind = 'object' THEN
        SELECT COUNT(*), STRING_AGG(
            'k' || OCTET_LENGTH(CONVERT_TO(key, 'UTF8'))::TEXT || ':' || key
                || public.research_signal_snapshot_v1_commitment_canonical(value),
            '' ORDER BY key COLLATE pg_catalog."C"
        )
        INTO item_count, body
        FROM JSONB_EACH(candidate);
        RETURN 'o' || item_count::TEXT || ':' || COALESCE(body, '');
    END IF;
    RAISE EXCEPTION USING
        ERRCODE = '22023',
        MESSAGE = 'unsupported Stage-4 commitment JSONB type';
END;
$function$;

CREATE OR REPLACE FUNCTION public.research_signal_snapshot_v1_event_commitment_payload(
    candidate public.research_events
)
RETURNS JSONB
LANGUAGE SQL
STABLE
STRICT
PARALLEL SAFE
SET search_path = pg_catalog, public
AS $function$
    SELECT JSONB_BUILD_OBJECT(
        'schema_version', (candidate).schema_version,
        'event_kind', (candidate).event_kind,
        'event_type', (candidate).event_type,
        'alert_time_utc', TO_CHAR(
            (candidate).alert_time_utc AT TIME ZONE 'UTC',
            'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
        ),
        'symbol', (candidate).symbol,
        'direction', (candidate).direction,
        'source_side', (candidate).source_side,
        'timeframe', (candidate).timeframe,
        'score', CASE WHEN (candidate).score IS NULL THEN NULL ELSE
            ENCODE(FLOAT8SEND((candidate).score), 'hex') END,
        'current_price', CASE WHEN (candidate).current_price IS NULL THEN NULL ELSE
            ENCODE(FLOAT8SEND((candidate).current_price), 'hex') END,
        'target_price', CASE WHEN (candidate).target_price IS NULL THEN NULL ELSE
            ENCODE(FLOAT8SEND((candidate).target_price), 'hex') END,
        'initial_target_distance_pct', CASE
            WHEN (candidate).initial_target_distance_pct IS NULL THEN NULL ELSE
            ENCODE(FLOAT8SEND((candidate).initial_target_distance_pct), 'hex') END,
        'categories', (candidate).categories,
        'setup_key', BTRIM((candidate).setup_key),
        'event_fingerprint', BTRIM((candidate).event_fingerprint),
        'strategy_version', (candidate).strategy_version,
        'code_version', (candidate).code_version,
        'engine_snapshot', (candidate).engine_snapshot
    );
$function$;

CREATE OR REPLACE FUNCTION public.research_signal_snapshot_v1_event_payload_sha256(
    candidate public.research_events
)
RETURNS TEXT
LANGUAGE SQL
STABLE
STRICT
PARALLEL SAFE
SET search_path = pg_catalog, public
AS $function$
    SELECT public.research_signal_snapshot_v1_text_sha256(
        public.research_signal_snapshot_v1_commitment_canonical(
            public.research_signal_snapshot_v1_event_commitment_payload(candidate)
        )
    );
$function$;

CREATE OR REPLACE FUNCTION public.research_signal_snapshot_v1_identity_canonical(
    candidate JSONB
)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
SET search_path = pg_catalog, public
AS $function$
DECLARE
    kind TEXT;
    body TEXT;
BEGIN
    kind := JSONB_TYPEOF(candidate);
    IF kind = 'object' THEN
        SELECT '{' || COALESCE(STRING_AGG(
            TO_JSONB(key)::TEXT || ':'
                || public.research_signal_snapshot_v1_identity_canonical(value),
            ',' ORDER BY key COLLATE pg_catalog."C"
        ), '') || '}'
        INTO body
        FROM JSONB_EACH(candidate);
        RETURN body;
    ELSIF kind = 'array' THEN
        SELECT '[' || COALESCE(STRING_AGG(
            public.research_signal_snapshot_v1_identity_canonical(value),
            ',' ORDER BY ordinal
        ), '') || ']'
        INTO body
        FROM JSONB_ARRAY_ELEMENTS(candidate)
             WITH ORDINALITY AS item(value, ordinal);
        RETURN body;
    ELSIF kind IN ('string', 'boolean', 'null') THEN
        RETURN candidate::TEXT;
    END IF;
    RAISE EXCEPTION USING
        ERRCODE = '22023',
        MESSAGE = 'numeric Stage-4 identity value is forbidden';
END;
$function$;

CREATE OR REPLACE FUNCTION public.research_signal_snapshot_v1_magnet_members(
    candidate public.research_events
)
RETURNS JSONB
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
SET search_path = pg_catalog, public
AS $function$
DECLARE
    raw JSONB;
    ordered JSONB;
BEGIN
    raw := (candidate).engine_snapshot #> '{magnet,members}';
    IF JSONB_TYPEOF(raw) IS DISTINCT FROM 'array'
       OR JSONB_ARRAY_LENGTH(raw) = 0
       OR EXISTS (
            SELECT 1
            FROM JSONB_ARRAY_ELEMENTS(raw) item
            WHERE JSONB_TYPEOF(item) IS DISTINCT FROM 'string'
              OR item #>> '{}' NOT IN (
                  '12h', '24h', '48h', '3d', '1w', '2w', '1m'
              )
       )
       OR (
            SELECT COUNT(*) FROM JSONB_ARRAY_ELEMENTS_TEXT(raw)
       ) IS DISTINCT FROM (
            SELECT COUNT(DISTINCT value)
            FROM JSONB_ARRAY_ELEMENTS_TEXT(raw) value
       )
    THEN
        RETURN NULL;
    END IF;
    SELECT JSONB_AGG(item ORDER BY item #>> '{}' COLLATE pg_catalog."C")
    INTO ordered
    FROM JSONB_ARRAY_ELEMENTS(raw) item;
    IF ordered IS DISTINCT FROM raw THEN
        RETURN NULL;
    END IF;
    RETURN ordered;
END;
$function$;

CREATE OR REPLACE FUNCTION public.research_signal_snapshot_v1_expected_setup_key(
    candidate public.research_events
)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
SET search_path = pg_catalog, public
AS $function$
DECLARE
    family TEXT;
    identity JSONB;
BEGIN
    family := CASE (candidate).event_type
        WHEN 'MAX_PAIN_CONFIRMATION_STATE' THEN 'MAX_PAIN'
        WHEN 'MAGNET_CONFIRMATION_STATE' THEN 'MAGNET'
        WHEN 'SILENT_COMBINED_CONFIRMATION_SNAPSHOT' THEN 'COMBINED'
        WHEN 'SIGNAL_SNAPSHOT_PROJECTION' THEN 'PROJECTION'
    END;
    identity := JSONB_BUILD_OBJECT(
        'contract_version', 'research-signal-snapshot-v1',
        'signal_family', family,
        'sample_type', (candidate).event_type
    );
    IF family = 'MAX_PAIN' THEN
        identity := identity || JSONB_BUILD_OBJECT(
            'source_side', (candidate).source_side
        );
    ELSIF family = 'MAGNET' THEN
        identity := identity || JSONB_BUILD_OBJECT(
            'magnet_side', (candidate).source_side,
            'members', public.research_signal_snapshot_v1_magnet_members(candidate)
        );
    ELSIF family = 'PROJECTION' THEN
        identity := identity || JSONB_BUILD_OBJECT('scope', 'SNAPSHOT_SET');
    END IF;
    RETURN public.research_signal_snapshot_v1_text_sha256(
        public.research_signal_snapshot_v1_identity_canonical(
            JSONB_BUILD_OBJECT(
                'symbol', (candidate).symbol,
                'direction', (candidate).direction,
                'timeframe', COALESCE((candidate).timeframe, ''),
                'event_family', 'DECISION_SAMPLE',
                'setup_identity', identity
            )
        )
    );
END;
$function$;

CREATE OR REPLACE FUNCTION public.research_signal_snapshot_v1_expected_fingerprint(
    candidate public.research_events
)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
SET search_path = pg_catalog, public
AS $function$
DECLARE
    family TEXT;
    snapshot_key TEXT;
    locator JSONB;
BEGIN
    family := CASE (candidate).event_type
        WHEN 'MAX_PAIN_CONFIRMATION_STATE' THEN 'MAX_PAIN'
        WHEN 'MAGNET_CONFIRMATION_STATE' THEN 'MAGNET'
        WHEN 'SILENT_COMBINED_CONFIRMATION_SNAPSHOT' THEN 'COMBINED'
        WHEN 'SIGNAL_SNAPSHOT_PROJECTION' THEN 'PROJECTION'
    END;
    snapshot_key := CASE family
        WHEN 'PROJECTION' THEN
            (candidate).engine_snapshot #>> '{projection,snapshot_key}'
        ELSE
            (candidate).engine_snapshot #>>
                '{signal_snapshot,archive_reference,snapshot_key}'
    END;
    locator := CASE family
        WHEN 'MAX_PAIN' THEN JSONB_BUILD_OBJECT(
            'timeframe', (candidate).timeframe,
            'source_side', (candidate).source_side
        )
        WHEN 'MAGNET' THEN JSONB_BUILD_OBJECT(
            'magnet_side', (candidate).source_side,
            'members', public.research_signal_snapshot_v1_magnet_members(candidate)
        )
        WHEN 'COMBINED' THEN JSONB_BUILD_OBJECT(
            'source_side', (candidate).source_side
        )
        ELSE JSONB_BUILD_OBJECT('scope', 'SNAPSHOT_SET')
    END;
    RETURN public.research_signal_snapshot_v1_text_sha256(
        public.research_signal_snapshot_v1_identity_canonical(
            JSONB_BUILD_OBJECT(
                'contract_version', 'research-signal-snapshot-v1',
                'snapshot_key', snapshot_key,
                'event_type', (candidate).event_type,
                'symbol', (candidate).symbol,
                'direction', (candidate).direction,
                'signal_family', family,
                'locator', locator
            )
        )
    );
END;
$function$;

CREATE OR REPLACE FUNCTION public.research_signal_snapshot_v1_nonnegative_integer(
    candidate_value JSONB
)
RETURNS BOOLEAN
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
SET search_path = pg_catalog, public
AS $function$
DECLARE
    parsed NUMERIC;
BEGIN
    IF NOT COALESCE(
        JSONB_TYPEOF(candidate_value) = 'number'
        AND candidate_value #>> '{}' ~ '^(0|[1-9][0-9]*)$',
        FALSE
    ) THEN
        RETURN FALSE;
    END IF;
    BEGIN
        parsed := (candidate_value #>> '{}')::NUMERIC;
    EXCEPTION
        WHEN numeric_value_out_of_range OR invalid_text_representation THEN
            RETURN FALSE;
    END;
    RETURN parsed BETWEEN 0 AND 9223372036854775807;
END;
$function$;

CREATE OR REPLACE FUNCTION public.research_signal_snapshot_v1_positive_bigint(
    candidate_value JSONB
)
RETURNS BOOLEAN
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
SET search_path = pg_catalog, public
AS $function$
DECLARE
    parsed NUMERIC;
BEGIN
    IF NOT public.research_signal_snapshot_v1_nonnegative_integer(
        candidate_value
    ) THEN
        RETURN FALSE;
    END IF;
    BEGIN
        parsed := (candidate_value #>> '{}')::NUMERIC;
    EXCEPTION
        WHEN numeric_value_out_of_range OR invalid_text_representation THEN
            RETURN FALSE;
    END;
    RETURN parsed BETWEEN 1 AND 9223372036854775807;
END;
$function$;

CREATE OR REPLACE FUNCTION public.research_signal_snapshot_v1_finite_number(
    candidate_value JSONB
)
RETURNS DOUBLE PRECISION
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
SET search_path = pg_catalog, public
AS $function$
DECLARE
    parsed DOUBLE PRECISION;
BEGIN
    IF JSONB_TYPEOF(candidate_value) IS DISTINCT FROM 'number' THEN
        RETURN NULL;
    END IF;
    BEGIN
        parsed := (candidate_value #>> '{}')::DOUBLE PRECISION;
    EXCEPTION
        WHEN numeric_value_out_of_range OR invalid_text_representation THEN
            RETURN NULL;
    END;
    IF parsed IN (
        'NaN'::DOUBLE PRECISION,
        'Infinity'::DOUBLE PRECISION,
        '-Infinity'::DOUBLE PRECISION
    ) THEN
        RETURN NULL;
    END IF;
    RETURN parsed;
END;
$function$;

CREATE OR REPLACE FUNCTION public.research_signal_snapshot_v1_key_count(
    candidate JSONB,
    target_key TEXT
)
RETURNS BIGINT
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
SET search_path = pg_catalog, public
AS $function$
DECLARE
    total BIGINT := 0;
    child JSONB;
BEGIN
    IF JSONB_TYPEOF(candidate) = 'object' THEN
        IF candidate ? target_key THEN
            total := 1;
        END IF;
        FOR child IN SELECT value FROM JSONB_EACH(candidate)
        LOOP
            total := total + public.research_signal_snapshot_v1_key_count(
                child, target_key
            );
        END LOOP;
    ELSIF JSONB_TYPEOF(candidate) = 'array' THEN
        FOR child IN SELECT value FROM JSONB_ARRAY_ELEMENTS(candidate)
        LOOP
            total := total + public.research_signal_snapshot_v1_key_count(
                child, target_key
            );
        END LOOP;
    END IF;
    RETURN total;
END;
$function$;

CREATE OR REPLACE FUNCTION public.assert_research_signal_snapshot_v1_envelope(
    candidate public.research_events
)
RETURNS VOID
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $function$
DECLARE
    reserved_type BOOLEAN := public.research_signal_snapshot_v1_reserved_type(
        candidate.event_type
    );
    reserved_stage BOOLEAN := (
        candidate.capture_stage = 'SILENT_SIGNAL_SNAPSHOT'
    );
    signal_snapshot JSONB;
    archive_reference JSONB;
    derivatives_reference JSONB;
    projection JSONB;
    projection_counts JSONB;
    official_price JSONB;
    price_oi_reference JSONB;
    futures_cvd_reference JSONB;
    spot_cvd_reference JSONB;
    expected_family TEXT;
    target_snapshot_set_id BIGINT;
    archive_snapshot_key TEXT;
    archive_payload_sha256 TEXT;
    archive_available_at_utc TIMESTAMPTZ;
    archive_cycle_id TEXT;
    archive_cycle_time_utc TIMESTAMPTZ;
    archive_collector_version TEXT;
    archive_source TEXT;
    archive_research_eligible BOOLEAN;
    decision_time_utc TIMESTAMPTZ;
    available_at_utc TIMESTAMPTZ;
    read_started_at_utc TIMESTAMPTZ;
    read_completed_at_utc TIMESTAMPTZ;
    archive_cycle_reference_time_utc TIMESTAMPTZ;
    official_fetched_at_utc TIMESTAMPTZ;
    official_observed_at_utc TIMESTAMPTZ;
    official_candle_open_time_utc TIMESTAMPTZ;
    official_candle_close_time_utc TIMESTAMPTZ;
    provenance_time_text TEXT;
    projection_status TEXT;
    count_max_pain BIGINT;
    count_magnet BIGINT;
    count_combined BIGINT;
    signal_event_count BIGINT;
    eligible_symbol_count BIGINT;
    evaluable_symbol_count BIGINT;
BEGIN
    -- Rows outside the reserved type/stage namespace remain untouched.
    IF NOT reserved_type AND NOT reserved_stage THEN
        RETURN;
    END IF;

    -- A reserved type cannot escape the reserved stage, and the reserved stage
    -- cannot be used by an unreviewed fifth event type.
    IF NOT reserved_type OR NOT reserved_stage THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'invalid Stage-4 signal snapshot type/capture-stage pairing';
    END IF;

    IF candidate.schema_version IS DISTINCT FROM 'research-event-v1'
       OR candidate.event_kind IS DISTINCT FROM 'DECISION_SAMPLE'
       OR candidate.strategy_version IS DISTINCT FROM 'signal-snapshot-v1'
       OR candidate.delivery_status IS DISTINCT FROM 'NOT_APPLICABLE'
       OR candidate.delivery_attempted_at_utc IS NOT NULL
       OR candidate.delivered_at_utc IS NOT NULL
       OR BTRIM(candidate.code_version) = ''
       OR BTRIM(candidate.runtime_session_id) = ''
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'invalid Stage-4 signal snapshot event authority envelope';
    END IF;

    IF NOT public.research_signal_snapshot_v1_sha256(candidate.setup_key)
       OR NOT public.research_signal_snapshot_v1_sha256(
           candidate.event_fingerprint
       )
       OR BTRIM(candidate.setup_key) IS DISTINCT FROM
            public.research_signal_snapshot_v1_expected_setup_key(candidate)
       OR BTRIM(candidate.event_fingerprint) IS DISTINCT FROM
            public.research_signal_snapshot_v1_expected_fingerprint(candidate)
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'invalid Stage-4 signal snapshot event identity';
    END IF;

    IF JSONB_TYPEOF(candidate.categories) IS DISTINCT FROM 'array'
       OR NOT candidate.categories @> '["DECISION_SAMPLE","SILENT"]'::JSONB
       OR EXISTS (
            SELECT 1
            FROM JSONB_ARRAY_ELEMENTS(candidate.categories) category
            WHERE JSONB_TYPEOF(category) IS DISTINCT FROM 'string'
               OR BTRIM(category #>> '{}') = ''
       )
       OR JSONB_ARRAY_LENGTH(candidate.categories) IS DISTINCT FROM (
            SELECT COUNT(DISTINCT category #>> '{}')
            FROM JSONB_ARRAY_ELEMENTS(candidate.categories) category
       )
       OR EXISTS (
            SELECT 1
            FROM (
                SELECT
                    category,
                    LAG(category) OVER (ORDER BY ordinal) AS prior
                FROM JSONB_ARRAY_ELEMENTS_TEXT(candidate.categories)
                     WITH ORDINALITY AS item(category, ordinal)
            ) ordered
            WHERE ordered.prior COLLATE pg_catalog."C"
                >= ordered.category COLLATE pg_catalog."C"
       )
       OR JSONB_TYPEOF(candidate.engine_snapshot) IS DISTINCT FROM 'object'
       OR JSONB_TYPEOF(
           candidate.engine_snapshot -> 'signal_snapshot'
       ) IS DISTINCT FROM 'object'
       OR public.research_signal_snapshot_v1_key_count(
            candidate.engine_snapshot, 'formula_authorized'
          ) <> 1
       OR public.research_signal_snapshot_v1_key_count(
            candidate.engine_snapshot, 'outcome_authorized'
          ) <> 1
       OR public.research_signal_snapshot_v1_key_count(
            candidate.engine_snapshot, 'telegram_delivery_allowed'
          ) <> 1
       OR public.research_signal_snapshot_v1_key_count(
            candidate.engine_snapshot, 'trade_execution_allowed'
          ) <> 1
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'invalid Stage-4 signal snapshot JSON envelope';
    END IF;

    signal_snapshot := candidate.engine_snapshot -> 'signal_snapshot';
    expected_family := CASE candidate.event_type
        WHEN 'MAX_PAIN_CONFIRMATION_STATE' THEN 'MAX_PAIN'
        WHEN 'MAGNET_CONFIRMATION_STATE' THEN 'MAGNET'
        WHEN 'SILENT_COMBINED_CONFIRMATION_SNAPSHOT' THEN 'COMBINED'
        WHEN 'SIGNAL_SNAPSHOT_PROJECTION' THEN 'PROJECTION'
    END;

    IF signal_snapshot ->> 'contract_version'
            IS DISTINCT FROM 'research-signal-snapshot-v1'
       OR signal_snapshot ->> 'signal_family' IS DISTINCT FROM expected_family
       OR signal_snapshot -> 'formula_authorized' IS DISTINCT FROM 'false'::JSONB
       OR signal_snapshot -> 'outcome_authorized' IS DISTINCT FROM 'false'::JSONB
       OR signal_snapshot -> 'telegram_delivery_allowed'
            IS DISTINCT FROM 'false'::JSONB
       OR signal_snapshot -> 'trade_execution_allowed'
            IS DISTINCT FROM 'false'::JSONB
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'Stage-4 signal snapshot cannot carry downstream authority';
    END IF;

    IF candidate.event_type = 'SIGNAL_SNAPSHOT_PROJECTION' THEN
        IF NOT candidate.engine_snapshot ?& ARRAY[
                'signal_snapshot', 'projection'
           ]::TEXT[]
           OR candidate.engine_snapshot - ARRAY[
                'signal_snapshot', 'projection'
           ]::TEXT[] <> '{}'::JSONB
           OR NOT signal_snapshot ?& ARRAY[
                'contract_version', 'signal_family', 'tier',
                'formula_authorized', 'outcome_authorized',
                'telegram_delivery_allowed', 'trade_execution_allowed'
            ]::TEXT[]
           OR signal_snapshot - ARRAY[
                'contract_version', 'signal_family', 'tier',
                'formula_authorized', 'outcome_authorized',
                'telegram_delivery_allowed', 'trade_execution_allowed'
            ]::TEXT[] <> '{}'::JSONB
           OR candidate.symbol IS DISTINCT FROM 'RESEARCH'
           OR candidate.direction IS DISTINCT FROM 'NEUTRAL'
           OR candidate.source_side IS NOT NULL
           OR candidate.timeframe IS NOT NULL
           OR candidate.score IS NOT NULL
           OR candidate.current_price IS NOT NULL
           OR candidate.target_price IS NOT NULL
           OR candidate.initial_target_distance_pct IS NOT NULL
           OR JSONB_TYPEOF(
               candidate.engine_snapshot -> 'projection'
           ) IS DISTINCT FROM 'object'
        THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                MESSAGE = 'invalid Stage-4 projection event shape';
        END IF;

        projection := candidate.engine_snapshot -> 'projection';
        IF NOT projection ?& ARRAY[
                'status', 'evaluation_status', 'reason', 'snapshot_set_id', 'snapshot_key',
                'set_payload_sha256', 'available_at_utc', 'eligible_symbols',
                'symbol_evaluations',
                'decision_time_utc', 'derivatives_read_started_at_utc',
                'derivatives_read_completed_at_utc', 'counts',
                'signal_event_count', 'signal_events_payload_sha256'
            ]::TEXT[]
           OR projection - ARRAY[
                'status', 'evaluation_status', 'reason', 'snapshot_set_id', 'snapshot_key',
                'set_payload_sha256', 'available_at_utc', 'eligible_symbols',
                'symbol_evaluations',
                'decision_time_utc', 'derivatives_read_started_at_utc',
                'derivatives_read_completed_at_utc', 'counts',
                'signal_event_count', 'signal_events_payload_sha256'
            ]::TEXT[] <> '{}'::JSONB
        THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                MESSAGE = 'invalid Stage-4 projection receipt keys';
        END IF;

        projection_status := projection ->> 'status';
        IF projection_status IS NULL
           OR projection_status NOT IN ('COMPLETED', 'MISSED_CAUSAL_WINDOW')
           OR signal_snapshot ->> 'tier' IS DISTINCT FROM projection_status
           OR projection ->> 'evaluation_status' NOT IN (
                'EVALUABLE', 'PARTIAL', 'UNEVALUABLE'
           )
           OR JSONB_ARRAY_LENGTH(candidate.categories) <> 3
           OR NOT candidate.categories @> JSONB_BUILD_ARRAY(projection_status)
           OR NOT public.research_signal_snapshot_v1_sha256(
               projection ->> 'snapshot_key'
           )
           OR NOT public.research_signal_snapshot_v1_sha256(
               projection ->> 'set_payload_sha256'
           )
           OR NOT public.research_signal_snapshot_v1_sha256(
               projection ->> 'signal_events_payload_sha256'
           )
           OR NOT public.research_signal_snapshot_v1_nonnegative_integer(
               projection -> 'snapshot_set_id'
           )
           OR (projection ->> 'snapshot_set_id')::BIGINT <= 0
           OR JSONB_TYPEOF(projection -> 'eligible_symbols')
                IS DISTINCT FROM 'array'
           OR JSONB_TYPEOF(projection -> 'symbol_evaluations')
                IS DISTINCT FROM 'array'
           OR JSONB_TYPEOF(projection -> 'counts') IS DISTINCT FROM 'object'
           OR NOT public.research_signal_snapshot_v1_nonnegative_integer(
               projection -> 'signal_event_count'
           )
        THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                MESSAGE = 'invalid Stage-4 projection receipt values';
        END IF;

        projection_counts := projection -> 'counts';
        IF NOT projection_counts ?& ARRAY[
                'max_pain', 'magnet', 'combined'
            ]::TEXT[]
           OR projection_counts - ARRAY[
                'max_pain', 'magnet', 'combined'
            ]::TEXT[] <> '{}'::JSONB
           OR NOT public.research_signal_snapshot_v1_nonnegative_integer(
               projection_counts -> 'max_pain'
           )
           OR NOT public.research_signal_snapshot_v1_nonnegative_integer(
               projection_counts -> 'magnet'
           )
           OR NOT public.research_signal_snapshot_v1_nonnegative_integer(
               projection_counts -> 'combined'
           )
        THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                MESSAGE = 'invalid Stage-4 projection counts';
        END IF;

        count_max_pain := (projection_counts ->> 'max_pain')::BIGINT;
        count_magnet := (projection_counts ->> 'magnet')::BIGINT;
        count_combined := (projection_counts ->> 'combined')::BIGINT;
        signal_event_count := (projection ->> 'signal_event_count')::BIGINT;
        IF signal_event_count IS DISTINCT FROM (
            count_max_pain + count_magnet + count_combined
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                MESSAGE = 'Stage-4 projection count total is inconsistent';
        END IF;

        target_snapshot_set_id := (
            projection ->> 'snapshot_set_id'
        )::BIGINT;
        BEGIN
            IF projection ->> 'available_at_utc' IS NULL
               OR projection ->> 'available_at_utc'
                    !~ '(Z|[+-][0-9]{2}:[0-9]{2})$'
               OR projection ->> 'decision_time_utc' IS NULL
               OR projection ->> 'decision_time_utc'
                    !~ '(Z|[+-][0-9]{2}:[0-9]{2})$'
            THEN
                RAISE EXCEPTION USING
                    ERRCODE = '22007',
                    MESSAGE = 'projection timestamp lacks an explicit timezone';
            END IF;
            available_at_utc := (
                projection ->> 'available_at_utc'
            )::TIMESTAMPTZ;
            decision_time_utc := (
                projection ->> 'decision_time_utc'
            )::TIMESTAMPTZ;
        EXCEPTION
            WHEN invalid_datetime_format OR datetime_field_overflow THEN
                RAISE EXCEPTION USING
                    ERRCODE = '23514',
                    MESSAGE = 'invalid Stage-4 projection timestamps';
        END;

        IF decision_time_utc IS DISTINCT FROM candidate.alert_time_utc THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                MESSAGE = 'Stage-4 projection decision time is not event time';
        END IF;

        SELECT
            BTRIM(archive_set.snapshot_key),
            BTRIM(archive_set.payload_sha256),
            archive_set.available_at_utc,
            archive_set.cycle_id,
            archive_set.cycle_time_utc,
            archive_set.collector_version,
            archive_set.source,
            archive_set.research_eligible
        INTO
            archive_snapshot_key,
            archive_payload_sha256,
            archive_available_at_utc,
            archive_cycle_id,
            archive_cycle_time_utc,
            archive_collector_version,
            archive_source,
            archive_research_eligible
        FROM public.research_max_pain_snapshot_sets archive_set
        WHERE archive_set.snapshot_set_id
                = target_snapshot_set_id;

        IF NOT FOUND
           OR archive_snapshot_key IS DISTINCT FROM projection ->> 'snapshot_key'
           OR archive_payload_sha256
                IS DISTINCT FROM projection ->> 'set_payload_sha256'
           OR archive_available_at_utc IS DISTINCT FROM available_at_utc
           OR archive_source IS DISTINCT FROM 'RESEARCH_PASSIVE'
           OR archive_research_eligible IS DISTINCT FROM TRUE
        THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                MESSAGE = 'Stage-4 projection archive provenance mismatch';
        END IF;

        IF EXISTS (
            SELECT 1
            FROM JSONB_ARRAY_ELEMENTS(projection -> 'eligible_symbols') item
            WHERE JSONB_TYPEOF(item) IS DISTINCT FROM 'string'
               OR item #>> '{}' !~ '^[A-Z0-9-]{1,20}$'
        )
           OR (
                SELECT COUNT(*)
                FROM JSONB_ARRAY_ELEMENTS_TEXT(
                    projection -> 'eligible_symbols'
                ) item
           ) IS DISTINCT FROM (
                SELECT COUNT(DISTINCT item)
                FROM JSONB_ARRAY_ELEMENTS_TEXT(
                    projection -> 'eligible_symbols'
                ) item
           )
           OR EXISTS (
                SELECT 1
                FROM public.research_max_pain_snapshot_symbols archive_symbol
                WHERE archive_symbol.snapshot_set_id = target_snapshot_set_id
                  AND archive_symbol.research_eligible = TRUE
                  AND NOT (
                      projection -> 'eligible_symbols' ? archive_symbol.symbol
                  )
           )
           OR EXISTS (
                SELECT 1
                FROM JSONB_ARRAY_ELEMENTS_TEXT(
                    projection -> 'eligible_symbols'
                ) projected_symbol
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM public.research_max_pain_snapshot_symbols archive_symbol
                    WHERE archive_symbol.snapshot_set_id = target_snapshot_set_id
                      AND archive_symbol.symbol = projected_symbol
                      AND archive_symbol.research_eligible = TRUE
                )
           )
        THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                MESSAGE = 'Stage-4 projection eligible-symbol provenance mismatch';
        END IF;

        eligible_symbol_count := JSONB_ARRAY_LENGTH(
            projection -> 'eligible_symbols'
        );
        IF JSONB_ARRAY_LENGTH(projection -> 'symbol_evaluations')
                IS DISTINCT FROM eligible_symbol_count
           OR EXISTS (
                SELECT 1
                FROM JSONB_ARRAY_ELEMENTS(
                    projection -> 'symbol_evaluations'
                ) item
                WHERE JSONB_TYPEOF(item) IS DISTINCT FROM 'object'
                   OR NOT item ?& ARRAY[
                        'symbol', 'status', 'reason'
                   ]::TEXT[]
                   OR item - ARRAY[
                        'symbol', 'status', 'reason'
                   ]::TEXT[] <> '{}'::JSONB
                   OR JSONB_TYPEOF(item -> 'symbol') IS DISTINCT FROM 'string'
                   OR item ->> 'symbol' !~ '^[A-Z0-9-]{1,20}$'
                   OR JSONB_TYPEOF(item -> 'status') IS DISTINCT FROM 'string'
                   OR item ->> 'status' NOT IN ('EVALUABLE', 'UNEVALUABLE')
                   OR (
                        item ->> 'status' = 'EVALUABLE'
                        AND item -> 'reason' IS DISTINCT FROM 'null'::JSONB
                   )
                   OR (
                        item ->> 'status' = 'UNEVALUABLE'
                        AND (
                            JSONB_TYPEOF(item -> 'reason')
                                IS DISTINCT FROM 'string'
                            OR item ->> 'reason' NOT IN (
                                'DERIVATIVES_SNAPSHOT_MISSING',
                                'DERIVATIVES_SNAPSHOT_INVALID',
                                'PRICE_OI_UNAVAILABLE',
                                'PRICE_OI_STALE',
                                'FUTURES_CVD_UNAVAILABLE',
                                'MISSED_CAUSAL_WINDOW'
                            )
                        )
                   )
                   OR NOT (
                        projection -> 'eligible_symbols' ? (item ->> 'symbol')
                   )
           )
           OR EXISTS (
                SELECT 1
                FROM (
                    SELECT
                        item ->> 'symbol' AS symbol,
                        LAG(item ->> 'symbol') OVER (ORDER BY ordinal) AS prior
                    FROM JSONB_ARRAY_ELEMENTS(
                        projection -> 'symbol_evaluations'
                    ) WITH ORDINALITY AS evaluation(item, ordinal)
                ) ordered
                WHERE ordered.prior COLLATE pg_catalog."C"
                    >= ordered.symbol COLLATE pg_catalog."C"
           )
           OR EXISTS (
                SELECT 1
                FROM (
                    SELECT
                        symbol,
                        LAG(symbol) OVER (ORDER BY ordinal) AS prior
                    FROM JSONB_ARRAY_ELEMENTS_TEXT(
                        projection -> 'eligible_symbols'
                    ) WITH ORDINALITY AS eligible(symbol, ordinal)
                ) ordered
                WHERE ordered.prior COLLATE pg_catalog."C"
                    >= ordered.symbol COLLATE pg_catalog."C"
           )
           OR EXISTS (
                SELECT 1
                FROM JSONB_ARRAY_ELEMENTS_TEXT(
                    projection -> 'eligible_symbols'
                ) eligible(symbol)
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM JSONB_ARRAY_ELEMENTS(
                        projection -> 'symbol_evaluations'
                    ) evaluation
                    WHERE evaluation ->> 'symbol' = eligible.symbol
                )
           )
        THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                MESSAGE = 'invalid Stage-4 per-symbol evaluation partition';
        END IF;

        SELECT COUNT(*) FILTER (
            WHERE item ->> 'status' = 'EVALUABLE'
        )
        INTO evaluable_symbol_count
        FROM JSONB_ARRAY_ELEMENTS(
            projection -> 'symbol_evaluations'
        ) item;

        IF projection_status = 'MISSED_CAUSAL_WINDOW' THEN
            IF projection ->> 'evaluation_status' IS DISTINCT FROM 'UNEVALUABLE'
               OR evaluable_symbol_count <> 0
               OR EXISTS (
                    SELECT 1
                    FROM JSONB_ARRAY_ELEMENTS(
                        projection -> 'symbol_evaluations'
                    ) item
                    WHERE item ->> 'reason' IS DISTINCT FROM 'MISSED_CAUSAL_WINDOW'
               )
            THEN
                RAISE EXCEPTION USING
                    ERRCODE = '23514',
                    MESSAGE = 'invalid Stage-4 missed symbol evaluation partition';
            END IF;
        ELSIF projection ->> 'evaluation_status' IS DISTINCT FROM (
            CASE
                WHEN evaluable_symbol_count = eligible_symbol_count
                    THEN 'EVALUABLE'
                WHEN evaluable_symbol_count > 0 THEN 'PARTIAL'
                ELSE 'UNEVALUABLE'
            END
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                MESSAGE = 'invalid Stage-4 completed evaluation status';
        END IF;

        IF projection_status = 'COMPLETED' THEN
            BEGIN
                IF projection ->> 'derivatives_read_started_at_utc' IS NULL
                   OR projection ->> 'derivatives_read_started_at_utc'
                        !~ '(Z|[+-][0-9]{2}:[0-9]{2})$'
                   OR projection ->> 'derivatives_read_completed_at_utc' IS NULL
                   OR projection ->> 'derivatives_read_completed_at_utc'
                        !~ '(Z|[+-][0-9]{2}:[0-9]{2})$'
                THEN
                    RAISE EXCEPTION USING
                        ERRCODE = '22007',
                        MESSAGE = 'completed-read timestamp lacks an explicit timezone';
                END IF;
                read_started_at_utc := (
                    projection ->> 'derivatives_read_started_at_utc'
                )::TIMESTAMPTZ;
                read_completed_at_utc := (
                    projection ->> 'derivatives_read_completed_at_utc'
                )::TIMESTAMPTZ;
            EXCEPTION
                WHEN invalid_datetime_format OR datetime_field_overflow THEN
                    RAISE EXCEPTION USING
                        ERRCODE = '23514',
                        MESSAGE = 'invalid Stage-4 completed-read timestamps';
            END;
            IF projection -> 'reason' IS DISTINCT FROM 'null'::JSONB
               OR available_at_utc > read_started_at_utc
               OR read_started_at_utc > read_completed_at_utc
               OR read_completed_at_utc > decision_time_utc
               OR decision_time_utc > available_at_utc + INTERVAL '15 minutes'
            THEN
                RAISE EXCEPTION USING
                    ERRCODE = '23514',
                    MESSAGE = 'invalid Stage-4 completed projection causality';
            END IF;
        ELSE
            IF JSONB_TYPEOF(projection -> 'reason') IS DISTINCT FROM 'string'
               OR BTRIM(projection ->> 'reason') = ''
               OR projection -> 'derivatives_read_started_at_utc'
                    IS DISTINCT FROM 'null'::JSONB
               OR projection -> 'derivatives_read_completed_at_utc'
                    IS DISTINCT FROM 'null'::JSONB
               OR count_max_pain <> 0
               OR count_magnet <> 0
               OR count_combined <> 0
               OR signal_event_count <> 0
               OR decision_time_utc <= available_at_utc + INTERVAL '15 minutes'
            THEN
                RAISE EXCEPTION USING
                    ERRCODE = '23514',
                    MESSAGE = 'invalid Stage-4 missed projection causality';
            END IF;
        END IF;

        RETURN;
    END IF;

    -- Signal events carry the same frozen archive and derivative-read
    -- provenance.  Their small metadata object is exact for contract v1.
    IF NOT signal_snapshot ?& ARRAY[
            'contract_version', 'signal_family', 'tier', 'decision_time_utc',
            'archive_reference', 'derivatives_reference',
            'dependency_lineage', 'formula_authorized', 'outcome_authorized',
            'telegram_delivery_allowed', 'trade_execution_allowed'
        ]::TEXT[]
       OR signal_snapshot - ARRAY[
            'contract_version', 'signal_family', 'tier', 'decision_time_utc',
            'archive_reference', 'derivatives_reference',
            'dependency_lineage', 'formula_authorized', 'outcome_authorized',
            'telegram_delivery_allowed', 'trade_execution_allowed'
        ]::TEXT[] <> '{}'::JSONB
       OR JSONB_TYPEOF(signal_snapshot -> 'archive_reference')
            IS DISTINCT FROM 'object'
       OR JSONB_TYPEOF(signal_snapshot -> 'derivatives_reference')
            IS DISTINCT FROM 'object'
       OR JSONB_TYPEOF(signal_snapshot -> 'dependency_lineage')
            IS DISTINCT FROM 'object'
       OR candidate.symbol !~ '^[A-Z0-9-]{1,20}$'
       OR candidate.direction NOT IN ('LONG', 'SHORT')
       OR candidate.symbol = 'RESEARCH'
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'invalid Stage-4 signal event shape';
    END IF;

    IF signal_snapshot -> 'dependency_lineage' IS DISTINCT FROM (CASE expected_family
        WHEN 'MAX_PAIN' THEN JSONB_BUILD_OBJECT(
            'logical_engine', 'MAX_PAIN_SCORE_AND_CONFIRMATION',
            'raw_sources', JSONB_BUILD_ARRAY('COINGLASS_MAX_PAIN'),
            'qualification_dependencies',
                JSONB_BUILD_ARRAY('PRICE_OI', 'FUTURES_CVD')
        )
        WHEN 'MAGNET' THEN JSONB_BUILD_OBJECT(
            'logical_engine', 'MAGNET_V1',
            'raw_sources', JSONB_BUILD_ARRAY('COINGLASS_MAX_PAIN'),
            'qualification_dependencies',
                JSONB_BUILD_ARRAY('PRICE_OI', 'FUTURES_CVD')
        )
        ELSE '{}'::JSONB
    END) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'invalid Stage-4 signal dependency lineage';
    END IF;

    IF candidate.event_type = 'MAX_PAIN_CONFIRMATION_STATE' THEN
        IF NOT candidate.engine_snapshot ?& ARRAY[
                'alert_side', 'distance_pct', 'score_components',
                'average_score_all_timeframes',
                'opposite_average_score_all_timeframes',
                'directional_scores_all_timeframes', 'opposite_score',
                'directional_edge', 'consensus_hits', 'consensus_total',
                'gap_consensus_supporting', 'gap_consensus_total',
                'near_share_pct', 'near_amount', 'far_amount',
                'cluster_candidate_count', 'cluster_members', 'cluster_count',
                'cluster_same_direction_count', 'component_sum_check',
                'calculation_validation_errors', 'duplicate_rows_removed',
                'balance', 'cluster', 'gap', 'maxpain_confirmation',
                'market_evidence', 'price_source', 'price_pair',
                'signal_snapshot'
           ]::TEXT[]
           OR candidate.engine_snapshot - ARRAY[
                'alert_side', 'distance_pct', 'score_components',
                'average_score_all_timeframes',
                'opposite_average_score_all_timeframes',
                'directional_scores_all_timeframes', 'opposite_score',
                'directional_edge', 'consensus_hits', 'consensus_total',
                'gap_consensus_supporting', 'gap_consensus_total',
                'near_share_pct', 'near_amount', 'far_amount',
                'cluster_candidate_count', 'cluster_members', 'cluster_count',
                'cluster_same_direction_count', 'component_sum_check',
                'calculation_validation_errors', 'duplicate_rows_removed',
                'balance', 'cluster', 'gap', 'maxpain_confirmation',
                'market_evidence', 'price_source', 'price_pair',
                'signal_snapshot'
           ]::TEXT[] <> '{}'::JSONB
           OR JSONB_TYPEOF(candidate.engine_snapshot -> 'price_source')
                IS DISTINCT FROM 'string'
           OR JSONB_TYPEOF(candidate.engine_snapshot -> 'price_pair')
                IS DISTINCT FROM 'string'
           OR BTRIM(candidate.engine_snapshot ->> 'price_source') = ''
           OR BTRIM(candidate.engine_snapshot ->> 'price_pair') = ''
           OR candidate.engine_snapshot ->> 'price_source'
                IS DISTINCT FROM signal_snapshot #>>
                    '{archive_reference,official_price,source}'
           OR candidate.engine_snapshot ->> 'price_pair'
                IS DISTINCT FROM signal_snapshot #>>
                    '{archive_reference,official_price,pair}'
           OR signal_snapshot ->> 'tier' IS NULL
           OR signal_snapshot ->> 'tier' NOT IN (
                'CONFIRMED', 'STRONG_CONFIRMED'
           )
           OR NOT candidate.categories @> JSONB_BUILD_ARRAY(
               signal_snapshot ->> 'tier'
           )
           OR candidate.categories ? (CASE signal_snapshot ->> 'tier'
                WHEN 'CONFIRMED' THEN 'STRONG_CONFIRMED'
                ELSE 'CONFIRMED'
              END)
           OR candidate.source_side NOT IN ('LONG', 'SHORT')
           OR candidate.direction IS DISTINCT FROM (CASE candidate.source_side
                WHEN 'SHORT' THEN 'LONG'
                WHEN 'LONG' THEN 'SHORT'
              END)
           OR candidate.timeframe IS NULL
           OR candidate.timeframe NOT IN (
               '12h', '24h', '48h', '3d', '1w', '2w', '1m'
           )
           OR candidate.score IS NULL
           OR candidate.current_price IS NULL
           OR candidate.target_price IS NULL
           OR candidate.initial_target_distance_pct IS NULL
           OR candidate.score NOT BETWEEN 65.0 AND 100.0
           OR candidate.engine_snapshot ->> 'alert_side'
                IS DISTINCT FROM candidate.source_side
           OR JSONB_TYPEOF(candidate.engine_snapshot -> 'score_components')
                IS DISTINCT FROM 'object'
           OR NOT (candidate.engine_snapshot -> 'score_components') ?& ARRAY[
                'directional_alignment', 'target_proximity',
                'cluster_confidence', 'relative_gap', 'consensus',
                'consensus_max', 'target_clustering', 'cluster_density',
                'cluster_coverage', 'cluster_liquidity_growth',
                'cluster_liquidity_multiplier'
              ]::TEXT[]
           OR (candidate.engine_snapshot -> 'score_components') - ARRAY[
                'directional_alignment', 'target_proximity',
                'cluster_confidence', 'relative_gap', 'consensus',
                'consensus_max', 'target_clustering', 'cluster_density',
                'cluster_coverage', 'cluster_liquidity_growth',
                'cluster_liquidity_multiplier'
              ]::TEXT[] <> '{}'::JSONB
           OR EXISTS (
                SELECT 1
                FROM JSONB_EACH(
                    candidate.engine_snapshot -> 'score_components'
                ) component
                WHERE public.research_signal_snapshot_v1_finite_number(
                    component.value
                ) IS NULL
           )
           OR public.research_signal_snapshot_v1_finite_number(
                candidate.engine_snapshot #>
                    '{score_components,directional_alignment}'
              ) NOT BETWEEN 0.0 AND 30.0
           OR public.research_signal_snapshot_v1_finite_number(
                candidate.engine_snapshot #> '{score_components,target_proximity}'
              ) NOT BETWEEN 0.0 AND 25.0
           OR public.research_signal_snapshot_v1_finite_number(
                candidate.engine_snapshot #>
                    '{score_components,cluster_confidence}'
              ) NOT BETWEEN 0.0 AND 30.0
           OR public.research_signal_snapshot_v1_finite_number(
                candidate.engine_snapshot #> '{score_components,relative_gap}'
              ) NOT BETWEEN 0.0 AND 15.0
           OR NOT public.research_signal_snapshot_v1_nonnegative_integer(
                candidate.engine_snapshot -> 'consensus_hits'
              )
           OR NOT public.research_signal_snapshot_v1_nonnegative_integer(
                candidate.engine_snapshot -> 'consensus_total'
              )
           OR public.research_signal_snapshot_v1_finite_number(
                candidate.engine_snapshot -> 'consensus_hits'
              ) > public.research_signal_snapshot_v1_finite_number(
                candidate.engine_snapshot -> 'consensus_total'
              )
           OR public.research_signal_snapshot_v1_finite_number(
                candidate.engine_snapshot -> 'consensus_total'
              ) > 7
           OR NOT public.research_signal_snapshot_v1_nonnegative_integer(
                candidate.engine_snapshot -> 'gap_consensus_supporting'
              )
           OR NOT public.research_signal_snapshot_v1_nonnegative_integer(
                candidate.engine_snapshot -> 'gap_consensus_total'
              )
           OR public.research_signal_snapshot_v1_finite_number(
                candidate.engine_snapshot -> 'gap_consensus_supporting'
              ) > public.research_signal_snapshot_v1_finite_number(
                candidate.engine_snapshot -> 'gap_consensus_total'
              )
           OR public.research_signal_snapshot_v1_finite_number(
                candidate.engine_snapshot -> 'gap_consensus_total'
              ) > 7
           OR NOT public.research_signal_snapshot_v1_nonnegative_integer(
                candidate.engine_snapshot -> 'cluster_count'
              )
           OR NOT public.research_signal_snapshot_v1_nonnegative_integer(
                candidate.engine_snapshot -> 'cluster_same_direction_count'
              )
           OR public.research_signal_snapshot_v1_finite_number(
                candidate.engine_snapshot -> 'cluster_count'
              ) > public.research_signal_snapshot_v1_finite_number(
                candidate.engine_snapshot -> 'cluster_same_direction_count'
              )
           OR public.research_signal_snapshot_v1_finite_number(
                candidate.engine_snapshot -> 'cluster_same_direction_count'
              ) > 7
           OR candidate.engine_snapshot -> 'duplicate_rows_removed'
                IS DISTINCT FROM '0'::JSONB
           OR ROUND((
                public.research_signal_snapshot_v1_finite_number(
                    candidate.engine_snapshot #>
                        '{score_components,directional_alignment}'
                )
                + public.research_signal_snapshot_v1_finite_number(
                    candidate.engine_snapshot #>
                        '{score_components,target_proximity}'
                )
                + public.research_signal_snapshot_v1_finite_number(
                    candidate.engine_snapshot #>
                        '{score_components,cluster_confidence}'
                )
                + public.research_signal_snapshot_v1_finite_number(
                    candidate.engine_snapshot #>
                        '{score_components,relative_gap}'
                )
              )::NUMERIC, 2)::DOUBLE PRECISION
                IS DISTINCT FROM candidate.score
           OR candidate.engine_snapshot #> '{score_components,consensus}'
                IS DISTINCT FROM candidate.engine_snapshot #>
                    '{score_components,directional_alignment}'
           OR candidate.engine_snapshot #> '{score_components,consensus_max}'
                IS DISTINCT FROM '30'::JSONB
           OR candidate.engine_snapshot #> '{score_components,target_clustering}'
                IS DISTINCT FROM candidate.engine_snapshot #>
                    '{score_components,cluster_confidence}'
           OR candidate.engine_snapshot #> '{score_components,cluster_confidence}'
                IS DISTINCT FROM candidate.engine_snapshot #> '{cluster,points}'
           OR candidate.engine_snapshot #> '{score_components,relative_gap}'
                IS DISTINCT FROM candidate.engine_snapshot #> '{gap,points}'
           OR candidate.engine_snapshot #> '{score_components,cluster_density}'
                IS DISTINCT FROM candidate.engine_snapshot #>
                    '{cluster,density_points}'
           OR candidate.engine_snapshot #> '{score_components,cluster_coverage}'
                IS DISTINCT FROM candidate.engine_snapshot #>
                    '{cluster,coverage_points}'
           OR candidate.engine_snapshot #>
                    '{score_components,cluster_liquidity_growth}'
                IS DISTINCT FROM candidate.engine_snapshot #>
                    '{cluster,growth_points}'
           OR candidate.engine_snapshot #>
                    '{score_components,cluster_liquidity_multiplier}'
                IS DISTINCT FROM candidate.engine_snapshot #>
                    '{cluster,liquidity_multiplier}'
           OR candidate.engine_snapshot -> 'near_share_pct'
                IS DISTINCT FROM candidate.engine_snapshot #>
                    '{balance,near_share_pct}'
           OR candidate.engine_snapshot -> 'cluster_members'
                IS DISTINCT FROM candidate.engine_snapshot #> '{cluster,members}'
           OR candidate.engine_snapshot -> 'cluster_count'
                IS DISTINCT FROM candidate.engine_snapshot #> '{cluster,count}'
           OR candidate.engine_snapshot #>> '{maxpain_confirmation,status}'
                IS DISTINCT FROM signal_snapshot ->> 'tier'
           OR candidate.engine_snapshot -> 'maxpain_confirmation'
                IS DISTINCT FROM candidate.engine_snapshot #>
                    '{market_evidence,confirmation}'
           OR candidate.engine_snapshot -> 'maxpain_confirmation'
                IS DISTINCT FROM JSONB_BUILD_OBJECT(
                    'status', CASE WHEN candidate.score >= 75.0
                        THEN 'STRONG_CONFIRMED' ELSE 'CONFIRMED' END,
                    'label', CASE WHEN candidate.score >= 75.0 THEN
                        'Max Pain Strong Confirmation — ציון 75+ עם Price+OI + Futures'
                        ELSE
                        'Max Pain Confirmed — ציון 65–74.99 עם Price+OI + Futures'
                    END,
                    'score_threshold', 65.0,
                    'strong_score_threshold', 75.0,
                    'score_ok', TRUE,
                    'score_confirmation', TRUE,
                    'strong_score_ok', candidate.score >= 75.0,
                    'early_shift_opposes', FALSE,
                    'oi_opposes', FALSE,
                    'supporting_families', 2,
                    'opposing_families', 0,
                    'strong_core', TRUE,
                    'strong_evidence_threshold', 25.0
                )
           OR candidate.engine_snapshot #>>
                '{market_evidence,confirmation,status}'
                IS DISTINCT FROM signal_snapshot ->> 'tier'
           OR candidate.engine_snapshot #>>
                '{market_evidence,expected_price_direction}'
                IS DISTINCT FROM (CASE candidate.direction
                    WHEN 'LONG' THEN 'BULLISH' ELSE 'BEARISH'
                END)
           OR candidate.engine_snapshot #>
                '{market_evidence,modules,positioning,available}'
                IS DISTINCT FROM 'true'::JSONB
           OR candidate.engine_snapshot #>
                '{market_evidence,modules,futures_flow,available}'
                IS DISTINCT FROM 'true'::JSONB
           OR candidate.engine_snapshot #>>
                '{market_evidence,modules,positioning,direction}'
                IS DISTINCT FROM (CASE candidate.direction
                    WHEN 'LONG' THEN 'BULLISH' ELSE 'BEARISH'
                END)
           OR candidate.engine_snapshot #>>
                '{market_evidence,modules,futures_flow,direction}'
                IS DISTINCT FROM (CASE candidate.direction
                    WHEN 'LONG' THEN 'BULLISH' ELSE 'BEARISH'
                END)
           OR candidate.engine_snapshot #>>
                '{market_evidence,modules,positioning,relation}'
                IS DISTINCT FROM 'SUPPORT'
           OR candidate.engine_snapshot #>>
                '{market_evidence,modules,futures_flow,relation}'
                IS DISTINCT FROM 'SUPPORT'
           OR public.research_signal_snapshot_v1_finite_number(
                candidate.engine_snapshot #>
                    '{market_evidence,modules,positioning,score}'
              ) IS NULL
           OR public.research_signal_snapshot_v1_finite_number(
                candidate.engine_snapshot #>
                    '{market_evidence,modules,futures_flow,score}'
              ) IS NULL
           OR ABS(public.research_signal_snapshot_v1_finite_number(
                candidate.engine_snapshot #>
                    '{market_evidence,modules,positioning,score}'
              )) < 25.0
           OR ABS(public.research_signal_snapshot_v1_finite_number(
                candidate.engine_snapshot #>
                    '{market_evidence,modules,positioning,score}'
              )) > 100.0
           OR ABS(public.research_signal_snapshot_v1_finite_number(
                candidate.engine_snapshot #>
                    '{market_evidence,modules,futures_flow,score}'
              )) < 25.0
           OR ABS(public.research_signal_snapshot_v1_finite_number(
                candidate.engine_snapshot #>
                    '{market_evidence,modules,futures_flow,score}'
              )) > 100.0
           OR SIGN(public.research_signal_snapshot_v1_finite_number(
                candidate.engine_snapshot #>
                    '{market_evidence,modules,positioning,score}'
              )) IS DISTINCT FROM (CASE candidate.direction
                    WHEN 'LONG' THEN 1.0 ELSE -1.0
              END)
           OR SIGN(public.research_signal_snapshot_v1_finite_number(
                candidate.engine_snapshot #>
                    '{market_evidence,modules,futures_flow,score}'
              )) IS DISTINCT FROM (CASE candidate.direction
                    WHEN 'LONG' THEN 1.0 ELSE -1.0
              END)
           OR (candidate.engine_snapshot #>> '{market_evidence,maxpain_score}')
                ::DOUBLE PRECISION IS DISTINCT FROM candidate.score
           OR (candidate.engine_snapshot ->> 'component_sum_check')
                ::DOUBLE PRECISION IS DISTINCT FROM candidate.score
           OR candidate.engine_snapshot -> 'calculation_validation_errors'
                <> '[]'::JSONB
           OR (candidate.categories ? 'TARGET_CLUSTER') IS DISTINCT FROM (
                COALESCE(
                    (candidate.engine_snapshot #>> '{cluster,points}')
                        ::DOUBLE PRECISION >= 18.0,
                    FALSE
                )
              )
           OR (candidate.categories ? 'RELATIVE_GAP_ADVANTAGE')
                IS DISTINCT FROM (
                    COALESCE(
                        (candidate.engine_snapshot #>> '{gap,advantage}')
                            ::DOUBLE PRECISION >= 0.40,
                        FALSE
                    )
                )
           OR (candidate.categories ? 'LIQUIDITY_BALANCE_SUPPORT')
                IS DISTINCT FROM (
                    COALESCE(
                        (candidate.engine_snapshot #>> '{balance,near_share_pct}')
                            ::DOUBLE PRECISION >= 60.0,
                        FALSE
                    )
                )
           OR (
                signal_snapshot ->> 'tier' = 'CONFIRMED'
                AND candidate.score >= 75.0
           )
           OR (
                signal_snapshot ->> 'tier' = 'STRONG_CONFIRMED'
                AND candidate.score < 75.0
           )
           OR EXISTS (
                SELECT 1
                FROM JSONB_ARRAY_ELEMENTS_TEXT(candidate.categories) category
                WHERE category NOT IN (
                    'DECISION_SAMPLE', 'SILENT',
                    'CONFIRMED', 'STRONG_CONFIRMED',
                    'NEAR_MAX_PAIN', 'TARGET_CLUSTER',
                    'RELATIVE_GAP_ADVANTAGE',
                    'LIQUIDITY_BALANCE_SUPPORT'
                )
           )
        THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                MESSAGE = 'invalid Stage-4 Max-Pain confirmation shape';
        END IF;
    ELSIF candidate.event_type = 'MAGNET_CONFIRMATION_STATE' THEN
        IF NOT candidate.engine_snapshot ?& ARRAY[
                'magnet', 'magnet_confirmation', 'market_evidence',
                'price_source', 'price_pair', 'signal_snapshot'
           ]::TEXT[]
           OR candidate.engine_snapshot - ARRAY[
                'magnet', 'magnet_confirmation', 'market_evidence',
                'price_source', 'price_pair', 'signal_snapshot'
           ]::TEXT[] <> '{}'::JSONB
           OR JSONB_TYPEOF(candidate.engine_snapshot -> 'price_source')
                IS DISTINCT FROM 'string'
           OR JSONB_TYPEOF(candidate.engine_snapshot -> 'price_pair')
                IS DISTINCT FROM 'string'
           OR BTRIM(candidate.engine_snapshot ->> 'price_source') = ''
           OR BTRIM(candidate.engine_snapshot ->> 'price_pair') = ''
           OR candidate.engine_snapshot ->> 'price_source'
                IS DISTINCT FROM signal_snapshot #>>
                    '{archive_reference,official_price,source}'
           OR candidate.engine_snapshot ->> 'price_pair'
                IS DISTINCT FROM signal_snapshot #>>
                    '{archive_reference,official_price,pair}'
           OR signal_snapshot ->> 'tier' IS NULL
           OR signal_snapshot ->> 'tier' NOT IN (
                'CONFIRMED', 'STRONG_CONFIRMED'
           )
           OR NOT candidate.categories @> JSONB_BUILD_ARRAY(
               signal_snapshot ->> 'tier'
           )
           OR candidate.source_side NOT IN ('UPPER', 'LOWER')
           OR candidate.direction IS DISTINCT FROM (CASE candidate.source_side
                WHEN 'UPPER' THEN 'LONG'
                WHEN 'LOWER' THEN 'SHORT'
              END)
           OR candidate.timeframe IS NOT NULL
           OR candidate.score IS NULL
           OR candidate.current_price IS NULL
           OR candidate.target_price IS NULL
           OR candidate.initial_target_distance_pct IS NULL
           OR candidate.score NOT BETWEEN 60.0 AND 100.0
           OR JSONB_TYPEOF(candidate.engine_snapshot -> 'magnet')
                IS DISTINCT FROM 'object'
           OR NOT (candidate.engine_snapshot -> 'magnet') ?& ARRAY[
                'side', 'count', 'members', 'min_target', 'max_target',
                'average_target', 'spread_pct', 'magnet_quality',
                'liquidity_edge_pct', 'gross_liquidity_timeframe',
                'gross_candidate_liquidity', 'gross_opposite_liquidity',
                'liquidity_calculation_version'
              ]::TEXT[]
           OR (candidate.engine_snapshot -> 'magnet') - ARRAY[
                'side', 'count', 'members', 'min_target', 'max_target',
                'average_target', 'spread_pct', 'magnet_quality',
                'liquidity_edge_pct', 'gross_liquidity_timeframe',
                'gross_candidate_liquidity', 'gross_opposite_liquidity',
                'liquidity_calculation_version'
              ]::TEXT[] <> '{}'::JSONB
           OR NOT public.research_signal_snapshot_v1_positive_bigint(
                candidate.engine_snapshot #> '{magnet,count}'
              )
           OR public.research_signal_snapshot_v1_finite_number(
                candidate.engine_snapshot #> '{magnet,min_target}'
              ) IS NULL
           OR public.research_signal_snapshot_v1_finite_number(
                candidate.engine_snapshot #> '{magnet,max_target}'
              ) IS NULL
           OR public.research_signal_snapshot_v1_finite_number(
                candidate.engine_snapshot #> '{magnet,average_target}'
              ) IS NULL
           OR public.research_signal_snapshot_v1_finite_number(
                candidate.engine_snapshot #> '{magnet,spread_pct}'
              ) IS NULL
           OR public.research_signal_snapshot_v1_finite_number(
                candidate.engine_snapshot #> '{magnet,magnet_quality}'
              ) IS NULL
           OR public.research_signal_snapshot_v1_finite_number(
                candidate.engine_snapshot #> '{magnet,liquidity_edge_pct}'
              ) IS NULL
           OR public.research_signal_snapshot_v1_finite_number(
                candidate.engine_snapshot #>
                    '{magnet,gross_candidate_liquidity}'
              ) IS NULL
           OR public.research_signal_snapshot_v1_finite_number(
                candidate.engine_snapshot #>
                    '{magnet,gross_opposite_liquidity}'
              ) IS NULL
           OR candidate.engine_snapshot #>>
                    '{magnet,liquidity_calculation_version}'
                IS DISTINCT FROM 'V3_WIDEST_CLUSTER_MEMBER_NO_DISTANCE'
           OR public.research_signal_snapshot_v1_magnet_members(candidate) IS NULL
           OR JSONB_ARRAY_LENGTH(
                public.research_signal_snapshot_v1_magnet_members(candidate)
              ) < 2
           OR candidate.engine_snapshot #>> '{magnet,side}'
                IS DISTINCT FROM candidate.source_side
           OR candidate.engine_snapshot #>> '{magnet_confirmation,status}'
                IS DISTINCT FROM signal_snapshot ->> 'tier'
           OR (candidate.engine_snapshot #>> '{magnet,magnet_quality}')
                ::DOUBLE PRECISION IS DISTINCT FROM candidate.score
           OR (candidate.engine_snapshot #>>
                '{magnet_confirmation,magnet_quality}')
                ::DOUBLE PRECISION IS DISTINCT FROM candidate.score
           OR (candidate.engine_snapshot #>> '{magnet,average_target}')
                ::DOUBLE PRECISION IS DISTINCT FROM candidate.target_price
           OR (candidate.engine_snapshot #>> '{magnet,count}')::BIGINT
                IS DISTINCT FROM JSONB_ARRAY_LENGTH(
                    public.research_signal_snapshot_v1_magnet_members(candidate)
                )::BIGINT
           OR (candidate.engine_snapshot #>>
                '{magnet,liquidity_edge_pct}')::DOUBLE PRECISION
                IS DISTINCT FROM (candidate.engine_snapshot #>>
                    '{magnet_confirmation,liquidity_edge_pct}')
                    ::DOUBLE PRECISION
           OR public.research_signal_snapshot_v1_finite_number(
                candidate.engine_snapshot #> '{magnet,liquidity_edge_pct}'
              ) <= -10.0
           OR candidate.engine_snapshot #>>
                    '{magnet_confirmation,liquidity_status}'
                IS DISTINCT FROM (CASE
                    WHEN public.research_signal_snapshot_v1_finite_number(
                        candidate.engine_snapshot #>
                            '{magnet,liquidity_edge_pct}'
                    ) >= 10.0 THEN 'SUPPORT'
                    ELSE 'NEUTRAL'
                END)
           OR candidate.engine_snapshot #>>
                    '{magnet_confirmation,liquidity_label}'
                IS DISTINCT FROM (CASE
                    WHEN public.research_signal_snapshot_v1_finite_number(
                        candidate.engine_snapshot #>
                            '{magnet,liquidity_edge_pct}'
                    ) >= 10.0 THEN 'נזילות תומכת במגנט'
                    ELSE 'נזילות מאוזנת / ניטרלית'
                END)
           OR signal_snapshot ->> 'tier' IS DISTINCT FROM (CASE
                WHEN candidate.score >= 75.0
                 AND public.research_signal_snapshot_v1_finite_number(
                    candidate.engine_snapshot #>
                        '{magnet,liquidity_edge_pct}'
                 ) >= 10.0 THEN 'STRONG_CONFIRMED'
                ELSE 'CONFIRMED'
              END)
           OR candidate.engine_snapshot #>> '{magnet_confirmation,label}'
                IS DISTINCT FROM (CASE
                    WHEN candidate.score >= 75.0
                     AND public.research_signal_snapshot_v1_finite_number(
                        candidate.engine_snapshot #>
                            '{magnet,liquidity_edge_pct}'
                     ) >= 10.0 THEN '🔥 Strong Magnet Confirmation'
                    ELSE '✅ Magnet Confirmed'
                END)
           OR JSONB_TYPEOF(
                candidate.engine_snapshot -> 'magnet_confirmation'
              ) IS DISTINCT FROM 'object'
           OR NOT (candidate.engine_snapshot -> 'magnet_confirmation') ?& ARRAY[
                'status', 'label', 'magnet_quality', 'liquidity_edge_pct',
                'liquidity_status', 'liquidity_label', 'derivatives'
              ]::TEXT[]
           OR (candidate.engine_snapshot -> 'magnet_confirmation') - ARRAY[
                'status', 'label', 'magnet_quality', 'liquidity_edge_pct',
                'liquidity_status', 'liquidity_label', 'derivatives'
              ]::TEXT[] <> '{}'::JSONB
           OR candidate.engine_snapshot #> '{magnet_confirmation,derivatives}'
                IS DISTINCT FROM JSONB_BUILD_OBJECT(
                    'status', 'CONFIRMED',
                    'label', 'Price+OI + Futures CVD מאשרים',
                    'supporting_families', 2,
                    'opposing_families', 0,
                    'early_shift_opposes', FALSE,
                    'oi_opposes', FALSE,
                    'strong_core', TRUE,
                    'positioning_score', ROUND(ABS(
                        public.research_signal_snapshot_v1_finite_number(
                            candidate.engine_snapshot #>
                                '{market_evidence,modules,positioning,score}'
                        )
                    )::NUMERIC, 4)::DOUBLE PRECISION,
                    'futures_score', ROUND(ABS(
                        public.research_signal_snapshot_v1_finite_number(
                            candidate.engine_snapshot #>
                                '{market_evidence,modules,futures_flow,score}'
                        )
                    )::NUMERIC, 4)::DOUBLE PRECISION,
                    'minimum_engine_score', 25.0
                )
           OR candidate.engine_snapshot #>>
                    '{market_evidence,expected_price_direction}'
                IS DISTINCT FROM (CASE candidate.direction
                    WHEN 'LONG' THEN 'BULLISH' ELSE 'BEARISH'
                END)
           OR candidate.engine_snapshot #>>
                    '{market_evidence,modules,positioning,relation}'
                IS DISTINCT FROM 'SUPPORT'
           OR candidate.engine_snapshot #>>
                    '{market_evidence,modules,futures_flow,relation}'
                IS DISTINCT FROM 'SUPPORT'
           OR candidate.engine_snapshot #>
                    '{market_evidence,modules,positioning,available}'
                IS DISTINCT FROM 'true'::JSONB
           OR candidate.engine_snapshot #>
                    '{market_evidence,modules,futures_flow,available}'
                IS DISTINCT FROM 'true'::JSONB
           OR candidate.engine_snapshot #>>
                    '{market_evidence,modules,positioning,direction}'
                IS DISTINCT FROM (CASE candidate.direction
                    WHEN 'LONG' THEN 'BULLISH' ELSE 'BEARISH'
                END)
           OR candidate.engine_snapshot #>>
                    '{market_evidence,modules,futures_flow,direction}'
                IS DISTINCT FROM (CASE candidate.direction
                    WHEN 'LONG' THEN 'BULLISH' ELSE 'BEARISH'
                END)
           OR public.research_signal_snapshot_v1_finite_number(
                candidate.engine_snapshot #>
                    '{market_evidence,modules,positioning,score}'
              ) IS NULL
           OR public.research_signal_snapshot_v1_finite_number(
                candidate.engine_snapshot #>
                    '{market_evidence,modules,futures_flow,score}'
              ) IS NULL
           OR ABS(public.research_signal_snapshot_v1_finite_number(
                candidate.engine_snapshot #>
                    '{market_evidence,modules,positioning,score}'
              )) < 25.0
           OR ABS(public.research_signal_snapshot_v1_finite_number(
                candidate.engine_snapshot #>
                    '{market_evidence,modules,positioning,score}'
              )) > 100.0
           OR ABS(public.research_signal_snapshot_v1_finite_number(
                candidate.engine_snapshot #>
                    '{market_evidence,modules,futures_flow,score}'
              )) < 25.0
           OR ABS(public.research_signal_snapshot_v1_finite_number(
                candidate.engine_snapshot #>
                    '{market_evidence,modules,futures_flow,score}'
              )) > 100.0
           OR SIGN(public.research_signal_snapshot_v1_finite_number(
                candidate.engine_snapshot #>
                    '{market_evidence,modules,positioning,score}'
              )) IS DISTINCT FROM (CASE candidate.direction
                    WHEN 'LONG' THEN 1.0 ELSE -1.0
              END)
           OR SIGN(public.research_signal_snapshot_v1_finite_number(
                candidate.engine_snapshot #>
                    '{market_evidence,modules,futures_flow,score}'
              )) IS DISTINCT FROM (CASE candidate.direction
                    WHEN 'LONG' THEN 1.0 ELSE -1.0
              END)
           OR candidate.engine_snapshot #>>
                '{magnet_confirmation,derivatives,status}'
                IS DISTINCT FROM 'CONFIRMED'
           OR (
                signal_snapshot ->> 'tier' = 'STRONG_CONFIRMED'
                AND (
                    candidate.score < 75.0
                    OR (candidate.engine_snapshot #>>
                        '{magnet,liquidity_edge_pct}')::DOUBLE PRECISION < 10.0
                    OR candidate.engine_snapshot #>>
                        '{magnet_confirmation,liquidity_status}'
                        IS DISTINCT FROM 'SUPPORT'
                )
           )
           OR JSONB_ARRAY_LENGTH(candidate.categories) <> 4
           OR NOT candidate.categories @> '["MAGNET"]'::JSONB
        THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                MESSAGE = 'invalid Stage-4 Magnet confirmation shape';
        END IF;
    ELSE
        IF NOT candidate.engine_snapshot ?& ARRAY[
                'signal_snapshot', 'vote_count', 'source_families',
                'indication_families', 'source_vote_policy',
                'maxpain_components', 'derivative_components',
                'magnet_component', 'spot_context', 'dependency_lineage',
                'top_item'
           ]::TEXT[]
           OR candidate.engine_snapshot - ARRAY[
                'signal_snapshot', 'vote_count', 'source_families',
                'indication_families', 'source_vote_policy',
                'maxpain_components', 'derivative_components',
                'magnet_component', 'spot_context', 'dependency_lineage',
                'top_item'
           ]::TEXT[] <> '{}'::JSONB
           OR signal_snapshot ->> 'tier' IS DISTINCT FROM 'CONFIRMED'
           OR candidate.source_side NOT IN ('LONG', 'SHORT')
           OR candidate.direction IS DISTINCT FROM (CASE candidate.source_side
                WHEN 'SHORT' THEN 'LONG'
                WHEN 'LONG' THEN 'SHORT'
              END)
           OR candidate.timeframe IS NOT NULL
           OR candidate.current_price IS NULL
           OR candidate.engine_snapshot ->> 'source_vote_policy'
                IS DISTINCT FROM 'INDEPENDENT_RAW_SOURCE_FAMILIES_V1'
           OR JSONB_TYPEOF(candidate.engine_snapshot -> 'source_families')
                IS DISTINCT FROM 'array'
           OR JSONB_TYPEOF(candidate.engine_snapshot -> 'indication_families')
                IS DISTINCT FROM 'array'
           OR JSONB_TYPEOF(candidate.engine_snapshot -> 'spot_context')
                IS DISTINCT FROM 'object'
           OR NOT (candidate.engine_snapshot -> 'spot_context') ?& ARRAY[
                'status', 'label', 'relation', 'score'
              ]::TEXT[]
           OR (candidate.engine_snapshot -> 'spot_context') - ARRAY[
                'status', 'label', 'relation', 'score'
              ]::TEXT[] <> '{}'::JSONB
           OR JSONB_TYPEOF(candidate.engine_snapshot #> '{spot_context,status}')
                IS DISTINCT FROM 'string'
           OR JSONB_TYPEOF(candidate.engine_snapshot #> '{spot_context,label}')
                IS DISTINCT FROM 'string'
           OR JSONB_TYPEOF(candidate.engine_snapshot #> '{spot_context,relation}')
                IS DISTINCT FROM 'string'
           OR candidate.engine_snapshot #>> '{spot_context,relation}'
                NOT IN ('SUPPORT', 'OPPOSE', 'NEUTRAL')
           OR public.research_signal_snapshot_v1_finite_number(
                candidate.engine_snapshot #> '{spot_context,score}'
              ) IS NULL
           OR public.research_signal_snapshot_v1_finite_number(
                candidate.engine_snapshot #> '{spot_context,score}'
              ) NOT BETWEEN -100.0 AND 100.0
           OR candidate.engine_snapshot #>> '{spot_context,status}'
                IS DISTINCT FROM (CASE candidate.engine_snapshot #>>
                    '{spot_context,relation}'
                    WHEN 'SUPPORT' THEN 'SUPPORTS'
                    WHEN 'OPPOSE' THEN 'DIVERGING'
                    ELSE 'NEUTRAL'
                END)
           OR candidate.engine_snapshot #>> '{spot_context,label}'
                IS DISTINCT FROM (CASE candidate.engine_snapshot #>>
                    '{spot_context,relation}'
                    WHEN 'SUPPORT' THEN 'Spot תומך'
                    WHEN 'OPPOSE' THEN 'Spot סותר / Divergence'
                    ELSE 'Spot ניטרלי'
                END)
           OR (
                candidate.engine_snapshot -> 'top_item' <> '{}'::JSONB
                AND candidate.engine_snapshot -> 'spot_context'
                    IS DISTINCT FROM candidate.engine_snapshot #>
                        '{top_item,market_evidence,spot_context}'
              )
           OR NOT public.research_signal_snapshot_v1_nonnegative_integer(
                candidate.engine_snapshot -> 'vote_count'
           )
           OR EXISTS (
                SELECT 1
                FROM JSONB_ARRAY_ELEMENTS_TEXT(candidate.categories) category
                WHERE category NOT IN ('DECISION_SAMPLE', 'SILENT')
                  AND NOT (
                      candidate.engine_snapshot -> 'source_families' ? category
                  )
                  AND NOT (
                      candidate.engine_snapshot -> 'indication_families' ? category
                  )
           )
        THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                MESSAGE = 'invalid Stage-4 Combined confirmation shape';
        END IF;

        IF JSONB_ARRAY_LENGTH(
                candidate.engine_snapshot -> 'source_families'
           ) < 2
           OR JSONB_ARRAY_LENGTH(
                candidate.engine_snapshot -> 'source_families'
           ) IS DISTINCT FROM (
                candidate.engine_snapshot ->> 'vote_count'
           )::BIGINT
           OR (
                SELECT COUNT(DISTINCT family)
                FROM JSONB_ARRAY_ELEMENTS_TEXT(
                    candidate.engine_snapshot -> 'source_families'
                ) family
           ) IS DISTINCT FROM JSONB_ARRAY_LENGTH(
                candidate.engine_snapshot -> 'source_families'
           )::BIGINT
           OR EXISTS (
                SELECT 1
                FROM JSONB_ARRAY_ELEMENTS(
                    candidate.engine_snapshot -> 'source_families'
                ) family
                WHERE JSONB_TYPEOF(family) IS DISTINCT FROM 'string'
                   OR family #>> '{}' NOT IN (
                        'COINGLASS_MAX_PAIN', 'PRICE_OI', 'FUTURES_CVD'
                   )
                   OR NOT candidate.categories @> JSONB_BUILD_ARRAY(
                        family #>> '{}'
                   )
           )
           OR (
                SELECT COUNT(DISTINCT family)
                FROM JSONB_ARRAY_ELEMENTS_TEXT(
                    candidate.engine_snapshot -> 'indication_families'
                ) family
           ) IS DISTINCT FROM JSONB_ARRAY_LENGTH(
                candidate.engine_snapshot -> 'indication_families'
           )::BIGINT
           OR EXISTS (
                SELECT 1
                FROM JSONB_ARRAY_ELEMENTS(
                    candidate.engine_snapshot -> 'indication_families'
                ) family
                WHERE JSONB_TYPEOF(family) IS DISTINCT FROM 'string'
                   OR family #>> '{}' NOT IN (
                        'MAX_PAIN', 'MAGNET', 'PRICE_OI', 'FUTURES_CVD'
                   )
                   OR NOT candidate.categories @> JSONB_BUILD_ARRAY(
                        family #>> '{}'
                   )
           )
           OR EXISTS (
                SELECT 1
                FROM (
                    SELECT family,
                           LAG(family) OVER (ORDER BY ordinal) AS prior
                    FROM JSONB_ARRAY_ELEMENTS_TEXT(
                        candidate.engine_snapshot -> 'source_families'
                    ) WITH ORDINALITY AS item(family, ordinal)
                ) ordered
                WHERE ordered.prior COLLATE pg_catalog."C"
                    >= ordered.family COLLATE pg_catalog."C"
           )
           OR EXISTS (
                SELECT 1
                FROM (
                    SELECT family,
                           LAG(family) OVER (ORDER BY ordinal) AS prior
                    FROM JSONB_ARRAY_ELEMENTS_TEXT(
                        candidate.engine_snapshot -> 'indication_families'
                    ) WITH ORDINALITY AS item(family, ordinal)
                ) ordered
                WHERE ordered.prior COLLATE pg_catalog."C"
                    >= ordered.family COLLATE pg_catalog."C"
           )
        THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                MESSAGE = 'invalid Stage-4 Combined independent-source votes';
        END IF;

        IF JSONB_TYPEOF(candidate.engine_snapshot -> 'maxpain_components')
                IS DISTINCT FROM 'array'
           OR JSONB_TYPEOF(candidate.engine_snapshot -> 'derivative_components')
                IS DISTINCT FROM 'object'
           OR NOT (candidate.engine_snapshot -> 'derivative_components')
                ?& ARRAY['PRICE_OI', 'FUTURES_CVD']::TEXT[]
           OR (candidate.engine_snapshot -> 'derivative_components')
                - ARRAY['PRICE_OI', 'FUTURES_CVD']::TEXT[] <> '{}'::JSONB
           OR EXISTS (
                SELECT 1
                FROM JSONB_EACH(
                    candidate.engine_snapshot -> 'derivative_components'
                ) component(family, value)
                WHERE JSONB_TYPEOF(value) IS DISTINCT FROM 'object'
                   OR NOT value ?& ARRAY[
                        'supports', 'score', 'relation', 'available'
                   ]::TEXT[]
                   OR value - ARRAY[
                        'supports', 'score', 'relation', 'available'
                   ]::TEXT[] <> '{}'::JSONB
                   OR JSONB_TYPEOF(value -> 'supports')
                        IS DISTINCT FROM 'boolean'
                   OR JSONB_TYPEOF(value -> 'available')
                        IS DISTINCT FROM 'boolean'
                   OR public.research_signal_snapshot_v1_finite_number(
                        value -> 'score'
                      ) IS NULL
                   OR public.research_signal_snapshot_v1_finite_number(
                        value -> 'score'
                      ) NOT BETWEEN -100.0 AND 100.0
                   OR JSONB_TYPEOF(value -> 'relation') IS DISTINCT FROM 'string'
                   OR value ->> 'relation' NOT IN (
                        'SUPPORT', 'OPPOSE', 'NEUTRAL'
                   )
                   OR (
                        candidate.engine_snapshot -> 'top_item' <> '{}'::JSONB
                        AND value -> 'available' IS DISTINCT FROM (CASE family
                            WHEN 'PRICE_OI' THEN candidate.engine_snapshot #>
                                '{top_item,market_evidence,modules,positioning,available}'
                            ELSE candidate.engine_snapshot #>
                                '{top_item,market_evidence,modules,futures_flow,available}'
                          END)
                   )
                   OR (
                        candidate.engine_snapshot -> 'top_item' <> '{}'::JSONB
                        AND value -> 'relation' IS DISTINCT FROM (CASE family
                            WHEN 'PRICE_OI' THEN candidate.engine_snapshot #>
                                '{top_item,market_evidence,modules,positioning,relation}'
                            ELSE candidate.engine_snapshot #>
                                '{top_item,market_evidence,modules,futures_flow,relation}'
                          END)
                   )
                   OR (
                        candidate.engine_snapshot -> 'top_item' <> '{}'::JSONB
                        AND value -> 'score' IS DISTINCT FROM (CASE family
                            WHEN 'PRICE_OI' THEN candidate.engine_snapshot #>
                                '{top_item,market_evidence,modules,positioning,score}'
                            ELSE candidate.engine_snapshot #>
                                '{top_item,market_evidence,modules,futures_flow,score}'
                          END)
                   )
                   OR (value ->> 'supports')::BOOLEAN IS DISTINCT FROM (
                        (value ->> 'available')::BOOLEAN
                        AND value ->> 'relation' = 'SUPPORT'
                        AND ABS(
                            public.research_signal_snapshot_v1_finite_number(
                                value -> 'score'
                            )
                        ) >= 65.0
                   )
                   OR (
                        (value ->> 'supports')::BOOLEAN
                        AND SIGN(
                            public.research_signal_snapshot_v1_finite_number(
                                value -> 'score'
                            )
                        )
                            IS DISTINCT FROM CASE candidate.direction
                                WHEN 'LONG' THEN 1::DOUBLE PRECISION
                                ELSE -1::DOUBLE PRECISION
                            END
                   )
           )
           OR (
                candidate.engine_snapshot -> 'source_families' ? 'PRICE_OI'
              ) IS DISTINCT FROM (
                candidate.engine_snapshot #>>
                    '{derivative_components,PRICE_OI,supports}'
              )::BOOLEAN
           OR (
                candidate.engine_snapshot -> 'source_families' ? 'FUTURES_CVD'
              ) IS DISTINCT FROM (
                candidate.engine_snapshot #>>
                    '{derivative_components,FUTURES_CVD,supports}'
              )::BOOLEAN
           OR (
                candidate.engine_snapshot -> 'indication_families' ? 'PRICE_OI'
              ) IS DISTINCT FROM (
                candidate.engine_snapshot #>>
                    '{derivative_components,PRICE_OI,supports}'
              )::BOOLEAN
           OR (
                candidate.engine_snapshot -> 'indication_families' ? 'FUTURES_CVD'
              ) IS DISTINCT FROM (
                candidate.engine_snapshot #>>
                    '{derivative_components,FUTURES_CVD,supports}'
              )::BOOLEAN
           OR (
                candidate.engine_snapshot -> 'source_families'
                    ? 'COINGLASS_MAX_PAIN'
              ) IS DISTINCT FROM (
                JSONB_ARRAY_LENGTH(
                    candidate.engine_snapshot -> 'maxpain_components'
                ) > 0
                OR candidate.engine_snapshot -> 'magnet_component' <> '{}'::JSONB
              )
           OR (
                candidate.engine_snapshot -> 'indication_families' ? 'MAX_PAIN'
              ) IS DISTINCT FROM (
                JSONB_ARRAY_LENGTH(
                    candidate.engine_snapshot -> 'maxpain_components'
                ) > 0
              )
           OR (
                candidate.engine_snapshot -> 'indication_families' ? 'MAGNET'
              ) IS DISTINCT FROM (
                candidate.engine_snapshot -> 'magnet_component' <> '{}'::JSONB
              )
        THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                MESSAGE = 'invalid Stage-4 Combined component votes';
        END IF;

        IF candidate.engine_snapshot -> 'dependency_lineage'
            IS DISTINCT FROM (
                CASE WHEN candidate.engine_snapshot -> 'source_families'
                        ? 'COINGLASS_MAX_PAIN'
                    THEN JSONB_BUILD_OBJECT(
                        'COINGLASS_MAX_PAIN', JSONB_BUILD_OBJECT(
                            'logical_engines', JSONB_BUILD_ARRAY(
                                'MAX_PAIN_SCORE_AND_CONFIRMATION', 'MAGNET_V1'
                            ),
                            'raw_sources', JSONB_BUILD_ARRAY(
                                'COINGLASS_MAX_PAIN'
                            ),
                            'qualification_dependencies', JSONB_BUILD_ARRAY(
                                'PRICE_OI', 'FUTURES_CVD'
                            ),
                            'deduplication_rule',
                                'MAX_PAIN_AND_MAGNET_SHARE_ONE_SOURCE_VOTE'
                        )
                    ) ELSE '{}'::JSONB END
                || CASE WHEN candidate.engine_snapshot -> 'source_families'
                        ? 'PRICE_OI'
                    THEN JSONB_BUILD_OBJECT(
                        'PRICE_OI', JSONB_BUILD_OBJECT(
                            'logical_engine', 'PRICE_OI_POSITIONING',
                            'raw_sources', JSONB_BUILD_ARRAY(
                                'SPOT_PRICE', 'OPEN_INTEREST'
                            ),
                            'qualification_dependencies', '[]'::JSONB
                        )
                    ) ELSE '{}'::JSONB END
                || CASE WHEN candidate.engine_snapshot -> 'source_families'
                        ? 'FUTURES_CVD'
                    THEN JSONB_BUILD_OBJECT(
                        'FUTURES_CVD', JSONB_BUILD_OBJECT(
                            'logical_engine', 'FUTURES_CVD',
                            'raw_sources', JSONB_BUILD_ARRAY(
                                'FUTURES_TAKER_BUY_SELL'
                            ),
                            'qualification_dependencies', '[]'::JSONB
                        )
                    ) ELSE '{}'::JSONB END
            )
        THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                MESSAGE = 'invalid Stage-4 Combined dependency lineage';
        END IF;
    END IF;

    -- PostgreSQL permits non-finite float8 values; the frozen envelope does not.
    IF candidate.score IN (
            'NaN'::DOUBLE PRECISION,
            'Infinity'::DOUBLE PRECISION,
            '-Infinity'::DOUBLE PRECISION
       )
       OR candidate.current_price IN (
            'NaN'::DOUBLE PRECISION,
            'Infinity'::DOUBLE PRECISION,
            '-Infinity'::DOUBLE PRECISION
       )
       OR candidate.target_price IN (
            'NaN'::DOUBLE PRECISION,
            'Infinity'::DOUBLE PRECISION,
            '-Infinity'::DOUBLE PRECISION
       )
       OR candidate.initial_target_distance_pct IN (
            'NaN'::DOUBLE PRECISION,
            'Infinity'::DOUBLE PRECISION,
            '-Infinity'::DOUBLE PRECISION
       )
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'Stage-4 signal event contains a non-finite number';
    END IF;

    archive_reference := signal_snapshot -> 'archive_reference';
    derivatives_reference := signal_snapshot -> 'derivatives_reference';
    IF NOT archive_reference ?& ARRAY[
            'snapshot_set_id', 'snapshot_key', 'set_payload_sha256',
            'symbol_manifest_payload_sha256', 'row_payload_sha256',
            'max_pain_targets',
            'archive_schema_version', 'method_version', 'cycle_id',
            'cycle_time_utc', 'available_at_utc', 'source',
            'collector_version', 'official_price'
        ]::TEXT[]
       OR archive_reference - ARRAY[
            'snapshot_set_id', 'snapshot_key', 'set_payload_sha256',
            'symbol_manifest_payload_sha256', 'row_payload_sha256',
            'max_pain_targets',
            'archive_schema_version', 'method_version', 'cycle_id',
            'cycle_time_utc', 'available_at_utc', 'source',
            'collector_version', 'official_price'
        ]::TEXT[] <> '{}'::JSONB
       OR NOT derivatives_reference ?& ARRAY[
            'read_started_at_utc', 'read_completed_at_utc', 'payload_sha256',
            'price_oi', 'futures_cvd', 'spot_cvd_context'
        ]::TEXT[]
       OR derivatives_reference - ARRAY[
            'read_started_at_utc', 'read_completed_at_utc', 'payload_sha256',
            'price_oi', 'futures_cvd', 'spot_cvd_context'
        ]::TEXT[] <> '{}'::JSONB
       OR JSONB_TYPEOF(derivatives_reference -> 'price_oi')
            IS DISTINCT FROM 'object'
       OR JSONB_TYPEOF(derivatives_reference -> 'futures_cvd')
            IS DISTINCT FROM 'object'
       OR JSONB_TYPEOF(derivatives_reference -> 'spot_cvd_context')
            IS DISTINCT FROM 'object'
       OR NOT public.research_signal_snapshot_v1_nonnegative_integer(
            archive_reference -> 'snapshot_set_id'
       )
       OR (archive_reference ->> 'snapshot_set_id')::BIGINT <= 0
       OR NOT public.research_signal_snapshot_v1_sha256(
            archive_reference ->> 'snapshot_key'
       )
       OR NOT public.research_signal_snapshot_v1_sha256(
            archive_reference ->> 'set_payload_sha256'
       )
       OR NOT public.research_signal_snapshot_v1_sha256(
            archive_reference ->> 'symbol_manifest_payload_sha256'
       )
       OR NOT public.research_signal_snapshot_v1_sha256(
            derivatives_reference ->> 'payload_sha256'
       )
       OR archive_reference ->> 'archive_schema_version'
            IS DISTINCT FROM 'research-max-pain-archive-v1'
       OR archive_reference ->> 'method_version'
            IS DISTINCT FROM 'coherent-max-pain-seven-timeframe-v1'
       OR archive_reference ->> 'source' IS DISTINCT FROM 'RESEARCH_PASSIVE'
       OR JSONB_TYPEOF(archive_reference -> 'row_payload_sha256')
            IS DISTINCT FROM 'array'
       OR JSONB_ARRAY_LENGTH(archive_reference -> 'row_payload_sha256') <> 7
       OR JSONB_TYPEOF(archive_reference -> 'max_pain_targets')
            IS DISTINCT FROM 'array'
       OR JSONB_ARRAY_LENGTH(archive_reference -> 'max_pain_targets') <> 7
       OR JSONB_TYPEOF(archive_reference -> 'official_price')
            IS DISTINCT FROM 'object'
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'invalid Stage-4 signal provenance envelope';
    END IF;

    official_price := archive_reference -> 'official_price';
    price_oi_reference := derivatives_reference -> 'price_oi';
    futures_cvd_reference := derivatives_reference -> 'futures_cvd';
    spot_cvd_reference := derivatives_reference -> 'spot_cvd_context';
    IF NOT official_price ?& ARRAY[
            'price', 'source', 'exchange', 'market', 'pair', 'instrument',
            'interval', 'fetched_at_utc', 'observed_at_utc',
            'candle_open_time_utc', 'candle_close_time_utc', 'policy_status'
       ]::TEXT[]
       OR official_price - ARRAY[
            'price', 'source', 'exchange', 'market', 'pair', 'instrument',
            'interval', 'fetched_at_utc', 'observed_at_utc',
            'candle_open_time_utc', 'candle_close_time_utc', 'policy_status'
       ]::TEXT[] <> '{}'::JSONB
       OR public.research_signal_snapshot_v1_finite_number(
            official_price -> 'price'
          ) IS NULL
       OR public.research_signal_snapshot_v1_finite_number(
            official_price -> 'price'
          ) <= 0.0
       OR JSONB_TYPEOF(official_price -> 'source') IS DISTINCT FROM 'string'
       OR JSONB_TYPEOF(official_price -> 'exchange') IS DISTINCT FROM 'string'
       OR JSONB_TYPEOF(official_price -> 'market') IS DISTINCT FROM 'string'
       OR JSONB_TYPEOF(official_price -> 'pair') IS DISTINCT FROM 'string'
       OR JSONB_TYPEOF(official_price -> 'interval') IS DISTINCT FROM 'string'
       OR JSONB_TYPEOF(official_price -> 'fetched_at_utc')
            IS DISTINCT FROM 'string'
       OR JSONB_TYPEOF(official_price -> 'observed_at_utc')
            IS DISTINCT FROM 'string'
       OR JSONB_TYPEOF(official_price -> 'candle_open_time_utc')
            NOT IN ('string', 'null')
       OR JSONB_TYPEOF(official_price -> 'candle_close_time_utc')
            NOT IN ('string', 'null')
       OR JSONB_TYPEOF(official_price -> 'policy_status')
            IS DISTINCT FROM 'string'
       OR candidate.current_price IS DISTINCT FROM
            public.research_signal_snapshot_v1_finite_number(
                official_price -> 'price'
            )
       OR NOT price_oi_reference ?& ARRAY[
            'source_table', 'source_snapshot_id', 'collected_at_utc',
            'price_fetched_at_utc', 'oi_fetched_at_utc', 'time_gap_seconds',
            'quality_status', 'price_source', 'oi_source',
            'age_minutes_at_read', 'maximum_age_minutes'
       ]::TEXT[]
       OR price_oi_reference - ARRAY[
            'source_table', 'source_snapshot_id', 'collected_at_utc',
            'price_fetched_at_utc', 'oi_fetched_at_utc', 'time_gap_seconds',
            'quality_status', 'price_source', 'oi_source',
            'age_minutes_at_read', 'maximum_age_minutes'
       ]::TEXT[] <> '{}'::JSONB
       OR price_oi_reference ->> 'source_table'
            IS DISTINCT FROM 'oi_regime_snapshots'
       OR NOT public.research_signal_snapshot_v1_positive_bigint(
            price_oi_reference -> 'source_snapshot_id'
       )
       OR price_oi_reference ->> 'quality_status' IS DISTINCT FROM 'PASS'
       OR JSONB_TYPEOF(price_oi_reference -> 'collected_at_utc')
            IS DISTINCT FROM 'string'
       OR JSONB_TYPEOF(price_oi_reference -> 'price_fetched_at_utc')
            IS DISTINCT FROM 'string'
       OR JSONB_TYPEOF(price_oi_reference -> 'oi_fetched_at_utc')
            IS DISTINCT FROM 'string'
       OR JSONB_TYPEOF(price_oi_reference -> 'price_source')
            IS DISTINCT FROM 'string'
       OR JSONB_TYPEOF(price_oi_reference -> 'oi_source')
            IS DISTINCT FROM 'string'
       OR BTRIM(price_oi_reference ->> 'price_source') IS NOT DISTINCT FROM ''
       OR BTRIM(price_oi_reference ->> 'oi_source') IS NOT DISTINCT FROM ''
       OR public.research_signal_snapshot_v1_finite_number(
            price_oi_reference -> 'time_gap_seconds'
          ) IS NULL
       OR public.research_signal_snapshot_v1_finite_number(
            price_oi_reference -> 'time_gap_seconds'
          ) < 0.0
       OR public.research_signal_snapshot_v1_finite_number(
            price_oi_reference -> 'time_gap_seconds'
          ) > 30.0
       OR public.research_signal_snapshot_v1_finite_number(
            price_oi_reference -> 'age_minutes_at_read'
          ) IS NULL
       OR public.research_signal_snapshot_v1_finite_number(
            price_oi_reference -> 'age_minutes_at_read'
          ) < 0.0
       OR price_oi_reference -> 'maximum_age_minutes'
            IS DISTINCT FROM '45'::JSONB
       OR NOT futures_cvd_reference ?& ARRAY[
            'source_table', 'latest_candle_time_utc',
            'latest_candle_close_utc', 'quality_status', 'freshness_status',
            'usable_for_confirmation', 'row_count', 'continuous_cvd_check'
       ]::TEXT[]
       OR futures_cvd_reference - ARRAY[
            'source_table', 'latest_candle_time_utc',
            'latest_candle_close_utc', 'quality_status', 'freshness_status',
            'usable_for_confirmation', 'row_count', 'continuous_cvd_check'
       ]::TEXT[] <> '{}'::JSONB
       OR futures_cvd_reference ->> 'source_table'
            IS DISTINCT FROM 'futures_taker_history'
       OR JSONB_TYPEOF(futures_cvd_reference -> 'latest_candle_time_utc')
            IS DISTINCT FROM 'string'
       OR JSONB_TYPEOF(futures_cvd_reference -> 'latest_candle_close_utc')
            IS DISTINCT FROM 'string'
       OR JSONB_TYPEOF(futures_cvd_reference -> 'quality_status')
            IS DISTINCT FROM 'string'
       OR JSONB_TYPEOF(futures_cvd_reference -> 'freshness_status')
            IS DISTINCT FROM 'string'
       OR futures_cvd_reference ->> 'quality_status' NOT IN ('PASS', 'WARNING')
       OR futures_cvd_reference ->> 'freshness_status' IS DISTINCT FROM 'FRESH'
       OR futures_cvd_reference -> 'usable_for_confirmation'
            IS DISTINCT FROM 'true'::JSONB
       OR NOT public.research_signal_snapshot_v1_positive_bigint(
            futures_cvd_reference -> 'row_count'
       )
       OR NOT COALESCE((
            JSONB_TYPEOF(futures_cvd_reference -> 'continuous_cvd_check')
                IN ('boolean', 'string')
            AND (
            futures_cvd_reference -> 'continuous_cvd_check' = 'true'::JSONB
            OR futures_cvd_reference ->> 'continuous_cvd_check' = 'PASS'
            )
       ), FALSE)
       OR NOT spot_cvd_reference ?& ARRAY[
            'source_table', 'latest_candle_time_utc',
            'latest_candle_close_utc', 'quality_status', 'freshness_status',
            'usable_for_confirmation', 'row_count', 'continuous_cvd_check'
       ]::TEXT[]
       OR spot_cvd_reference - ARRAY[
            'source_table', 'latest_candle_time_utc',
            'latest_candle_close_utc', 'quality_status', 'freshness_status',
            'usable_for_confirmation', 'row_count', 'continuous_cvd_check'
       ]::TEXT[] <> '{}'::JSONB
       OR spot_cvd_reference ->> 'source_table'
            IS DISTINCT FROM 'spot_taker_history'
       OR NOT COALESCE((
            (
                spot_cvd_reference -> 'latest_candle_time_utc'
                    IS NOT DISTINCT FROM 'null'::JSONB
                AND spot_cvd_reference -> 'latest_candle_close_utc'
                    IS NOT DISTINCT FROM 'null'::JSONB
                AND spot_cvd_reference ->> 'quality_status'
                    IS NOT DISTINCT FROM 'NO_DATA'
                AND spot_cvd_reference -> 'freshness_status'
                    IS NOT DISTINCT FROM 'null'::JSONB
                AND spot_cvd_reference -> 'usable_for_confirmation'
                    IS NOT DISTINCT FROM 'null'::JSONB
                AND spot_cvd_reference -> 'row_count'
                    IS NOT DISTINCT FROM '0'::JSONB
                AND spot_cvd_reference -> 'continuous_cvd_check'
                    IS NOT DISTINCT FROM 'null'::JSONB
            )
            OR (
                JSONB_TYPEOF(
                    spot_cvd_reference -> 'latest_candle_time_utc'
                ) IS NOT DISTINCT FROM 'string'
                AND JSONB_TYPEOF(
                    spot_cvd_reference -> 'latest_candle_close_utc'
                ) IS NOT DISTINCT FROM 'string'
                AND JSONB_TYPEOF(
                    spot_cvd_reference -> 'quality_status'
                ) IS NOT DISTINCT FROM 'string'
                AND spot_cvd_reference ->> 'quality_status'
                    IN ('PASS', 'WARNING')
                AND JSONB_TYPEOF(
                    spot_cvd_reference -> 'freshness_status'
                ) IS NOT DISTINCT FROM 'string'
                AND spot_cvd_reference ->> 'freshness_status'
                    IN ('FRESH', 'STALE')
                AND JSONB_TYPEOF(
                    spot_cvd_reference -> 'usable_for_confirmation'
                ) IS NOT DISTINCT FROM 'boolean'
                AND public.research_signal_snapshot_v1_positive_bigint(
                    spot_cvd_reference -> 'row_count'
                )
                AND (
                    JSONB_TYPEOF(
                        spot_cvd_reference -> 'continuous_cvd_check'
                    ) = 'boolean'
                    OR spot_cvd_reference ->> 'continuous_cvd_check' = 'PASS'
                )
            )
       ), FALSE)
       OR candidate.target_price IS NULL
            AND candidate.initial_target_distance_pct IS NOT NULL
       OR candidate.target_price IS NOT NULL
            AND (
                candidate.target_price <= 0.0
                OR candidate.initial_target_distance_pct IS NULL
                OR candidate.initial_target_distance_pct < 0.0
                OR ABS(
                    candidate.initial_target_distance_pct
                    - ROUND((
                        ABS(candidate.target_price - candidate.current_price)
                        / candidate.current_price * 100.0
                    )::NUMERIC, 8)::DOUBLE PRECISION
                ) > 0.00000001
                OR (candidate.target_price > candidate.current_price)
                    IS DISTINCT FROM (candidate.direction = 'LONG')
            )
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'invalid Stage-4 frozen price or derivative provenance';
    END IF;

    target_snapshot_set_id := (
        archive_reference ->> 'snapshot_set_id'
    )::BIGINT;
    BEGIN
        IF signal_snapshot ->> 'decision_time_utc' IS NULL
           OR signal_snapshot ->> 'decision_time_utc'
                !~ '(Z|[+-][0-9]{2}:[0-9]{2})$'
           OR archive_reference ->> 'available_at_utc' IS NULL
           OR archive_reference ->> 'available_at_utc'
                !~ '(Z|[+-][0-9]{2}:[0-9]{2})$'
           OR archive_reference ->> 'cycle_time_utc' IS NULL
           OR archive_reference ->> 'cycle_time_utc'
                !~ '(Z|[+-][0-9]{2}:[0-9]{2})$'
           OR official_price ->> 'fetched_at_utc' IS NULL
           OR official_price ->> 'fetched_at_utc'
                !~ '(Z|[+-][0-9]{2}:[0-9]{2})$'
           OR official_price ->> 'observed_at_utc' IS NULL
           OR official_price ->> 'observed_at_utc'
                !~ '(Z|[+-][0-9]{2}:[0-9]{2})$'
           OR (
                official_price ->> 'candle_open_time_utc' IS NOT NULL
                AND official_price ->> 'candle_open_time_utc'
                    !~ '(Z|[+-][0-9]{2}:[0-9]{2})$'
              )
           OR (
                official_price ->> 'candle_close_time_utc' IS NOT NULL
                AND official_price ->> 'candle_close_time_utc'
                    !~ '(Z|[+-][0-9]{2}:[0-9]{2})$'
              )
           OR derivatives_reference ->> 'read_started_at_utc' IS NULL
           OR derivatives_reference ->> 'read_started_at_utc'
                !~ '(Z|[+-][0-9]{2}:[0-9]{2})$'
           OR derivatives_reference ->> 'read_completed_at_utc' IS NULL
           OR derivatives_reference ->> 'read_completed_at_utc'
                !~ '(Z|[+-][0-9]{2}:[0-9]{2})$'
        THEN
            RAISE EXCEPTION USING
                ERRCODE = '22007',
                MESSAGE = 'signal provenance timestamp lacks an explicit timezone';
        END IF;
        decision_time_utc := (
            signal_snapshot ->> 'decision_time_utc'
        )::TIMESTAMPTZ;
        available_at_utc := (
            archive_reference ->> 'available_at_utc'
        )::TIMESTAMPTZ;
        archive_cycle_reference_time_utc := (
            archive_reference ->> 'cycle_time_utc'
        )::TIMESTAMPTZ;
        official_fetched_at_utc := (
            official_price ->> 'fetched_at_utc'
        )::TIMESTAMPTZ;
        official_observed_at_utc := (
            official_price ->> 'observed_at_utc'
        )::TIMESTAMPTZ;
        official_candle_open_time_utc := CASE
            WHEN official_price -> 'candle_open_time_utc' = 'null'::JSONB
                THEN NULL
            ELSE (official_price ->> 'candle_open_time_utc')::TIMESTAMPTZ
        END;
        official_candle_close_time_utc := CASE
            WHEN official_price -> 'candle_close_time_utc' = 'null'::JSONB
                THEN NULL
            ELSE (official_price ->> 'candle_close_time_utc')::TIMESTAMPTZ
        END;
        read_started_at_utc := (
            derivatives_reference ->> 'read_started_at_utc'
        )::TIMESTAMPTZ;
        read_completed_at_utc := (
            derivatives_reference ->> 'read_completed_at_utc'
        )::TIMESTAMPTZ;
    EXCEPTION
        WHEN invalid_datetime_format OR datetime_field_overflow THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                MESSAGE = 'invalid Stage-4 signal provenance timestamps';
    END;

    IF decision_time_utc IS DISTINCT FROM candidate.alert_time_utc
       OR available_at_utc > read_started_at_utc
       OR read_started_at_utc > read_completed_at_utc
       OR read_completed_at_utc > decision_time_utc
       OR decision_time_utc > available_at_utc + INTERVAL '15 minutes'
       OR official_fetched_at_utc > available_at_utc
       OR official_observed_at_utc > available_at_utc
       OR official_candle_close_time_utc > available_at_utc
       OR official_candle_open_time_utc > official_candle_close_time_utc
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'invalid Stage-4 signal event-time causality';
    END IF;

    -- Source observations used for confirmation must exist no later than the
    -- end of the declared derivative read, not merely before decision time.
    FOREACH provenance_time_text IN ARRAY ARRAY[
        derivatives_reference #>> '{price_oi,collected_at_utc}',
        derivatives_reference #>> '{price_oi,price_fetched_at_utc}',
        derivatives_reference #>> '{price_oi,oi_fetched_at_utc}',
        derivatives_reference #>> '{futures_cvd,latest_candle_time_utc}',
        derivatives_reference #>> '{futures_cvd,latest_candle_close_utc}',
        derivatives_reference #>> '{spot_cvd_context,latest_candle_time_utc}',
        derivatives_reference #>> '{spot_cvd_context,latest_candle_close_utc}'
    ]
    LOOP
        IF provenance_time_text IS NOT NULL THEN
            BEGIN
                IF provenance_time_text
                        !~ '(Z|[+-][0-9]{2}:[0-9]{2})$'
                   OR provenance_time_text::TIMESTAMPTZ > read_completed_at_utc
                THEN
                    RAISE EXCEPTION USING
                        ERRCODE = '22007',
                        MESSAGE = 'derivative source timestamp is invalid or post-read';
                END IF;
            EXCEPTION
                WHEN invalid_datetime_format OR datetime_field_overflow THEN
                    RAISE EXCEPTION USING
                        ERRCODE = '23514',
                        MESSAGE = 'invalid Stage-4 derivative source timestamp';
            END;
        END IF;
    END LOOP;

    -- Every cast below is safe only after the protected parsing loop above.
    IF (price_oi_reference ->> 'collected_at_utc')::TIMESTAMPTZ
            < read_completed_at_utc - INTERVAL '45 minutes'
       OR ABS(
            public.research_signal_snapshot_v1_finite_number(
                price_oi_reference -> 'time_gap_seconds'
            ) - ABS(EXTRACT(EPOCH FROM (
                (price_oi_reference ->> 'oi_fetched_at_utc')::TIMESTAMPTZ
                - (price_oi_reference ->> 'price_fetched_at_utc')::TIMESTAMPTZ
            )))
       ) > 0.000001
       OR ABS(
            (price_oi_reference ->> 'age_minutes_at_read')::NUMERIC
            - ROUND((EXTRACT(EPOCH FROM (
                read_completed_at_utc
                - (price_oi_reference ->> 'collected_at_utc')::TIMESTAMPTZ
            )) / 60.0)::NUMERIC, 6)
       ) > 0.000001
       OR (futures_cvd_reference ->> 'latest_candle_close_utc')::TIMESTAMPTZ
            < read_completed_at_utc - INTERVAL '30 minutes'
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'Stage-4 derivative provenance is stale or inconsistent';
    END IF;

    SELECT
        BTRIM(archive_set.snapshot_key),
        BTRIM(archive_set.payload_sha256),
        archive_set.available_at_utc,
        archive_set.cycle_id,
        archive_set.cycle_time_utc,
        archive_set.collector_version,
        archive_set.source,
        archive_set.research_eligible
    INTO
        archive_snapshot_key,
        archive_payload_sha256,
        archive_available_at_utc,
        archive_cycle_id,
        archive_cycle_time_utc,
        archive_collector_version,
        archive_source,
        archive_research_eligible
    FROM public.research_max_pain_snapshot_sets archive_set
    WHERE archive_set.snapshot_set_id
            = target_snapshot_set_id;

    IF NOT FOUND
       OR archive_snapshot_key
            IS DISTINCT FROM archive_reference ->> 'snapshot_key'
       OR archive_payload_sha256
            IS DISTINCT FROM archive_reference ->> 'set_payload_sha256'
       OR archive_available_at_utc IS DISTINCT FROM available_at_utc
       OR archive_cycle_id IS DISTINCT FROM archive_reference ->> 'cycle_id'
       OR archive_cycle_time_utc IS DISTINCT FROM
            archive_cycle_reference_time_utc
       OR archive_collector_version
            IS DISTINCT FROM archive_reference ->> 'collector_version'
       OR archive_source IS DISTINCT FROM 'RESEARCH_PASSIVE'
       OR archive_research_eligible IS DISTINCT FROM TRUE
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'Stage-4 signal archive-set provenance mismatch';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM public.research_max_pain_snapshot_symbols archive_symbol
        WHERE archive_symbol.snapshot_set_id = target_snapshot_set_id
          AND archive_symbol.symbol = candidate.symbol
          AND archive_symbol.research_eligible = TRUE
          AND BTRIM(archive_symbol.payload_sha256)
                = archive_reference ->> 'symbol_manifest_payload_sha256'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'Stage-4 signal symbol-manifest provenance mismatch';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM public.research_max_pain_snapshot_rows archive_row
        WHERE archive_row.snapshot_set_id = target_snapshot_set_id
          AND archive_row.symbol = candidate.symbol
          AND archive_row.timeframe = '12h'
          AND (
              archive_row.current_price IS DISTINCT FROM
                    public.research_signal_snapshot_v1_finite_number(
                        official_price -> 'price'
                    )
              OR archive_row.price_source
                    IS DISTINCT FROM official_price ->> 'source'
              OR archive_row.price_exchange
                    IS DISTINCT FROM official_price ->> 'exchange'
              OR archive_row.price_market
                    IS DISTINCT FROM official_price ->> 'market'
              OR archive_row.price_pair
                    IS DISTINCT FROM official_price ->> 'pair'
              OR archive_row.price_instrument
                    IS DISTINCT FROM official_price ->> 'instrument'
              OR archive_row.price_source_policy_status
                    IS DISTINCT FROM official_price ->> 'policy_status'
              OR archive_row.price_fetched_at_utc IS DISTINCT FROM
                    official_fetched_at_utc
              OR archive_row.raw_provenance ->> 'price_interval'
                    IS DISTINCT FROM official_price ->> 'interval'
              OR archive_row.raw_provenance ->> 'price_observed_at_utc'
                    IS DISTINCT FROM official_price ->> 'observed_at_utc'
              OR archive_row.raw_provenance ->> 'price_candle_open_time_utc'
                    IS DISTINCT FROM official_price ->> 'candle_open_time_utc'
              OR archive_row.raw_provenance ->> 'price_candle_close_time_utc'
                    IS DISTINCT FROM official_price ->> 'candle_close_time_utc'
          )
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'Stage-4 official price does not match frozen archive rows';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM JSONB_ARRAY_ELEMENTS(
            archive_reference -> 'row_payload_sha256'
        ) item
        WHERE JSONB_TYPEOF(item) IS DISTINCT FROM 'object'
           OR NOT item ?& ARRAY['timeframe', 'payload_sha256']::TEXT[]
           OR item - ARRAY['timeframe', 'payload_sha256']::TEXT[]
                <> '{}'::JSONB
           OR item ->> 'timeframe' NOT IN (
                '12h', '24h', '48h', '3d', '1w', '2w', '1m'
           )
           OR NOT public.research_signal_snapshot_v1_sha256(
                item ->> 'payload_sha256'
           )
    )
       OR (
            SELECT COUNT(DISTINCT item ->> 'timeframe')
            FROM JSONB_ARRAY_ELEMENTS(
                archive_reference -> 'row_payload_sha256'
            ) item
       ) <> 7
       OR EXISTS (
            SELECT 1
            FROM JSONB_ARRAY_ELEMENTS(
                archive_reference -> 'row_payload_sha256'
            ) item
            WHERE NOT EXISTS (
                SELECT 1
                FROM public.research_max_pain_snapshot_rows archive_row
                WHERE archive_row.snapshot_set_id = target_snapshot_set_id
                  AND archive_row.symbol = candidate.symbol
                  AND archive_row.timeframe = item ->> 'timeframe'
                  AND BTRIM(archive_row.payload_sha256)
                        = item ->> 'payload_sha256'
            )
       )
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'Stage-4 signal row provenance mismatch';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM JSONB_ARRAY_ELEMENTS(
            archive_reference -> 'max_pain_targets'
        ) item
        WHERE JSONB_TYPEOF(item) IS DISTINCT FROM 'object'
           OR NOT item ?& ARRAY[
                'timeframe', 'short_max_pain', 'long_max_pain',
                'short_liquidation_amount', 'long_liquidation_amount'
           ]::TEXT[]
           OR item - ARRAY[
                'timeframe', 'short_max_pain', 'long_max_pain',
                'short_liquidation_amount', 'long_liquidation_amount'
           ]::TEXT[] <> '{}'::JSONB
           OR item ->> 'timeframe' NOT IN (
                '12h', '24h', '48h', '3d', '1w', '2w', '1m'
           )
           OR public.research_signal_snapshot_v1_finite_number(
                item -> 'short_max_pain'
              ) IS NULL
           OR public.research_signal_snapshot_v1_finite_number(
                item -> 'short_max_pain'
              ) <= 0.0
           OR public.research_signal_snapshot_v1_finite_number(
                item -> 'long_max_pain'
              ) IS NULL
           OR public.research_signal_snapshot_v1_finite_number(
                item -> 'long_max_pain'
              ) <= 0.0
           OR public.research_signal_snapshot_v1_finite_number(
                item -> 'short_liquidation_amount'
              ) IS NULL
           OR public.research_signal_snapshot_v1_finite_number(
                item -> 'short_liquidation_amount'
              ) < 0.0
           OR public.research_signal_snapshot_v1_finite_number(
                item -> 'long_liquidation_amount'
              ) IS NULL
           OR public.research_signal_snapshot_v1_finite_number(
                item -> 'long_liquidation_amount'
              ) < 0.0
    )
       OR (
            SELECT COUNT(DISTINCT item ->> 'timeframe')
            FROM JSONB_ARRAY_ELEMENTS(
                archive_reference -> 'max_pain_targets'
            ) item
       ) <> 7
       OR EXISTS (
            SELECT 1
            FROM JSONB_ARRAY_ELEMENTS(
                archive_reference -> 'max_pain_targets'
            ) item
            WHERE NOT EXISTS (
                SELECT 1
                FROM public.research_max_pain_snapshot_rows archive_row
                WHERE archive_row.snapshot_set_id = target_snapshot_set_id
                  AND archive_row.symbol = candidate.symbol
                  AND archive_row.timeframe = item ->> 'timeframe'
                  AND archive_row.short_max_pain
                        = public.research_signal_snapshot_v1_finite_number(
                            item -> 'short_max_pain'
                        )
                  AND archive_row.long_max_pain
                        = public.research_signal_snapshot_v1_finite_number(
                            item -> 'long_max_pain'
                        )
                  AND archive_row.short_liquidation_amount
                        = public.research_signal_snapshot_v1_finite_number(
                            item -> 'short_liquidation_amount'
                        )
                  AND archive_row.long_liquidation_amount
                        = public.research_signal_snapshot_v1_finite_number(
                            item -> 'long_liquidation_amount'
                        )
            )
       )
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'Stage-4 signal frozen-target provenance mismatch';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM JSONB_ARRAY_ELEMENTS(archive_reference -> 'row_payload_sha256')
             WITH ORDINALITY AS item(value, ordinal)
        WHERE value ->> 'timeframe' IS DISTINCT FROM
            (ARRAY['12h','24h','48h','3d','1w','2w','1m'])[ordinal]
    ) OR EXISTS (
        SELECT 1
        FROM JSONB_ARRAY_ELEMENTS(archive_reference -> 'max_pain_targets')
             WITH ORDINALITY AS item(value, ordinal)
        WHERE value ->> 'timeframe' IS DISTINCT FROM
            (ARRAY['12h','24h','48h','3d','1w','2w','1m'])[ordinal]
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'Stage-4 frozen archive arrays are not canonical';
    END IF;

    IF candidate.event_type = 'MAX_PAIN_CONFIRMATION_STATE'
       AND NOT EXISTS (
            SELECT 1
            FROM public.research_max_pain_snapshot_rows archive_row
            WHERE archive_row.snapshot_set_id = target_snapshot_set_id
              AND archive_row.symbol = candidate.symbol
              AND archive_row.timeframe = candidate.timeframe
              AND candidate.target_price IS NOT DISTINCT FROM CASE
                    WHEN candidate.source_side = 'SHORT'
                        THEN archive_row.short_max_pain
                    ELSE archive_row.long_max_pain
                  END
              AND public.research_signal_snapshot_v1_finite_number(
                    candidate.engine_snapshot -> 'distance_pct'
                  ) IS NOT NULL
              AND ABS(
                    public.research_signal_snapshot_v1_finite_number(
                        candidate.engine_snapshot -> 'distance_pct'
                    ) - candidate.initial_target_distance_pct
                  ) <= 0.00000001
              AND (CASE candidate.source_side
                    WHEN 'SHORT' THEN archive_row.short_target_abs_distance_pct
                    ELSE archive_row.long_target_abs_distance_pct
                  END) IS NOT NULL
              AND ABS(
                    public.research_signal_snapshot_v1_finite_number(
                        candidate.engine_snapshot -> 'distance_pct'
                    ) - CASE candidate.source_side
                        WHEN 'SHORT' THEN archive_row.short_target_abs_distance_pct
                        ELSE archive_row.long_target_abs_distance_pct
                      END
                  ) <= 0.00000001
              AND (candidate.categories ? 'NEAR_MAX_PAIN') IS NOT DISTINCT FROM (
                    (CASE candidate.source_side
                        WHEN 'SHORT' THEN archive_row.short_target_abs_distance_pct
                        ELSE archive_row.long_target_abs_distance_pct
                     END) <= CASE
                        WHEN candidate.symbol = 'BTC' THEN 2.5
                        WHEN candidate.symbol = 'ETH' THEN 2.7
                        WHEN COALESCE(archive_row.rank, 999) <= 10 THEN 3.0
                        WHEN COALESCE(archive_row.rank, 999) <= 20 THEN 3.5
                        ELSE 4.0
                     END
                  )
       )
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'Stage-4 Max-Pain target is not the frozen side target';
    END IF;

    IF candidate.event_type = 'MAGNET_CONFIRMATION_STATE'
       AND (
            EXISTS (
                SELECT 1
                FROM (
                    SELECT member,
                           LAG(member) OVER (ORDER BY ordinal) AS prior
                    FROM JSONB_ARRAY_ELEMENTS_TEXT(
                        public.research_signal_snapshot_v1_magnet_members(candidate)
                    ) WITH ORDINALITY AS item(member, ordinal)
                ) ordered
                WHERE ordered.prior COLLATE pg_catalog."C"
                    >= ordered.member COLLATE pg_catalog."C"
            )
            OR (
                SELECT COUNT(*)
                FROM public.research_max_pain_snapshot_rows archive_row
                WHERE archive_row.snapshot_set_id = target_snapshot_set_id
                  AND archive_row.symbol = candidate.symbol
                  AND public.research_signal_snapshot_v1_magnet_members(candidate)
                        ? archive_row.timeframe
                  AND CASE candidate.source_side
                        WHEN 'UPPER' THEN
                            archive_row.short_max_pain > archive_row.current_price
                        ELSE
                            archive_row.long_max_pain < archive_row.current_price
                      END
            ) IS DISTINCT FROM JSONB_ARRAY_LENGTH(
                public.research_signal_snapshot_v1_magnet_members(candidate)
            )::BIGINT
            OR EXISTS (
                SELECT 1
                FROM public.research_max_pain_snapshot_rows outsider
                CROSS JOIN LATERAL (
                    SELECT
                        MIN(CASE candidate.source_side
                            WHEN 'UPPER' THEN member_row.short_max_pain
                            ELSE member_row.long_max_pain END) AS min_target,
                        MAX(CASE candidate.source_side
                            WHEN 'UPPER' THEN member_row.short_max_pain
                            ELSE member_row.long_max_pain END) AS max_target,
                        SUM(CASE candidate.source_side
                            WHEN 'UPPER' THEN member_row.short_max_pain
                            ELSE member_row.long_max_pain END) AS target_sum,
                        COUNT(*)::DOUBLE PRECISION AS member_count
                    FROM public.research_max_pain_snapshot_rows member_row
                    WHERE member_row.snapshot_set_id = target_snapshot_set_id
                      AND member_row.symbol = candidate.symbol
                      AND public.research_signal_snapshot_v1_magnet_members(
                            candidate
                          ) ? member_row.timeframe
                ) claimed
                WHERE outsider.snapshot_set_id = target_snapshot_set_id
                  AND outsider.symbol = candidate.symbol
                  AND NOT public.research_signal_snapshot_v1_magnet_members(
                        candidate
                      ) ? outsider.timeframe
                  AND CASE candidate.source_side
                        WHEN 'UPPER' THEN
                            outsider.short_max_pain > outsider.current_price
                        ELSE
                            outsider.long_max_pain < outsider.current_price
                      END
                  AND (
                        GREATEST(
                            claimed.max_target,
                            CASE candidate.source_side
                                WHEN 'UPPER' THEN outsider.short_max_pain
                                ELSE outsider.long_max_pain
                            END
                        )
                        - LEAST(
                            claimed.min_target,
                            CASE candidate.source_side
                                WHEN 'UPPER' THEN outsider.short_max_pain
                                ELSE outsider.long_max_pain
                            END
                        )
                      ) / (
                        (
                            claimed.target_sum
                            + CASE candidate.source_side
                                WHEN 'UPPER' THEN outsider.short_max_pain
                                ELSE outsider.long_max_pain
                              END
                        ) / (claimed.member_count + 1.0)
                      ) * 100.0 <= 1.000000000001
            )
            OR NOT EXISTS (
                SELECT 1
                FROM (
                    SELECT
                        MIN(CASE candidate.source_side
                            WHEN 'UPPER' THEN archive_row.short_max_pain
                            ELSE archive_row.long_max_pain END) AS min_target,
                        MAX(CASE candidate.source_side
                            WHEN 'UPPER' THEN archive_row.short_max_pain
                            ELSE archive_row.long_max_pain END) AS max_target,
                        AVG(CASE candidate.source_side
                            WHEN 'UPPER' THEN archive_row.short_max_pain
                            ELSE archive_row.long_max_pain END) AS average_target
                    FROM public.research_max_pain_snapshot_rows archive_row
                    WHERE archive_row.snapshot_set_id = target_snapshot_set_id
                      AND archive_row.symbol = candidate.symbol
                      AND public.research_signal_snapshot_v1_magnet_members(candidate)
                            ? archive_row.timeframe
                ) geometry
                WHERE geometry.min_target IS NOT NULL
                  AND ABS(
                      geometry.min_target
                      - (candidate.engine_snapshot #>> '{magnet,min_target}')
                            ::DOUBLE PRECISION
                  ) <= 0.00000001
                  AND ABS(
                      geometry.max_target
                      - (candidate.engine_snapshot #>> '{magnet,max_target}')
                            ::DOUBLE PRECISION
                  ) <= 0.00000001
                  AND ABS(
                      geometry.average_target
                      - candidate.target_price
                  ) <= 0.00000001
                  AND ABS(
                      ROUND((
                          (geometry.max_target - geometry.min_target)
                          / geometry.average_target * 100.0
                      )::NUMERIC, 4)::DOUBLE PRECISION
                      - (candidate.engine_snapshot #>> '{magnet,spread_pct}')
                            ::DOUBLE PRECISION
                  ) <= 0.00000001
                  AND ABS(
                      ROUND((
                          100.0 - 50.0 * LEAST(
                              1.0,
                              (geometry.max_target - geometry.min_target)
                              / geometry.average_target * 100.0
                          )
                      )::NUMERIC, 2)::DOUBLE PRECISION
                      - candidate.score
                  ) <= 0.00000001
            )
            OR NOT EXISTS (
                SELECT 1
                FROM public.research_max_pain_snapshot_rows widest
                WHERE widest.snapshot_set_id = target_snapshot_set_id
                  AND widest.symbol = candidate.symbol
                  AND public.research_signal_snapshot_v1_magnet_members(candidate)
                        ? widest.timeframe
                  AND widest.timeframe = candidate.engine_snapshot #>>
                        '{magnet,gross_liquidity_timeframe}'
                  AND NOT EXISTS (
                        SELECT 1
                        FROM public.research_max_pain_snapshot_rows later_member
                        WHERE later_member.snapshot_set_id = target_snapshot_set_id
                          AND later_member.symbol = candidate.symbol
                          AND public.research_signal_snapshot_v1_magnet_members(
                                candidate
                              ) ? later_member.timeframe
                          AND CASE later_member.timeframe
                                WHEN '12h' THEN 1 WHEN '24h' THEN 2
                                WHEN '48h' THEN 3 WHEN '3d' THEN 4
                                WHEN '1w' THEN 5 WHEN '2w' THEN 6
                                WHEN '1m' THEN 7 ELSE 99
                              END > CASE widest.timeframe
                                WHEN '12h' THEN 1 WHEN '24h' THEN 2
                                WHEN '48h' THEN 3 WHEN '3d' THEN 4
                                WHEN '1w' THEN 5 WHEN '2w' THEN 6
                                WHEN '1m' THEN 7 ELSE 99
                              END
                  )
                  AND ABS(
                        public.research_signal_snapshot_v1_finite_number(
                            candidate.engine_snapshot #>
                                '{magnet,gross_candidate_liquidity}'
                        ) - CASE candidate.source_side
                            WHEN 'UPPER' THEN widest.short_liquidation_amount
                            ELSE widest.long_liquidation_amount
                          END
                      ) <= 0.00000001
                  AND ABS(
                        public.research_signal_snapshot_v1_finite_number(
                            candidate.engine_snapshot #>
                                '{magnet,gross_opposite_liquidity}'
                        ) - CASE candidate.source_side
                            WHEN 'UPPER' THEN widest.long_liquidation_amount
                            ELSE widest.short_liquidation_amount
                          END
                      ) <= 0.00000001
                  AND (
                        CASE candidate.source_side
                            WHEN 'UPPER' THEN widest.short_liquidation_amount
                            ELSE widest.long_liquidation_amount
                        END
                        + CASE candidate.source_side
                            WHEN 'UPPER' THEN widest.long_liquidation_amount
                            ELSE widest.short_liquidation_amount
                        END
                      ) > 0.0
                  AND ABS(
                        public.research_signal_snapshot_v1_finite_number(
                            candidate.engine_snapshot #>
                                '{magnet,liquidity_edge_pct}'
                        ) - ROUND((
                            (
                                CASE candidate.source_side
                                    WHEN 'UPPER' THEN widest.short_liquidation_amount
                                    ELSE widest.long_liquidation_amount
                                END
                                - CASE candidate.source_side
                                    WHEN 'UPPER' THEN widest.long_liquidation_amount
                                    ELSE widest.short_liquidation_amount
                                END
                            ) / (
                                CASE candidate.source_side
                                    WHEN 'UPPER' THEN widest.short_liquidation_amount
                                    ELSE widest.long_liquidation_amount
                                END
                                + CASE candidate.source_side
                                    WHEN 'UPPER' THEN widest.long_liquidation_amount
                                    ELSE widest.short_liquidation_amount
                                END
                            ) * 100.0
                        )::NUMERIC, 2)::DOUBLE PRECISION
                      ) <= 0.00000001
            )
       )
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'Stage-4 Magnet geometry is not frozen-archive derived';
    END IF;

    IF candidate.event_type = 'SILENT_COMBINED_CONFIRMATION_SNAPSHOT'
       AND (
            (
                candidate.engine_snapshot -> 'top_item' = '{}'::JSONB
                AND (
                    candidate.score IS NOT NULL
                    OR candidate.target_price IS NOT NULL
                    OR candidate.initial_target_distance_pct IS NOT NULL
                    OR JSONB_ARRAY_LENGTH(
                        candidate.engine_snapshot -> 'maxpain_components'
                    ) > 0
                )
            )
            OR (
                candidate.engine_snapshot -> 'top_item' <> '{}'::JSONB
                AND (
                    candidate.score IS NULL
                    OR candidate.target_price IS NULL
                    OR candidate.initial_target_distance_pct IS NULL
                    OR (candidate.engine_snapshot #>>
                        '{top_item,component_sum_check}')::DOUBLE PRECISION
                        IS DISTINCT FROM candidate.score
                    OR NOT EXISTS (
                        SELECT 1
                        FROM public.research_max_pain_snapshot_rows archive_row
                        WHERE archive_row.snapshot_set_id = target_snapshot_set_id
                          AND archive_row.symbol = candidate.symbol
                          AND candidate.target_price IS NOT DISTINCT FROM CASE
                                WHEN candidate.source_side = 'SHORT'
                                    THEN archive_row.short_max_pain
                                ELSE archive_row.long_max_pain
                              END
                    )
                    OR (
                        candidate.engine_snapshot -> 'indication_families'
                            ? 'MAX_PAIN'
                       ) IS DISTINCT FROM EXISTS (
                        SELECT 1
                        FROM public.research_events family_event
                        WHERE family_event.capture_stage =
                                'SILENT_SIGNAL_SNAPSHOT'
                          AND family_event.event_type =
                                'MAX_PAIN_CONFIRMATION_STATE'
                          AND family_event.symbol = candidate.symbol
                          AND family_event.direction = candidate.direction
                          AND family_event.timeframe =
                                candidate.engine_snapshot #>>
                                    '{maxpain_components,0,timeframe}'
                          AND family_event.score IS NOT DISTINCT FROM
                                candidate.score
                          AND family_event.current_price IS NOT DISTINCT FROM
                                candidate.current_price
                          AND family_event.target_price IS NOT DISTINCT FROM
                                candidate.target_price
                          AND family_event.initial_target_distance_pct
                                IS NOT DISTINCT FROM
                                    candidate.initial_target_distance_pct
                          AND family_event.engine_snapshot #>>
                                '{signal_snapshot,archive_reference,snapshot_key}'
                                = archive_reference ->> 'snapshot_key'
                          AND family_event.engine_snapshot - 'signal_snapshot'
                                IS NOT DISTINCT FROM
                                    candidate.engine_snapshot -> 'top_item'
                    )
                )
            )
            OR EXISTS (
                SELECT 1
                FROM JSONB_ARRAY_ELEMENTS(
                    candidate.engine_snapshot -> 'maxpain_components'
                ) component
                WHERE JSONB_TYPEOF(component) IS DISTINCT FROM 'object'
                   OR NOT component ?& ARRAY[
                        'timeframe', 'tier', 'score', 'types',
                        'near_share_pct', 'reasons'
                   ]::TEXT[]
                   OR component - ARRAY[
                        'timeframe', 'tier', 'score', 'types',
                        'near_share_pct', 'reasons'
                   ]::TEXT[] <> '{}'::JSONB
                   OR component ->> 'timeframe' NOT IN (
                        '12h', '24h', '48h', '3d', '1w', '2w', '1m'
                   )
                   OR component ->> 'tier' NOT IN (
                        'CONFIRMED', 'STRONG_CONFIRMED'
                   )
                   OR public.research_signal_snapshot_v1_finite_number(
                        component -> 'score'
                      ) IS NULL
                   OR JSONB_TYPEOF(component -> 'types')
                        IS DISTINCT FROM 'array'
                   OR JSONB_TYPEOF(component -> 'near_share_pct')
                        NOT IN ('number', 'null')
                   OR JSONB_TYPEOF(component -> 'reasons')
                        IS DISTINCT FROM 'array'
                   OR component -> 'reasons' IS DISTINCT FROM (
                        JSONB_BUILD_ARRAY('MAX_PAIN_SCORE_65_NATIVE')
                        || CASE WHEN
                            public.research_signal_snapshot_v1_finite_number(
                                component -> 'score'
                            ) > 80.0
                            THEN JSONB_BUILD_ARRAY('SCORE_OVER_80')
                            ELSE '[]'::JSONB
                           END
                        || CASE WHEN JSONB_ARRAY_LENGTH(
                            component -> 'types'
                           ) >= 3
                            THEN JSONB_BUILD_ARRAY('THREE_ANOMALIES')
                            ELSE '[]'::JSONB
                           END
                        || CASE WHEN COALESCE(
                            public.research_signal_snapshot_v1_finite_number(
                                component -> 'near_share_pct'
                            ) >= 60.0,
                            FALSE
                           )
                            THEN JSONB_BUILD_ARRAY('LIQUIDITY_60')
                            ELSE '[]'::JSONB
                           END
                      )
                   OR NOT EXISTS (
                        SELECT 1
                        FROM public.research_events family_event
                        WHERE family_event.capture_stage = 'SILENT_SIGNAL_SNAPSHOT'
                          AND family_event.event_type =
                                'MAX_PAIN_CONFIRMATION_STATE'
                          AND family_event.symbol = candidate.symbol
                          AND family_event.direction = candidate.direction
                          AND family_event.timeframe = component ->> 'timeframe'
                          AND family_event.score IS NOT DISTINCT FROM
                                public.research_signal_snapshot_v1_finite_number(
                                    component -> 'score'
                                )
                          AND family_event.engine_snapshot #>>
                                '{signal_snapshot,tier}'
                                = component ->> 'tier'
                          AND family_event.engine_snapshot -> 'near_share_pct'
                                IS NOT DISTINCT FROM component -> 'near_share_pct'
                          AND component -> 'types' IS NOT DISTINCT FROM (
                                SELECT COALESCE(
                                    JSONB_AGG(
                                        TO_JSONB(category)
                                        ORDER BY category COLLATE pg_catalog."C"
                                    ),
                                    '[]'::JSONB
                                )
                                FROM JSONB_ARRAY_ELEMENTS_TEXT(
                                    family_event.categories
                                ) category
                                WHERE category NOT IN (
                                    'DECISION_SAMPLE', 'SILENT',
                                    'CONFIRMED', 'STRONG_CONFIRMED'
                                )
                          )
                          AND family_event.engine_snapshot #>>
                                '{signal_snapshot,archive_reference,snapshot_key}'
                                = archive_reference ->> 'snapshot_key'
                   )
            )
            OR (
                SELECT COUNT(DISTINCT component ->> 'timeframe')
                FROM JSONB_ARRAY_ELEMENTS(
                    candidate.engine_snapshot -> 'maxpain_components'
                ) component
              ) IS DISTINCT FROM JSONB_ARRAY_LENGTH(
                    candidate.engine_snapshot -> 'maxpain_components'
              )::BIGINT
            OR JSONB_ARRAY_LENGTH(
                    candidate.engine_snapshot -> 'maxpain_components'
               ) IS DISTINCT FROM (
                    SELECT COUNT(*)::INTEGER
                    FROM public.research_events family_event
                    WHERE family_event.capture_stage =
                            'SILENT_SIGNAL_SNAPSHOT'
                      AND family_event.event_type =
                            'MAX_PAIN_CONFIRMATION_STATE'
                      AND family_event.symbol = candidate.symbol
                      AND family_event.direction = candidate.direction
                      AND family_event.engine_snapshot #>>
                            '{signal_snapshot,archive_reference,snapshot_key}'
                            = archive_reference ->> 'snapshot_key'
               )
            OR EXISTS (
                SELECT 1
                FROM (
                    SELECT
                        public.research_signal_snapshot_v1_finite_number(
                            component -> 'score'
                        ) AS score,
                        component ->> 'timeframe' AS timeframe,
                        LAG(
                            public.research_signal_snapshot_v1_finite_number(
                                component -> 'score'
                            )
                        ) OVER (ORDER BY ordinal) AS prior_score,
                        LAG(component ->> 'timeframe')
                            OVER (ORDER BY ordinal) AS prior_timeframe
                    FROM JSONB_ARRAY_ELEMENTS(
                        candidate.engine_snapshot -> 'maxpain_components'
                    ) WITH ORDINALITY AS item(component, ordinal)
                ) ordered
                WHERE ordered.prior_score < ordered.score
                   OR (
                        ordered.prior_score = ordered.score
                        AND ordered.prior_timeframe COLLATE pg_catalog."C"
                            >= ordered.timeframe COLLATE pg_catalog."C"
                   )
            )
            OR (
                candidate.engine_snapshot -> 'magnet_component' <> '{}'::JSONB
                AND (
                    NOT (candidate.engine_snapshot -> 'magnet_component')
                        ?& ARRAY['tier', 'magnet', 'confirmation']::TEXT[]
                    OR (candidate.engine_snapshot -> 'magnet_component')
                        - ARRAY['tier', 'magnet', 'confirmation']::TEXT[]
                        <> '{}'::JSONB
                    OR NOT EXISTS (
                    SELECT 1
                    FROM public.research_events family_event
                    WHERE family_event.capture_stage = 'SILENT_SIGNAL_SNAPSHOT'
                      AND family_event.event_type = 'MAGNET_CONFIRMATION_STATE'
                      AND family_event.symbol = candidate.symbol
                      AND family_event.direction = candidate.direction
                      AND family_event.engine_snapshot #>>
                            '{signal_snapshot,tier}'
                            = candidate.engine_snapshot #>>
                                '{magnet_component,tier}'
                      AND family_event.engine_snapshot #>>
                            '{signal_snapshot,archive_reference,snapshot_key}'
                            = archive_reference ->> 'snapshot_key'
                      AND candidate.engine_snapshot -> 'magnet_component'
                            IS NOT DISTINCT FROM JSONB_BUILD_OBJECT(
                                'tier', family_event.engine_snapshot #>>
                                    '{signal_snapshot,tier}',
                                'magnet', family_event.engine_snapshot -> 'magnet',
                                'confirmation', family_event.engine_snapshot ->
                                    'magnet_confirmation'
                            )
                    )
                    OR EXISTS (
                        SELECT 1
                        FROM public.research_events better_event
                        WHERE better_event.capture_stage =
                                'SILENT_SIGNAL_SNAPSHOT'
                          AND better_event.event_type =
                                'MAGNET_CONFIRMATION_STATE'
                          AND better_event.symbol = candidate.symbol
                          AND better_event.direction = candidate.direction
                          AND better_event.engine_snapshot #>>
                                '{signal_snapshot,archive_reference,snapshot_key}'
                                = archive_reference ->> 'snapshot_key'
                          AND ARRAY[
                                CASE better_event.engine_snapshot #>>
                                    '{signal_snapshot,tier}'
                                    WHEN 'STRONG_CONFIRMED' THEN 1.0 ELSE 0.0
                                END,
                                better_event.score,
                                COALESCE(
                                    public.research_signal_snapshot_v1_finite_number(
                                        better_event.engine_snapshot #>
                                            '{magnet,liquidity_edge_pct}'
                                    ),
                                    0.0
                                ),
                                public.research_signal_snapshot_v1_finite_number(
                                    better_event.engine_snapshot #>
                                        '{magnet,count}'
                                ),
                                -better_event.target_price,
                                -public.research_signal_snapshot_v1_finite_number(
                                    better_event.engine_snapshot #>
                                        '{magnet,min_target}'
                                )
                              ] > ARRAY[
                                CASE candidate.engine_snapshot #>>
                                    '{magnet_component,tier}'
                                    WHEN 'STRONG_CONFIRMED' THEN 1.0 ELSE 0.0
                                END,
                                public.research_signal_snapshot_v1_finite_number(
                                    candidate.engine_snapshot #>
                                        '{magnet_component,magnet,magnet_quality}'
                                ),
                                COALESCE(
                                    public.research_signal_snapshot_v1_finite_number(
                                        candidate.engine_snapshot #>
                                            '{magnet_component,magnet,liquidity_edge_pct}'
                                    ),
                                    0.0
                                ),
                                public.research_signal_snapshot_v1_finite_number(
                                    candidate.engine_snapshot #>
                                        '{magnet_component,magnet,count}'
                                ),
                                -public.research_signal_snapshot_v1_finite_number(
                                    candidate.engine_snapshot #>
                                        '{magnet_component,magnet,average_target}'
                                ),
                                -public.research_signal_snapshot_v1_finite_number(
                                    candidate.engine_snapshot #>
                                        '{magnet_component,magnet,min_target}'
                                )
                              ]
                    )
                )
            )
       )
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'Stage-4 Combined evidence is not family-event derived';
    END IF;
END;
$function$;

CREATE OR REPLACE FUNCTION public.validate_research_signal_snapshot_v1_envelope()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $function$
DECLARE
    target_snapshot_key TEXT;
    prior_event public.research_events%ROWTYPE;
BEGIN
    PERFORM public.assert_research_signal_snapshot_v1_envelope(NEW);

    IF NEW.capture_stage = 'SILENT_SIGNAL_SNAPSHOT'
       OR public.research_signal_snapshot_v1_reserved_type(NEW.event_type)
    THEN
        target_snapshot_key := CASE NEW.event_type
            WHEN 'SIGNAL_SNAPSHOT_PROJECTION' THEN
                NEW.engine_snapshot #>> '{projection,snapshot_key}'
            ELSE
                NEW.engine_snapshot #>>
                    '{signal_snapshot,archive_reference,snapshot_key}'
        END;

        -- Serialize every writer touching one immutable snapshot.  The
        -- projection receipt is the terminal marker and must be inserted last.
        -- Hash collisions only add harmless serialization; they cannot weaken
        -- the validation below.
        PERFORM pg_catalog.pg_advisory_xact_lock(
            pg_catalog.hashtext('signal-snapshot-v1:' || target_snapshot_key)
        );

        IF NEW.event_type <> 'SIGNAL_SNAPSHOT_PROJECTION'
           AND EXISTS (
                SELECT 1
                FROM public.research_events marker
                WHERE marker.event_type = 'SIGNAL_SNAPSHOT_PROJECTION'
                  AND marker.capture_stage = 'SILENT_SIGNAL_SNAPSHOT'
                  AND marker.engine_snapshot #>> '{projection,snapshot_key}'
                        = target_snapshot_key
           )
        THEN
            SELECT *
            INTO prior_event
            FROM public.research_events existing
            WHERE BTRIM(existing.event_fingerprint)
                    = BTRIM(NEW.event_fingerprint);

            IF NOT FOUND
               OR prior_event.capture_stage
                    IS DISTINCT FROM 'SILENT_SIGNAL_SNAPSHOT'
               OR prior_event.event_type IS DISTINCT FROM NEW.event_type
               OR public.research_signal_snapshot_v1_event_payload_sha256(
                    prior_event
                  ) IS DISTINCT FROM
                  public.research_signal_snapshot_v1_event_payload_sha256(NEW)
            THEN
                RAISE EXCEPTION USING
                    ERRCODE = '55000',
                    MESSAGE = 'Stage-4 projection receipt is terminal; signal append rejected';
            END IF;
        END IF;
    END IF;
    RETURN NEW;
END;
$function$;

CREATE OR REPLACE FUNCTION public.assert_research_signal_snapshot_v1_set_complete(
    target_snapshot_key TEXT
)
RETURNS VOID
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $function$
DECLARE
    marker_count BIGINT;
    marker JSONB;
    marker_status TEXT;
    expected_max_pain BIGINT;
    expected_magnet BIGINT;
    expected_combined BIGINT;
    expected_total BIGINT;
    actual_max_pain BIGINT;
    actual_magnet BIGINT;
    actual_combined BIGINT;
    actual_total BIGINT;
    expected_payload_sha256 TEXT;
    actual_payload_sha256 TEXT;
    marker_code_version TEXT;
    marker_runtime_session_id TEXT;
BEGIN
    SELECT COUNT(*)
    INTO marker_count
    FROM public.research_events
    WHERE event_type = 'SIGNAL_SNAPSHOT_PROJECTION'
      AND capture_stage = 'SILENT_SIGNAL_SNAPSHOT'
      AND engine_snapshot #>> '{projection,snapshot_key}' = target_snapshot_key;

    IF marker_count IS DISTINCT FROM 1 THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'Stage-4 snapshot set requires exactly one projection receipt';
    END IF;

    SELECT engine_snapshot, code_version, runtime_session_id
    INTO STRICT marker, marker_code_version, marker_runtime_session_id
    FROM public.research_events
    WHERE event_type = 'SIGNAL_SNAPSHOT_PROJECTION'
      AND capture_stage = 'SILENT_SIGNAL_SNAPSHOT'
      AND engine_snapshot #>> '{projection,snapshot_key}' = target_snapshot_key;

    marker_status := marker #>> '{projection,status}';
    expected_max_pain := (
        marker #>> '{projection,counts,max_pain}'
    )::BIGINT;
    expected_magnet := (
        marker #>> '{projection,counts,magnet}'
    )::BIGINT;
    expected_combined := (
        marker #>> '{projection,counts,combined}'
    )::BIGINT;
    expected_total := (
        marker #>> '{projection,signal_event_count}'
    )::BIGINT;
    expected_payload_sha256 := marker #>>
        '{projection,signal_events_payload_sha256}';

    SELECT
        COUNT(*) FILTER (
            WHERE event_type = 'MAX_PAIN_CONFIRMATION_STATE'
        ),
        COUNT(*) FILTER (
            WHERE event_type = 'MAGNET_CONFIRMATION_STATE'
        ),
        COUNT(*) FILTER (
            WHERE event_type = 'SILENT_COMBINED_CONFIRMATION_SNAPSHOT'
        ),
        COUNT(*),
        public.research_signal_snapshot_v1_text_sha256(
            'research-signal-event-set-v1:' || COUNT(*)::TEXT || ':'
            || COALESCE(STRING_AGG(
                public.research_signal_snapshot_v1_event_payload_sha256(
                    event_row
                ),
                '' ORDER BY BTRIM(event_row.event_fingerprint)
                    COLLATE pg_catalog."C"
            ), '')
        )
    INTO actual_max_pain, actual_magnet, actual_combined,
         actual_total, actual_payload_sha256
    FROM public.research_events event_row
    WHERE event_row.capture_stage = 'SILENT_SIGNAL_SNAPSHOT'
      AND event_row.event_type IN (
          'MAX_PAIN_CONFIRMATION_STATE',
          'MAGNET_CONFIRMATION_STATE',
          'SILENT_COMBINED_CONFIRMATION_SNAPSHOT'
      )
      AND event_row.engine_snapshot #>>
            '{signal_snapshot,archive_reference,snapshot_key}'
            = target_snapshot_key;

    IF actual_payload_sha256 IS DISTINCT FROM expected_payload_sha256 THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'Stage-4 projection signal payload commitment mismatch';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM public.research_events event_row
        WHERE event_row.capture_stage = 'SILENT_SIGNAL_SNAPSHOT'
          AND event_row.event_type IN (
              'MAX_PAIN_CONFIRMATION_STATE',
              'MAGNET_CONFIRMATION_STATE',
              'SILENT_COMBINED_CONFIRMATION_SNAPSHOT'
          )
          AND event_row.engine_snapshot #>>
                '{signal_snapshot,archive_reference,snapshot_key}'
                = target_snapshot_key
          AND (
              event_row.code_version IS DISTINCT FROM marker_code_version
              OR event_row.runtime_session_id
                    IS DISTINCT FROM marker_runtime_session_id
              OR
              event_row.alert_time_utc IS DISTINCT FROM (
                  marker #>> '{projection,decision_time_utc}'
              )::TIMESTAMPTZ
              OR event_row.engine_snapshot #>>
                    '{signal_snapshot,derivatives_reference,read_started_at_utc}'
                    IS DISTINCT FROM marker #>>
                    '{projection,derivatives_read_started_at_utc}'
              OR event_row.engine_snapshot #>>
                    '{signal_snapshot,derivatives_reference,read_completed_at_utc}'
                    IS DISTINCT FROM marker #>>
                    '{projection,derivatives_read_completed_at_utc}'
              OR NOT EXISTS (
                  SELECT 1
                  FROM JSONB_ARRAY_ELEMENTS(
                      marker #> '{projection,symbol_evaluations}'
                  ) evaluation
                  WHERE evaluation ->> 'symbol' = event_row.symbol
                    AND evaluation ->> 'status' = 'EVALUABLE'
              )
          )
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'Stage-4 signal does not belong to its evaluable projection';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM public.research_events event_row
        WHERE event_row.capture_stage = 'SILENT_SIGNAL_SNAPSHOT'
          AND event_row.event_type IN (
              'MAX_PAIN_CONFIRMATION_STATE',
              'MAGNET_CONFIRMATION_STATE',
              'SILENT_COMBINED_CONFIRMATION_SNAPSHOT'
          )
          AND event_row.engine_snapshot #>>
                '{signal_snapshot,archive_reference,snapshot_key}'
                = target_snapshot_key
        GROUP BY event_row.symbol
        HAVING COUNT(DISTINCT (
            event_row.engine_snapshot #>
                '{signal_snapshot,derivatives_reference}'
        )) <> 1
           OR COUNT(DISTINCT (
            event_row.engine_snapshot #>
                '{signal_snapshot,archive_reference}'
        )) <> 1
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'Stage-4 symbol events disagree on frozen provenance';
    END IF;

    IF marker_status = 'MISSED_CAUSAL_WINDOW' THEN
        IF actual_total <> 0 THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                MESSAGE = 'missed Stage-4 projection cannot own signal events';
        END IF;
    ELSIF actual_max_pain IS DISTINCT FROM expected_max_pain
       OR actual_magnet IS DISTINCT FROM expected_magnet
       OR actual_combined IS DISTINCT FROM expected_combined
       OR actual_total IS DISTINCT FROM expected_total
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'Stage-4 projection receipt does not match persisted signals';
    END IF;
END;
$function$;

CREATE OR REPLACE FUNCTION public.validate_research_signal_snapshot_v1_set_complete()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $function$
DECLARE
    target_snapshot_key TEXT;
    stage4_row BOOLEAN := (
        NEW.capture_stage = 'SILENT_SIGNAL_SNAPSHOT'
        OR public.research_signal_snapshot_v1_reserved_type(NEW.event_type)
    );
BEGIN
    -- This deferred AFTER trigger sees the final row after every BEFORE trigger.
    -- Reassert both identity and envelope so a later trigger cannot rewrite a
    -- row after the named BEFORE guards have approved it.
    IF stage4_row AND (
        SESSION_USER <> 'research_signal_snapshot_writer_v1'
        OR CURRENT_USER <> 'research_signal_snapshot_writer_v1'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '42501',
            MESSAGE = 'Stage-4 signal snapshot requires its dedicated writer';
    END IF;
    IF SESSION_USER = 'research_signal_snapshot_writer_v1'
       AND NOT stage4_row
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '42501',
            MESSAGE = 'Stage-4 writer cannot create non-Stage-4 research events';
    END IF;
    PERFORM public.assert_research_signal_snapshot_v1_envelope(NEW);
    IF NOT stage4_row THEN
        RETURN NEW;
    END IF;

    target_snapshot_key := CASE NEW.event_type
        WHEN 'SIGNAL_SNAPSHOT_PROJECTION' THEN
            NEW.engine_snapshot #>> '{projection,snapshot_key}'
        ELSE
            NEW.engine_snapshot #>>
                '{signal_snapshot,archive_reference,snapshot_key}'
    END;
    IF NEW.event_type = 'SIGNAL_SNAPSHOT_PROJECTION' THEN
        PERFORM public.assert_research_signal_snapshot_v1_set_complete(
            target_snapshot_key
        );
    ELSIF NOT EXISTS (
        SELECT 1
        FROM public.research_events marker
        WHERE marker.event_type = 'SIGNAL_SNAPSHOT_PROJECTION'
          AND marker.capture_stage = 'SILENT_SIGNAL_SNAPSHOT'
          AND marker.engine_snapshot #>> '{projection,snapshot_key}'
                = target_snapshot_key
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'Stage-4 signal requires its terminal projection receipt';
    END IF;
    RETURN NEW;
END;
$function$;

CREATE OR REPLACE FUNCTION public.prevent_research_signal_snapshot_v1_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $function$
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF OLD.capture_stage = 'SILENT_SIGNAL_SNAPSHOT'
           OR public.research_signal_snapshot_v1_reserved_type(OLD.event_type)
        THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'Stage-4 signal snapshot events are append-only';
        END IF;
        RETURN OLD;
    END IF;

    IF OLD.capture_stage = 'SILENT_SIGNAL_SNAPSHOT'
       OR NEW.capture_stage = 'SILENT_SIGNAL_SNAPSHOT'
       OR public.research_signal_snapshot_v1_reserved_type(OLD.event_type)
       OR public.research_signal_snapshot_v1_reserved_type(NEW.event_type)
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'Stage-4 signal snapshot events are append-only';
    END IF;
    RETURN NEW;
END;
$function$;

CREATE OR REPLACE FUNCTION public.prevent_research_signal_snapshot_v1_truncate()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $function$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM public.research_events
        WHERE capture_stage = 'SILENT_SIGNAL_SNAPSHOT'
           OR public.research_signal_snapshot_v1_reserved_type(event_type)
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'research_events contains Stage-4 rows and cannot be truncated';
    END IF;
    RETURN NULL;
END;
$function$;

-- CREATE OR REPLACE preserves a pre-existing function owner and some ALTERed
-- attributes.  Normalize every contract function to the trusted table owner
-- before any writer permission is granted.
DO $function_owners$
DECLARE
    trusted_owner NAME;
    function_row RECORD;
    function_count INTEGER := 0;
BEGIN
    SELECT pg_catalog.pg_get_userbyid(relation_row.relowner)
    INTO trusted_owner
    FROM pg_catalog.pg_class relation_row
    WHERE relation_row.oid = 'public.research_events'::REGCLASS;

    IF trusted_owner IS NULL
       OR trusted_owner = 'research_signal_snapshot_writer_v1'
    THEN
        RAISE EXCEPTION
            'Stage-4 validator functions require a trusted non-writer owner';
    END IF;

    FOR function_row IN
        SELECT function_catalog.oid::REGPROCEDURE AS identity
        FROM pg_catalog.pg_proc function_catalog
        JOIN pg_catalog.pg_namespace function_namespace
          ON function_namespace.oid = function_catalog.pronamespace
        WHERE function_namespace.nspname = 'public'
          AND function_catalog.proname = ANY (ARRAY[
              'research_signal_snapshot_v1_reserved_type',
              'assert_research_signal_snapshot_v1_writer',
              'research_signal_snapshot_v1_sha256',
              'research_signal_snapshot_v1_text_sha256',
              'research_signal_snapshot_v1_commitment_canonical',
              'research_signal_snapshot_v1_event_commitment_payload',
              'research_signal_snapshot_v1_event_payload_sha256',
              'research_signal_snapshot_v1_identity_canonical',
              'research_signal_snapshot_v1_magnet_members',
              'research_signal_snapshot_v1_expected_setup_key',
              'research_signal_snapshot_v1_expected_fingerprint',
              'research_signal_snapshot_v1_nonnegative_integer',
              'research_signal_snapshot_v1_positive_bigint',
              'research_signal_snapshot_v1_finite_number',
              'research_signal_snapshot_v1_key_count',
              'assert_research_signal_snapshot_v1_envelope',
              'validate_research_signal_snapshot_v1_envelope',
              'assert_research_signal_snapshot_v1_set_complete',
              'validate_research_signal_snapshot_v1_set_complete',
              'prevent_research_signal_snapshot_v1_mutation',
              'prevent_research_signal_snapshot_v1_truncate'
          ]::NAME[])
    LOOP
        function_count := function_count + 1;
        EXECUTE FORMAT('ALTER FUNCTION %s SECURITY INVOKER', function_row.identity);
        EXECUTE FORMAT('ALTER FUNCTION %s RESET ALL', function_row.identity);
        EXECUTE FORMAT(
            'ALTER FUNCTION %s SET search_path TO pg_catalog, public',
            function_row.identity
        );
        EXECUTE FORMAT(
            'ALTER FUNCTION %s OWNER TO %I',
            function_row.identity,
            trusted_owner
        );
    END LOOP;

    IF function_count <> 21 THEN
        RAISE EXCEPTION
            'Expected 21 exact Stage-4 functions, found %', function_count;
    END IF;
END;
$function_owners$;

-- Fail closed over any rows written before this migration.  In a transactional
-- migration runner, a failure leaves all prior guards unchanged.  This file
-- must therefore be applied as one transaction (as schema-admin does).

DO $insert_triggers$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_catalog.pg_trigger trigger_row
        WHERE trigger_row.tgrelid = 'public.research_events'::REGCLASS
          AND NOT trigger_row.tgisinternal
          AND (trigger_row.tgtype::INTEGER & 4) = 4
          AND trigger_row.tgname NOT IN (
                'trg_research_signal_snapshot_v1_writer',
                'trg_research_signal_snapshot_v1_envelope',
                'trg_research_signal_snapshot_v1_set_complete'
          )
    ) THEN
        RAISE EXCEPTION
            'Unexpected research_events INSERT trigger blocks Stage-4 installation';
    END IF;
END;
$insert_triggers$;

DO $migration$
DECLARE
    candidate public.research_events%ROWTYPE;
    target_snapshot_key TEXT;
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_trigger trigger_row
        WHERE trigger_row.tgrelid = 'public.research_events'::regclass
          AND trigger_row.tgname =
              'trg_research_signal_snapshot_v1_writer'
          AND NOT trigger_row.tgisinternal
    ) AND EXISTS (
        SELECT 1
        FROM public.research_events
        WHERE capture_stage = 'SILENT_SIGNAL_SNAPSHOT'
           OR public.research_signal_snapshot_v1_reserved_type(event_type)
    ) THEN
        RAISE EXCEPTION
            'Untrusted pre-migration Stage-4 rows require manual quarantine';
    END IF;

    FOR candidate IN
        SELECT *
        FROM public.research_events
        WHERE capture_stage = 'SILENT_SIGNAL_SNAPSHOT'
           OR public.research_signal_snapshot_v1_reserved_type(event_type)
    LOOP
        PERFORM public.assert_research_signal_snapshot_v1_envelope(candidate);
    END LOOP;

    FOR target_snapshot_key IN
        SELECT DISTINCT CASE event_type
            WHEN 'SIGNAL_SNAPSHOT_PROJECTION' THEN
                engine_snapshot #>> '{projection,snapshot_key}'
            ELSE
                engine_snapshot #>>
                    '{signal_snapshot,archive_reference,snapshot_key}'
        END
        FROM public.research_events
        WHERE capture_stage = 'SILENT_SIGNAL_SNAPSHOT'
           OR public.research_signal_snapshot_v1_reserved_type(event_type)
    LOOP
        PERFORM public.assert_research_signal_snapshot_v1_set_complete(
            target_snapshot_key
        );
    END LOOP;
END;
$migration$;

DROP INDEX IF EXISTS public.uq_research_signal_snapshot_projection_key_v1;

CREATE UNIQUE INDEX uq_research_signal_snapshot_projection_key_v1
ON public.research_events (
    (engine_snapshot #>> '{projection,snapshot_key}')
)
WHERE event_type = 'SIGNAL_SNAPSHOT_PROJECTION'
  AND capture_stage = 'SILENT_SIGNAL_SNAPSHOT';

DROP INDEX IF EXISTS public.idx_research_signal_snapshot_archive_key_v1;

CREATE INDEX idx_research_signal_snapshot_archive_key_v1
ON public.research_events (
    (engine_snapshot #>>
        '{signal_snapshot,archive_reference,snapshot_key}')
)
WHERE capture_stage = 'SILENT_SIGNAL_SNAPSHOT'
  AND event_type IN (
      'MAX_PAIN_CONFIRMATION_STATE',
      'MAGNET_CONFIRMATION_STATE',
      'SILENT_COMBINED_CONFIRMATION_SNAPSHOT'
  );

DROP TRIGGER IF EXISTS trg_research_signal_snapshot_v1_writer
    ON public.research_events;

CREATE TRIGGER trg_research_signal_snapshot_v1_writer
BEFORE INSERT OR UPDATE ON public.research_events
FOR EACH ROW EXECUTE FUNCTION public.assert_research_signal_snapshot_v1_writer();

ALTER TABLE public.research_events
    ENABLE ALWAYS TRIGGER trg_research_signal_snapshot_v1_writer;

DROP TRIGGER IF EXISTS trg_research_signal_snapshot_v1_envelope
    ON public.research_events;

CREATE TRIGGER trg_research_signal_snapshot_v1_envelope
BEFORE INSERT OR UPDATE ON public.research_events
FOR EACH ROW EXECUTE FUNCTION public.validate_research_signal_snapshot_v1_envelope();

ALTER TABLE public.research_events
    ENABLE ALWAYS TRIGGER trg_research_signal_snapshot_v1_envelope;

DROP TRIGGER IF EXISTS trg_research_signal_snapshot_v1_set_complete
    ON public.research_events;

CREATE CONSTRAINT TRIGGER trg_research_signal_snapshot_v1_set_complete
AFTER INSERT ON public.research_events
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION public.validate_research_signal_snapshot_v1_set_complete();

ALTER TABLE public.research_events
    ENABLE ALWAYS TRIGGER trg_research_signal_snapshot_v1_set_complete;

DROP TRIGGER IF EXISTS trg_research_signal_snapshot_v1_immutable
    ON public.research_events;

CREATE TRIGGER trg_research_signal_snapshot_v1_immutable
BEFORE UPDATE OR DELETE ON public.research_events
FOR EACH ROW EXECUTE FUNCTION public.prevent_research_signal_snapshot_v1_mutation();

ALTER TABLE public.research_events
    ENABLE ALWAYS TRIGGER trg_research_signal_snapshot_v1_immutable;

DROP TRIGGER IF EXISTS trg_research_signal_snapshot_v1_no_truncate
    ON public.research_events;

CREATE TRIGGER trg_research_signal_snapshot_v1_no_truncate
BEFORE TRUNCATE ON public.research_events
FOR EACH STATEMENT
EXECUTE FUNCTION public.prevent_research_signal_snapshot_v1_truncate();

ALTER TABLE public.research_events
    ENABLE ALWAYS TRIGGER trg_research_signal_snapshot_v1_no_truncate;

REVOKE CREATE ON SCHEMA public
    FROM research_signal_snapshot_writer_v1;
GRANT USAGE ON SCHEMA public
    TO research_signal_snapshot_writer_v1;

REVOKE UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER
    ON TABLE public.research_events
    FROM PUBLIC, research_signal_snapshot_writer_v1;
GRANT SELECT, INSERT ON TABLE public.research_events
    TO research_signal_snapshot_writer_v1;
REVOKE ALL ON SEQUENCE public.research_events_event_id_seq
    FROM PUBLIC, research_signal_snapshot_writer_v1;
GRANT USAGE ON SEQUENCE public.research_events_event_id_seq
    TO research_signal_snapshot_writer_v1;

GRANT SELECT ON TABLE
    public.research_max_pain_snapshot_sets,
    public.research_max_pain_snapshot_symbols,
    public.research_max_pain_snapshot_rows
    TO research_signal_snapshot_writer_v1;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON TABLE
    public.research_max_pain_snapshot_sets,
    public.research_max_pain_snapshot_symbols,
    public.research_max_pain_snapshot_rows
    FROM PUBLIC, research_signal_snapshot_writer_v1;

-- Table-level REVOKE does not remove stale per-column grants.  Remove every
-- explicit column privilege held by PUBLIC or the dedicated writer; the exact
-- table-level SELECT/INSERT surface above remains authoritative.
DO $column_acl_cleanup$
DECLARE
    relation_name TEXT;
    grant_row RECORD;
    writer_oid OID := (
        SELECT role_row.oid
        FROM pg_catalog.pg_roles role_row
        WHERE role_row.rolname = 'research_signal_snapshot_writer_v1'
    );
    grantee_sql TEXT;
BEGIN
    FOREACH relation_name IN ARRAY ARRAY[
        'research_events',
        'research_max_pain_snapshot_sets',
        'research_max_pain_snapshot_symbols',
        'research_max_pain_snapshot_rows'
    ] LOOP
        FOR grant_row IN
            SELECT DISTINCT
                   attribute.attname,
                   acl.grantee,
                   acl.privilege_type
            FROM pg_catalog.pg_attribute attribute
            CROSS JOIN LATERAL pg_catalog.aclexplode(attribute.attacl) acl
            WHERE attribute.attrelid = pg_catalog.to_regclass(
                      'public.' || relation_name
                  )
              AND attribute.attnum > 0
              AND NOT attribute.attisdropped
              AND acl.grantee IN (0, writer_oid)
        LOOP
            IF grant_row.privilege_type NOT IN (
                'SELECT', 'INSERT', 'UPDATE', 'REFERENCES'
            ) THEN
                RAISE EXCEPTION
                    'Unexpected Stage-4 column privilege % on public.%.%',
                    grant_row.privilege_type,
                    relation_name,
                    grant_row.attname;
            END IF;
            grantee_sql := CASE
                WHEN grant_row.grantee = 0 THEN 'PUBLIC'
                ELSE 'research_signal_snapshot_writer_v1'
            END;
            EXECUTE pg_catalog.format(
                'REVOKE %s (%I) ON TABLE public.%I FROM %s CASCADE',
                grant_row.privilege_type,
                grant_row.attname,
                relation_name,
                grantee_sql
            );
        END LOOP;
    END LOOP;
END;
$column_acl_cleanup$;

-- No application role needs to install triggers on the shared event table.
-- Remove every explicit non-owner TRIGGER grant so the catalog attestation can
-- rule out an insert-rewrite seam between validation and persistence.
DO $trigger_acl_cleanup$
DECLARE
    grant_row RECORD;
    grantee_sql TEXT;
    owner_oid OID := (
        SELECT relation_row.relowner
        FROM pg_catalog.pg_class relation_row
        WHERE relation_row.oid = 'public.research_events'::REGCLASS
    );
BEGIN
    FOR grant_row IN
        SELECT DISTINCT acl.grantee
        FROM pg_catalog.pg_class relation_row
        CROSS JOIN LATERAL pg_catalog.aclexplode(
            COALESCE(
                relation_row.relacl,
                pg_catalog.acldefault('r', relation_row.relowner)
            )
        ) acl
        WHERE relation_row.oid = 'public.research_events'::REGCLASS
          AND acl.privilege_type = 'TRIGGER'
          AND acl.grantee <> owner_oid
    LOOP
        grantee_sql := CASE
            WHEN grant_row.grantee = 0 THEN 'PUBLIC'
            ELSE pg_catalog.quote_ident(
                pg_catalog.pg_get_userbyid(grant_row.grantee)
            )
        END;
        EXECUTE pg_catalog.format(
            'REVOKE TRIGGER ON TABLE public.research_events FROM %s CASCADE',
            grantee_sql
        );
    END LOOP;
END;
$trigger_acl_cleanup$;

REVOKE ALL ON SEQUENCE
    public.research_max_pain_snapshot_sets_snapshot_set_id_seq,
    public.research_max_pain_snapshot_rows_snapshot_row_id_seq
    FROM PUBLIC, research_signal_snapshot_writer_v1;

REVOKE ALL ON FUNCTION
    public.validate_research_signal_snapshot_v1_set_complete()
    FROM PUBLIC, research_signal_snapshot_writer_v1;

COMMENT ON INDEX public.uq_research_signal_snapshot_projection_key_v1 IS
    'Exactly one terminal Stage-4 projection receipt per immutable Max-Pain snapshot key.';

COMMENT ON INDEX public.idx_research_signal_snapshot_archive_key_v1 IS
    'Bounded Stage-4 set validation by immutable Max-Pain snapshot key.';
