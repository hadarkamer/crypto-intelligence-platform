-- Immutable Stage-4 explicit no-signal outcome carriers v1
--
-- Materializes a closed price path only for a cell proven absent from one
-- terminal COMPLETED Stage-4 projection.  The identity is projection x symbol
-- x direction x horizon.  This migration creates no Formula, delivery, LIVE,
-- Telegram or trading authority.  Roles/passwords are provisioned out of band.

SET LOCAL search_path = pg_catalog;
SET LOCAL TIME ZONE 'UTC';
SET LOCAL DateStyle = 'ISO, YMD';
SET LOCAL IntervalStyle = 'postgres';
SET LOCAL extra_float_digits = 3;
SET LOCAL quote_all_identifiers = off;

DO $preflight$
DECLARE
    writer_row RECORD;
    reader_row RECORD;
    trusted_owner OID;
    source_name TEXT;
    source_oid OID;
    existing_raw_oid OID;
    existing_raw_kind TEXT;
    existing_raw_owner OID;
    existing_raw_comment TEXT;
    expected_raw_catalog_sha256 TEXT;
    actual_raw_catalog_sha256 TEXT;
    raw_catalog JSONB;
BEGIN
    IF pg_catalog.current_setting('server_version_num')::INTEGER < 150000 THEN
        RAISE EXCEPTION 'Stage-4 no-signal outcomes require PostgreSQL 15+';
    END IF;
    SELECT * INTO writer_row FROM pg_catalog.pg_roles
     WHERE rolname = 'research_stage4_no_signal_outcome_writer_v1';
    IF NOT FOUND OR NOT writer_row.rolcanlogin OR writer_row.rolinherit
       OR writer_row.rolsuper OR writer_row.rolcreatedb
       OR writer_row.rolcreaterole OR writer_row.rolreplication
       OR writer_row.rolbypassrls THEN
        RAISE EXCEPTION
            'research_stage4_no_signal_outcome_writer_v1 must be an unprivileged NOINHERIT LOGIN role';
    END IF;
    SELECT * INTO reader_row FROM pg_catalog.pg_roles
     WHERE rolname = 'research_formula_exploration_reader_v1';
    IF NOT FOUND OR NOT reader_row.rolcanlogin OR reader_row.rolinherit
       OR reader_row.rolsuper OR reader_row.rolcreatedb
       OR reader_row.rolcreaterole OR reader_row.rolreplication
       OR reader_row.rolbypassrls THEN
        RAISE EXCEPTION
            'research_formula_exploration_reader_v1 must remain an unprivileged NOINHERIT LOGIN role';
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_auth_members membership
         WHERE membership.member IN (writer_row.oid, reader_row.oid)
            OR membership.roleid IN (writer_row.oid, reader_row.oid)
    ) THEN
        RAISE EXCEPTION 'Stage-4 no-signal roles cannot participate in membership';
    END IF;
    IF pg_catalog.has_schema_privilege(
           'research_stage4_no_signal_outcome_writer_v1', 'public', 'CREATE'
       ) OR pg_catalog.has_schema_privilege(
           'research_formula_exploration_reader_v1', 'public', 'CREATE'
       ) OR pg_catalog.has_database_privilege(
           'research_stage4_no_signal_outcome_writer_v1',
           pg_catalog.current_database(), 'CREATE'
       ) OR pg_catalog.has_database_privilege(
           'research_formula_exploration_reader_v1',
           pg_catalog.current_database(), 'CREATE'
       ) THEN
        RAISE EXCEPTION
            'Stage-4 no-signal roles cannot create database or schema objects';
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_database database_row
         WHERE database_row.datname=pg_catalog.current_database()
           AND database_row.datdba IN (writer_row.oid, reader_row.oid)
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_namespace namespace_row
         WHERE namespace_row.nspname='public'
           AND namespace_row.nspowner IN (writer_row.oid, reader_row.oid)
    ) THEN
        RAISE EXCEPTION 'Stage-4 no-signal roles cannot own the database or schema';
    END IF;

    FOREACH source_name IN ARRAY ARRAY[
        'research_events',
        'research_max_pain_snapshot_sets',
        'research_max_pain_snapshot_symbols',
        'research_max_pain_snapshot_rows',
        'research_formula_exploration_stage4_v1',
        'research_formula_exploration_outcomes_v1'
    ] LOOP
        source_oid := pg_catalog.to_regclass('public.' || source_name);
        IF source_oid IS NULL THEN
            RAISE EXCEPTION 'Required source public.% is missing', source_name;
        END IF;
    END LOOP;
    SELECT relation.relowner INTO trusted_owner
      FROM pg_catalog.pg_class relation
     WHERE relation.oid = 'public.research_events'::REGCLASS;
    IF trusted_owner IS NULL
       OR pg_catalog.pg_get_userbyid(trusted_owner) <> SESSION_USER
       OR trusted_owner IN (writer_row.oid, reader_row.oid)
       OR EXISTS (
           SELECT 1 FROM pg_catalog.pg_class relation
            WHERE relation.oid IN (
                'public.research_max_pain_snapshot_sets'::REGCLASS,
                'public.research_max_pain_snapshot_symbols'::REGCLASS,
                'public.research_max_pain_snapshot_rows'::REGCLASS,
                'public.research_formula_exploration_stage4_v1'::REGCLASS,
                'public.research_formula_exploration_outcomes_v1'::REGCLASS
            ) AND relation.relowner <> trusted_owner
       ) THEN
        RAISE EXCEPTION 'Stage-4 no-signal migration requires one trusted source owner';
    END IF;
    IF COALESCE(pg_catalog.obj_description(
           'public.research_formula_exploration_stage4_v1'::REGCLASS,
           'pg_class'
       ), '') NOT LIKE 'stage4-wave-v5-authoritative-source-v1;%'
       OR COALESCE(pg_catalog.obj_description(
           'public.research_formula_exploration_outcomes_v1'::REGCLASS,
           'pg_class'
       ), '') NOT LIKE 'stage4-formula-exploration-outcomes-v1;%' THEN
        RAISE EXCEPTION 'Migrations 024/025 catalog receipts are missing';
    END IF;

    existing_raw_oid := pg_catalog.to_regclass(
        'public.research_stage4_no_signal_outcomes_v1'
    );
    IF existing_raw_oid IS NOT NULL THEN
        SELECT relation.relkind::TEXT, relation.relowner,
               pg_catalog.obj_description(relation.oid, 'pg_class')
          INTO existing_raw_kind, existing_raw_owner, existing_raw_comment
          FROM pg_catalog.pg_class relation
         WHERE relation.oid=existing_raw_oid;
        IF existing_raw_kind IS DISTINCT FROM 'r'
           OR existing_raw_owner IS DISTINCT FROM trusted_owner THEN
            RAISE EXCEPTION
                'Existing Stage-4 no-signal carrier has an unsafe kind or owner';
        END IF;
        SELECT JSONB_BUILD_OBJECT(
            'columns', COALESCE((
                SELECT JSONB_AGG(JSONB_BUILD_OBJECT(
                           'ordinal', attribute.attnum,
                           'name', attribute.attname,
                           'type', pg_catalog.format_type(
                               attribute.atttypid, attribute.atttypmod
                           ),
                           'not_null', attribute.attnotnull,
                           'identity', attribute.attidentity,
                           'generated', attribute.attgenerated,
                           'collation', attribute.attcollation::REGCOLLATION::TEXT,
                           'default', pg_catalog.pg_get_expr(
                               default_row.adbin, default_row.adrelid, FALSE
                           )
                       ) ORDER BY attribute.attnum)
                  FROM pg_catalog.pg_attribute attribute
                  LEFT JOIN pg_catalog.pg_attrdef default_row
                    ON default_row.adrelid=attribute.attrelid
                   AND default_row.adnum=attribute.attnum
                 WHERE attribute.attrelid=existing_raw_oid
                   AND attribute.attnum>0 AND NOT attribute.attisdropped
            ), '[]'::JSONB),
            'constraints', COALESCE((
                SELECT JSONB_AGG(JSONB_BUILD_OBJECT(
                           'name', constraint_row.conname,
                           'type', constraint_row.contype,
                           'deferrable', constraint_row.condeferrable,
                           'deferred', constraint_row.condeferred,
                           'validated', constraint_row.convalidated,
                           'no_inherit', constraint_row.connoinherit,
                           'definition', pg_catalog.pg_get_constraintdef(
                               constraint_row.oid, FALSE
                           )
                       ) ORDER BY constraint_row.conname)
                  FROM pg_catalog.pg_constraint constraint_row
                 WHERE constraint_row.conrelid=existing_raw_oid
            ), '[]'::JSONB),
            'indexes', COALESCE((
                SELECT JSONB_AGG(JSONB_BUILD_OBJECT(
                           'name', index_relation.relname,
                           'access_method', access_method.amname,
                           'unique', index_row.indisunique,
                           'primary', index_row.indisprimary,
                           'exclusion', index_row.indisexclusion,
                           'immediate', index_row.indimmediate,
                           'valid', index_row.indisvalid,
                           'ready', index_row.indisready,
                           'live', index_row.indislive,
                           'definition', pg_catalog.pg_get_indexdef(
                               index_row.indexrelid
                           )
                       ) ORDER BY index_relation.relname)
                  FROM pg_catalog.pg_index index_row
                  JOIN pg_catalog.pg_class index_relation
                    ON index_relation.oid=index_row.indexrelid
                  JOIN pg_catalog.pg_am access_method
                    ON access_method.oid=index_relation.relam
                 WHERE index_row.indrelid=existing_raw_oid
            ), '[]'::JSONB)
        ) INTO raw_catalog;
        actual_raw_catalog_sha256 := ENCODE(SHA256(CONVERT_TO(
            raw_catalog::TEXT, 'UTF8'
        )), 'hex');
        expected_raw_catalog_sha256 := SUBSTRING(
            existing_raw_comment
            FROM 'raw_catalog_sha256=([0-9a-f]{64})(;|$)'
        );
        IF existing_raw_comment NOT LIKE
               'stage4-explicit-no-signal-outcomes-raw-v1;%'
           OR expected_raw_catalog_sha256 IS NULL
           OR expected_raw_catalog_sha256 IS DISTINCT FROM
                actual_raw_catalog_sha256 THEN
            RAISE EXCEPTION
                'Existing Stage-4 no-signal carrier catalog receipt is invalid';
        END IF;
    END IF;
END;
$preflight$;

LOCK TABLE public.research_events IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE public.research_max_pain_snapshot_sets IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE public.research_max_pain_snapshot_symbols IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE public.research_max_pain_snapshot_rows IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE public.research_formula_exploration_stage4_v1 IN ACCESS SHARE MODE;
LOCK TABLE public.research_formula_exploration_outcomes_v1 IN ACCESS SHARE MODE;

SET LOCAL search_path = public;

CREATE TABLE IF NOT EXISTS public.research_stage4_no_signal_outcomes_v1 (
    projection_event_id BIGINT NOT NULL
        REFERENCES public.research_events(event_id) ON DELETE RESTRICT,
    projection_event_fingerprint CHAR(64) NOT NULL CHECK (
        BTRIM(projection_event_fingerprint) ~ '^[0-9a-f]{64}$'
    ),
    snapshot_set_id BIGINT NOT NULL
        REFERENCES public.research_max_pain_snapshot_sets(snapshot_set_id)
        ON DELETE RESTRICT,
    snapshot_key CHAR(64) NOT NULL CHECK (
        BTRIM(snapshot_key) ~ '^[0-9a-f]{64}$'
    ),
    symbol TEXT NOT NULL CHECK (BTRIM(symbol) ~ '^[A-Z0-9-]{1,20}$'),
    direction TEXT NOT NULL CHECK (direction IN ('LONG', 'SHORT')),
    horizon_minutes INTEGER NOT NULL CHECK (
        horizon_minutes IN (60, 240, 720, 1440)
    ),
    decision_time_utc TIMESTAMPTZ NOT NULL,
    absence_basis TEXT NOT NULL CHECK (
        absence_basis =
            'COMPLETED_PROJECTION_EVALUABLE_SYMBOL_WITHOUT_SIGNAL'
    ),
    reference_receipt JSONB NOT NULL CHECK (
        JSONB_TYPEOF(reference_receipt) = 'object'
    ),
    reference_receipt_sha256 CHAR(64) NOT NULL CHECK (
        BTRIM(reference_receipt_sha256) ~ '^[0-9a-f]{64}$'
    ),
    cell_identity_sha256 CHAR(64) NOT NULL UNIQUE CHECK (
        BTRIM(cell_identity_sha256) ~ '^[0-9a-f]{64}$'
    ),
    measured_at_utc TIMESTAMPTZ NOT NULL,
    reference_price DOUBLE PRECISION NOT NULL CHECK (reference_price > 0),
    price_at_horizon DOUBLE PRECISION NOT NULL CHECK (price_at_horizon > 0),
    raw_return_pct DOUBLE PRECISION NOT NULL,
    directional_return_pct DOUBLE PRECISION NOT NULL,
    max_favorable_price DOUBLE PRECISION NOT NULL CHECK (
        max_favorable_price > 0
    ),
    max_adverse_price DOUBLE PRECISION NOT NULL CHECK (max_adverse_price > 0),
    mfe_pct DOUBLE PRECISION NOT NULL CHECK (mfe_pct >= 0),
    mae_pct DOUBLE PRECISION NOT NULL CHECK (mae_pct >= 0),
    time_to_first_progress_seconds INTEGER CHECK (
        time_to_first_progress_seconds IS NULL
        OR time_to_first_progress_seconds >= 0
    ),
    time_to_mfe_seconds INTEGER NOT NULL CHECK (time_to_mfe_seconds >= 0),
    path_resolution_seconds INTEGER NOT NULL CHECK (
        path_resolution_seconds = 60
    ),
    path_samples INTEGER NOT NULL CHECK (path_samples > 0),
    outcome_method_version TEXT NOT NULL CHECK (
        outcome_method_version =
            'canonical-spot-1m-ohlc-path-v3+stage4-no-signal-frozen-archive-input-v1'
    ),
    price_source TEXT NOT NULL CHECK (BTRIM(price_source) <> ''),
    data_quality_status TEXT NOT NULL CHECK (
        data_quality_status IN (
            'VERIFIED_BINANCE_SPOT_1M_CLOSED_CANDLES',
            'VERIFIED_HYPERLIQUID_SPOT_1M_CLOSED_CANDLES'
        )
    ),
    outcome_payload_sha256 CHAR(64) NOT NULL CHECK (
        BTRIM(outcome_payload_sha256) ~ '^[0-9a-f]{64}$'
    ),
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (
        projection_event_id, symbol, direction, horizon_minutes
    )
);

DO $raw_shape_assertions$
DECLARE
    raw_oid OID := 'public.research_stage4_no_signal_outcomes_v1'::REGCLASS;
    trusted_owner OID := (
        SELECT relowner FROM pg_catalog.pg_class
         WHERE oid='public.research_events'::REGCLASS
    );
    expected_columns TEXT[] := ARRAY[
        'projection_event_id|bigint|true|false',
        'projection_event_fingerprint|character(64)|true|false',
        'snapshot_set_id|bigint|true|false',
        'snapshot_key|character(64)|true|false',
        'symbol|text|true|false',
        'direction|text|true|false',
        'horizon_minutes|integer|true|false',
        'decision_time_utc|timestamp with time zone|true|false',
        'absence_basis|text|true|false',
        'reference_receipt|jsonb|true|false',
        'reference_receipt_sha256|character(64)|true|false',
        'cell_identity_sha256|character(64)|true|false',
        'measured_at_utc|timestamp with time zone|true|false',
        'reference_price|double precision|true|false',
        'price_at_horizon|double precision|true|false',
        'raw_return_pct|double precision|true|false',
        'directional_return_pct|double precision|true|false',
        'max_favorable_price|double precision|true|false',
        'max_adverse_price|double precision|true|false',
        'mfe_pct|double precision|true|false',
        'mae_pct|double precision|true|false',
        'time_to_first_progress_seconds|integer|false|false',
        'time_to_mfe_seconds|integer|true|false',
        'path_resolution_seconds|integer|true|false',
        'path_samples|integer|true|false',
        'outcome_method_version|text|true|false',
        'price_source|text|true|false',
        'data_quality_status|text|true|false',
        'outcome_payload_sha256|character(64)|true|false',
        'created_at_utc|timestamp with time zone|true|true'
    ]::TEXT[];
    actual_columns TEXT[];
    constraint_count BIGINT;
BEGIN
    SELECT ARRAY_AGG(
               attribute.attname::TEXT || '|' ||
               pg_catalog.format_type(
                   attribute.atttypid, attribute.atttypmod
               ) || '|' || attribute.attnotnull::TEXT || '|' ||
               (default_row.oid IS NOT NULL)::TEXT
               ORDER BY attribute.attnum
           )
      INTO actual_columns
      FROM pg_catalog.pg_attribute attribute
      LEFT JOIN pg_catalog.pg_attrdef default_row
        ON default_row.adrelid=attribute.attrelid
       AND default_row.adnum=attribute.attnum
     WHERE attribute.attrelid=raw_oid
       AND attribute.attnum>0 AND NOT attribute.attisdropped;
    SELECT COUNT(*) INTO constraint_count
      FROM pg_catalog.pg_constraint constraint_row
     WHERE constraint_row.conrelid=raw_oid;
    IF actual_columns IS DISTINCT FROM expected_columns
       OR NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_class relation
             WHERE relation.oid=raw_oid AND relation.relkind='r'
               AND relation.relpersistence='p'
               AND NOT relation.relispartition
               AND NOT relation.relrowsecurity
               AND NOT relation.relforcerowsecurity
               AND relation.relowner=trusted_owner
       )
       OR EXISTS (
            SELECT 1 FROM pg_catalog.pg_policy policy_row
             WHERE policy_row.polrelid=raw_oid
       )
       OR EXISTS (
            SELECT 1 FROM pg_catalog.pg_rewrite rewrite_row
             WHERE rewrite_row.ev_class=raw_oid
       )
       OR constraint_count<>27
       OR EXISTS (
            SELECT 1 FROM pg_catalog.pg_constraint constraint_row
             WHERE constraint_row.conrelid=raw_oid
               AND (NOT constraint_row.convalidated
                    OR constraint_row.condeferrable
                    OR constraint_row.condeferred)
       )
       OR (SELECT COUNT(*) FROM pg_catalog.pg_constraint constraint_row
            WHERE constraint_row.conrelid=raw_oid
              AND constraint_row.contype='c')<>23
       OR (SELECT COUNT(*) FROM pg_catalog.pg_constraint constraint_row
            WHERE constraint_row.conrelid=raw_oid
              AND constraint_row.contype='f')<>2
       OR (SELECT COUNT(*) FROM pg_catalog.pg_constraint constraint_row
            WHERE constraint_row.conrelid=raw_oid
              AND constraint_row.contype='p')<>1
       OR (SELECT COUNT(*) FROM pg_catalog.pg_constraint constraint_row
            WHERE constraint_row.conrelid=raw_oid
              AND constraint_row.contype='u')<>1
       OR NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_constraint constraint_row
             WHERE constraint_row.conrelid=raw_oid
               AND constraint_row.contype='f'
               AND constraint_row.confrelid='public.research_events'::REGCLASS
               AND constraint_row.confdeltype='r'
               AND constraint_row.conkey=ARRAY[(
                   SELECT attnum FROM pg_catalog.pg_attribute
                    WHERE attrelid=raw_oid
                      AND attname='projection_event_id'
               )]::SMALLINT[]
       )
       OR NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_constraint constraint_row
             WHERE constraint_row.conrelid=raw_oid
               AND constraint_row.contype='f'
               AND constraint_row.confrelid=
                   'public.research_max_pain_snapshot_sets'::REGCLASS
               AND constraint_row.confdeltype='r'
               AND constraint_row.conkey=ARRAY[(
                   SELECT attnum FROM pg_catalog.pg_attribute
                    WHERE attrelid=raw_oid AND attname='snapshot_set_id'
               )]::SMALLINT[]
       )
       OR NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_constraint constraint_row
             WHERE constraint_row.conrelid=raw_oid
               AND constraint_row.contype='p'
               AND constraint_row.conkey=ARRAY[
                   (SELECT attnum FROM pg_catalog.pg_attribute
                     WHERE attrelid=raw_oid
                       AND attname='projection_event_id'),
                   (SELECT attnum FROM pg_catalog.pg_attribute
                     WHERE attrelid=raw_oid AND attname='symbol'),
                   (SELECT attnum FROM pg_catalog.pg_attribute
                     WHERE attrelid=raw_oid AND attname='direction'),
                   (SELECT attnum FROM pg_catalog.pg_attribute
                     WHERE attrelid=raw_oid AND attname='horizon_minutes')
               ]::SMALLINT[]
       )
       OR NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_constraint constraint_row
             WHERE constraint_row.conrelid=raw_oid
               AND constraint_row.contype='u'
               AND constraint_row.conkey=ARRAY[(
                   SELECT attnum FROM pg_catalog.pg_attribute
                    WHERE attrelid=raw_oid
                      AND attname='cell_identity_sha256'
               )]::SMALLINT[]
       ) THEN
        RAISE EXCEPTION
            'Stage-4 no-signal raw carrier shape/constraints are not exact';
    END IF;
END;
$raw_shape_assertions$;

CREATE OR REPLACE FUNCTION public.validate_research_stage4_no_signal_outcome_v1()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog, public
SET timezone = 'UTC'
AS $function$
DECLARE
    projection public.research_events%ROWTYPE;
    archive_set public.research_max_pain_snapshot_sets%ROWTYPE;
    manifest public.research_max_pain_snapshot_symbols%ROWTYPE;
    reference_row public.research_max_pain_snapshot_rows%ROWTYPE;
    expected_reference JSONB;
    raw_price JSONB;
    evaluation_count BIGINT;
    signal_count BIGINT;
    archive_row_count BIGINT;
    price_signature_count BIGINT;
    observed_at TIMESTAMPTZ;
    expected_raw_return DOUBLE PRECISION;
    expected_directional_return DOUBLE PRECISION;
    expected_mfe DOUBLE PRECISION;
    expected_mae DOUBLE PRECISION;
    observed_seconds BIGINT;
    decision_epoch_ms BIGINT;
    horizon_epoch_ms BIGINT;
    first_path_open_ms BIGINT;
    last_path_open_ms BIGINT;
    expected_path_samples BIGINT;
    expected_measured_at TIMESTAMPTZ;
    source_fields JSONB;
    source_token_count BIGINT;
    source_key_count BIGINT;
    source_tokens_well_formed BOOLEAN;
    source_observed_at TIMESTAMPTZ;
    source_fetched_at TIMESTAMPTZ;
    source_observed_age DOUBLE PRECISION;
    source_fetched_age DOUBLE PRECISION;
    hash_tags TEXT[];
    hash_values TEXT[];
    canonical_hash_payload TEXT;
    hash_index INTEGER;
BEGIN
    IF SESSION_USER <> 'research_stage4_no_signal_outcome_writer_v1'
       OR CURRENT_USER <> 'research_stage4_no_signal_outcome_writer_v1' THEN
        RAISE EXCEPTION USING ERRCODE='42501',
            MESSAGE='Stage-4 no-signal outcome requires its dedicated writer';
    END IF;

    SELECT * INTO STRICT projection
      FROM public.research_events event_row
     WHERE event_row.event_id = NEW.projection_event_id;
    IF projection.event_kind IS DISTINCT FROM 'DECISION_SAMPLE'
       OR projection.event_type IS DISTINCT FROM 'SIGNAL_SNAPSHOT_PROJECTION'
       OR projection.capture_stage IS DISTINCT FROM 'SILENT_SIGNAL_SNAPSHOT'
       OR projection.strategy_version IS DISTINCT FROM 'signal-snapshot-v1'
       OR projection.delivery_status IS DISTINCT FROM 'NOT_APPLICABLE'
       OR projection.symbol IS DISTINCT FROM 'RESEARCH'
       OR projection.direction IS DISTINCT FROM 'NEUTRAL'
       OR projection.delivery_attempted_at_utc IS NOT NULL
       OR projection.delivered_at_utc IS NOT NULL
       OR NOT (projection.categories @>
            '["DECISION_SAMPLE","SILENT","COMPLETED"]'::JSONB)
       OR projection.engine_snapshot #>>
            '{signal_snapshot,contract_version}' IS DISTINCT FROM
            'research-signal-snapshot-v1'
       OR projection.engine_snapshot #>>
            '{signal_snapshot,signal_family}' IS DISTINCT FROM 'PROJECTION'
       OR projection.engine_snapshot #>>
            '{signal_snapshot,tier}' IS DISTINCT FROM 'COMPLETED'
       OR projection.engine_snapshot #>>
            '{projection,status}' IS DISTINCT FROM 'COMPLETED'
       OR projection.engine_snapshot #>
            '{signal_snapshot,formula_authorized}' IS DISTINCT FROM 'false'::JSONB
       OR projection.engine_snapshot #>
            '{signal_snapshot,outcome_authorized}' IS DISTINCT FROM 'false'::JSONB
       OR projection.engine_snapshot #>
            '{signal_snapshot,telegram_delivery_allowed}' IS DISTINCT FROM 'false'::JSONB
       OR projection.engine_snapshot #>
            '{signal_snapshot,trade_execution_allowed}' IS DISTINCT FROM 'false'::JSONB
       OR BTRIM(projection.event_fingerprint) IS DISTINCT FROM
            BTRIM(NEW.projection_event_fingerprint)
       OR projection.alert_time_utc IS DISTINCT FROM NEW.decision_time_utc
       OR projection.engine_snapshot #>> '{projection,snapshot_key}'
            IS DISTINCT FROM BTRIM(NEW.snapshot_key)
       OR (projection.engine_snapshot #>> '{projection,snapshot_set_id}')::BIGINT
            IS DISTINCT FROM NEW.snapshot_set_id THEN
        RAISE EXCEPTION USING ERRCODE='23514',
            MESSAGE='no-signal carrier projection identity is invalid';
    END IF;

    SELECT COUNT(*) INTO evaluation_count
      FROM JSONB_ARRAY_ELEMENTS(
          projection.engine_snapshot #> '{projection,symbol_evaluations}'
      ) evaluation
     WHERE evaluation ->> 'symbol' = NEW.symbol
       AND evaluation ->> 'status' = 'EVALUABLE'
       AND evaluation -> 'reason' IS NOT DISTINCT FROM 'null'::JSONB;
    IF evaluation_count <> 1 THEN
        RAISE EXCEPTION USING ERRCODE='23514',
            MESSAGE='no-signal carrier requires one evaluable symbol cell';
    END IF;

    SELECT COUNT(*) INTO signal_count
      FROM public.research_events signal
     WHERE signal.capture_stage = 'SILENT_SIGNAL_SNAPSHOT'
       AND signal.event_type IN (
           'MAX_PAIN_CONFIRMATION_STATE',
           'MAGNET_CONFIRMATION_STATE',
           'SILENT_COMBINED_CONFIRMATION_SNAPSHOT'
       )
       AND signal.symbol = NEW.symbol
       AND signal.direction = NEW.direction
       AND signal.engine_snapshot #>>
           '{signal_snapshot,archive_reference,snapshot_key}' =
           BTRIM(NEW.snapshot_key);
    IF signal_count <> 0 THEN
        RAISE EXCEPTION USING ERRCODE='23514',
            MESSAGE='no-signal carrier cannot label a cell containing a signal';
    END IF;

    SELECT * INTO STRICT archive_set
      FROM public.research_max_pain_snapshot_sets source
     WHERE source.snapshot_set_id = NEW.snapshot_set_id;
    SELECT * INTO STRICT manifest
      FROM public.research_max_pain_snapshot_symbols source
     WHERE source.snapshot_set_id = NEW.snapshot_set_id
       AND source.symbol = NEW.symbol;
    SELECT * INTO STRICT reference_row
      FROM public.research_max_pain_snapshot_rows source
     WHERE source.snapshot_set_id = NEW.snapshot_set_id
       AND source.symbol = NEW.symbol
       AND source.timeframe = '12h';
    IF BTRIM(archive_set.snapshot_key) IS DISTINCT FROM BTRIM(NEW.snapshot_key)
       OR BTRIM(archive_set.payload_sha256) IS DISTINCT FROM
            projection.engine_snapshot #>> '{projection,set_payload_sha256}'
       OR archive_set.source IS DISTINCT FROM 'RESEARCH_PASSIVE'
       OR archive_set.research_eligible IS DISTINCT FROM TRUE
       OR manifest.research_eligible IS DISTINCT FROM TRUE
       OR manifest.complete_7of7 IS DISTINCT FROM TRUE
       OR manifest.price_overlay_coherent IS DISTINCT FROM TRUE
       OR reference_row.row_valid IS DISTINCT FROM TRUE
       OR reference_row.freshness_status IS DISTINCT FROM 'FRESH'
       OR reference_row.price_source_policy_status IS DISTINCT FROM 'PASS' THEN
        RAISE EXCEPTION USING ERRCODE='23514',
            MESSAGE='no-signal carrier archive source is not eligible';
    END IF;
    SELECT COUNT(*), COUNT(DISTINCT ROW(
               child.current_price, COALESCE(child.price_source, ''),
               COALESCE(child.price_exchange, ''),
               COALESCE(child.price_market, ''),
               COALESCE(child.price_pair, ''),
               COALESCE(child.price_instrument, ''), child.price_fetched_at_utc
           ))
      INTO archive_row_count, price_signature_count
      FROM public.research_max_pain_snapshot_rows child
     WHERE child.snapshot_set_id = NEW.snapshot_set_id
       AND child.symbol = NEW.symbol
       AND child.row_valid = TRUE
       AND child.freshness_status = 'FRESH';
    IF archive_row_count <> 7 OR price_signature_count <> 1 THEN
        RAISE EXCEPTION USING ERRCODE='23514',
            MESSAGE='no-signal carrier requires one coherent seven-row archive price';
    END IF;

    raw_price := reference_row.raw_provenance;
    BEGIN
        observed_at := (raw_price ->> 'price_observed_at_utc')::TIMESTAMPTZ;
    EXCEPTION WHEN invalid_datetime_format OR datetime_field_overflow THEN
        RAISE EXCEPTION USING ERRCODE='23514',
            MESSAGE='no-signal observed price timestamp is invalid';
    END;
    IF raw_price ->> 'price_interval' IS DISTINCT FROM '1m'
       OR raw_price ->> 'price_observed_at_utc' IS NULL
       OR raw_price ->> 'price_observed_at_utc'
            !~ '(Z|[+-][0-9]{2}:[0-9]{2})$'
       OR (
            raw_price ->> 'price_candle_open_time_utc' IS NOT NULL
            AND raw_price ->> 'price_candle_open_time_utc'
                !~ '(Z|[+-][0-9]{2}:[0-9]{2})$'
       )
       OR (
            raw_price ->> 'price_candle_close_time_utc' IS NOT NULL
            AND raw_price ->> 'price_candle_close_time_utc'
                !~ '(Z|[+-][0-9]{2}:[0-9]{2})$'
       )
       OR observed_at IS NULL
       OR reference_row.price_fetched_at_utc IS NULL
       OR observed_at > reference_row.price_fetched_at_utc
       OR reference_row.price_fetched_at_utc > NEW.decision_time_utc
       OR NEW.decision_time_utc - observed_at > INTERVAL '60 minutes'
       OR NEW.decision_time_utc - reference_row.price_fetched_at_utc >
            INTERVAL '60 minutes'
       OR reference_row.current_price IS NULL
       OR (
           NEW.symbol = 'HYPE' AND NOT (
               reference_row.price_source='hyperliquid'
               AND reference_row.price_exchange='hyperliquid'
               AND reference_row.price_market='spot'
               AND reference_row.price_pair='HYPE/USDT'
               AND reference_row.price_instrument='@107'
           )
       ) OR (
           NEW.symbol <> 'HYPE' AND NOT (
               reference_row.price_source='binance_spot'
               AND reference_row.price_exchange='binance'
               AND reference_row.price_market='spot'
               AND REPLACE(reference_row.price_pair, '/', '')=
                   NEW.symbol || 'USDT'
           )
       ) THEN
        RAISE EXCEPTION USING ERRCODE='23514',
            MESSAGE='no-signal carrier frozen price provenance is invalid';
    END IF;

    expected_reference := JSONB_BUILD_OBJECT(
        'contract_version', 'stage4-no-signal-frozen-archive-price-reference-v1',
        'projection_event_id', projection.event_id,
        'projection_event_fingerprint', BTRIM(projection.event_fingerprint),
        'snapshot_set_id', archive_set.snapshot_set_id,
        'snapshot_key', BTRIM(archive_set.snapshot_key),
        'set_payload_sha256', BTRIM(archive_set.payload_sha256),
        'symbol', NEW.symbol,
        'symbol_manifest_payload_sha256', BTRIM(manifest.payload_sha256),
        'source_timeframe', '12h',
        'snapshot_row_id', reference_row.snapshot_row_id,
        'snapshot_row_payload_sha256', BTRIM(reference_row.payload_sha256),
        'official_price', JSONB_BUILD_OBJECT(
            'price', reference_row.current_price,
            'source', reference_row.price_source,
            'exchange', reference_row.price_exchange,
            'market', reference_row.price_market,
            'pair', reference_row.price_pair,
            'instrument', COALESCE(reference_row.price_instrument, ''),
            'interval', raw_price ->> 'price_interval',
            'fetched_at_utc', TO_CHAR(
                reference_row.price_fetched_at_utc AT TIME ZONE 'UTC',
                'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
            ),
            'observed_at_utc', TO_CHAR(
                observed_at AT TIME ZONE 'UTC',
                'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
            ),
            'candle_open_time_utc', CASE
                WHEN raw_price ->> 'price_candle_open_time_utc' IS NULL THEN NULL
                ELSE TO_CHAR(
                    (raw_price ->> 'price_candle_open_time_utc')::TIMESTAMPTZ
                        AT TIME ZONE 'UTC',
                    'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
                ) END,
            'candle_close_time_utc', CASE
                WHEN raw_price ->> 'price_candle_close_time_utc' IS NULL THEN NULL
                ELSE TO_CHAR(
                    (raw_price ->> 'price_candle_close_time_utc')::TIMESTAMPTZ
                        AT TIME ZONE 'UTC',
                    'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
                ) END,
            'policy_status', reference_row.price_source_policy_status
        )
    );
    IF NEW.reference_receipt IS DISTINCT FROM expected_reference
       OR NEW.reference_price IS DISTINCT FROM reference_row.current_price THEN
        RAISE EXCEPTION USING ERRCODE='23514',
            MESSAGE='no-signal carrier reference receipt mismatch';
    END IF;

    decision_epoch_ms := FLOOR(
        EXTRACT(EPOCH FROM NEW.decision_time_utc) * 1000
    )::BIGINT;
    horizon_epoch_ms := FLOOR(EXTRACT(EPOCH FROM (
        NEW.decision_time_utc
        + NEW.horizon_minutes * INTERVAL '1 minute'
    )) * 1000)::BIGINT;
    first_path_open_ms := ((decision_epoch_ms + 59999) / 60000) * 60000;
    last_path_open_ms := ((horizon_epoch_ms - 59999) / 60000) * 60000;
    expected_path_samples := CASE
        WHEN last_path_open_ms < first_path_open_ms THEN 0
        ELSE ((last_path_open_ms - first_path_open_ms) / 60000) + 1
    END;
    expected_measured_at := TIMESTAMPTZ 'epoch'
        + (last_path_open_ms + 59999) * INTERVAL '1 millisecond';

    SELECT COUNT(*),
           COUNT(DISTINCT SPLIT_PART(token.value, '=', 1)),
           BOOL_AND(POSITION('=' IN token.value) > 1),
           JSONB_OBJECT_AGG(
               SPLIT_PART(token.value, '=', 1),
               SUBSTRING(token.value FROM POSITION('=' IN token.value) + 1)
           )
      INTO source_token_count, source_key_count,
           source_tokens_well_formed, source_fields
      FROM pg_catalog.unnest(
          pg_catalog.string_to_array(NEW.price_source, '|')
      ) token(value);
    BEGIN
        source_observed_at :=
            (source_fields ->> 'observed_at_utc')::TIMESTAMPTZ;
        source_fetched_at :=
            (source_fields ->> 'fetched_at_utc')::TIMESTAMPTZ;
        source_observed_age :=
            (source_fields ->> 'observed_age_seconds')::DOUBLE PRECISION;
        source_fetched_age :=
            (source_fields ->> 'fetched_age_seconds')::DOUBLE PRECISION;
    EXCEPTION WHEN invalid_text_representation OR invalid_datetime_format
                   OR datetime_field_overflow THEN
        RAISE EXCEPTION USING ERRCODE='23514',
            MESSAGE='no-signal outcome path provenance is malformed';
    END;

    IF NEW.outcome_method_version IS DISTINCT FROM
          'canonical-spot-1m-ohlc-path-v3+stage4-no-signal-frozen-archive-input-v1'
       OR NEW.absence_basis IS DISTINCT FROM
          'COMPLETED_PROJECTION_EVALUABLE_SYMBOL_WITHOUT_SIGNAL'
       OR NEW.path_resolution_seconds IS DISTINCT FROM 60
       OR expected_path_samples <= 0
       OR NEW.path_samples IS DISTINCT FROM expected_path_samples
       OR NEW.measured_at_utc IS DISTINCT FROM expected_measured_at
       OR pg_catalog.transaction_timestamp() <
            NEW.decision_time_utc + NEW.horizon_minutes * INTERVAL '1 minute'
       OR NEW.measured_at_utc > pg_catalog.transaction_timestamp()
       OR NEW.data_quality_status IS DISTINCT FROM (CASE
            WHEN NEW.symbol='HYPE' THEN
                'VERIFIED_HYPERLIQUID_SPOT_1M_CLOSED_CANDLES'
            ELSE 'VERIFIED_BINANCE_SPOT_1M_CLOSED_CANDLES' END)
       OR source_token_count IS DISTINCT FROM 16
       OR source_key_count IS DISTINCT FROM 16
       OR source_tokens_well_formed IS DISTINCT FROM TRUE
       OR NOT (source_fields ?& ARRAY[
            'reference', 'admission_policy', 'semantics', 'source',
            'exchange', 'market', 'pair', 'instrument',
            'observed_at_utc', 'fetched_at_utc',
            'observed_age_seconds', 'fetched_age_seconds',
            'snapshot_set_id', 'snapshot_key', 'path', 'provenance'
       ])
       OR source_fields ->> 'reference' IS DISTINCT FROM
            'reference_policy=stage4-no-signal-frozen-archive-price-reference-v1'
       OR source_fields ->> 'admission_policy' IS DISTINCT FROM
            'stage4-no-signal-completed-projection-evaluable-cell-admission-v1'
       OR source_fields ->> 'semantics' IS DISTINCT FROM
            'post_decision_path_metrics_relative_to_frozen_archive_input_price;not_trade_entry_return'
       OR source_fields ->> 'source' IS DISTINCT FROM reference_row.price_source
       OR source_fields ->> 'exchange' IS DISTINCT FROM reference_row.price_exchange
       OR source_fields ->> 'market' IS DISTINCT FROM reference_row.price_market
       OR source_fields ->> 'pair' IS DISTINCT FROM reference_row.price_pair
       OR source_fields ->> 'instrument' IS DISTINCT FROM
            COALESCE(reference_row.price_instrument, '')
       OR source_observed_at IS DISTINCT FROM observed_at
       OR source_fetched_at IS DISTINCT FROM reference_row.price_fetched_at_utc
       OR source_observed_age IN (
            'NaN'::FLOAT8, 'Infinity'::FLOAT8, '-Infinity'::FLOAT8
       )
       OR source_fetched_age IN (
            'NaN'::FLOAT8, 'Infinity'::FLOAT8, '-Infinity'::FLOAT8
       )
       OR source_observed_age < 0 OR source_observed_age > 3600
       OR source_fetched_age < 0 OR source_fetched_age > 3600
       OR ABS(source_observed_age - EXTRACT(EPOCH FROM (
            NEW.decision_time_utc - source_observed_at
       ))) > 0.000001
       OR ABS(source_fetched_age - EXTRACT(EPOCH FROM (
            NEW.decision_time_utc - source_fetched_at
       ))) > 0.000001
       OR source_fields ->> 'snapshot_set_id' IS DISTINCT FROM
            NEW.snapshot_set_id::TEXT
       OR source_fields ->> 'snapshot_key' IS DISTINCT FROM
            BTRIM(NEW.snapshot_key)
       OR source_fields ->> 'path' IS DISTINCT FROM (CASE
            WHEN NEW.symbol='HYPE' THEN 'hyperliquid_spot:HYPE/USDT:1m'
            WHEN NEW.symbol='1000PEPE' THEN 'binance_spot:PEPEUSDT:1m'
            ELSE 'binance_spot:' || NEW.symbol || 'USDT:1m' END)
       OR source_fields ->> 'provenance' IS DISTINCT FROM
            'EXCHANGE_API_HISTORICAL_CANDLES_IMPORTED'
       OR NEW.reference_price IN ('NaN'::FLOAT8, 'Infinity'::FLOAT8, '-Infinity'::FLOAT8)
       OR NEW.price_at_horizon IN ('NaN'::FLOAT8, 'Infinity'::FLOAT8, '-Infinity'::FLOAT8)
       OR NEW.raw_return_pct IN ('NaN'::FLOAT8, 'Infinity'::FLOAT8, '-Infinity'::FLOAT8)
       OR NEW.directional_return_pct IN ('NaN'::FLOAT8, 'Infinity'::FLOAT8, '-Infinity'::FLOAT8)
       OR NEW.max_favorable_price IN ('NaN'::FLOAT8, 'Infinity'::FLOAT8, '-Infinity'::FLOAT8)
       OR NEW.max_adverse_price IN ('NaN'::FLOAT8, 'Infinity'::FLOAT8, '-Infinity'::FLOAT8)
       OR NEW.mfe_pct IN ('NaN'::FLOAT8, 'Infinity'::FLOAT8, '-Infinity'::FLOAT8)
       OR NEW.mae_pct IN ('NaN'::FLOAT8, 'Infinity'::FLOAT8, '-Infinity'::FLOAT8) THEN
        RAISE EXCEPTION USING ERRCODE='23514',
            MESSAGE='no-signal carrier closed path envelope is invalid';
    END IF;

    expected_raw_return :=
        (NEW.price_at_horizon - NEW.reference_price) / NEW.reference_price * 100.0;
    expected_directional_return := CASE NEW.direction
        WHEN 'LONG' THEN expected_raw_return ELSE -expected_raw_return END;
    expected_mfe := CASE NEW.direction
        WHEN 'LONG' THEN GREATEST(
            0.0, (NEW.max_favorable_price - NEW.reference_price)
                 / NEW.reference_price * 100.0
        ) ELSE GREATEST(
            0.0, (NEW.reference_price - NEW.max_favorable_price)
                 / NEW.reference_price * 100.0
        ) END;
    expected_mae := CASE NEW.direction
        WHEN 'LONG' THEN GREATEST(
            0.0, (NEW.reference_price - NEW.max_adverse_price)
                 / NEW.reference_price * 100.0
        ) ELSE GREATEST(
            0.0, (NEW.max_adverse_price - NEW.reference_price)
                 / NEW.reference_price * 100.0
        ) END;
    observed_seconds := FLOOR(EXTRACT(EPOCH FROM
        NEW.measured_at_utc - NEW.decision_time_utc));
    IF ABS(NEW.raw_return_pct - expected_raw_return) > 0.00000001
       OR ABS(NEW.directional_return_pct - expected_directional_return) > 0.00000001
       OR ABS(NEW.mfe_pct - expected_mfe) > 0.00000001
       OR ABS(NEW.mae_pct - expected_mae) > 0.00000001
       OR (NEW.direction='LONG' AND (
           NEW.max_favorable_price < GREATEST(NEW.reference_price, NEW.price_at_horizon)
           OR NEW.max_adverse_price > LEAST(NEW.reference_price, NEW.price_at_horizon)
       ))
       OR (NEW.direction='SHORT' AND (
           NEW.max_favorable_price > LEAST(NEW.reference_price, NEW.price_at_horizon)
           OR NEW.max_adverse_price < GREATEST(NEW.reference_price, NEW.price_at_horizon)
       ))
       OR NEW.time_to_mfe_seconds > observed_seconds
       OR NEW.time_to_first_progress_seconds > observed_seconds
       OR (NEW.mfe_pct=0.0 AND (
           NEW.time_to_mfe_seconds<>0
           OR NEW.time_to_first_progress_seconds IS NOT NULL
       ))
       OR (NEW.mfe_pct>0.0 AND (
           NEW.time_to_first_progress_seconds IS NULL
           OR NEW.time_to_first_progress_seconds > NEW.time_to_mfe_seconds
       )) THEN
        RAISE EXCEPTION USING ERRCODE='23514',
            MESSAGE='no-signal carrier path metrics are inconsistent';
    END IF;

    NEW.created_at_utc := pg_catalog.transaction_timestamp();
    NEW.cell_identity_sha256 := ENCODE(SHA256(CONVERT_TO(
        'stage4-explicit-no-signal-outcome-carrier-v1|'
        || BTRIM(NEW.projection_event_fingerprint) || '|'
        || NEW.symbol || '|' || NEW.direction || '|'
        || NEW.horizon_minutes::TEXT, 'UTF8'
    )), 'hex');

    hash_tags := ARRAY[
        'hash_contract_version',
        'reference_contract_version',
        'projection_event_id',
        'projection_event_fingerprint',
        'snapshot_set_id',
        'snapshot_key',
        'set_payload_sha256',
        'symbol',
        'symbol_manifest_payload_sha256',
        'source_timeframe',
        'snapshot_row_id',
        'snapshot_row_payload_sha256',
        'official_price_float8_hex',
        'official_price_source',
        'official_price_exchange',
        'official_price_market',
        'official_price_pair',
        'official_price_instrument',
        'official_price_interval',
        'official_price_fetched_at_utc',
        'official_price_observed_at_utc',
        'official_price_candle_open_time_utc',
        'official_price_candle_close_time_utc',
        'official_price_policy_status'
    ]::TEXT[];
    hash_values := ARRAY[
        'stage4-no-signal-reference-receipt-hash-v1',
        expected_reference ->> 'contract_version',
        (expected_reference ->> 'projection_event_id'),
        expected_reference ->> 'projection_event_fingerprint',
        (expected_reference ->> 'snapshot_set_id'),
        expected_reference ->> 'snapshot_key',
        expected_reference ->> 'set_payload_sha256',
        expected_reference ->> 'symbol',
        expected_reference ->> 'symbol_manifest_payload_sha256',
        expected_reference ->> 'source_timeframe',
        (expected_reference ->> 'snapshot_row_id'),
        expected_reference ->> 'snapshot_row_payload_sha256',
        ENCODE(FLOAT8SEND(NEW.reference_price), 'hex'),
        expected_reference #>> '{official_price,source}',
        expected_reference #>> '{official_price,exchange}',
        expected_reference #>> '{official_price,market}',
        expected_reference #>> '{official_price,pair}',
        expected_reference #>> '{official_price,instrument}',
        expected_reference #>> '{official_price,interval}',
        expected_reference #>> '{official_price,fetched_at_utc}',
        expected_reference #>> '{official_price,observed_at_utc}',
        expected_reference #>> '{official_price,candle_open_time_utc}',
        expected_reference #>> '{official_price,candle_close_time_utc}',
        expected_reference #>> '{official_price,policy_status}'
    ]::TEXT[];
    canonical_hash_payload := '';
    FOR hash_index IN 1..pg_catalog.cardinality(hash_tags) LOOP
        canonical_hash_payload := canonical_hash_payload
            || CASE WHEN hash_index=1 THEN '' ELSE pg_catalog.chr(31) END
            || hash_tags[hash_index] || '='
            || CASE WHEN hash_values[hash_index] IS NULL THEN '-1:' ELSE
                pg_catalog.octet_length(pg_catalog.convert_to(
                    hash_values[hash_index], 'UTF8'
                ))::TEXT || ':' || hash_values[hash_index] END;
    END LOOP;
    NEW.reference_receipt_sha256 := ENCODE(SHA256(CONVERT_TO(
        canonical_hash_payload, 'UTF8'
    )), 'hex');

    hash_tags := ARRAY[
        'hash_contract_version',
        'carrier_contract_version',
        'projection_event_id',
        'projection_event_fingerprint',
        'snapshot_set_id',
        'snapshot_key',
        'symbol',
        'direction',
        'horizon_minutes',
        'decision_time_utc',
        'absence_basis',
        'cell_identity_sha256',
        'reference_receipt_sha256',
        'measured_at_utc',
        'reference_price_float8_hex',
        'price_at_horizon_float8_hex',
        'raw_return_pct_float8_hex',
        'directional_return_pct_float8_hex',
        'max_favorable_price_float8_hex',
        'max_adverse_price_float8_hex',
        'mfe_pct_float8_hex',
        'mae_pct_float8_hex',
        'time_to_first_progress_seconds',
        'time_to_mfe_seconds',
        'path_resolution_seconds',
        'path_samples',
        'outcome_method_version',
        'price_source',
        'data_quality_status'
    ]::TEXT[];
    hash_values := ARRAY[
        'stage4-no-signal-outcome-payload-hash-v1',
        'stage4-explicit-no-signal-outcome-carrier-v1',
        NEW.projection_event_id::TEXT,
        BTRIM(NEW.projection_event_fingerprint),
        NEW.snapshot_set_id::TEXT,
        BTRIM(NEW.snapshot_key),
        NEW.symbol,
        NEW.direction,
        NEW.horizon_minutes::TEXT,
        TO_CHAR(NEW.decision_time_utc AT TIME ZONE 'UTC',
                'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
        NEW.absence_basis,
        BTRIM(NEW.cell_identity_sha256),
        BTRIM(NEW.reference_receipt_sha256),
        TO_CHAR(NEW.measured_at_utc AT TIME ZONE 'UTC',
                'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
        ENCODE(FLOAT8SEND(NEW.reference_price), 'hex'),
        ENCODE(FLOAT8SEND(NEW.price_at_horizon), 'hex'),
        ENCODE(FLOAT8SEND(NEW.raw_return_pct), 'hex'),
        ENCODE(FLOAT8SEND(NEW.directional_return_pct), 'hex'),
        ENCODE(FLOAT8SEND(NEW.max_favorable_price), 'hex'),
        ENCODE(FLOAT8SEND(NEW.max_adverse_price), 'hex'),
        ENCODE(FLOAT8SEND(NEW.mfe_pct), 'hex'),
        ENCODE(FLOAT8SEND(NEW.mae_pct), 'hex'),
        NEW.time_to_first_progress_seconds::TEXT,
        NEW.time_to_mfe_seconds::TEXT,
        NEW.path_resolution_seconds::TEXT,
        NEW.path_samples::TEXT,
        NEW.outcome_method_version,
        NEW.price_source,
        NEW.data_quality_status
    ]::TEXT[];
    canonical_hash_payload := '';
    FOR hash_index IN 1..pg_catalog.cardinality(hash_tags) LOOP
        canonical_hash_payload := canonical_hash_payload
            || CASE WHEN hash_index=1 THEN '' ELSE pg_catalog.chr(31) END
            || hash_tags[hash_index] || '='
            || CASE WHEN hash_values[hash_index] IS NULL THEN '-1:' ELSE
                pg_catalog.octet_length(pg_catalog.convert_to(
                    hash_values[hash_index], 'UTF8'
                ))::TEXT || ':' || hash_values[hash_index] END;
    END LOOP;
    NEW.outcome_payload_sha256 := ENCODE(SHA256(CONVERT_TO(
        canonical_hash_payload, 'UTF8'
    )), 'hex');
    RETURN NEW;
END;
$function$;

CREATE OR REPLACE FUNCTION public.prevent_research_stage4_no_signal_outcome_v1_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog, public
AS $function$
BEGIN
    RAISE EXCEPTION USING ERRCODE='55000',
        MESSAGE='Stage-4 no-signal outcomes are append-only';
END;
$function$;

CREATE OR REPLACE FUNCTION public.prevent_research_stage4_no_signal_outcome_v1_truncate()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog, public
AS $function$
BEGIN
    RAISE EXCEPTION USING ERRCODE='55000',
        MESSAGE='Stage-4 no-signal outcomes cannot be truncated';
END;
$function$;

DROP TRIGGER IF EXISTS trg_research_stage4_no_signal_outcome_v1_validate
    ON public.research_stage4_no_signal_outcomes_v1;
CREATE TRIGGER trg_research_stage4_no_signal_outcome_v1_validate
BEFORE INSERT ON public.research_stage4_no_signal_outcomes_v1
FOR EACH ROW EXECUTE FUNCTION
    public.validate_research_stage4_no_signal_outcome_v1();
ALTER TABLE public.research_stage4_no_signal_outcomes_v1 ENABLE ALWAYS TRIGGER
    trg_research_stage4_no_signal_outcome_v1_validate;

DROP TRIGGER IF EXISTS trg_research_stage4_no_signal_outcome_v1_immutable
    ON public.research_stage4_no_signal_outcomes_v1;
CREATE TRIGGER trg_research_stage4_no_signal_outcome_v1_immutable
BEFORE UPDATE OR DELETE ON public.research_stage4_no_signal_outcomes_v1
FOR EACH ROW EXECUTE FUNCTION
    public.prevent_research_stage4_no_signal_outcome_v1_mutation();
ALTER TABLE public.research_stage4_no_signal_outcomes_v1 ENABLE ALWAYS TRIGGER
    trg_research_stage4_no_signal_outcome_v1_immutable;

DROP TRIGGER IF EXISTS trg_research_stage4_no_signal_outcome_v1_no_truncate
    ON public.research_stage4_no_signal_outcomes_v1;
CREATE TRIGGER trg_research_stage4_no_signal_outcome_v1_no_truncate
BEFORE TRUNCATE ON public.research_stage4_no_signal_outcomes_v1
FOR EACH STATEMENT EXECUTE FUNCTION
    public.prevent_research_stage4_no_signal_outcome_v1_truncate();
ALTER TABLE public.research_stage4_no_signal_outcomes_v1 ENABLE ALWAYS TRIGGER
    trg_research_stage4_no_signal_outcome_v1_no_truncate;

CREATE INDEX IF NOT EXISTS idx_research_stage4_no_signal_outcomes_due_v1
ON public.research_stage4_no_signal_outcomes_v1 (
    horizon_minutes, decision_time_utc, projection_event_id
);

DO $index_assertions$
DECLARE
    raw_oid OID := 'public.research_stage4_no_signal_outcomes_v1'::REGCLASS;
    raw_index_names TEXT[];
    due_index OID := pg_catalog.to_regclass(
        'public.idx_research_stage4_no_signal_outcomes_due_v1'
    );
BEGIN
    SELECT ARRAY_AGG(index_relation.relname ORDER BY index_relation.relname)
      INTO raw_index_names
      FROM pg_catalog.pg_index index_row
      JOIN pg_catalog.pg_class index_relation
        ON index_relation.oid=index_row.indexrelid
     WHERE index_row.indrelid=raw_oid;
    IF raw_index_names IS DISTINCT FROM ARRAY[
           'idx_research_stage4_no_signal_outcomes_due_v1',
           'research_stage4_no_signal_outcomes_v1_cell_identity_sha256_key',
           'research_stage4_no_signal_outcomes_v1_pkey'
       ]::TEXT[]
       OR EXISTS (
            SELECT 1 FROM pg_catalog.pg_index index_row
            JOIN pg_catalog.pg_class index_relation
              ON index_relation.oid=index_row.indexrelid
            JOIN pg_catalog.pg_am access_method
              ON access_method.oid=index_relation.relam
             WHERE index_row.indrelid=raw_oid
               AND (NOT index_row.indisvalid OR NOT index_row.indisready
                    OR NOT index_row.indislive
                    OR access_method.amname<>'btree'
                    OR index_row.indexprs IS NOT NULL
                    OR index_row.indpred IS NOT NULL)
       )
       OR NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_index index_row
             WHERE index_row.indexrelid=due_index
               AND index_row.indrelid=raw_oid
               AND NOT index_row.indisunique
               AND NOT index_row.indisprimary
               AND index_row.indnkeyatts=3
               AND index_row.indnatts=3
               AND pg_catalog.pg_get_indexdef(due_index, 1, FALSE)=
                   'horizon_minutes'
               AND pg_catalog.pg_get_indexdef(due_index, 2, FALSE)=
                   'decision_time_utc'
               AND pg_catalog.pg_get_indexdef(due_index, 3, FALSE)=
                   'projection_event_id'
       ) THEN
        RAISE EXCEPTION 'Stage-4 no-signal index contract is not exact';
    END IF;
END;
$index_assertions$;

CREATE OR REPLACE VIEW
    public.research_formula_exploration_no_signal_outcomes_v1
WITH (security_barrier = true, security_invoker = false)
AS
SELECT
    outcome.projection_event_id,
    outcome.projection_event_fingerprint,
    outcome.snapshot_set_id,
    outcome.snapshot_key,
    outcome.symbol,
    outcome.direction,
    outcome.horizon_minutes,
    outcome.decision_time_utc,
    outcome.absence_basis,
    outcome.reference_receipt,
    outcome.reference_receipt_sha256,
    outcome.cell_identity_sha256,
    outcome.measured_at_utc,
    outcome.reference_price,
    outcome.price_at_horizon,
    outcome.raw_return_pct,
    outcome.directional_return_pct,
    outcome.max_favorable_price,
    outcome.max_adverse_price,
    outcome.mfe_pct,
    outcome.mae_pct,
    outcome.time_to_first_progress_seconds,
    outcome.time_to_mfe_seconds,
    outcome.path_resolution_seconds,
    outcome.path_samples,
    outcome.outcome_method_version,
    outcome.price_source,
    outcome.data_quality_status,
    outcome.outcome_payload_sha256,
    outcome.created_at_utc AS outcome_created_at
FROM public.research_stage4_no_signal_outcomes_v1 outcome
JOIN public.research_formula_exploration_stage4_v1 projection
  ON projection.event_id = outcome.projection_event_id
 AND BTRIM(projection.event_fingerprint) =
     BTRIM(outcome.projection_event_fingerprint)
 AND projection.archive_snapshot_set_id = outcome.snapshot_set_id
 AND BTRIM(projection.archive_snapshot_key) = BTRIM(outcome.snapshot_key)
WHERE projection.event_type = 'SIGNAL_SNAPSHOT_PROJECTION'
  AND projection.capture_stage = 'SILENT_SIGNAL_SNAPSHOT'
  AND projection.engine_snapshot #>> '{projection,status}' = 'COMPLETED';

REVOKE ALL ON TABLE public.research_stage4_no_signal_outcomes_v1
    FROM PUBLIC,
         research_stage4_no_signal_outcome_writer_v1,
         research_formula_exploration_reader_v1;
REVOKE ALL ON TABLE
    public.research_formula_exploration_no_signal_outcomes_v1
    FROM PUBLIC,
         research_stage4_no_signal_outcome_writer_v1,
         research_formula_exploration_reader_v1;

DO $target_acl_cleanup$
DECLARE
    relation_name TEXT;
    relation_oid OID;
    owner_oid OID;
    grant_row RECORD;
    grantee_sql TEXT;
BEGIN
    FOREACH relation_name IN ARRAY ARRAY[
        'research_stage4_no_signal_outcomes_v1',
        'research_formula_exploration_no_signal_outcomes_v1'
    ] LOOP
        relation_oid := pg_catalog.to_regclass('public.' || relation_name);
        SELECT relowner INTO owner_oid
          FROM pg_catalog.pg_class WHERE oid=relation_oid;
        FOR grant_row IN
            SELECT DISTINCT acl.grantee
              FROM pg_catalog.pg_class relation
              CROSS JOIN LATERAL pg_catalog.aclexplode(
                  COALESCE(relation.relacl,
                           pg_catalog.acldefault('r', relation.relowner))
              ) acl
             WHERE relation.oid=relation_oid
               AND acl.grantee<>owner_oid
        LOOP
            grantee_sql := CASE WHEN grant_row.grantee=0 THEN 'PUBLIC'
                ELSE pg_catalog.quote_ident(
                    pg_catalog.pg_get_userbyid(grant_row.grantee)
                ) END;
            EXECUTE pg_catalog.format(
                'REVOKE ALL PRIVILEGES ON TABLE public.%I FROM %s CASCADE',
                relation_name, grantee_sql
            );
        END LOOP;
        FOR grant_row IN
            SELECT attribute.attname, acl.grantee, acl.privilege_type
              FROM pg_catalog.pg_attribute attribute
              CROSS JOIN LATERAL pg_catalog.aclexplode(attribute.attacl) acl
             WHERE attribute.attrelid=relation_oid
               AND attribute.attnum>0 AND NOT attribute.attisdropped
        LOOP
            grantee_sql := CASE WHEN grant_row.grantee=0 THEN 'PUBLIC'
                ELSE pg_catalog.quote_ident(
                    pg_catalog.pg_get_userbyid(grant_row.grantee)
                ) END;
            EXECUTE pg_catalog.format(
                'REVOKE %s (%I) ON TABLE public.%I FROM %s CASCADE',
                grant_row.privilege_type, grant_row.attname,
                relation_name, grantee_sql
            );
        END LOOP;
    END LOOP;
END;
$target_acl_cleanup$;

REVOKE CREATE ON SCHEMA public
    FROM research_stage4_no_signal_outcome_writer_v1,
         research_formula_exploration_reader_v1;
GRANT USAGE ON SCHEMA public
    TO research_stage4_no_signal_outcome_writer_v1,
       research_formula_exploration_reader_v1;
REVOKE ALL PRIVILEGES ON TABLE
    public.research_events,
    public.research_max_pain_snapshot_sets,
    public.research_max_pain_snapshot_symbols,
    public.research_max_pain_snapshot_rows
    FROM research_stage4_no_signal_outcome_writer_v1 RESTRICT;
GRANT SELECT ON TABLE
    public.research_events,
    public.research_max_pain_snapshot_sets,
    public.research_max_pain_snapshot_symbols,
    public.research_max_pain_snapshot_rows
    TO research_stage4_no_signal_outcome_writer_v1;
GRANT SELECT, INSERT ON TABLE
    public.research_stage4_no_signal_outcomes_v1
    TO research_stage4_no_signal_outcome_writer_v1;
GRANT SELECT ON TABLE
    public.research_formula_exploration_no_signal_outcomes_v1
    TO research_formula_exploration_reader_v1;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON TABLE
    public.research_events,
    public.research_max_pain_snapshot_sets,
    public.research_max_pain_snapshot_symbols,
    public.research_max_pain_snapshot_rows
    FROM PUBLIC, research_stage4_no_signal_outcome_writer_v1;
REVOKE UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON TABLE
    public.research_stage4_no_signal_outcomes_v1
    FROM PUBLIC, research_stage4_no_signal_outcome_writer_v1;

DO $source_column_acl_cleanup$
DECLARE
    writer_oid OID := (
        SELECT oid FROM pg_catalog.pg_roles
         WHERE rolname='research_stage4_no_signal_outcome_writer_v1'
    );
    relation_name TEXT;
    grant_row RECORD;
BEGIN
    FOREACH relation_name IN ARRAY ARRAY[
        'research_events',
        'research_max_pain_snapshot_sets',
        'research_max_pain_snapshot_symbols',
        'research_max_pain_snapshot_rows'
    ] LOOP
        FOR grant_row IN
            SELECT attribute.attname, acl.privilege_type
              FROM pg_catalog.pg_attribute attribute
              CROSS JOIN LATERAL pg_catalog.aclexplode(attribute.attacl) acl
             WHERE attribute.attrelid=pg_catalog.to_regclass(
                       'public.' || relation_name
                   )
               AND attribute.attnum>0 AND NOT attribute.attisdropped
               AND acl.grantee=writer_oid
        LOOP
            EXECUTE pg_catalog.format(
                'REVOKE %s (%I) ON TABLE public.%I FROM %I CASCADE',
                grant_row.privilege_type, grant_row.attname, relation_name,
                'research_stage4_no_signal_outcome_writer_v1'
            );
        END LOOP;
    END LOOP;
END;
$source_column_acl_cleanup$;

REVOKE ALL ON FUNCTION
    public.validate_research_stage4_no_signal_outcome_v1(),
    public.prevent_research_stage4_no_signal_outcome_v1_mutation(),
    public.prevent_research_stage4_no_signal_outcome_v1_truncate()
    FROM PUBLIC,
         research_stage4_no_signal_outcome_writer_v1,
         research_formula_exploration_reader_v1;

SET LOCAL search_path = pg_catalog;

DO $receipt_and_acl_assertions$
DECLARE
    writer_oid OID := (
        SELECT oid FROM pg_catalog.pg_roles
         WHERE rolname='research_stage4_no_signal_outcome_writer_v1'
    );
    reader_oid OID := (
        SELECT oid FROM pg_catalog.pg_roles
         WHERE rolname='research_formula_exploration_reader_v1'
    );
    trusted_owner OID := (
        SELECT relowner FROM pg_catalog.pg_class
         WHERE oid='public.research_events'::REGCLASS
    );
    view_definition_sha256 TEXT;
    stage4_comment TEXT;
    stage4_source_catalog_sha256 TEXT;
    raw_oid OID := 'public.research_stage4_no_signal_outcomes_v1'::REGCLASS;
    raw_catalog JSONB;
    trigger_catalog JSONB;
    raw_catalog_sha256 TEXT;
    trigger_catalog_sha256 TEXT;
BEGIN
    IF (SELECT COUNT(*) FROM pg_catalog.pg_trigger trigger_row
         WHERE trigger_row.tgrelid=raw_oid
           AND NOT trigger_row.tgisinternal)<>3
       OR EXISTS (
            SELECT 1 FROM pg_catalog.pg_trigger trigger_row
             WHERE trigger_row.tgrelid=raw_oid
               AND NOT trigger_row.tgisinternal
               AND (trigger_row.tgconstraint<>0
                    OR trigger_row.tgdeferrable
                    OR trigger_row.tginitdeferred
                    OR trigger_row.tgqual IS NOT NULL)
       )
       OR NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_trigger trigger_row
            JOIN pg_catalog.pg_proc function_row
              ON function_row.oid=trigger_row.tgfoid
             WHERE trigger_row.tgrelid=raw_oid
               AND trigger_row.tgname=
                   'trg_research_stage4_no_signal_outcome_v1_validate'
               AND NOT trigger_row.tgisinternal
               AND trigger_row.tgenabled='A'
               AND trigger_row.tgtype=7
               AND function_row.pronamespace='public'::REGNAMESPACE
               AND function_row.pronargs=0
               AND function_row.proname=
                   'validate_research_stage4_no_signal_outcome_v1'
       )
       OR NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_trigger trigger_row
            JOIN pg_catalog.pg_proc function_row
              ON function_row.oid=trigger_row.tgfoid
             WHERE trigger_row.tgrelid=raw_oid
               AND trigger_row.tgname=
                   'trg_research_stage4_no_signal_outcome_v1_immutable'
               AND NOT trigger_row.tgisinternal
               AND trigger_row.tgenabled='A'
               AND trigger_row.tgtype=27
               AND function_row.pronamespace='public'::REGNAMESPACE
               AND function_row.pronargs=0
               AND function_row.proname=
                   'prevent_research_stage4_no_signal_outcome_v1_mutation'
       )
       OR NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_trigger trigger_row
            JOIN pg_catalog.pg_proc function_row
              ON function_row.oid=trigger_row.tgfoid
             WHERE trigger_row.tgrelid=raw_oid
               AND trigger_row.tgname=
                   'trg_research_stage4_no_signal_outcome_v1_no_truncate'
               AND NOT trigger_row.tgisinternal
               AND trigger_row.tgenabled='A'
               AND trigger_row.tgtype=34
               AND function_row.pronamespace='public'::REGNAMESPACE
               AND function_row.pronargs=0
               AND function_row.proname=
                   'prevent_research_stage4_no_signal_outcome_v1_truncate'
       )
       OR (SELECT COUNT(*) FROM pg_catalog.pg_proc function_row
            JOIN pg_catalog.pg_namespace namespace_row
              ON namespace_row.oid=function_row.pronamespace
           WHERE namespace_row.nspname='public'
             AND function_row.proname IN (
                 'validate_research_stage4_no_signal_outcome_v1',
                 'prevent_research_stage4_no_signal_outcome_v1_mutation',
                 'prevent_research_stage4_no_signal_outcome_v1_truncate'
             ) AND function_row.pronargs=0)<>3
       OR EXISTS (
            SELECT 1 FROM pg_catalog.pg_proc function_row
            JOIN pg_catalog.pg_namespace namespace_row
              ON namespace_row.oid=function_row.pronamespace
            JOIN pg_catalog.pg_language language_row
              ON language_row.oid=function_row.prolang
           WHERE namespace_row.nspname='public'
             AND function_row.proname IN (
                 'validate_research_stage4_no_signal_outcome_v1',
                 'prevent_research_stage4_no_signal_outcome_v1_mutation',
                 'prevent_research_stage4_no_signal_outcome_v1_truncate'
             ) AND function_row.pronargs=0
             AND (language_row.lanname<>'plpgsql'
                  OR function_row.proowner<>trusted_owner
                  OR function_row.prorettype<>'pg_catalog.trigger'::REGTYPE
                  OR function_row.prosecdef OR function_row.proleakproof
                  OR function_row.provolatile<>'v'
                  OR function_row.proparallel<>'u')
       )
       OR NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_proc function_row
            JOIN pg_catalog.pg_namespace namespace_row
              ON namespace_row.oid=function_row.pronamespace
           WHERE namespace_row.nspname='public'
             AND function_row.proname=
                 'validate_research_stage4_no_signal_outcome_v1'
             AND function_row.pronargs=0
             AND pg_catalog.cardinality(function_row.proconfig)=2
             AND EXISTS (
                 SELECT 1
                   FROM pg_catalog.unnest(function_row.proconfig)
                       AS config(setting)
                  WHERE pg_catalog.lower(config.setting)=
                        'search_path=pg_catalog, public'
             )
             AND EXISTS (
                 SELECT 1
                   FROM pg_catalog.unnest(function_row.proconfig)
                       AS config(setting)
                  WHERE pg_catalog.lower(config.setting)='timezone=utc'
             )
       )
       OR EXISTS (
            SELECT 1 FROM pg_catalog.pg_proc function_row
            JOIN pg_catalog.pg_namespace namespace_row
              ON namespace_row.oid=function_row.pronamespace
           WHERE namespace_row.nspname='public'
             AND function_row.proname IN (
                 'prevent_research_stage4_no_signal_outcome_v1_mutation',
                 'prevent_research_stage4_no_signal_outcome_v1_truncate'
             ) AND function_row.pronargs=0
             AND NOT (
                 pg_catalog.cardinality(function_row.proconfig)=1
                 AND EXISTS (
                     SELECT 1
                       FROM pg_catalog.unnest(function_row.proconfig)
                           AS config(setting)
                      WHERE pg_catalog.lower(config.setting)=
                            'search_path=pg_catalog, public'
                 )
             )
       )
       OR EXISTS (
            SELECT 1 FROM pg_catalog.pg_proc function_row
            JOIN pg_catalog.pg_namespace namespace_row
              ON namespace_row.oid=function_row.pronamespace
            CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(
                function_row.proacl,
                pg_catalog.acldefault('f', function_row.proowner)
            )) acl
           WHERE namespace_row.nspname='public'
             AND function_row.proname IN (
                 'validate_research_stage4_no_signal_outcome_v1',
                 'prevent_research_stage4_no_signal_outcome_v1_mutation',
                 'prevent_research_stage4_no_signal_outcome_v1_truncate'
             ) AND function_row.pronargs=0
             AND acl.grantee<>function_row.proowner
       ) THEN
        RAISE EXCEPTION
            'Stage-4 no-signal trigger/function contract is not exact';
    END IF;

    SELECT JSONB_BUILD_OBJECT(
        'columns', COALESCE((
            SELECT JSONB_AGG(JSONB_BUILD_OBJECT(
                       'ordinal', attribute.attnum,
                       'name', attribute.attname,
                       'type', pg_catalog.format_type(
                           attribute.atttypid, attribute.atttypmod
                       ),
                       'not_null', attribute.attnotnull,
                       'identity', attribute.attidentity,
                       'generated', attribute.attgenerated,
                       'collation', attribute.attcollation::REGCOLLATION::TEXT,
                       'default', pg_catalog.pg_get_expr(
                           default_row.adbin, default_row.adrelid, FALSE
                       )
                   ) ORDER BY attribute.attnum)
              FROM pg_catalog.pg_attribute attribute
              LEFT JOIN pg_catalog.pg_attrdef default_row
                ON default_row.adrelid=attribute.attrelid
               AND default_row.adnum=attribute.attnum
             WHERE attribute.attrelid=raw_oid
               AND attribute.attnum>0 AND NOT attribute.attisdropped
        ), '[]'::JSONB),
        'constraints', COALESCE((
            SELECT JSONB_AGG(JSONB_BUILD_OBJECT(
                       'name', constraint_row.conname,
                       'type', constraint_row.contype,
                       'deferrable', constraint_row.condeferrable,
                       'deferred', constraint_row.condeferred,
                       'validated', constraint_row.convalidated,
                       'no_inherit', constraint_row.connoinherit,
                       'definition', pg_catalog.pg_get_constraintdef(
                           constraint_row.oid, FALSE
                       )
                   ) ORDER BY constraint_row.conname)
              FROM pg_catalog.pg_constraint constraint_row
             WHERE constraint_row.conrelid=raw_oid
        ), '[]'::JSONB),
        'indexes', COALESCE((
            SELECT JSONB_AGG(JSONB_BUILD_OBJECT(
                       'name', index_relation.relname,
                       'access_method', access_method.amname,
                       'unique', index_row.indisunique,
                       'primary', index_row.indisprimary,
                       'exclusion', index_row.indisexclusion,
                       'immediate', index_row.indimmediate,
                       'valid', index_row.indisvalid,
                       'ready', index_row.indisready,
                       'live', index_row.indislive,
                       'definition', pg_catalog.pg_get_indexdef(
                           index_row.indexrelid
                       )
                   ) ORDER BY index_relation.relname)
              FROM pg_catalog.pg_index index_row
              JOIN pg_catalog.pg_class index_relation
                ON index_relation.oid=index_row.indexrelid
              JOIN pg_catalog.pg_am access_method
                ON access_method.oid=index_relation.relam
             WHERE index_row.indrelid=raw_oid
        ), '[]'::JSONB)
    ) INTO raw_catalog;
    SELECT JSONB_BUILD_OBJECT(
        'triggers', COALESCE((
            SELECT JSONB_AGG(JSONB_BUILD_OBJECT(
                       'name', trigger_row.tgname,
                       'type', trigger_row.tgtype,
                       'enabled', trigger_row.tgenabled,
                       'function', function_namespace.nspname || '.' ||
                           function_row.proname || '()',
                       'definition', pg_catalog.pg_get_triggerdef(
                           trigger_row.oid, FALSE
                       )
                   ) ORDER BY trigger_row.tgname)
              FROM pg_catalog.pg_trigger trigger_row
              JOIN pg_catalog.pg_proc function_row
                ON function_row.oid=trigger_row.tgfoid
              JOIN pg_catalog.pg_namespace function_namespace
                ON function_namespace.oid=function_row.pronamespace
             WHERE trigger_row.tgrelid=raw_oid
               AND NOT trigger_row.tgisinternal
        ), '[]'::JSONB),
        'functions', COALESCE((
            SELECT JSONB_AGG(JSONB_BUILD_OBJECT(
                       'name', function_row.proname,
                       'owner', function_row.proowner,
                       'security_definer', function_row.prosecdef,
                       'leakproof', function_row.proleakproof,
                       'volatile', function_row.provolatile,
                       'parallel', function_row.proparallel,
                       'language', function_language.lanname,
                       'acl', COALESCE(
                           pg_catalog.to_jsonb(function_row.proacl),
                           'null'::JSONB
                       ),
                       'config', COALESCE(
                           pg_catalog.to_jsonb(function_row.proconfig),
                           '[]'::JSONB
                       ),
                       'body_sha256', ENCODE(SHA256(CONVERT_TO(
                           function_row.prosrc, 'UTF8'
                       )), 'hex')
                   ) ORDER BY function_row.proname)
              FROM pg_catalog.pg_proc function_row
              JOIN pg_catalog.pg_namespace function_namespace
                ON function_namespace.oid=function_row.pronamespace
              JOIN pg_catalog.pg_language function_language
                ON function_language.oid=function_row.prolang
             WHERE function_namespace.nspname='public'
               AND function_row.proname IN (
                   'validate_research_stage4_no_signal_outcome_v1',
                   'prevent_research_stage4_no_signal_outcome_v1_mutation',
                   'prevent_research_stage4_no_signal_outcome_v1_truncate'
               ) AND function_row.pronargs=0
        ), '[]'::JSONB)
    ) INTO trigger_catalog;
    raw_catalog_sha256 := ENCODE(SHA256(CONVERT_TO(
        raw_catalog::TEXT, 'UTF8'
    )), 'hex');
    trigger_catalog_sha256 := ENCODE(SHA256(CONVERT_TO(
        trigger_catalog::TEXT, 'UTF8'
    )), 'hex');

    IF NOT pg_catalog.has_table_privilege(
        'research_stage4_no_signal_outcome_writer_v1',
        'public.research_stage4_no_signal_outcomes_v1', 'SELECT, INSERT'
    ) OR pg_catalog.has_table_privilege(
        'research_stage4_no_signal_outcome_writer_v1',
        'public.research_stage4_no_signal_outcomes_v1',
        'UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER'
    ) OR NOT pg_catalog.has_table_privilege(
        'research_formula_exploration_reader_v1',
        'public.research_formula_exploration_no_signal_outcomes_v1', 'SELECT'
    ) OR pg_catalog.has_table_privilege(
        'research_formula_exploration_reader_v1',
        'public.research_stage4_no_signal_outcomes_v1',
        'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER'
    ) OR pg_catalog.has_table_privilege(
        'research_formula_exploration_reader_v1',
        'public.research_formula_exploration_no_signal_outcomes_v1',
        'INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER'
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_class relation
         WHERE relation.oid=raw_oid
           AND (relation.relrowsecurity OR relation.relforcerowsecurity)
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_class relation
         WHERE relation.oid=
               'public.research_formula_exploration_no_signal_outcomes_v1'::REGCLASS
           AND (relation.relkind<>'v'
                OR relation.relowner<>trusted_owner
                OR COALESCE(
                    pg_catalog.cardinality(relation.reloptions), 0
                )<>2
                OR NOT COALESCE(
                    relation.reloptions, ARRAY[]::TEXT[]
                ) @> ARRAY[
                    'security_barrier=true',
                    'security_invoker=false'
                ]::TEXT[])
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_policy policy_row
         WHERE policy_row.polrelid=raw_oid
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_rewrite rewrite_row
         WHERE rewrite_row.ev_class=raw_oid
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_class relation
        CROSS JOIN LATERAL pg_catalog.aclexplode(
            COALESCE(relation.relacl,
                     pg_catalog.acldefault('r', relation.relowner))
        ) acl
        WHERE relation.oid =
              'public.research_formula_exploration_no_signal_outcomes_v1'::REGCLASS
          AND acl.grantee<>trusted_owner
          AND NOT (
              acl.grantee=reader_oid
              AND acl.privilege_type='SELECT'
              AND NOT acl.is_grantable
          )
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_class relation
        CROSS JOIN LATERAL pg_catalog.aclexplode(
            COALESCE(relation.relacl,
                     pg_catalog.acldefault('r', relation.relowner))
        ) acl
        WHERE relation.oid=raw_oid
          AND acl.grantee<>trusted_owner
          AND NOT (
              acl.grantee=writer_oid
              AND acl.privilege_type IN ('SELECT', 'INSERT')
              AND NOT acl.is_grantable
          )
    ) OR (SELECT COUNT(*) FROM pg_catalog.pg_class relation
          CROSS JOIN LATERAL pg_catalog.aclexplode(
              COALESCE(relation.relacl,
                       pg_catalog.acldefault('r', relation.relowner))
          ) acl
         WHERE relation.oid=raw_oid
           AND acl.grantee=writer_oid)<>2
    OR (SELECT COUNT(*) FROM pg_catalog.pg_class relation
        CROSS JOIN LATERAL pg_catalog.aclexplode(
            COALESCE(relation.relacl,
                     pg_catalog.acldefault('r', relation.relowner))
        ) acl
       WHERE relation.oid=
             'public.research_formula_exploration_no_signal_outcomes_v1'::REGCLASS
         AND acl.grantee=reader_oid)<>1
    OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_attribute attribute
         WHERE attribute.attrelid IN (
             raw_oid,
             'public.research_formula_exploration_no_signal_outcomes_v1'::REGCLASS
         ) AND attribute.attnum>0 AND NOT attribute.attisdropped
           AND COALESCE(pg_catalog.cardinality(attribute.attacl), 0)<>0
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_attribute attribute
        CROSS JOIN LATERAL pg_catalog.aclexplode(attribute.attacl) acl
         WHERE attribute.attrelid IN (
             'public.research_events'::REGCLASS,
             'public.research_max_pain_snapshot_sets'::REGCLASS,
             'public.research_max_pain_snapshot_symbols'::REGCLASS,
             'public.research_max_pain_snapshot_rows'::REGCLASS
         ) AND attribute.attnum>0 AND NOT attribute.attisdropped
           AND acl.grantee=writer_oid
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_class relation
         WHERE relation.oid IN (
             'public.research_events'::REGCLASS,
             'public.research_max_pain_snapshot_sets'::REGCLASS,
             'public.research_max_pain_snapshot_symbols'::REGCLASS,
             'public.research_max_pain_snapshot_rows'::REGCLASS
         ) AND (
             SELECT COUNT(*)
               FROM pg_catalog.aclexplode(COALESCE(
                   relation.relacl,
                   pg_catalog.acldefault('r', relation.relowner)
               )) acl
              WHERE acl.grantee=writer_oid
         )<>1
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_class relation
        CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(
            relation.relacl,
            pg_catalog.acldefault('r', relation.relowner)
        )) acl
         WHERE relation.oid IN (
             'public.research_events'::REGCLASS,
             'public.research_max_pain_snapshot_sets'::REGCLASS,
             'public.research_max_pain_snapshot_symbols'::REGCLASS,
             'public.research_max_pain_snapshot_rows'::REGCLASS
         ) AND acl.grantee=writer_oid
           AND (acl.privilege_type<>'SELECT' OR acl.is_grantable)
    ) OR NOT pg_catalog.has_schema_privilege(
        'research_stage4_no_signal_outcome_writer_v1', 'public', 'USAGE'
    ) OR pg_catalog.has_schema_privilege(
        'research_stage4_no_signal_outcome_writer_v1', 'public', 'CREATE'
    ) OR NOT pg_catalog.has_schema_privilege(
        'research_formula_exploration_reader_v1', 'public', 'USAGE'
    ) OR pg_catalog.has_schema_privilege(
        'research_formula_exploration_reader_v1', 'public', 'CREATE'
    ) OR pg_catalog.has_database_privilege(
        'research_stage4_no_signal_outcome_writer_v1',
        pg_catalog.current_database(), 'CREATE'
    ) OR pg_catalog.has_database_privilege(
        'research_formula_exploration_reader_v1',
        pg_catalog.current_database(), 'CREATE'
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_database database_row
         WHERE database_row.datname=pg_catalog.current_database()
           AND database_row.datdba IN (writer_oid, reader_oid)
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_namespace namespace_row
         WHERE namespace_row.nspname='public'
           AND namespace_row.nspowner IN (writer_oid, reader_oid)
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_auth_members membership
         WHERE membership.member IN (writer_oid, reader_oid)
            OR membership.roleid IN (writer_oid, reader_oid)
    ) THEN
        RAISE EXCEPTION 'Stage-4 no-signal outcome ACL is unsafe';
    END IF;
    SELECT ENCODE(SHA256(CONVERT_TO(
               pg_catalog.pg_get_viewdef(view_rule.ev_class, FALSE), 'UTF8'
           )), 'hex')
      INTO view_definition_sha256
      FROM pg_catalog.pg_rewrite view_rule
     WHERE view_rule.ev_class =
           'public.research_formula_exploration_no_signal_outcomes_v1'::REGCLASS
       AND view_rule.rulename='_RETURN';
    stage4_comment := pg_catalog.obj_description(
        'public.research_formula_exploration_stage4_v1'::REGCLASS,
        'pg_class'
    );
    stage4_source_catalog_sha256 := SUBSTRING(
        stage4_comment FROM 'source_catalog_sha256=([0-9a-f]{64})(;|$)'
    );
    IF view_definition_sha256 IS NULL
       OR view_definition_sha256 !~ '^[0-9a-f]{64}$'
       OR raw_catalog_sha256 IS NULL
       OR raw_catalog_sha256 !~ '^[0-9a-f]{64}$'
       OR trigger_catalog_sha256 IS NULL
       OR trigger_catalog_sha256 !~ '^[0-9a-f]{64}$'
       OR stage4_source_catalog_sha256 IS NULL
       OR stage4_source_catalog_sha256 !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION 'Stage-4 no-signal view receipt is incomplete';
    END IF;
    EXECUTE pg_catalog.format(
        'COMMENT ON TABLE public.%I IS %L',
        'research_stage4_no_signal_outcomes_v1',
        'stage4-explicit-no-signal-outcomes-raw-v1; '
        || 'append-only cell carrier; '
        || 'reference_hash_contract='
        || 'stage4-no-signal-reference-receipt-hash-v1; '
        || 'outcome_hash_contract='
        || 'stage4-no-signal-outcome-payload-hash-v1; '
        || 'raw_catalog_sha256='
        || raw_catalog_sha256
        || '; trigger_catalog_sha256=' || trigger_catalog_sha256
    );
    EXECUTE pg_catalog.format(
        'COMMENT ON VIEW public.%I IS %L',
        'research_formula_exploration_no_signal_outcomes_v1',
        'stage4-formula-exploration-no-signal-outcomes-v1; '
        || 'append-only explicit no-signal closed-path labels; no Formula, '
        || 'delivery, LIVE, Telegram or trading authority; '
        || 'reference_hash_contract='
        || 'stage4-no-signal-reference-receipt-hash-v1; '
        || 'outcome_hash_contract='
        || 'stage4-no-signal-outcome-payload-hash-v1; '
        || 'view_definition_sha256=' || view_definition_sha256
        || '; raw_catalog_sha256=' || raw_catalog_sha256
        || '; trigger_catalog_sha256=' || trigger_catalog_sha256
        || '; stage4_source_catalog_sha256='
        || stage4_source_catalog_sha256
    );

    IF pg_catalog.obj_description(raw_oid, 'pg_class') NOT LIKE
           'stage4-explicit-no-signal-outcomes-raw-v1;%raw_catalog_sha256='
           || raw_catalog_sha256 || '; trigger_catalog_sha256='
           || trigger_catalog_sha256
       OR pg_catalog.obj_description(
           'public.research_formula_exploration_no_signal_outcomes_v1'::REGCLASS,
           'pg_class'
       ) NOT LIKE
           'stage4-formula-exploration-no-signal-outcomes-v1;%'
           || 'view_definition_sha256=' || view_definition_sha256
           || '; raw_catalog_sha256=' || raw_catalog_sha256
           || '; trigger_catalog_sha256=' || trigger_catalog_sha256
           || '; stage4_source_catalog_sha256='
           || stage4_source_catalog_sha256 THEN
        RAISE EXCEPTION 'Stage-4 no-signal catalog comments are incomplete';
    END IF;
END;
$receipt_and_acl_assertions$;

-- Basic manual rollback (execute only in an approved transaction after
-- disabling RESEARCH_STAGE4_NO_SIGNAL_OUTCOME_DATABASE_URL; export the table
-- first if its research history must be retained):
-- REVOKE SELECT ON TABLE public.research_events,
--   public.research_max_pain_snapshot_sets,
--   public.research_max_pain_snapshot_symbols,
--   public.research_max_pain_snapshot_rows
--   FROM research_stage4_no_signal_outcome_writer_v1;
-- REVOKE SELECT, INSERT ON TABLE
--   public.research_stage4_no_signal_outcomes_v1
--   FROM research_stage4_no_signal_outcome_writer_v1;
-- REVOKE ALL ON TABLE public.research_formula_exploration_no_signal_outcomes_v1
--   FROM PUBLIC, research_formula_exploration_reader_v1,
--        research_stage4_no_signal_outcome_writer_v1;
-- DROP VIEW IF EXISTS public.research_formula_exploration_no_signal_outcomes_v1;
-- DROP TABLE IF EXISTS public.research_stage4_no_signal_outcomes_v1;
-- DROP FUNCTION IF EXISTS public.validate_research_stage4_no_signal_outcome_v1();
-- DROP FUNCTION IF EXISTS public.prevent_research_stage4_no_signal_outcome_v1_mutation();
-- DROP FUNCTION IF EXISTS public.prevent_research_stage4_no_signal_outcome_v1_truncate();
-- REVOKE USAGE ON SCHEMA public
--   FROM research_stage4_no_signal_outcome_writer_v1;
-- Rollback verification must require that public.nspacl has no direct grantee
-- entry for research_stage4_no_signal_outcome_writer_v1.  Effective USAGE may
-- remain inherited from PUBLIC; PostgreSQL has no per-role DENY, so do not
-- revoke schema USAGE from PUBLIC.  Effective SELECT on every source relation
-- must still be false after the relation-level revokes above.

RESET search_path;
RESET TIME ZONE;
RESET DateStyle;
RESET IntervalStyle;
RESET extra_float_digits;
RESET quote_all_identifiers;
