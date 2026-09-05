-- Authoritative read-only Formula exploration outcomes v1
--
-- Exposes only outcome labels attached to exact Stage-4 signal rows already
-- admitted by migration 024's security-barrier view.  Method, quality and
-- causal timestamps are deliberately exposed without being trusted here;
-- the dedicated reader must validate them fail-closed in its read-only,
-- repeatable-read transaction before attaching any label.
--
-- This migration creates no outcome writer, Formula entry, delivery path,
-- LIVE authority, trading authority or migration-audit row.  The repository
-- has no schema-migration audit relation; the view comment is its immutable
-- catalog receipt.  Roles and passwords remain provisioned out of band.

SET LOCAL search_path = pg_catalog;
SET LOCAL TIME ZONE 'UTC';
SET LOCAL DateStyle = 'ISO, YMD';
SET LOCAL IntervalStyle = 'postgres';
SET LOCAL extra_float_digits = 3;
SET LOCAL quote_all_identifiers = off;

DO $preflight$
DECLARE
    reader_row RECORD;
    outcome_owner_oid OID;
    event_owner_oid OID;
    stage4_owner_oid OID;
    existing_view_oid OID;
    stage4_comment TEXT;
    stage4_source_catalog_sha256 TEXT;
    expected_columns TEXT[] := ARRAY[
        'event_id:bigint:not-null',
        'horizon_minutes:integer:not-null',
        'measured_at_utc:timestamp with time zone:not-null',
        'reference_price:double precision:not-null',
        'price_at_horizon:double precision:nullable',
        'raw_return_pct:double precision:nullable',
        'directional_return_pct:double precision:nullable',
        'max_favorable_price:double precision:nullable',
        'max_adverse_price:double precision:nullable',
        'mfe_pct:double precision:nullable',
        'mae_pct:double precision:nullable',
        'time_to_first_progress_seconds:integer:nullable',
        'time_to_mfe_seconds:integer:nullable',
        'path_resolution_seconds:integer:nullable',
        'path_samples:integer:nullable',
        'outcome_method_version:text:not-null',
        'price_source:text:nullable',
        'data_quality_status:text:nullable',
        'created_at:timestamp with time zone:not-null'
    ]::TEXT[];
    actual_columns TEXT[];
BEGIN
    IF pg_catalog.current_setting('server_version_num')::INTEGER < 150000 THEN
        RAISE EXCEPTION
            'Formula exploration outcomes view requires PostgreSQL 15 or newer';
    END IF;

    SELECT * INTO reader_row
    FROM pg_catalog.pg_roles
    WHERE rolname = 'research_formula_exploration_reader_v1';
    IF NOT FOUND
       OR NOT reader_row.rolcanlogin
       OR reader_row.rolinherit
       OR reader_row.rolsuper
       OR reader_row.rolcreatedb
       OR reader_row.rolcreaterole
       OR reader_row.rolreplication
       OR reader_row.rolbypassrls THEN
        RAISE EXCEPTION
            'research_formula_exploration_reader_v1 must remain an unprivileged NOINHERIT LOGIN role';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM pg_catalog.pg_auth_members membership
        WHERE membership.member = reader_row.oid
           OR membership.roleid = reader_row.oid
    ) THEN
        RAISE EXCEPTION
            'Formula exploration reader must not participate in role membership';
    END IF;

    IF pg_catalog.to_regclass('public.research_alert_outcomes') IS NULL
       OR NOT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_class relation_row
            WHERE relation_row.oid =
                  'public.research_alert_outcomes'::REGCLASS
              AND relation_row.relkind IN ('r', 'p')
       ) THEN
        RAISE EXCEPTION
            'Required source table public.research_alert_outcomes is missing';
    END IF;
    IF pg_catalog.to_regclass(
           'public.research_formula_exploration_stage4_v1'
       ) IS NULL OR NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_class view_row
        WHERE view_row.oid =
              'public.research_formula_exploration_stage4_v1'::REGCLASS
          AND view_row.relkind = 'v'
          AND view_row.reloptions @> ARRAY[
                'security_barrier=true', 'security_invoker=false'
              ]::TEXT[]
          AND CARDINALITY(view_row.reloptions) = 2
    ) THEN
        RAISE EXCEPTION
            'Migration 024 Stage-4 security-barrier view is missing or unsafe';
    END IF;

    SELECT relation_row.relowner INTO outcome_owner_oid
    FROM pg_catalog.pg_class relation_row
    WHERE relation_row.oid = 'public.research_alert_outcomes'::REGCLASS;
    SELECT relation_row.relowner INTO event_owner_oid
    FROM pg_catalog.pg_class relation_row
    WHERE relation_row.oid = 'public.research_events'::REGCLASS;
    SELECT relation_row.relowner INTO stage4_owner_oid
    FROM pg_catalog.pg_class relation_row
    WHERE relation_row.oid =
          'public.research_formula_exploration_stage4_v1'::REGCLASS;
    IF outcome_owner_oid IS NULL
       OR outcome_owner_oid <> event_owner_oid
       OR outcome_owner_oid <> stage4_owner_oid
       OR pg_catalog.pg_get_userbyid(outcome_owner_oid) <> SESSION_USER
       OR outcome_owner_oid = reader_row.oid THEN
        RAISE EXCEPTION
            'Outcome table, Stage-4 source and migration session require one trusted owner';
    END IF;

    SELECT ARRAY_AGG(
               attribute.attname::TEXT || ':' ||
               pg_catalog.format_type(
                   attribute.atttypid, attribute.atttypmod
               ) || ':' ||
               CASE WHEN attribute.attnotnull
                    THEN 'not-null' ELSE 'nullable' END
               ORDER BY expected.ordinality
           )
    INTO actual_columns
    FROM UNNEST(expected_columns) WITH ORDINALITY expected(spec, ordinality)
    LEFT JOIN pg_catalog.pg_attribute attribute
      ON attribute.attrelid = 'public.research_alert_outcomes'::REGCLASS
     AND attribute.attname = SPLIT_PART(expected.spec, ':', 1)
     AND attribute.attnum > 0
     AND NOT attribute.attisdropped;
    IF actual_columns IS DISTINCT FROM expected_columns THEN
        RAISE EXCEPTION
            'research_alert_outcomes lacks the exact required outcome columns';
    END IF;

    stage4_comment := pg_catalog.obj_description(
        'public.research_formula_exploration_stage4_v1'::REGCLASS,
        'pg_class'
    );
    stage4_source_catalog_sha256 := SUBSTRING(
        stage4_comment FROM 'source_catalog_sha256=([0-9a-f]{64})(;|$)'
    );
    IF stage4_source_catalog_sha256 IS NULL THEN
        RAISE EXCEPTION
            'Migration 024 Stage-4 source catalog receipt is missing';
    END IF;

    existing_view_oid := pg_catalog.to_regclass(
        'public.research_formula_exploration_outcomes_v1'
    );
    IF existing_view_oid IS NOT NULL AND NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_class existing
        WHERE existing.oid = existing_view_oid
          AND existing.relkind = 'v'
          AND existing.relowner = outcome_owner_oid
    ) THEN
        RAISE EXCEPTION
            'Existing Formula exploration outcomes object has an unsafe kind or owner';
    END IF;
END;
$preflight$;

-- Close source/view definition races for the duration of the schema
-- installer's transaction.  The installer also serializes migrations with
-- its advisory transaction lock.
LOCK TABLE public.research_alert_outcomes IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE public.research_formula_exploration_stage4_v1
    IN ACCESS SHARE MODE;

SET LOCAL search_path = public;

CREATE OR REPLACE VIEW public.research_formula_exploration_outcomes_v1
WITH (security_barrier = true, security_invoker = false)
AS
SELECT
    outcome_row.event_id,
    outcome_row.horizon_minutes,
    outcome_row.measured_at_utc,
    outcome_row.reference_price,
    outcome_row.price_at_horizon,
    outcome_row.raw_return_pct,
    outcome_row.directional_return_pct,
    outcome_row.max_favorable_price,
    outcome_row.max_adverse_price,
    outcome_row.mfe_pct,
    outcome_row.mae_pct,
    outcome_row.time_to_first_progress_seconds,
    outcome_row.time_to_mfe_seconds,
    outcome_row.path_resolution_seconds,
    outcome_row.path_samples,
    outcome_row.outcome_method_version,
    outcome_row.price_source,
    outcome_row.data_quality_status,
    outcome_row.created_at AS outcome_created_at
FROM public.research_alert_outcomes outcome_row
JOIN public.research_formula_exploration_stage4_v1 stage4_row
  ON stage4_row.event_id = outcome_row.event_id
WHERE stage4_row.schema_version = 'research-event-v1'
  AND stage4_row.event_kind = 'DECISION_SAMPLE'
  AND stage4_row.event_type IN (
        'MAX_PAIN_CONFIRMATION_STATE',
        'MAGNET_CONFIRMATION_STATE',
        'SILENT_COMBINED_CONFIRMATION_SNAPSHOT'
      )
  AND stage4_row.direction IN ('LONG', 'SHORT')
  AND stage4_row.capture_stage = 'SILENT_SIGNAL_SNAPSHOT'
  AND stage4_row.strategy_version = 'signal-snapshot-v1'
  AND stage4_row.delivery_status = 'NOT_APPLICABLE'
  AND stage4_row.delivery_attempted_at_utc IS NULL
  AND stage4_row.delivered_at_utc IS NULL
  AND stage4_row.engine_snapshot #>>
        '{signal_snapshot,contract_version}' = 'research-signal-snapshot-v1'
  AND stage4_row.engine_snapshot #>> '{signal_snapshot,signal_family}' = CASE
        WHEN stage4_row.event_type = 'MAX_PAIN_CONFIRMATION_STATE'
            THEN 'MAX_PAIN'
        WHEN stage4_row.event_type = 'MAGNET_CONFIRMATION_STATE'
            THEN 'MAGNET'
        WHEN stage4_row.event_type =
             'SILENT_COMBINED_CONFIRMATION_SNAPSHOT'
            THEN 'COMBINED'
      END
  AND CASE
        WHEN stage4_row.event_type =
             'SILENT_COMBINED_CONFIRMATION_SNAPSHOT'
            THEN stage4_row.engine_snapshot #>> '{signal_snapshot,tier}' =
                 'CONFIRMED'
        ELSE stage4_row.engine_snapshot #>> '{signal_snapshot,tier}' IN (
                 'CONFIRMED', 'STRONG_CONFIRMED'
             )
      END
  AND stage4_row.engine_snapshot #>
        '{signal_snapshot,formula_authorized}' = 'false'::JSONB
  AND stage4_row.engine_snapshot #>
        '{signal_snapshot,outcome_authorized}' = 'false'::JSONB
  AND stage4_row.engine_snapshot #>
        '{signal_snapshot,telegram_delivery_allowed}' = 'false'::JSONB
  AND stage4_row.engine_snapshot #>
        '{signal_snapshot,trade_execution_allowed}' = 'false'::JSONB;

-- Normalize stale view grants before installing the one fixed reader grant.
DO $view_acl_cleanup$
DECLARE
    grant_row RECORD;
    owner_oid OID := (
        SELECT relation_row.relowner
        FROM pg_catalog.pg_class relation_row
        WHERE relation_row.oid =
              'public.research_formula_exploration_outcomes_v1'::REGCLASS
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
        WHERE relation_row.oid =
              'public.research_formula_exploration_outcomes_v1'::REGCLASS
          AND acl.grantee <> owner_oid
    LOOP
        EXECUTE pg_catalog.format(
            'REVOKE ALL PRIVILEGES ON TABLE public.%I FROM %s CASCADE',
            'research_formula_exploration_outcomes_v1',
            CASE WHEN grant_row.grantee = 0 THEN 'PUBLIC'
                 ELSE pg_catalog.quote_ident(
                     pg_catalog.pg_get_userbyid(grant_row.grantee)
                 ) END
        );
    END LOOP;
END;
$view_acl_cleanup$;

REVOKE ALL ON TABLE public.research_formula_exploration_outcomes_v1
    FROM PUBLIC, research_formula_exploration_reader_v1;
REVOKE ALL ON TABLE public.research_alert_outcomes
    FROM PUBLIC, research_formula_exploration_reader_v1;

-- Relation-level REVOKE does not remove explicit column grants.  Remove all
-- column grants from the view and every PUBLIC/reader grant from the raw
-- outcome table before granting view SELECT back at relation level only.
DO $column_acl_cleanup$
DECLARE
    reader_oid OID := (
        SELECT role_row.oid
        FROM pg_catalog.pg_roles role_row
        WHERE role_row.rolname = 'research_formula_exploration_reader_v1'
    );
    relation_name TEXT;
    grant_row RECORD;
BEGIN
    FOREACH relation_name IN ARRAY ARRAY[
        'research_alert_outcomes',
        'research_formula_exploration_outcomes_v1'
    ] LOOP
        FOR grant_row IN
            SELECT attribute.attname, acl.grantee, acl.privilege_type
            FROM pg_catalog.pg_attribute attribute
            CROSS JOIN LATERAL pg_catalog.aclexplode(attribute.attacl) acl
            WHERE attribute.attrelid = pg_catalog.to_regclass(
                      'public.' || relation_name
                  )
              AND attribute.attnum > 0
              AND NOT attribute.attisdropped
              AND (
                  relation_name =
                      'research_formula_exploration_outcomes_v1'
                  OR acl.grantee IN (0, reader_oid)
              )
        LOOP
            EXECUTE pg_catalog.format(
                'REVOKE %s (%I) ON TABLE public.%I FROM %s CASCADE',
                grant_row.privilege_type,
                grant_row.attname,
                relation_name,
                CASE WHEN grant_row.grantee = 0 THEN 'PUBLIC'
                     ELSE pg_catalog.quote_ident(
                         pg_catalog.pg_get_userbyid(grant_row.grantee)
                     ) END
            );
        END LOOP;
    END LOOP;
END;
$column_acl_cleanup$;

REVOKE CREATE ON SCHEMA public
    FROM research_formula_exploration_reader_v1;
GRANT USAGE ON SCHEMA public
    TO research_formula_exploration_reader_v1;
GRANT SELECT ON TABLE public.research_formula_exploration_outcomes_v1
    TO research_formula_exploration_reader_v1;

SET LOCAL search_path = pg_catalog;

DO $view_receipt_and_assertions$
DECLARE
    reader_oid OID := (
        SELECT role_row.oid
        FROM pg_catalog.pg_roles role_row
        WHERE role_row.rolname = 'research_formula_exploration_reader_v1'
    );
    trusted_owner_oid OID := (
        SELECT relation_row.relowner
        FROM pg_catalog.pg_class relation_row
        WHERE relation_row.oid = 'public.research_alert_outcomes'::REGCLASS
    );
    stage4_comment TEXT;
    stage4_source_catalog_sha256 TEXT;
    view_definition_sha256 TEXT;
    view_comment TEXT;
    actual_columns TEXT[];
    dependencies TEXT[];
BEGIN
    SELECT ARRAY_AGG(attribute.attname::TEXT ORDER BY attribute.attnum)
    INTO actual_columns
    FROM pg_catalog.pg_attribute attribute
    WHERE attribute.attrelid =
          'public.research_formula_exploration_outcomes_v1'::REGCLASS
      AND attribute.attnum > 0
      AND NOT attribute.attisdropped;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_class view_row
        WHERE view_row.oid =
              'public.research_formula_exploration_outcomes_v1'::REGCLASS
          AND view_row.relkind = 'v'
          AND view_row.relowner = trusted_owner_oid
          AND view_row.reloptions @> ARRAY[
                'security_barrier=true', 'security_invoker=false'
              ]::TEXT[]
          AND CARDINALITY(view_row.reloptions) = 2
    ) OR actual_columns IS DISTINCT FROM ARRAY[
        'event_id',
        'horizon_minutes',
        'measured_at_utc',
        'reference_price',
        'price_at_horizon',
        'raw_return_pct',
        'directional_return_pct',
        'max_favorable_price',
        'max_adverse_price',
        'mfe_pct',
        'mae_pct',
        'time_to_first_progress_seconds',
        'time_to_mfe_seconds',
        'path_resolution_seconds',
        'path_samples',
        'outcome_method_version',
        'price_source',
        'data_quality_status',
        'outcome_created_at'
    ]::TEXT[] THEN
        RAISE EXCEPTION
            'Formula exploration outcomes view has an unsafe shape or owner';
    END IF;

    SELECT ARRAY_AGG(
               DISTINCT dependency_namespace.nspname || '.' ||
                        dependency_relation.relname
               ORDER BY dependency_namespace.nspname || '.' ||
                        dependency_relation.relname
           )
    INTO dependencies
    FROM pg_catalog.pg_rewrite rule_row
    JOIN pg_catalog.pg_depend dependency_row
      ON dependency_row.classid = 'pg_catalog.pg_rewrite'::REGCLASS
     AND dependency_row.objid = rule_row.oid
    JOIN pg_catalog.pg_class dependency_relation
      ON dependency_relation.oid = dependency_row.refobjid
    JOIN pg_catalog.pg_namespace dependency_namespace
      ON dependency_namespace.oid = dependency_relation.relnamespace
    WHERE rule_row.ev_class =
          'public.research_formula_exploration_outcomes_v1'::REGCLASS
      AND rule_row.rulename = '_RETURN'
      AND dependency_relation.oid <> rule_row.ev_class
      AND dependency_relation.relkind IN ('r', 'p', 'v');
    IF dependencies IS DISTINCT FROM ARRAY[
        'public.research_alert_outcomes',
        'public.research_formula_exploration_stage4_v1'
    ]::TEXT[] THEN
        RAISE EXCEPTION
            'Formula exploration outcomes view has unsafe dependencies';
    END IF;

    IF NOT pg_catalog.has_table_privilege(
        'research_formula_exploration_reader_v1',
        'public.research_formula_exploration_outcomes_v1',
        'SELECT'
    ) OR pg_catalog.has_table_privilege(
        'research_formula_exploration_reader_v1',
        'public.research_formula_exploration_outcomes_v1',
        'INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER'
    ) OR EXISTS (
        SELECT 1
        FROM pg_catalog.pg_class view_row
        CROSS JOIN LATERAL pg_catalog.aclexplode(
            COALESCE(
                view_row.relacl,
                pg_catalog.acldefault('r', view_row.relowner)
            )
        ) acl
        WHERE view_row.oid =
              'public.research_formula_exploration_outcomes_v1'::REGCLASS
          AND acl.grantee NOT IN (trusted_owner_oid, reader_oid)
    ) OR EXISTS (
        SELECT 1
        FROM pg_catalog.pg_class view_row
        CROSS JOIN LATERAL pg_catalog.aclexplode(
            COALESCE(
                view_row.relacl,
                pg_catalog.acldefault('r', view_row.relowner)
            )
        ) acl
        WHERE view_row.oid =
              'public.research_formula_exploration_outcomes_v1'::REGCLASS
          AND acl.grantee = reader_oid
          AND (acl.privilege_type <> 'SELECT' OR acl.is_grantable)
    ) OR EXISTS (
        SELECT 1
        FROM pg_catalog.pg_attribute attribute
        WHERE attribute.attrelid =
              'public.research_formula_exploration_outcomes_v1'::REGCLASS
          AND attribute.attnum > 0
          AND NOT attribute.attisdropped
          AND COALESCE(CARDINALITY(attribute.attacl), 0) <> 0
    ) THEN
        RAISE EXCEPTION
            'Formula exploration outcomes view has an unsafe ACL';
    END IF;

    IF pg_catalog.has_table_privilege(
        'research_formula_exploration_reader_v1',
        'public.research_alert_outcomes',
        'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER'
    ) OR pg_catalog.has_any_column_privilege(
        'research_formula_exploration_reader_v1',
        'public.research_alert_outcomes',
        'SELECT, INSERT, UPDATE, REFERENCES'
    ) OR NOT pg_catalog.has_schema_privilege(
        'research_formula_exploration_reader_v1', 'public', 'USAGE'
    ) OR pg_catalog.has_schema_privilege(
        'research_formula_exploration_reader_v1', 'public', 'CREATE'
    ) OR EXISTS (
        SELECT 1
        FROM pg_catalog.pg_auth_members membership
        WHERE membership.member = reader_oid
           OR membership.roleid = reader_oid
    ) THEN
        RAISE EXCEPTION
            'Formula exploration outcome reader boundary is not intact';
    END IF;

    SELECT ENCODE(SHA256(CONVERT_TO(
        pg_catalog.pg_get_viewdef(rule_row.ev_class, FALSE), 'UTF8'
    )), 'hex')
    INTO view_definition_sha256
    FROM pg_catalog.pg_rewrite rule_row
    WHERE rule_row.ev_class =
          'public.research_formula_exploration_outcomes_v1'::REGCLASS
      AND rule_row.rulename = '_RETURN';
    stage4_comment := pg_catalog.obj_description(
        'public.research_formula_exploration_stage4_v1'::REGCLASS,
        'pg_class'
    );
    stage4_source_catalog_sha256 := SUBSTRING(
        stage4_comment FROM 'source_catalog_sha256=([0-9a-f]{64})(;|$)'
    );
    IF view_definition_sha256 !~ '^[0-9a-f]{64}$'
       OR stage4_source_catalog_sha256 !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION
            'Formula exploration outcomes view receipt is incomplete';
    END IF;
    EXECUTE pg_catalog.format(
        'COMMENT ON VIEW public.%I IS %L',
        'research_formula_exploration_outcomes_v1',
        'stage4-formula-exploration-outcomes-v1; read-only Stage-4 signal '
        || 'outcome labels; method, quality and causal-time validation remain '
        || 'reader-side; no Formula, delivery, LIVE or trading authority; '
        || 'view_definition_sha256=' || view_definition_sha256
        || '; stage4_source_catalog_sha256='
        || stage4_source_catalog_sha256
    );
    view_comment := pg_catalog.obj_description(
        'public.research_formula_exploration_outcomes_v1'::REGCLASS,
        'pg_class'
    );
    IF view_comment NOT LIKE
           'stage4-formula-exploration-outcomes-v1;%'
       OR view_comment NOT LIKE
           '%view_definition_sha256=' || view_definition_sha256 || '%'
       OR view_comment NOT LIKE
           '%stage4_source_catalog_sha256='
           || stage4_source_catalog_sha256 THEN
        RAISE EXCEPTION
            'Formula exploration outcomes view comment receipt is incomplete';
    END IF;
END;
$view_receipt_and_assertions$;

-- Basic manual rollback (execute only in its own approved transaction):
-- REVOKE ALL ON TABLE public.research_formula_exploration_outcomes_v1
--     FROM PUBLIC, research_formula_exploration_reader_v1;
-- DROP VIEW IF EXISTS public.research_formula_exploration_outcomes_v1;

RESET search_path;
RESET TIME ZONE;
RESET DateStyle;
RESET IntervalStyle;
RESET extra_float_digits;
RESET quote_all_identifiers;
