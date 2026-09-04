-- Durable Stage-4 signal outcome due-scan state v1
--
-- This relation is operational cursor state only.  It is not evidence, an
-- outcome, Formula authority, delivery authority or trading authority.  A
-- frozen upper key bounds each lap so continuous source ingress cannot keep
-- the scanner from wrapping and revisiting older incomplete horizons.

SET LOCAL search_path = pg_catalog;
SET LOCAL TIME ZONE 'UTC';
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '30s';

DO $preflight$
DECLARE
    event_owner_oid OID;
    outcome_owner_oid OID;
    existing_state_oid OID;
BEGIN
    SELECT relation_row.relowner
      INTO event_owner_oid
      FROM pg_catalog.pg_class relation_row
     WHERE relation_row.oid =
           pg_catalog.to_regclass('public.research_events')
       AND relation_row.relkind = 'r';
    SELECT relation_row.relowner
      INTO outcome_owner_oid
      FROM pg_catalog.pg_class relation_row
     WHERE relation_row.oid =
           pg_catalog.to_regclass('public.research_alert_outcomes')
       AND relation_row.relkind = 'r';
    IF event_owner_oid IS NULL
       OR outcome_owner_oid IS NULL
       OR event_owner_oid <> outcome_owner_oid
       OR pg_catalog.pg_get_userbyid(event_owner_oid) <> SESSION_USER THEN
        RAISE EXCEPTION
            'Stage-4 scan state requires one trusted event/outcome/session owner';
    END IF;

    existing_state_oid := pg_catalog.to_regclass(
        'public.research_stage4_signal_scan_state_v1'
    );
    IF existing_state_oid IS NOT NULL AND NOT EXISTS (
        SELECT 1
          FROM pg_catalog.pg_class relation_row
         WHERE relation_row.oid = existing_state_oid
           AND relation_row.relkind = 'r'
           AND relation_row.relowner = event_owner_oid
    ) THEN
        RAISE EXCEPTION
            'Existing Stage-4 scan state has an unsafe kind or owner';
    END IF;
END;
$preflight$;

CREATE TABLE IF NOT EXISTS
    public.research_stage4_signal_scan_state_v1 (
        scan_key TEXT PRIMARY KEY,
        state_version TEXT NOT NULL,
        cursor_alert_time_utc TIMESTAMPTZ,
        cursor_event_id BIGINT,
        lap_upper_alert_time_utc TIMESTAMPTZ,
        lap_upper_event_id BIGINT,
        completed_laps BIGINT NOT NULL DEFAULT 0,
        pages_scanned BIGINT NOT NULL DEFAULT 0,
        candidates_scanned BIGINT NOT NULL DEFAULT 0,
        updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT research_stage4_signal_scan_key_ck CHECK (
            scan_key = 'STAGE4_SIGNAL_DUE_V1'
        ),
        CONSTRAINT research_stage4_signal_scan_state_version_ck CHECK (
            state_version = 'stage4-signal-due-scan-state-v1'
        ),
        CONSTRAINT research_stage4_signal_scan_cursor_pair_ck CHECK (
            (cursor_alert_time_utc IS NULL) = (cursor_event_id IS NULL)
        ),
        CONSTRAINT research_stage4_signal_scan_upper_pair_ck CHECK (
            (lap_upper_alert_time_utc IS NULL) =
            (lap_upper_event_id IS NULL)
        ),
        CONSTRAINT research_stage4_signal_scan_cursor_requires_upper_ck CHECK (
            cursor_alert_time_utc IS NULL
            OR lap_upper_alert_time_utc IS NOT NULL
        ),
        CONSTRAINT research_stage4_signal_scan_positive_ids_ck CHECK (
            (cursor_event_id IS NULL OR cursor_event_id > 0)
            AND (lap_upper_event_id IS NULL OR lap_upper_event_id > 0)
        ),
        CONSTRAINT research_stage4_signal_scan_cursor_within_lap_ck CHECK (
            cursor_alert_time_utc IS NULL
            OR ROW(cursor_alert_time_utc, cursor_event_id) <=
               ROW(lap_upper_alert_time_utc, lap_upper_event_id)
        ),
        CONSTRAINT research_stage4_signal_scan_counters_ck CHECK (
            completed_laps >= 0
            AND pages_scanned >= 0
            AND candidates_scanned >= 0
        )
    );

COMMENT ON TABLE
    public.research_stage4_signal_scan_state_v1 IS
    'contract_version=stage4-signal-due-scan-state-v1;scope=stage4_signal_only;authority=operational_cursor_only;lap_upper=frozen';

-- Reapplication must preserve an active lap and cursor.  The migration only
-- seeds the one exact contract row when it does not yet exist.
INSERT INTO public.research_stage4_signal_scan_state_v1 (
    scan_key, state_version
) VALUES (
    'STAGE4_SIGNAL_DUE_V1', 'stage4-signal-due-scan-state-v1'
)
ON CONFLICT (scan_key) DO NOTHING;

-- Normalize default-privilege or stale grants to an owner-only relation.
-- The existing outcome worker uses the trusted research owner connection;
-- no reader, no-signal writer or application role is granted authority here.
DO $owner_only_acl$
DECLARE
    state_oid OID := 'public.research_stage4_signal_scan_state_v1'::REGCLASS;
    owner_oid OID := (
        SELECT relation_row.relowner
          FROM pg_catalog.pg_class relation_row
         WHERE relation_row.oid = state_oid
    );
    grant_row RECORD;
    grantee_sql TEXT;
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
         WHERE relation_row.oid = state_oid
           AND acl.grantee <> owner_oid
    LOOP
        grantee_sql := CASE
            WHEN grant_row.grantee = 0 THEN 'PUBLIC'
            ELSE pg_catalog.quote_ident(
                pg_catalog.pg_get_userbyid(grant_row.grantee)
            )
        END;
        EXECUTE pg_catalog.format(
            'REVOKE ALL PRIVILEGES ON TABLE public.%I FROM %s CASCADE',
            'research_stage4_signal_scan_state_v1',
            grantee_sql
        );
    END LOOP;

    FOR grant_row IN
        SELECT attribute.attname,
               acl.grantee,
               acl.privilege_type
          FROM pg_catalog.pg_attribute attribute
          CROSS JOIN LATERAL pg_catalog.aclexplode(attribute.attacl) acl
         WHERE attribute.attrelid = state_oid
           AND attribute.attnum > 0
           AND NOT attribute.attisdropped
           AND acl.grantee <> owner_oid
    LOOP
        grantee_sql := CASE
            WHEN grant_row.grantee = 0 THEN 'PUBLIC'
            ELSE pg_catalog.quote_ident(
                pg_catalog.pg_get_userbyid(grant_row.grantee)
            )
        END;
        EXECUTE pg_catalog.format(
            'REVOKE %s (%I) ON TABLE public.%I FROM %s CASCADE',
            grant_row.privilege_type,
            grant_row.attname,
            'research_stage4_signal_scan_state_v1',
            grantee_sql
        );
    END LOOP;
END;
$owner_only_acl$;

SET LOCAL search_path = pg_catalog;

DO $postflight$
DECLARE
    state_oid OID := pg_catalog.to_regclass(
        'public.research_stage4_signal_scan_state_v1'
    );
    owner_oid OID;
    expected_columns TEXT[] := ARRAY[
        'scan_key:text:not-null',
        'state_version:text:not-null',
        'cursor_alert_time_utc:timestamp with time zone:nullable',
        'cursor_event_id:bigint:nullable',
        'lap_upper_alert_time_utc:timestamp with time zone:nullable',
        'lap_upper_event_id:bigint:nullable',
        'completed_laps:bigint:not-null',
        'pages_scanned:bigint:not-null',
        'candidates_scanned:bigint:not-null',
        'updated_at_utc:timestamp with time zone:not-null'
    ]::TEXT[];
    actual_columns TEXT[];
    expected_constraints TEXT[] := ARRAY[
        'research_stage4_signal_scan_counters_ck',
        'research_stage4_signal_scan_cursor_pair_ck',
        'research_stage4_signal_scan_cursor_requires_upper_ck',
        'research_stage4_signal_scan_cursor_within_lap_ck',
        'research_stage4_signal_scan_key_ck',
        'research_stage4_signal_scan_positive_ids_ck',
        'research_stage4_signal_scan_state_v1_pkey',
        'research_stage4_signal_scan_state_version_ck',
        'research_stage4_signal_scan_upper_pair_ck'
    ]::TEXT[];
    actual_constraints TEXT[];
    actual_defaults TEXT[];
    constraint_count BIGINT;
    row_count BIGINT;
BEGIN
    IF state_oid IS NULL OR NOT EXISTS (
        SELECT 1
          FROM pg_catalog.pg_class relation_row
         WHERE relation_row.oid = state_oid
           AND relation_row.relkind = 'r'
           AND relation_row.relpersistence = 'p'
           AND NOT relation_row.relrowsecurity
           AND NOT relation_row.relforcerowsecurity
    ) THEN
        RAISE EXCEPTION 'Stage-4 signal scan state table is missing';
    END IF;

    SELECT relation_row.relowner
      INTO owner_oid
      FROM pg_catalog.pg_class relation_row
     WHERE relation_row.oid = state_oid;
    IF pg_catalog.pg_get_userbyid(owner_oid) <> SESSION_USER THEN
        RAISE EXCEPTION 'Stage-4 signal scan state owner changed';
    END IF;

    SELECT ARRAY_AGG(
               attribute.attname::TEXT || ':' ||
               pg_catalog.format_type(
                   attribute.atttypid, attribute.atttypmod
               ) || ':' ||
               CASE WHEN attribute.attnotnull
                    THEN 'not-null' ELSE 'nullable' END
               ORDER BY attribute.attnum
           )
      INTO actual_columns
      FROM pg_catalog.pg_attribute attribute
     WHERE attribute.attrelid = state_oid
       AND attribute.attnum > 0
       AND NOT attribute.attisdropped;
    IF actual_columns IS DISTINCT FROM expected_columns THEN
        RAISE EXCEPTION 'Stage-4 signal scan state columns changed';
    END IF;

    SELECT ARRAY_AGG(constraint_row.conname::TEXT ORDER BY constraint_row.conname)
      INTO actual_constraints
      FROM pg_catalog.pg_constraint constraint_row
     WHERE constraint_row.conrelid = state_oid
       AND constraint_row.contype IN ('c', 'p')
       AND constraint_row.convalidated
       AND NOT constraint_row.condeferrable;
    SELECT COUNT(*) INTO constraint_count
      FROM pg_catalog.pg_constraint constraint_row
     WHERE constraint_row.conrelid = state_oid;
    IF actual_constraints IS DISTINCT FROM expected_constraints THEN
        RAISE EXCEPTION 'Stage-4 signal scan state constraints changed';
    ELSIF constraint_count <> pg_catalog.cardinality(expected_constraints)
       OR (
           SELECT pg_catalog.pg_get_constraintdef(
                      constraint_row.oid, FALSE
                  )
             FROM pg_catalog.pg_constraint constraint_row
            WHERE constraint_row.conrelid = state_oid
              AND constraint_row.conname =
                  'research_stage4_signal_scan_state_v1_pkey'
       ) IS DISTINCT FROM 'PRIMARY KEY (scan_key)' THEN
        RAISE EXCEPTION 'Stage-4 signal scan state constraint shape changed';
    END IF;

    SELECT ARRAY_AGG(
               attribute.attname::TEXT || ':' ||
               pg_catalog.pg_get_expr(
                   default_row.adbin, default_row.adrelid
               )
               ORDER BY attribute.attname
           )
      INTO actual_defaults
      FROM pg_catalog.pg_attrdef default_row
      JOIN pg_catalog.pg_attribute attribute
        ON attribute.attrelid = default_row.adrelid
       AND attribute.attnum = default_row.adnum
     WHERE default_row.adrelid = state_oid;
    IF actual_defaults IS DISTINCT FROM ARRAY[
           'candidates_scanned:0',
           'completed_laps:0',
           'pages_scanned:0',
           'updated_at_utc:now()'
       ]::TEXT[] THEN
        RAISE EXCEPTION 'Stage-4 signal scan state defaults changed';
    END IF;

    SELECT COUNT(*) INTO row_count
      FROM public.research_stage4_signal_scan_state_v1;
    IF row_count <> 1 OR NOT EXISTS (
        SELECT 1
          FROM public.research_stage4_signal_scan_state_v1 state_row
         WHERE state_row.scan_key = 'STAGE4_SIGNAL_DUE_V1'
           AND state_row.state_version =
               'stage4-signal-due-scan-state-v1'
    ) THEN
        RAISE EXCEPTION 'Stage-4 signal scan state seed row is invalid';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM pg_catalog.pg_class relation_row
          CROSS JOIN LATERAL pg_catalog.aclexplode(
              COALESCE(
                  relation_row.relacl,
                  pg_catalog.acldefault('r', relation_row.relowner)
              )
          ) acl
         WHERE relation_row.oid = state_oid
           AND acl.grantee <> owner_oid
    ) OR EXISTS (
        SELECT 1
          FROM pg_catalog.pg_attribute attribute
          CROSS JOIN LATERAL pg_catalog.aclexplode(attribute.attacl) acl
         WHERE attribute.attrelid = state_oid
           AND attribute.attnum > 0
           AND NOT attribute.attisdropped
           AND acl.grantee <> owner_oid
    ) THEN
        RAISE EXCEPTION 'Stage-4 signal scan state ACL is not owner-only';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM pg_catalog.pg_trigger trigger_row
         WHERE trigger_row.tgrelid = state_oid
           AND NOT trigger_row.tgisinternal
    ) OR EXISTS (
        SELECT 1
          FROM pg_catalog.pg_policy policy_row
         WHERE policy_row.polrelid = state_oid
    ) OR EXISTS (
        SELECT 1
          FROM pg_catalog.pg_rewrite rewrite_row
         WHERE rewrite_row.ev_class = state_oid
           AND rewrite_row.rulename <> '_RETURN'
    ) THEN
        RAISE EXCEPTION 'Stage-4 signal scan state has unsafe enforcement';
    END IF;

    IF pg_catalog.obj_description(state_oid, 'pg_class') IS DISTINCT FROM
       'contract_version=stage4-signal-due-scan-state-v1;scope=stage4_signal_only;authority=operational_cursor_only;lap_upper=frozen' THEN
        RAISE EXCEPTION 'Stage-4 signal scan state receipt changed';
    END IF;
END;
$postflight$;

-- Explicit manual rollback.  Stop the Research Outcome worker first.  This
-- discards only operational scan progress;
-- immutable source events and already written outcomes remain untouched and
-- will be discovered again from the beginning of a new bounded lap.
-- DROP TABLE IF EXISTS
--     public.research_stage4_signal_scan_state_v1;
