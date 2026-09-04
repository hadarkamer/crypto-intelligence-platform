-- Authoritative read-only Formula exploration source v1 (Stage 7)
--
-- Two security-barrier views expose only the frozen Stage-4 and Wave-v5
-- records needed by the pure exploration cohort contract.  The dedicated
-- reader receives no source-table authority.  The application must still
-- attest the catalogs and validate every receipt in the same read-only,
-- repeatable-read transaction that reads these views.
--
-- This migration creates no outcome carrier, writer, worker, Formula entry,
-- delivery path, LIVE authority or trading path.  Roles and passwords are
-- provisioned out of band.

SET LOCAL search_path = pg_catalog;
SET LOCAL TIME ZONE 'UTC';
SET LOCAL DateStyle = 'ISO, YMD';
SET LOCAL IntervalStyle = 'postgres';
SET LOCAL extra_float_digits = 3;
SET LOCAL quote_all_identifiers = off;

DO $roles_and_sources$
DECLARE
    reader_row RECORD;
    wave_owner_row RECORD;
    stage4_owner_oid OID;
    stage4_owner_name NAME;
    source_oid OID;
    source_name TEXT;
BEGIN
    IF pg_catalog.current_setting('server_version_num')::INTEGER < 150000 THEN
        RAISE EXCEPTION
            'Formula exploration reader requires PostgreSQL 15 or newer';
    END IF;

    SELECT * INTO reader_row
    FROM pg_catalog.pg_roles
    WHERE rolname = 'research_formula_exploration_reader_v1';
    IF NOT FOUND THEN
        RAISE EXCEPTION
            'Required LOGIN role research_formula_exploration_reader_v1 is missing';
    END IF;
    IF NOT reader_row.rolcanlogin
       OR reader_row.rolinherit
       OR reader_row.rolsuper
       OR reader_row.rolcreatedb
       OR reader_row.rolcreaterole
       OR reader_row.rolreplication
       OR reader_row.rolbypassrls THEN
        RAISE EXCEPTION
            'research_formula_exploration_reader_v1 must be an unprivileged NOINHERIT LOGIN role';
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
    IF EXISTS (
        SELECT 1
        FROM pg_catalog.pg_database database_row
        WHERE database_row.datname = pg_catalog.current_database()
          AND database_row.datdba = reader_row.oid
    ) OR EXISTS (
        SELECT 1
        FROM pg_catalog.pg_namespace namespace_row
        WHERE namespace_row.nspname = 'public'
          AND namespace_row.nspowner = reader_row.oid
    ) THEN
        RAISE EXCEPTION
            'Formula exploration reader cannot own its database or schema';
    END IF;
    IF pg_catalog.has_schema_privilege(
        'research_formula_exploration_reader_v1', 'public', 'CREATE'
    ) THEN
        RAISE EXCEPTION
            'Formula exploration reader cannot create objects in schema public';
    END IF;

    FOREACH source_name IN ARRAY ARRAY[
        'research_events',
        'research_max_pain_snapshot_sets',
        'research_max_pain_snapshot_symbols',
        'research_max_pain_snapshot_rows',
        'research_price_collection_attempts',
        'research_neutral_price_anchors',
        'research_market_movement_transitions',
        'research_market_movement_memberships'
    ] LOOP
        source_oid := pg_catalog.to_regclass('public.' || source_name);
        IF source_oid IS NULL OR NOT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_class relation_row
            WHERE relation_row.oid = source_oid
              AND relation_row.relkind IN ('r', 'p')
        ) THEN
            RAISE EXCEPTION 'Required source table public.% is missing', source_name;
        END IF;
        IF EXISTS (
            SELECT 1
            FROM pg_catalog.pg_class relation_row
            WHERE relation_row.oid = source_oid
              AND relation_row.relowner = reader_row.oid
        ) THEN
            RAISE EXCEPTION
                'Formula exploration reader cannot own source table public.%',
                source_name;
        END IF;
    END LOOP;

    SELECT relation_row.relowner
    INTO stage4_owner_oid
    FROM pg_catalog.pg_class relation_row
    WHERE relation_row.oid = 'public.research_events'::REGCLASS;
    stage4_owner_name := pg_catalog.pg_get_userbyid(stage4_owner_oid);
    IF stage4_owner_name IS NULL
       OR stage4_owner_name IN (
            'research_signal_snapshot_writer_v1',
            'research_market_movement_owner',
            'research_market_movement_writer_v5',
            'research_formula_exploration_reader_v1'
       )
       OR stage4_owner_name <> SESSION_USER THEN
        RAISE EXCEPTION
            'Migration session must be the trusted Stage-4 source owner';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM pg_catalog.pg_class relation_row
        WHERE relation_row.oid IN (
            'public.research_max_pain_snapshot_sets'::REGCLASS,
            'public.research_max_pain_snapshot_symbols'::REGCLASS,
            'public.research_max_pain_snapshot_rows'::REGCLASS
        )
          AND relation_row.relowner <> stage4_owner_oid
    ) THEN
        RAISE EXCEPTION
            'Stage-4 event and Max-Pain archive tables require one trusted owner';
    END IF;

    SELECT * INTO wave_owner_row
    FROM pg_catalog.pg_roles
    WHERE rolname = 'research_market_movement_owner';
    IF NOT FOUND
       OR wave_owner_row.rolcanlogin
       OR wave_owner_row.rolinherit
       OR wave_owner_row.rolsuper
       OR wave_owner_row.rolcreatedb
       OR wave_owner_row.rolcreaterole
       OR wave_owner_row.rolreplication
       OR wave_owner_row.rolbypassrls THEN
        RAISE EXCEPTION
            'research_market_movement_owner must remain an unprivileged NOLOGIN role';
    END IF;
    IF NOT pg_catalog.pg_has_role(
        SESSION_USER, 'research_market_movement_owner', 'MEMBER'
    ) THEN
        RAISE EXCEPTION
            'Migration session must be able to SET ROLE research_market_movement_owner';
    END IF;
END;
$roles_and_sources$;

-- These locks close the catalog-attestation/view-install race.  Both sets are
-- held through the transaction used by the supported schema installer.
LOCK TABLE
    public.research_events,
    public.research_max_pain_snapshot_sets,
    public.research_max_pain_snapshot_symbols,
    public.research_max_pain_snapshot_rows
IN SHARE ROW EXCLUSIVE MODE;

SET LOCAL ROLE research_market_movement_owner;
LOCK TABLE
    public.research_price_collection_attempts,
    public.research_neutral_price_anchors,
    public.research_market_movement_transitions,
    public.research_market_movement_memberships
IN SHARE ROW EXCLUSIVE MODE;
RESET ROLE;

DO $stage4_boundary$
DECLARE
    trusted_owner OID := (
        SELECT relation_row.relowner
        FROM pg_catalog.pg_class relation_row
        WHERE relation_row.oid = 'public.research_events'::REGCLASS
    );
    writer_row RECORD;
    spec RECORD;
    expected_triggers TEXT[] := ARRAY[
        'trg_research_signal_snapshot_v1_envelope',
        'trg_research_signal_snapshot_v1_immutable',
        'trg_research_signal_snapshot_v1_no_truncate',
        'trg_research_signal_snapshot_v1_set_complete',
        'trg_research_signal_snapshot_v1_writer'
    ]::TEXT[];
    actual_triggers TEXT[];
BEGIN
    SELECT * INTO writer_row
    FROM pg_catalog.pg_roles
    WHERE rolname = 'research_signal_snapshot_writer_v1';
    IF NOT FOUND
       OR NOT writer_row.rolcanlogin
       OR writer_row.rolinherit
       OR writer_row.rolsuper
       OR writer_row.rolcreatedb
       OR writer_row.rolcreaterole
       OR writer_row.rolreplication
       OR writer_row.rolbypassrls
       OR EXISTS (
            SELECT 1
            FROM pg_catalog.pg_auth_members membership
            WHERE membership.member = writer_row.oid
               OR membership.roleid = writer_row.oid
       ) THEN
        RAISE EXCEPTION
            'Migration 023 dedicated Stage-4 writer boundary is not intact';
    END IF;
    IF NOT pg_catalog.has_table_privilege(
        'research_signal_snapshot_writer_v1',
        'public.research_events', 'SELECT'
    ) OR NOT pg_catalog.has_table_privilege(
        'research_signal_snapshot_writer_v1',
        'public.research_events', 'INSERT'
    ) OR pg_catalog.has_table_privilege(
        'research_signal_snapshot_writer_v1',
        'public.research_events',
        'UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER'
    ) OR EXISTS (
        SELECT 1
        FROM (VALUES
            ('research_max_pain_snapshot_sets'),
            ('research_max_pain_snapshot_symbols'),
            ('research_max_pain_snapshot_rows')
        ) AS source(relation_name)
        WHERE NOT pg_catalog.has_table_privilege(
                  'research_signal_snapshot_writer_v1',
                  'public.' || source.relation_name,
                  'SELECT'
              )
           OR pg_catalog.has_table_privilege(
                  'research_signal_snapshot_writer_v1',
                  'public.' || source.relation_name,
                  'INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER'
              )
    ) THEN
        RAISE EXCEPTION
            'Migration 023 Stage-4 writer ACL is not intact';
    END IF;

    SELECT ARRAY_AGG(trigger_row.tgname::TEXT ORDER BY trigger_row.tgname::TEXT)
    INTO actual_triggers
    FROM pg_catalog.pg_trigger trigger_row
    WHERE trigger_row.tgrelid = 'public.research_events'::REGCLASS
      AND NOT trigger_row.tgisinternal
      AND trigger_row.tgname LIKE 'trg_research_signal_snapshot_v1_%';
    IF actual_triggers IS DISTINCT FROM expected_triggers OR EXISTS (
        SELECT 1
        FROM pg_catalog.pg_trigger trigger_row
        JOIN pg_catalog.pg_proc function_row
          ON function_row.oid = trigger_row.tgfoid
        JOIN pg_catalog.pg_namespace function_namespace
          ON function_namespace.oid = function_row.pronamespace
        WHERE trigger_row.tgrelid = 'public.research_events'::REGCLASS
          AND NOT trigger_row.tgisinternal
          AND trigger_row.tgname = ANY(expected_triggers)
          AND (
              trigger_row.tgenabled <> 'A'
              OR trigger_row.tgqual IS NOT NULL
              OR trigger_row.tgtype::INTEGER <> CASE trigger_row.tgname
                    WHEN 'trg_research_signal_snapshot_v1_writer' THEN 23
                    WHEN 'trg_research_signal_snapshot_v1_envelope' THEN 23
                    WHEN 'trg_research_signal_snapshot_v1_set_complete' THEN 5
                    WHEN 'trg_research_signal_snapshot_v1_immutable' THEN 27
                    WHEN 'trg_research_signal_snapshot_v1_no_truncate' THEN 34
                 END
              OR trigger_row.tgdeferrable <> (
                    trigger_row.tgname =
                        'trg_research_signal_snapshot_v1_set_complete'
                 )
              OR trigger_row.tginitdeferred <> (
                    trigger_row.tgname =
                        'trg_research_signal_snapshot_v1_set_complete'
                 )
              OR (trigger_row.tgconstraint <> 0) <> (
                    trigger_row.tgname =
                        'trg_research_signal_snapshot_v1_set_complete'
                 )
              OR function_namespace.nspname <> 'public'
              OR function_row.proowner <> trusted_owner
              OR function_row.prosecdef
              OR function_row.proconfig IS DISTINCT FROM ARRAY[
                    'search_path=pg_catalog, public'
                 ]::TEXT[]
              OR function_row.proname <> CASE trigger_row.tgname
                    WHEN 'trg_research_signal_snapshot_v1_writer'
                        THEN 'assert_research_signal_snapshot_v1_writer'
                    WHEN 'trg_research_signal_snapshot_v1_envelope'
                        THEN 'validate_research_signal_snapshot_v1_envelope'
                    WHEN 'trg_research_signal_snapshot_v1_set_complete'
                        THEN 'validate_research_signal_snapshot_v1_set_complete'
                    WHEN 'trg_research_signal_snapshot_v1_immutable'
                        THEN 'prevent_research_signal_snapshot_v1_mutation'
                    WHEN 'trg_research_signal_snapshot_v1_no_truncate'
                        THEN 'prevent_research_signal_snapshot_v1_truncate'
                 END
          )
    ) THEN
        RAISE EXCEPTION
            'Migration 023 Stage-4 trigger inventory is not intact';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM pg_catalog.pg_trigger trigger_row
        WHERE trigger_row.tgrelid = 'public.research_events'::REGCLASS
          AND NOT trigger_row.tgisinternal
          AND (trigger_row.tgtype::INTEGER & 4) = 4
          AND trigger_row.tgname <> ALL(expected_triggers)
    ) THEN
        RAISE EXCEPTION
            'Unexpected Stage-4 INSERT trigger invalidates source authority';
    END IF;

    -- Migration 007 is part of the same installer transaction.  Freeze its
    -- three completion/append-only/no-truncate bindings per archive table so
    -- the catalog receipt cannot bless a weakened pre-existing boundary.
    FOR spec IN
        SELECT * FROM (VALUES
            ('research_max_pain_snapshot_sets', ARRAY[
                'trg_research_max_pain_set_complete',
                'trg_research_max_pain_sets_append_only',
                'trg_research_max_pain_sets_no_truncate'
            ]::TEXT[]),
            ('research_max_pain_snapshot_symbols', ARRAY[
                'trg_research_max_pain_symbol_complete',
                'trg_research_max_pain_symbols_append_only',
                'trg_research_max_pain_symbols_no_truncate'
            ]::TEXT[]),
            ('research_max_pain_snapshot_rows', ARRAY[
                'trg_research_max_pain_row_complete',
                'trg_research_max_pain_rows_append_only',
                'trg_research_max_pain_rows_no_truncate'
            ]::TEXT[])
        ) AS expected(relation_name, trigger_names)
    LOOP
        SELECT ARRAY_AGG(
            trigger_row.tgname::TEXT ORDER BY trigger_row.tgname::TEXT
        )
        INTO actual_triggers
        FROM pg_catalog.pg_trigger trigger_row
        WHERE trigger_row.tgrelid = pg_catalog.to_regclass(
                  'public.' || spec.relation_name
              )
          AND NOT trigger_row.tgisinternal;
        IF actual_triggers IS DISTINCT FROM spec.trigger_names OR EXISTS (
            SELECT 1
            FROM pg_catalog.pg_trigger trigger_row
            JOIN pg_catalog.pg_proc function_row
              ON function_row.oid = trigger_row.tgfoid
            JOIN pg_catalog.pg_namespace function_namespace
              ON function_namespace.oid = function_row.pronamespace
            WHERE trigger_row.tgrelid = pg_catalog.to_regclass(
                      'public.' || spec.relation_name
                  )
              AND NOT trigger_row.tgisinternal
              AND (
                  trigger_row.tgenabled <> 'O'
                  OR trigger_row.tgqual IS NOT NULL
                  OR trigger_row.tgtype::INTEGER <> CASE
                      WHEN trigger_row.tgname LIKE '%_complete' THEN 5
                      WHEN trigger_row.tgname LIKE '%_append_only' THEN 27
                      WHEN trigger_row.tgname LIKE '%_no_truncate' THEN 34
                      ELSE -1
                  END
                  OR trigger_row.tgdeferrable <> (
                      trigger_row.tgname LIKE '%_complete'
                  )
                  OR trigger_row.tginitdeferred <> (
                      trigger_row.tgname LIKE '%_complete'
                  )
                  OR (trigger_row.tgconstraint <> 0) <> (
                      trigger_row.tgname LIKE '%_complete'
                  )
                  OR function_namespace.nspname <> 'public'
                  OR function_row.proowner <> trusted_owner
                  OR function_row.proname <> CASE
                      WHEN trigger_row.tgname LIKE '%_complete'
                          THEN 'assert_research_max_pain_snapshot_complete'
                      ELSE 'prevent_research_max_pain_archive_mutation'
                  END
              )
        ) THEN
            RAISE EXCEPTION
                'Migration 007 trigger inventory is not intact on public.%',
                spec.relation_name;
        END IF;
    END LOOP;

    IF (
        SELECT COUNT(*)
        FROM pg_catalog.pg_proc function_row
        JOIN pg_catalog.pg_namespace function_namespace
          ON function_namespace.oid = function_row.pronamespace
        JOIN pg_catalog.pg_language language_row
          ON language_row.oid = function_row.prolang
        WHERE function_namespace.nspname = 'public'
          AND function_row.proname = ANY(ARRAY[
              'assert_research_max_pain_snapshot_complete',
              'prevent_research_max_pain_archive_mutation'
          ]::NAME[])
          AND function_row.pronargs = 0
          AND pg_catalog.pg_get_function_identity_arguments(
                  function_row.oid
              ) = ''
          AND function_row.prorettype = 'pg_catalog.trigger'::REGTYPE
          AND function_row.proowner = trusted_owner
          AND NOT function_row.prosecdef
          AND function_row.proconfig IS NULL
          AND function_row.provolatile = 'v'
          AND NOT function_row.proisstrict
          AND function_row.proparallel = 'u'
          AND function_row.proacl IS NULL
          AND language_row.lanname = 'plpgsql'
          AND pg_catalog.pg_get_functiondef(function_row.oid) IS NOT NULL
    ) <> 2 THEN
        RAISE EXCEPTION
            'Migration 007 Max-Pain archive functions are not intact';
    END IF;

    IF (
        SELECT COUNT(*)
        FROM pg_catalog.pg_proc function_row
        JOIN pg_catalog.pg_namespace function_namespace
          ON function_namespace.oid = function_row.pronamespace
        WHERE function_namespace.nspname = 'public'
          AND function_row.proname = ANY(ARRAY[
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
          AND function_row.proowner = trusted_owner
          AND NOT function_row.prosecdef
          AND function_row.proconfig IS NOT DISTINCT FROM ARRAY[
                'search_path=pg_catalog, public'
              ]::TEXT[]
    ) <> 21 THEN
        RAISE EXCEPTION
            'Migration 023 Stage-4 function inventory is not intact';
    END IF;

    IF NOT COALESCE((
        SELECT COUNT(*) = 2
           AND BOOL_AND(
                index_namespace.nspname = 'public'
                AND index_class.relkind = 'i'
                AND index_class.relpersistence = 'p'
                AND index_class.relowner = trusted_owner
                AND access_method.amname = 'btree'
                AND index_row.indisvalid
                AND index_row.indisready
                AND index_row.indislive
                AND index_row.indimmediate
                AND NOT index_row.indisprimary
                AND NOT index_row.indisexclusion
                AND NOT index_row.indcheckxmin
                AND NOT index_row.indisreplident
                AND index_row.indnatts = 1
                AND index_row.indnkeyatts = 1
                AND index_row.indexprs IS NOT NULL
                AND index_row.indpred IS NOT NULL
                AND ARRAY(
                    SELECT key_entry.value
                    FROM UNNEST(index_row.indkey::SMALLINT[])
                         WITH ORDINALITY AS key_entry(value, ordinality)
                    ORDER BY key_entry.ordinality
                ) = ARRAY[0]::SMALLINT[]
                AND ARRAY(
                    SELECT option_entry.value
                    FROM UNNEST(index_row.indoption::SMALLINT[])
                         WITH ORDINALITY AS option_entry(value, ordinality)
                    ORDER BY option_entry.ordinality
                ) = ARRAY[0]::SMALLINT[]
                AND pg_catalog.pg_get_indexdef(index_row.indexrelid) IS NOT NULL
                AND CASE index_class.relname
                    WHEN 'uq_research_signal_snapshot_projection_key_v1' THEN
                        index_row.indisunique
                        AND POSITION(
                            '{projection,snapshot_key}' IN
                            pg_catalog.pg_get_indexdef(index_row.indexrelid)
                        ) > 0
                        AND POSITION(
                            'SIGNAL_SNAPSHOT_PROJECTION' IN
                            pg_catalog.pg_get_expr(
                                index_row.indpred,
                                index_row.indrelid,
                                FALSE
                            )
                        ) > 0
                        AND POSITION(
                            'SILENT_SIGNAL_SNAPSHOT' IN
                            pg_catalog.pg_get_expr(
                                index_row.indpred,
                                index_row.indrelid,
                                FALSE
                            )
                        ) > 0
                    WHEN 'idx_research_signal_snapshot_archive_key_v1' THEN
                        NOT index_row.indisunique
                        AND POSITION(
                            '{signal_snapshot,archive_reference,snapshot_key}' IN
                            pg_catalog.pg_get_indexdef(index_row.indexrelid)
                        ) > 0
                        AND POSITION(
                            'MAX_PAIN_CONFIRMATION_STATE' IN
                            pg_catalog.pg_get_expr(
                                index_row.indpred,
                                index_row.indrelid,
                                FALSE
                            )
                        ) > 0
                        AND POSITION(
                            'MAGNET_CONFIRMATION_STATE' IN
                            pg_catalog.pg_get_expr(
                                index_row.indpred,
                                index_row.indrelid,
                                FALSE
                            )
                        ) > 0
                        AND POSITION(
                            'SILENT_COMBINED_CONFIRMATION_SNAPSHOT' IN
                            pg_catalog.pg_get_expr(
                                index_row.indpred,
                                index_row.indrelid,
                                FALSE
                            )
                        ) > 0
                        AND POSITION(
                            'SILENT_SIGNAL_SNAPSHOT' IN
                            pg_catalog.pg_get_expr(
                                index_row.indpred,
                                index_row.indrelid,
                                FALSE
                            )
                        ) > 0
                    ELSE FALSE
                END
           )
        FROM pg_catalog.pg_index index_row
        JOIN pg_catalog.pg_class index_class
          ON index_class.oid = index_row.indexrelid
        JOIN pg_catalog.pg_namespace index_namespace
          ON index_namespace.oid = index_class.relnamespace
        JOIN pg_catalog.pg_am access_method
          ON access_method.oid = index_class.relam
        WHERE index_row.indrelid = 'public.research_events'::REGCLASS
          AND index_class.relname IN (
              'uq_research_signal_snapshot_projection_key_v1',
              'idx_research_signal_snapshot_archive_key_v1'
          )
    ), FALSE) THEN
        RAISE EXCEPTION
            'Migration 023 Stage-4 index definitions are not intact';
    END IF;

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
        SELECT 1 FROM pg_catalog.pg_policy policy_row
        WHERE policy_row.polrelid IN (
            'public.research_events'::REGCLASS,
            'public.research_max_pain_snapshot_sets'::REGCLASS,
            'public.research_max_pain_snapshot_symbols'::REGCLASS,
            'public.research_max_pain_snapshot_rows'::REGCLASS
        )
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_rewrite rule_row
        WHERE rule_row.ev_class IN (
            'public.research_events'::REGCLASS,
            'public.research_max_pain_snapshot_sets'::REGCLASS,
            'public.research_max_pain_snapshot_symbols'::REGCLASS,
            'public.research_max_pain_snapshot_rows'::REGCLASS
        )
          AND rule_row.rulename <> '_RETURN'
    ) THEN
        RAISE EXCEPTION
            'Stage-4 sources cannot use RLS, policies or rewrite rules';
    END IF;
END;
$stage4_boundary$;

DO $wave_boundary$
DECLARE
    owner_oid OID := (
        SELECT role_row.oid FROM pg_catalog.pg_roles role_row
        WHERE role_row.rolname = 'research_market_movement_owner'
    );
    writer_oid OID := (
        SELECT role_row.oid FROM pg_catalog.pg_roles role_row
        WHERE role_row.rolname = 'research_market_movement_writer_v5'
    );
    writer_row RECORD;
    spec RECORD;
    actual_triggers TEXT[];
    actual_columns TEXT[];
    actual_check_fingerprints TEXT[];
    actual_count BIGINT;
    matched_count BIGINT;
BEGIN
    SELECT * INTO writer_row
    FROM pg_catalog.pg_roles
    WHERE rolname = 'research_market_movement_writer_v5';
    IF NOT FOUND
       OR NOT writer_row.rolcanlogin
       OR writer_row.rolinherit
       OR writer_row.rolsuper
       OR writer_row.rolcreatedb
       OR writer_row.rolcreaterole
       OR writer_row.rolreplication
       OR writer_row.rolbypassrls
       OR EXISTS (
            SELECT 1 FROM pg_catalog.pg_auth_members membership
            WHERE membership.member = writer_row.oid
               OR membership.roleid = writer_row.oid
       ) THEN
        RAISE EXCEPTION
            'Migration 022 dedicated Wave-v5 writer boundary is not intact';
    END IF;

    FOR spec IN
        SELECT * FROM (VALUES
            ('research_price_collection_attempts', ARRAY[
                'trg_market_movement_attempt_writer',
                'trg_neutral_price_attempt_complete',
                'trg_research_price_collection_attempts_append_only',
                'trg_research_price_collection_attempts_no_truncate'
            ]::TEXT[]),
            ('research_neutral_price_anchors', ARRAY[
                'trg_market_movement_anchor_writer',
                'trg_neutral_price_anchor_complete',
                'trg_research_neutral_price_anchors_append_only',
                'trg_research_neutral_price_anchors_no_truncate'
            ]::TEXT[]),
            ('research_market_movement_transitions', ARRAY[
                'trg_market_movement_transition_complete',
                'trg_market_movement_transition_writer',
                'trg_research_market_movement_transitions_append_only',
                'trg_research_market_movement_transitions_no_truncate',
                'trg_validate_market_movement_transition_insert'
            ]::TEXT[]),
            ('research_market_movement_memberships', ARRAY[
                'trg_market_movement_membership_complete',
                'trg_market_movement_membership_writer',
                'trg_research_market_movement_memberships_append_only',
                'trg_research_market_movement_memberships_no_truncate'
            ]::TEXT[])
        ) AS expected(relation_name, trigger_names)
    LOOP
        IF (
            SELECT relation_row.relowner
            FROM pg_catalog.pg_class relation_row
            WHERE relation_row.oid = pg_catalog.to_regclass(
                'public.' || spec.relation_name
            )
        ) IS DISTINCT FROM owner_oid THEN
            RAISE EXCEPTION
                'Wave-v5 source public.% has an unexpected owner',
                spec.relation_name;
        END IF;
        SELECT ARRAY_AGG(
            trigger_row.tgname::TEXT ORDER BY trigger_row.tgname::TEXT
        ) INTO actual_triggers
        FROM pg_catalog.pg_trigger trigger_row
        WHERE trigger_row.tgrelid = pg_catalog.to_regclass(
                  'public.' || spec.relation_name
              )
          AND NOT trigger_row.tgisinternal;
        IF actual_triggers IS DISTINCT FROM spec.trigger_names OR EXISTS (
            SELECT 1
            FROM pg_catalog.pg_trigger trigger_row
            JOIN pg_catalog.pg_proc function_row
              ON function_row.oid = trigger_row.tgfoid
            JOIN pg_catalog.pg_namespace function_namespace
              ON function_namespace.oid = function_row.pronamespace
            WHERE trigger_row.tgrelid = pg_catalog.to_regclass(
                      'public.' || spec.relation_name
                  )
              AND NOT trigger_row.tgisinternal
              AND (
                  trigger_row.tgenabled <> 'A'
                  OR trigger_row.tgqual IS NOT NULL
                  OR trigger_row.tgtype::INTEGER <> CASE
                      WHEN trigger_row.tgname LIKE '%_append_only' THEN 27
                      WHEN trigger_row.tgname LIKE '%_no_truncate' THEN 34
                      WHEN trigger_row.tgname IN (
                          'trg_neutral_price_attempt_complete',
                          'trg_neutral_price_anchor_complete',
                          'trg_market_movement_transition_complete',
                          'trg_market_movement_membership_complete'
                      ) THEN 5
                      ELSE 7
                  END
                  OR trigger_row.tgdeferrable <> (
                      trigger_row.tgname IN (
                          'trg_neutral_price_attempt_complete',
                          'trg_neutral_price_anchor_complete',
                          'trg_market_movement_transition_complete',
                          'trg_market_movement_membership_complete'
                      )
                  )
                  OR trigger_row.tginitdeferred <> (
                      trigger_row.tgname IN (
                          'trg_neutral_price_attempt_complete',
                          'trg_neutral_price_anchor_complete',
                          'trg_market_movement_transition_complete',
                          'trg_market_movement_membership_complete'
                      )
                  )
                  OR (trigger_row.tgconstraint <> 0) <> (
                      trigger_row.tgname IN (
                          'trg_neutral_price_attempt_complete',
                          'trg_neutral_price_anchor_complete',
                          'trg_market_movement_transition_complete',
                          'trg_market_movement_membership_complete'
                      )
                  )
                  OR function_namespace.nspname <> 'public'
                  OR function_row.proowner <> owner_oid
                  OR function_row.proconfig IS DISTINCT FROM
                      ARRAY['search_path=pg_catalog']::TEXT[]
                  OR function_row.proname <> CASE
                      WHEN trigger_row.tgname LIKE '%_append_only'
                        OR trigger_row.tgname LIKE '%_no_truncate'
                          THEN 'prevent_market_movement_archive_mutation'
                      WHEN trigger_row.tgname LIKE '%_writer'
                          THEN 'assert_market_movement_writer_v5'
                      WHEN trigger_row.tgname IN (
                          'trg_neutral_price_attempt_complete',
                          'trg_neutral_price_anchor_complete'
                      ) THEN 'assert_neutral_price_attempt_anchor_complete'
                      WHEN trigger_row.tgname =
                          'trg_validate_market_movement_transition_insert'
                          THEN 'validate_market_movement_transition_insert'
                      ELSE 'assert_market_movement_receipt_complete'
                  END
                  OR function_row.prosecdef <> (
                      function_row.proname IN (
                          'assert_neutral_price_attempt_anchor_complete',
                          'validate_market_movement_transition_insert',
                          'assert_market_movement_receipt_complete'
                      )
                  )
              )
        ) THEN
            RAISE EXCEPTION
                'Migration 022 trigger inventory is not intact on public.%',
                spec.relation_name;
        END IF;
        IF NOT pg_catalog.has_table_privilege(
            'research_market_movement_writer_v5',
            'public.' || spec.relation_name,
            'SELECT'
        ) OR NOT pg_catalog.has_table_privilege(
            'research_market_movement_writer_v5',
            'public.' || spec.relation_name,
            'INSERT'
        ) OR pg_catalog.has_table_privilege(
            'research_market_movement_writer_v5',
            'public.' || spec.relation_name,
            'UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER'
        ) THEN
            RAISE EXCEPTION
                'Migration 022 writer ACL is not intact on public.%',
                spec.relation_name;
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
            WHERE relation_row.oid = pg_catalog.to_regclass(
                      'public.' || spec.relation_name
                  )
              AND acl.grantee <> owner_oid
              AND NOT (
                  acl.grantee = writer_oid
                  AND acl.privilege_type IN ('SELECT', 'INSERT')
                  AND NOT acl.is_grantable
              )
        ) OR EXISTS (
            SELECT 1
            FROM pg_catalog.pg_attribute attribute
            WHERE attribute.attrelid = pg_catalog.to_regclass(
                      'public.' || spec.relation_name
                  )
              AND attribute.attnum > 0
              AND NOT attribute.attisdropped
              AND COALESCE(CARDINALITY(attribute.attacl), 0) <> 0
        ) THEN
            RAISE EXCEPTION
                'Migration 022 source ACL is not intact on public.%',
                spec.relation_name;
        END IF;
    END LOOP;

    IF (
        SELECT COUNT(*)
        FROM pg_catalog.pg_proc function_row
        JOIN pg_catalog.pg_namespace function_namespace
          ON function_namespace.oid = function_row.pronamespace
        WHERE function_namespace.nspname = 'public'
          AND function_row.proname = ANY(ARRAY[
              'assert_market_movement_writer_v5',
              'prevent_market_movement_archive_mutation',
              'assert_neutral_price_attempt_anchor_complete',
              'validate_market_movement_transition_insert',
              'assert_market_movement_receipt_complete'
          ]::NAME[])
          AND function_row.proowner = owner_oid
          AND function_row.proconfig IS NOT DISTINCT FROM
              ARRAY['search_path=pg_catalog']::TEXT[]
          AND function_row.prosecdef = (function_row.proname = ANY(ARRAY[
              'assert_neutral_price_attempt_anchor_complete',
              'validate_market_movement_transition_insert',
              'assert_market_movement_receipt_complete'
          ]::NAME[]))
    ) <> 5 THEN
        RAISE EXCEPTION
            'Migration 022 Wave-v5 function inventory is not intact';
    END IF;

    -- Mirror migration 022's exact typed source boundary.  Additional,
    -- reordered or weakened columns cannot silently flow through the views.
    FOR spec IN
        SELECT * FROM (VALUES
            ('research_price_collection_attempts', ARRAY[
                'attempt_receipt_sha256|character(64)|true',
                'contract_version|text|true',
                'symbol|text|true',
                'eligible_at_utc|timestamp with time zone|true',
                'decision_time_utc|timestamp with time zone|true',
                'evaluation_status|text|true',
                'evaluation_reason|text|true',
                'anchor_id|character(64)|false',
                'anchor_receipt_sha256|character(64)|false',
                'attempt_receipt|jsonb|true',
                'created_at_utc|timestamp with time zone|true'
            ]::TEXT[]),
            ('research_neutral_price_anchors', ARRAY[
                'anchor_id|character(64)|true',
                'anchor_receipt_sha256|character(64)|true',
                'contract_version|text|true',
                'symbol|text|true',
                'origin|text|true',
                'sampler_version|text|true',
                'eligible_at_utc|timestamp with time zone|true',
                'decision_time_utc|timestamp with time zone|true',
                'source_price_candle_open_utc|timestamp with time zone|true',
                'source_price_candle_close_utc|timestamp with time zone|true',
                'observed_at_utc|timestamp with time zone|true',
                'refresh_completed_at_utc|timestamp with time zone|true',
                'price|numeric|true',
                'source|text|true',
                'upstream_source|text|true',
                'price_exchange|text|true',
                'price_market|text|true',
                'price_pair|text|true',
                'price_instrument_id|text|true',
                'price_timeframe|text|true',
                'quality_status|text|true',
                'fallback_used|boolean|true',
                'fallback_policy|text|true',
                'price_candle_identity_basis|text|true',
                'source_input_fingerprint|character(64)|false',
                'source_record_created_at_utc|timestamp with time zone|false',
                'anchor_receipt|jsonb|true',
                'created_at_utc|timestamp with time zone|true'
            ]::TEXT[]),
            ('research_market_movement_transitions', ARRAY[
                'transition_receipt_sha256|character(64)|true',
                'contract_version|text|true',
                'previous_transition_receipt_sha256|character(64)|false',
                'chain_ordinal|bigint|true',
                'transition_type|text|true',
                'stream_id|character(64)|true',
                'namespace|text|true',
                'symbol|text|true',
                'movement_id|character(64)|true',
                'trigger_anchor_id|character(64)|true',
                'trigger_eligible_at_utc|timestamp with time zone|true',
                'trigger_decision_time_utc|timestamp with time zone|true',
                'pre_state_sha256|character(64)|false',
                'post_state_sha256|character(64)|true',
                'post_state|jsonb|true',
                'transition_receipt|jsonb|true',
                'created_at_utc|timestamp with time zone|true'
            ]::TEXT[]),
            ('research_market_movement_memberships', ARRAY[
                'membership_receipt_sha256|character(64)|true',
                'emitted_by_transition_receipt_sha256|character(64)|true',
                'contract_version|text|true',
                'stream_id|character(64)|true',
                'movement_id|character(64)|true',
                'anchor_id|character(64)|true',
                'anchor_receipt_sha256|character(64)|true',
                'ordinal|bigint|true',
                'classification|text|true',
                'eligible_at_utc|timestamp with time zone|true',
                'decision_time_utc|timestamp with time zone|true',
                'price|numeric|true',
                'membership_receipt|jsonb|true',
                'created_at_utc|timestamp with time zone|true'
            ]::TEXT[])
        ) AS expected(relation_name, column_shape)
    LOOP
        SELECT ARRAY_AGG(
            attribute.attname::TEXT || '|' ||
            pg_catalog.format_type(attribute.atttypid, attribute.atttypmod) ||
            '|' || attribute.attnotnull::TEXT
            ORDER BY attribute.attnum
        ) INTO actual_columns
        FROM pg_catalog.pg_attribute attribute
        WHERE attribute.attrelid = pg_catalog.to_regclass(
                  'public.' || spec.relation_name
              )
          AND attribute.attnum > 0
          AND NOT attribute.attisdropped;
        IF actual_columns IS DISTINCT FROM spec.column_shape OR EXISTS (
            SELECT 1 FROM pg_catalog.pg_class relation_row
            WHERE relation_row.oid = pg_catalog.to_regclass(
                      'public.' || spec.relation_name
                  )
              AND (
                  relation_row.relkind <> 'r'
                  OR relation_row.relpersistence <> 'p'
                  OR relation_row.relispartition
              )
        ) OR EXISTS (
            SELECT 1 FROM pg_catalog.pg_inherits inheritance
            WHERE inheritance.inhrelid = pg_catalog.to_regclass(
                      'public.' || spec.relation_name
                  )
               OR inheritance.inhparent = pg_catalog.to_regclass(
                      'public.' || spec.relation_name
                  )
        ) THEN
            RAISE EXCEPTION
                'Migration 022 typed shape is not intact on public.%',
                spec.relation_name;
        END IF;
    END LOOP;

    -- CHECK definitions are compared by the exact normalized fingerprints
    -- frozen and replay-validated by migration 022.
    FOR spec IN
        SELECT * FROM (VALUES
            ('research_price_collection_attempts', ARRAY[
                '101:35e0c7d231e11cd02807fad624047322',
                '118:a06492c49d553ed96c214689eab82c02',
                '149:850c094701ad971fab62533fb4a50afa',
                '201:946775873df7aace2400349a65e92c81',
                '253:9f3cd5dc12c67885a8f447c9ca979bb2',
                '43:174ae3ee51f8117198c09dc04b418c94',
                '48:edad99e1b6186c170335dfeb4172aacd',
                '53:f033921fdc4cf65eb19865757bdfb012',
                '69:e2534c7d14ed8c41ee3ae1502b2ff7a1',
                '731:32b037d1a68e27e580b98e7f83d8eb8c',
                '76:b627e856a9413bbb983293453341e125',
                '77:7062500815bfadb3b3570539200c2e03',
                '78:76240724217db924e2719abd761c5ee7'
            ]::TEXT[]),
            ('research_neutral_price_anchors', ARRAY[
                '107:77b2ff3b91004f58ef5fdb34e8d92845',
                '118:a06492c49d553ed96c214689eab82c02',
                '180:bc0c46a847de2806f570ca0c83d97cde',
                '2004:243cdd76a95e66cd21d57a6a11780126',
                '200:7b356873cc07da0d9f56bf16e6008609',
                '28:29918cab80af56f0bea2d58d190182ab',
                '32:fced97618ebc1d6163da4286f0f271e6',
                '36:5dd0077d8ceaaf63f13d7cfce7cd5d2d',
                '38:11e190cf450a3bdaf2627ea9c8a97637',
                '40:e3e8c3c04ada8d8c49eeedc98a3b0091',
                '41:071171d5b52341e2de9b40f112623f34',
                '41:d94c954e79b461b2d89a582c4d31ddfc',
                '42:f09daf17232bb642afaa3628e7736a12',
                '43:16b93122422138e75b64dc19a0e6ebf1',
                '45:d35e004990a254e98b2c13fe1914502d',
                '48:edad99e1b6186c170335dfeb4172aacd',
                '52:c217ad79dadbdbda2ea9c0f91c69aea3',
                '56:2671fb4b9c65a94d984e971019c8b748',
                '59:7ca1830e0bfc24867b51e8a7b92d1c4d',
                '683:c9127c46bf54cf1900387fa5fccb5b43',
                '68:e36471f70c2b73a224e705e23608338a',
                '690:6d0d0d49ff0f4fba2555600003d28c17',
                '69:4b426326050f343d36968b54d82b70c2',
                '76:f70887594a51d25ec818268bfb451548',
                '78:76240724217db924e2719abd761c5ee7',
                '82:100306eb6056112ed42fbe5208d4d7e5',
                '860:274124791e0529c014b84b5544f53472'
            ]::TEXT[]),
            ('research_market_movement_transitions', ARRAY[
                '108:202338a45cfb589e3c84b28ba6e04883',
                '127:c6fb257c515209de9bbd6d0622e6cb0d',
                '1344:0cd91de5bef7fcf36feb4ff972640e7e',
                '224:326a0988c8aa9835b386a8aad75a96f9',
                '24:3fb32e754ac9d6e0b35573e586250ebc',
                '2556:55deecf4b1cf5994269d2fbffa7b713f',
                '335:82f828193e82e9441279ac9393b0d814',
                '363:264e2fabf12394a9594b22763c445103',
                '401:bd3908ca52a7a268723fb400e7fb5f27',
                '48:b8641e411ce7831d0a17ff99cc6b15da',
                '562:e6070d534e328253adecea6869eec718',
                '56:ee01db11ee23a2d2f16a23149408a498',
                '56:fc983293aea08c47e352fec6b8253053',
                '58:b7e6e38eafe276d155d46078d28b08b5',
                '64:41c36dffb43022816c0dc0cdd6be91d7',
                '64:80e0a249e1bcbb1694b39231f04fd254',
                '727:ea39175ebe52c4f6dc3129b49634705b',
                '72:999853d8b047693817cd087ef4a6c8da',
                '78:76240724217db924e2719abd761c5ee7',
                '888:2032e48f19103f679428ac131e8223b9',
                '91:fd2dfa236f81ea1ac59b6f393b4db8a8',
                '998:a0099f6a7872f16a1366820e572ffffa'
            ]::TEXT[]),
            ('research_market_movement_memberships', ARRAY[
                '123:f9b5c4b09fc904df312f1ebb429631a6',
                '18:5abb45294b53ea2a7896ee9819c24980',
                '283:c758d9585a638f7a7c416ce80efe0116',
                '56:41e438b6ab298fca334e4b6a5daee679',
                '56:ee01db11ee23a2d2f16a23149408a498',
                '58:b7e6e38eafe276d155d46078d28b08b5',
                '59:7ca1830e0bfc24867b51e8a7b92d1c4d',
                '72:c8c9fa86bd4d616e6ca5a205566f76ff',
                '78:76240724217db924e2719abd761c5ee7',
                '865:e47e2609a99e00bcc97a9daad8387fc8'
            ]::TEXT[])
        ) AS expected(relation_name, check_fingerprints)
    LOOP
        SELECT ARRAY_AGG(fingerprint ORDER BY fingerprint), COUNT(*)
        INTO actual_check_fingerprints, actual_count
        FROM pg_catalog.pg_constraint constraint_row
        CROSS JOIN LATERAL (
            SELECT LENGTH(normalized_definition)::TEXT || ':' ||
                   MD5(normalized_definition) AS fingerprint
            FROM (
                SELECT REGEXP_REPLACE(
                    pg_catalog.pg_get_constraintdef(
                        constraint_row.oid, FALSE
                    ),
                    '[[:space:]]+', '', 'g'
                ) AS normalized_definition
            ) normalized
        ) fingerprint_row
        WHERE constraint_row.conrelid = pg_catalog.to_regclass(
                  'public.' || spec.relation_name
              )
          AND constraint_row.contype = 'c'
          AND constraint_row.convalidated
          AND constraint_row.conislocal
          AND constraint_row.coninhcount = 0;
        IF actual_count <> CARDINALITY(spec.check_fingerprints)
           OR actual_check_fingerprints IS DISTINCT FROM
              spec.check_fingerprints THEN
            RAISE EXCEPTION
                'Migration 022 CHECK fingerprints are not intact on public.%',
                spec.relation_name;
        END IF;
    END LOOP;

    -- Exact PK/UQ/FK graph.  Names are intentionally irrelevant, but columns,
    -- referenced relations, actions and deferral semantics are not.
    FOR spec IN
        SELECT * FROM (VALUES
            ('research_price_collection_attempts', 'p',
             ARRAY['attempt_receipt_sha256']::TEXT[], NULL::TEXT,
             NULL::TEXT[], FALSE, FALSE, NULL::TEXT),
            ('research_price_collection_attempts', 'f',
             ARRAY['anchor_id', 'anchor_receipt_sha256']::TEXT[],
             'research_neutral_price_anchors',
             ARRAY['anchor_id', 'anchor_receipt_sha256']::TEXT[],
             TRUE, TRUE, 'r'),
            ('research_neutral_price_anchors', 'p',
             ARRAY['anchor_id']::TEXT[], NULL::TEXT,
             NULL::TEXT[], FALSE, FALSE, NULL::TEXT),
            ('research_neutral_price_anchors', 'u',
             ARRAY['anchor_receipt_sha256']::TEXT[], NULL::TEXT,
             NULL::TEXT[], FALSE, FALSE, NULL::TEXT),
            ('research_neutral_price_anchors', 'u',
             ARRAY['contract_version', 'symbol', 'eligible_at_utc']::TEXT[],
             NULL::TEXT, NULL::TEXT[], FALSE, FALSE, NULL::TEXT),
            ('research_neutral_price_anchors', 'u',
             ARRAY['anchor_id', 'anchor_receipt_sha256']::TEXT[], NULL::TEXT,
             NULL::TEXT[], FALSE, FALSE, NULL::TEXT),
            ('research_market_movement_transitions', 'p',
             ARRAY['transition_receipt_sha256']::TEXT[], NULL::TEXT,
             NULL::TEXT[], FALSE, FALSE, NULL::TEXT),
            ('research_market_movement_transitions', 'u',
             ARRAY['stream_id', 'chain_ordinal']::TEXT[], NULL::TEXT,
             NULL::TEXT[], FALSE, FALSE, NULL::TEXT),
            ('research_market_movement_transitions', 'u',
             ARRAY['stream_id', 'trigger_anchor_id', 'transition_type']::TEXT[],
             NULL::TEXT, NULL::TEXT[], FALSE, FALSE, NULL::TEXT),
            ('research_market_movement_transitions', 'f',
             ARRAY['previous_transition_receipt_sha256']::TEXT[],
             'research_market_movement_transitions',
             ARRAY['transition_receipt_sha256']::TEXT[], TRUE, TRUE, 'r'),
            ('research_market_movement_transitions', 'f',
             ARRAY['trigger_anchor_id']::TEXT[],
             'research_neutral_price_anchors', ARRAY['anchor_id']::TEXT[],
             FALSE, FALSE, 'r'),
            ('research_market_movement_memberships', 'p',
             ARRAY['membership_receipt_sha256']::TEXT[], NULL::TEXT,
             NULL::TEXT[], FALSE, FALSE, NULL::TEXT),
            ('research_market_movement_memberships', 'u',
             ARRAY['emitted_by_transition_receipt_sha256']::TEXT[], NULL::TEXT,
             NULL::TEXT[], FALSE, FALSE, NULL::TEXT),
            ('research_market_movement_memberships', 'u',
             ARRAY['stream_id', 'anchor_id']::TEXT[], NULL::TEXT,
             NULL::TEXT[], FALSE, FALSE, NULL::TEXT),
            ('research_market_movement_memberships', 'u',
             ARRAY['stream_id', 'movement_id', 'ordinal']::TEXT[], NULL::TEXT,
             NULL::TEXT[], FALSE, FALSE, NULL::TEXT),
            ('research_market_movement_memberships', 'f',
             ARRAY['emitted_by_transition_receipt_sha256']::TEXT[],
             'research_market_movement_transitions',
             ARRAY['transition_receipt_sha256']::TEXT[], TRUE, TRUE, 'r'),
            ('research_market_movement_memberships', 'f',
             ARRAY['anchor_id', 'anchor_receipt_sha256']::TEXT[],
             'research_neutral_price_anchors',
             ARRAY['anchor_id', 'anchor_receipt_sha256']::TEXT[],
             TRUE, TRUE, 'r')
        ) AS expected(
            relation_name, constraint_type, local_columns,
            reference_relation, reference_columns,
            is_deferrable, is_deferred, delete_action
        )
    LOOP
        SELECT COUNT(*) INTO matched_count
        FROM pg_catalog.pg_constraint constraint_row
        WHERE constraint_row.conrelid = pg_catalog.to_regclass(
                  'public.' || spec.relation_name
              )
          AND constraint_row.contype::TEXT = spec.constraint_type
          AND constraint_row.convalidated
          AND constraint_row.condeferrable = spec.is_deferrable
          AND constraint_row.condeferred = spec.is_deferred
          AND COALESCE(
              (pg_catalog.to_jsonb(constraint_row)->>'conenforced')::BOOLEAN,
              TRUE
          )
          AND (
              SELECT ARRAY_AGG(
                  attribute.attname::TEXT ORDER BY key_column.ordinality
              )
              FROM UNNEST(constraint_row.conkey)
                   WITH ORDINALITY AS key_column(attnum, ordinality)
              JOIN pg_catalog.pg_attribute attribute
                ON attribute.attrelid = constraint_row.conrelid
               AND attribute.attnum = key_column.attnum
          ) = spec.local_columns
          AND (
              spec.constraint_type <> 'f'
              OR (
                  constraint_row.confrelid = pg_catalog.to_regclass(
                      'public.' || spec.reference_relation
                  )
                  AND (
                      SELECT ARRAY_AGG(
                          attribute.attname::TEXT
                          ORDER BY key_column.ordinality
                      )
                      FROM UNNEST(constraint_row.confkey)
                           WITH ORDINALITY AS key_column(attnum, ordinality)
                      JOIN pg_catalog.pg_attribute attribute
                        ON attribute.attrelid = constraint_row.confrelid
                       AND attribute.attnum = key_column.attnum
                  ) = spec.reference_columns
                  AND constraint_row.confdeltype::TEXT = spec.delete_action
                  AND constraint_row.confupdtype::TEXT = 'a'
                  AND constraint_row.confmatchtype::TEXT = 's'
              )
          );
        IF matched_count <> 1 THEN
            RAISE EXCEPTION
                'Migration 022 % constraint is not exact on public.% (%)',
                spec.constraint_type, spec.relation_name, spec.local_columns;
        END IF;
    END LOOP;

    FOR spec IN
        SELECT * FROM (VALUES
            ('research_price_collection_attempts', 2::BIGINT),
            ('research_neutral_price_anchors', 4::BIGINT),
            ('research_market_movement_transitions', 5::BIGINT),
            ('research_market_movement_memberships', 6::BIGINT)
        ) AS expected(relation_name, constraint_count)
    LOOP
        SELECT COUNT(*) INTO actual_count
        FROM pg_catalog.pg_constraint constraint_row
        WHERE constraint_row.conrelid = pg_catalog.to_regclass(
                  'public.' || spec.relation_name
              )
          AND constraint_row.contype::TEXT IN ('p', 'u', 'f');
        IF actual_count <> spec.constraint_count THEN
            RAISE EXCEPTION
                'Migration 022 PK/UQ/FK inventory is not exact on public.%',
                spec.relation_name;
        END IF;
    END LOOP;

    IF EXISTS (
        SELECT 1
        FROM pg_catalog.pg_class relation_row
        WHERE relation_row.oid IN (
            'public.research_price_collection_attempts'::REGCLASS,
            'public.research_neutral_price_anchors'::REGCLASS,
            'public.research_market_movement_transitions'::REGCLASS,
            'public.research_market_movement_memberships'::REGCLASS
        )
          AND (relation_row.relrowsecurity OR relation_row.relforcerowsecurity)
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_policy policy_row
        WHERE policy_row.polrelid IN (
            'public.research_price_collection_attempts'::REGCLASS,
            'public.research_neutral_price_anchors'::REGCLASS,
            'public.research_market_movement_transitions'::REGCLASS,
            'public.research_market_movement_memberships'::REGCLASS
        )
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_rewrite rule_row
        WHERE rule_row.ev_class IN (
            'public.research_price_collection_attempts'::REGCLASS,
            'public.research_neutral_price_anchors'::REGCLASS,
            'public.research_market_movement_transitions'::REGCLASS,
            'public.research_market_movement_memberships'::REGCLASS
        )
    ) OR EXISTS (
        SELECT 1
        FROM pg_catalog.pg_constraint constraint_row
        WHERE constraint_row.conrelid IN (
            'public.research_price_collection_attempts'::REGCLASS,
            'public.research_neutral_price_anchors'::REGCLASS,
            'public.research_market_movement_transitions'::REGCLASS,
            'public.research_market_movement_memberships'::REGCLASS
        )
          AND NOT constraint_row.convalidated
    ) THEN
        RAISE EXCEPTION
            'Migration 022 Wave-v5 visibility or constraint boundary is not intact';
    END IF;

    FOR spec IN
        SELECT * FROM (VALUES
            ('research_price_collection_attempts',
             'idx_neutral_price_attempt_slot', FALSE,
             ARRAY['symbol', 'eligible_at_utc', 'decision_time_utc']::TEXT[],
             ARRAY[0, 0, 0]::SMALLINT[], NULL::TEXT),
            ('research_price_collection_attempts',
             'uq_neutral_price_attempt_evaluable_anchor', TRUE,
             ARRAY['anchor_id']::TEXT[], ARRAY[0]::SMALLINT[],
             'evaluation_status=''EVALUABLE''::text'),
            ('research_neutral_price_anchors',
             'idx_neutral_price_anchor_slot', FALSE,
             ARRAY['symbol', 'eligible_at_utc', 'anchor_id']::TEXT[],
             ARRAY[0, 0, 0]::SMALLINT[], NULL::TEXT),
            ('research_market_movement_transitions',
             'uq_market_movement_transition_root', TRUE,
             ARRAY['stream_id']::TEXT[], ARRAY[0]::SMALLINT[],
             'previous_transition_receipt_sha256ISNULL'),
            ('research_market_movement_transitions',
             'uq_market_movement_transition_successor', TRUE,
             ARRAY['previous_transition_receipt_sha256']::TEXT[],
             ARRAY[0]::SMALLINT[],
             'previous_transition_receipt_sha256ISNOTNULL'),
            ('research_market_movement_transitions',
             'idx_market_movement_transition_head', FALSE,
             ARRAY['stream_id', 'chain_ordinal']::TEXT[],
             ARRAY[0, 3]::SMALLINT[], NULL::TEXT),
            ('research_market_movement_memberships',
             'idx_market_movement_membership_movement', FALSE,
             ARRAY['stream_id', 'movement_id', 'ordinal']::TEXT[],
             ARRAY[0, 0, 0]::SMALLINT[], NULL::TEXT),
            ('research_market_movement_memberships',
             'idx_market_movement_membership_slot', FALSE,
             ARRAY['eligible_at_utc', 'stream_id']::TEXT[],
             ARRAY[0, 0]::SMALLINT[], NULL::TEXT)
        ) AS expected(
            relation_name, index_name, is_unique, key_columns,
            key_options, normalized_predicate
        )
    LOOP
        SELECT COUNT(*) INTO matched_count
        FROM pg_catalog.pg_index index_row
        JOIN pg_catalog.pg_class index_relation
          ON index_relation.oid = index_row.indexrelid
        JOIN pg_catalog.pg_am access_method
          ON access_method.oid = index_relation.relam
        WHERE index_row.indexrelid = pg_catalog.to_regclass(
                  'public.' || spec.index_name
              )
          AND index_row.indrelid = pg_catalog.to_regclass(
                  'public.' || spec.relation_name
              )
          AND index_relation.relnamespace = 'public'::REGNAMESPACE
          AND index_relation.relkind = 'i'
          AND index_relation.relpersistence = 'p'
          AND access_method.amname = 'btree'
          AND index_row.indisunique = spec.is_unique
          AND index_row.indisvalid
          AND index_row.indisready
          AND index_row.indislive
          AND index_row.indimmediate
          AND NOT index_row.indisexclusion
          AND NOT index_row.indcheckxmin
          AND NOT index_row.indisreplident
          AND index_row.indexprs IS NULL
          AND index_row.indnatts = index_row.indnkeyatts
          AND index_row.indnkeyatts = CARDINALITY(spec.key_columns)
          AND pg_catalog.pg_get_indexdef(index_row.indexrelid) IS NOT NULL
          AND (
              SELECT ARRAY_AGG(
                  attribute.attname::TEXT ORDER BY key_column.ordinality
              )
              FROM UNNEST(index_row.indkey::SMALLINT[])
                   WITH ORDINALITY AS key_column(attnum, ordinality)
              JOIN pg_catalog.pg_attribute attribute
                ON attribute.attrelid = index_row.indrelid
               AND attribute.attnum = key_column.attnum
          ) = spec.key_columns
          AND ARRAY(
              SELECT key_option.value
              FROM UNNEST(index_row.indoption::SMALLINT[])
                   WITH ORDINALITY AS key_option(value, ordinality)
              ORDER BY key_option.ordinality
          ) = spec.key_options
          AND BTRIM(
              REGEXP_REPLACE(
                  pg_catalog.pg_get_expr(
                      index_row.indpred, index_row.indrelid, FALSE
                  ),
                  '[[:space:]]+', '', 'g'
              ),
              '()'
          ) IS NOT DISTINCT FROM spec.normalized_predicate;
        IF matched_count <> 1 THEN
            RAISE EXCEPTION
                'Migration 022 index public.% is not exact', spec.index_name;
        END IF;
    END LOOP;

    FOR spec IN
        SELECT * FROM (VALUES
            ('research_price_collection_attempts', 3::BIGINT),
            ('research_neutral_price_anchors', 5::BIGINT),
            ('research_market_movement_transitions', 6::BIGINT),
            ('research_market_movement_memberships', 6::BIGINT)
        ) AS expected(relation_name, index_count)
    LOOP
        SELECT COUNT(*) INTO actual_count
        FROM pg_catalog.pg_index index_row
        WHERE index_row.indrelid = pg_catalog.to_regclass(
                  'public.' || spec.relation_name
              );
        IF actual_count <> spec.index_count THEN
            RAISE EXCEPTION
                'Migration 022 index inventory is not exact on public.%',
                spec.relation_name;
        END IF;
    END LOOP;
END;
$wave_boundary$;

SET LOCAL search_path = public;

CREATE OR REPLACE VIEW public.research_formula_exploration_stage4_v1
WITH (security_barrier = true, security_invoker = false)
AS
WITH stage4_event AS (
    SELECT
        event_row.*,
        CASE
            WHEN event_row.event_type = 'SIGNAL_SNAPSHOT_PROJECTION' THEN
                NULLIF(
                    event_row.engine_snapshot #>>
                        '{projection,snapshot_set_id}',
                    ''
                )::BIGINT
            ELSE
                NULLIF(
                    event_row.engine_snapshot #>>
                        '{signal_snapshot,archive_reference,snapshot_set_id}',
                    ''
                )::BIGINT
        END AS claimed_snapshot_set_id,
        CASE
            WHEN event_row.event_type = 'SIGNAL_SNAPSHOT_PROJECTION' THEN
                event_row.engine_snapshot #>> '{projection,snapshot_key}'
            ELSE
                event_row.engine_snapshot #>>
                    '{signal_snapshot,archive_reference,snapshot_key}'
        END AS claimed_snapshot_key
    FROM public.research_events event_row
    WHERE event_row.capture_stage = 'SILENT_SIGNAL_SNAPSHOT'
       OR event_row.event_type IN (
            'MAX_PAIN_CONFIRMATION_STATE',
            'MAGNET_CONFIRMATION_STATE',
            'SILENT_COMBINED_CONFIRMATION_SNAPSHOT',
            'SIGNAL_SNAPSHOT_PROJECTION'
       )
)
SELECT
    event_row.event_id,
    event_row.schema_version,
    event_row.event_kind,
    event_row.event_type,
    event_row.alert_time_utc,
    event_row.symbol,
    event_row.direction,
    event_row.source_side,
    event_row.timeframe,
    event_row.score,
    event_row.current_price,
    event_row.target_price,
    event_row.initial_target_distance_pct,
    event_row.categories,
    event_row.setup_key,
    event_row.event_fingerprint,
    event_row.strategy_version,
    event_row.code_version,
    event_row.runtime_session_id,
    event_row.capture_stage,
    event_row.delivery_status,
    event_row.delivery_attempted_at_utc,
    event_row.delivered_at_utc,
    event_row.engine_snapshot,
    event_row.created_at AS event_created_at,
    event_row.claimed_snapshot_set_id,
    event_row.claimed_snapshot_key,
    archive_row.snapshot_set_id AS archive_snapshot_set_id,
    archive_row.snapshot_key AS archive_snapshot_key,
    archive_row.payload_sha256 AS archive_payload_sha256,
    archive_row.cycle_time_utc AS archive_cycle_time_utc,
    archive_row.available_at_utc AS archive_available_at_utc,
    archive_row.source AS archive_source,
    archive_row.research_eligible AS archive_research_eligible,
    archive_row.created_at_utc AS archive_created_at_utc
FROM stage4_event event_row
LEFT JOIN public.research_max_pain_snapshot_sets archive_row
  ON archive_row.snapshot_set_id = event_row.claimed_snapshot_set_id
 AND BTRIM(archive_row.snapshot_key) = event_row.claimed_snapshot_key;

DO $stage4_view_definition_receipt$
DECLARE
    view_definition_sha256 TEXT;
BEGIN
    SELECT ENCODE(SHA256(CONVERT_TO(
        pg_catalog.pg_get_viewdef(rule_row.ev_class, FALSE), 'UTF8'
    )), 'hex')
    INTO view_definition_sha256
    FROM pg_catalog.pg_rewrite rule_row
    WHERE rule_row.ev_class =
          'public.research_formula_exploration_stage4_v1'::REGCLASS
      AND rule_row.rulename = '_RETURN';
    IF view_definition_sha256 IS NULL
       OR view_definition_sha256 !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION 'Stage-4 source view lacks one canonical rewrite rule';
    END IF;
    EXECUTE pg_catalog.format(
        'COMMENT ON VIEW public.%I IS %L',
        'research_formula_exploration_stage4_v1',
        'stage4-wave-v5-authoritative-source-v1; read-only Stage-4 cohort '
        || 'source; no Formula, outcome, delivery, LIVE or trading authority; '
        || 'view_definition_sha256=' || view_definition_sha256
    );
END;
$stage4_view_definition_receipt$;

SET LOCAL ROLE research_market_movement_owner;
SET LOCAL search_path = public;

CREATE OR REPLACE VIEW public.research_formula_exploration_wave_v5_v1
WITH (security_barrier = true, security_invoker = false)
AS
SELECT
    membership_row.membership_receipt_sha256,
    membership_row.emitted_by_transition_receipt_sha256,
    membership_row.contract_version AS membership_contract_version,
    membership_row.stream_id AS membership_stream_id,
    membership_row.movement_id AS membership_movement_id,
    membership_row.anchor_id AS membership_anchor_id,
    membership_row.anchor_receipt_sha256 AS membership_anchor_receipt_sha256,
    membership_row.ordinal AS membership_ordinal,
    membership_row.classification AS membership_classification,
    membership_row.eligible_at_utc AS membership_eligible_at_utc,
    membership_row.decision_time_utc AS membership_decision_time_utc,
    membership_row.price AS membership_price,
    membership_row.membership_receipt,
    membership_row.created_at_utc AS membership_created_at_utc,
    transition_row.transition_receipt_sha256,
    transition_row.previous_transition_receipt_sha256,
    transition_row.contract_version AS transition_contract_version,
    transition_row.chain_ordinal AS transition_chain_ordinal,
    transition_row.transition_type,
    transition_row.stream_id AS transition_stream_id,
    transition_row.namespace AS transition_namespace,
    transition_row.symbol AS transition_symbol,
    transition_row.movement_id AS transition_movement_id,
    transition_row.trigger_anchor_id AS transition_trigger_anchor_id,
    transition_row.trigger_eligible_at_utc AS
        transition_trigger_eligible_at_utc,
    transition_row.trigger_decision_time_utc AS
        transition_trigger_decision_time_utc,
    transition_row.pre_state_sha256 AS transition_pre_state_sha256,
    transition_row.post_state_sha256 AS transition_post_state_sha256,
    transition_row.post_state AS transition_post_state,
    transition_row.transition_receipt,
    transition_row.created_at_utc AS transition_created_at_utc,
    anchor_row.anchor_id,
    anchor_row.anchor_receipt_sha256,
    anchor_row.contract_version AS anchor_contract_version,
    anchor_row.symbol AS anchor_symbol,
    anchor_row.origin AS anchor_origin,
    anchor_row.sampler_version AS anchor_sampler_version,
    anchor_row.eligible_at_utc AS anchor_eligible_at_utc,
    anchor_row.decision_time_utc AS anchor_decision_time_utc,
    anchor_row.source_price_candle_open_utc AS
        anchor_source_price_candle_open_utc,
    anchor_row.source_price_candle_close_utc AS
        anchor_source_price_candle_close_utc,
    anchor_row.observed_at_utc AS anchor_observed_at_utc,
    anchor_row.refresh_completed_at_utc AS anchor_refresh_completed_at_utc,
    anchor_row.price AS anchor_price,
    anchor_row.source AS anchor_source,
    anchor_row.upstream_source AS anchor_upstream_source,
    anchor_row.price_exchange AS anchor_price_exchange,
    anchor_row.price_market AS anchor_price_market,
    anchor_row.price_pair AS anchor_price_pair,
    anchor_row.price_instrument_id AS anchor_price_instrument_id,
    anchor_row.price_timeframe AS anchor_price_timeframe,
    anchor_row.quality_status AS anchor_quality_status,
    anchor_row.fallback_used AS anchor_fallback_used,
    anchor_row.fallback_policy AS anchor_fallback_policy,
    anchor_row.price_candle_identity_basis AS
        anchor_price_candle_identity_basis,
    anchor_row.source_input_fingerprint AS anchor_source_input_fingerprint,
    anchor_row.source_record_created_at_utc AS
        anchor_source_record_created_at_utc,
    anchor_row.anchor_receipt,
    anchor_row.created_at_utc AS anchor_created_at_utc
FROM public.research_market_movement_memberships membership_row
LEFT JOIN public.research_market_movement_transitions transition_row
  ON transition_row.transition_receipt_sha256 =
     membership_row.emitted_by_transition_receipt_sha256
LEFT JOIN public.research_neutral_price_anchors anchor_row
  ON anchor_row.anchor_id = membership_row.anchor_id
 AND anchor_row.anchor_receipt_sha256 =
     membership_row.anchor_receipt_sha256;

DO $wave_view_definition_receipt$
DECLARE
    view_definition_sha256 TEXT;
BEGIN
    SELECT ENCODE(SHA256(CONVERT_TO(
        pg_catalog.pg_get_viewdef(rule_row.ev_class, FALSE), 'UTF8'
    )), 'hex')
    INTO view_definition_sha256
    FROM pg_catalog.pg_rewrite rule_row
    WHERE rule_row.ev_class =
          'public.research_formula_exploration_wave_v5_v1'::REGCLASS
      AND rule_row.rulename = '_RETURN';
    IF view_definition_sha256 IS NULL
       OR view_definition_sha256 !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION 'Wave-v5 source view lacks one canonical rewrite rule';
    END IF;
    EXECUTE pg_catalog.format(
        'COMMENT ON VIEW public.%I IS %L',
        'research_formula_exploration_wave_v5_v1',
        'stage4-wave-v5-authoritative-source-v1; read-only Wave-v5 grouping '
        || 'source; no Formula, outcome, delivery, LIVE or trading authority; '
        || 'view_definition_sha256=' || view_definition_sha256
    );
END;
$wave_view_definition_receipt$;

-- The Wave owner is the view owner already.  Normalize every stale grant
-- before admitting the one fixed reader.
DO $wave_view_acl_cleanup$
DECLARE
    grant_row RECORD;
    owner_oid OID := (
        SELECT relation_row.relowner
        FROM pg_catalog.pg_class relation_row
        WHERE relation_row.oid =
            'public.research_formula_exploration_wave_v5_v1'::REGCLASS
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
              'public.research_formula_exploration_wave_v5_v1'::REGCLASS
          AND acl.grantee <> owner_oid
    LOOP
        EXECUTE pg_catalog.format(
            'REVOKE ALL PRIVILEGES ON TABLE public.%I FROM %s CASCADE',
            'research_formula_exploration_wave_v5_v1',
            CASE WHEN grant_row.grantee = 0 THEN 'PUBLIC'
                 ELSE pg_catalog.quote_ident(
                     pg_catalog.pg_get_userbyid(grant_row.grantee)
                 ) END
        );
    END LOOP;
END;
$wave_view_acl_cleanup$;

REVOKE ALL ON TABLE public.research_formula_exploration_wave_v5_v1
    FROM PUBLIC, research_formula_exploration_reader_v1;
GRANT SELECT ON TABLE public.research_formula_exploration_wave_v5_v1
    TO research_formula_exploration_reader_v1;
REVOKE ALL ON TABLE
    public.research_price_collection_attempts,
    public.research_neutral_price_anchors,
    public.research_market_movement_transitions,
    public.research_market_movement_memberships
    FROM research_formula_exploration_reader_v1;

DO $wave_column_acl_cleanup$
DECLARE
    relation_name TEXT;
    grant_row RECORD;
BEGIN
    FOREACH relation_name IN ARRAY ARRAY[
        'research_price_collection_attempts',
        'research_neutral_price_anchors',
        'research_market_movement_transitions',
        'research_market_movement_memberships',
        'research_formula_exploration_wave_v5_v1'
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
                      'research_formula_exploration_wave_v5_v1'
                  OR acl.grantee = (
                      SELECT role_row.oid FROM pg_catalog.pg_roles role_row
                      WHERE role_row.rolname =
                          'research_formula_exploration_reader_v1'
                  )
              )
        LOOP
            EXECUTE pg_catalog.format(
                'REVOKE %s (%I) ON TABLE public.%I FROM %s CASCADE',
                grant_row.privilege_type,
                grant_row.attname,
                relation_name,
                CASE
                    WHEN grant_row.grantee = 0 THEN 'PUBLIC'
                    ELSE pg_catalog.quote_ident(
                        pg_catalog.pg_get_userbyid(grant_row.grantee)
                    )
                END
            );
        END LOOP;
    END LOOP;
END;
$wave_column_acl_cleanup$;

RESET ROLE;
SET LOCAL search_path = public;

REVOKE CREATE ON SCHEMA public
    FROM research_formula_exploration_reader_v1;
GRANT USAGE ON SCHEMA public
    TO research_formula_exploration_reader_v1;

REVOKE ALL ON TABLE public.research_formula_exploration_stage4_v1
    FROM PUBLIC, research_formula_exploration_reader_v1;

DO $stage4_view_acl_cleanup$
DECLARE
    grant_row RECORD;
    owner_oid OID := (
        SELECT relation_row.relowner
        FROM pg_catalog.pg_class relation_row
        WHERE relation_row.oid =
            'public.research_formula_exploration_stage4_v1'::REGCLASS
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
              'public.research_formula_exploration_stage4_v1'::REGCLASS
          AND acl.grantee <> owner_oid
    LOOP
        EXECUTE pg_catalog.format(
            'REVOKE ALL PRIVILEGES ON TABLE public.%I FROM %s CASCADE',
            'research_formula_exploration_stage4_v1',
            CASE WHEN grant_row.grantee = 0 THEN 'PUBLIC'
                 ELSE pg_catalog.quote_ident(
                     pg_catalog.pg_get_userbyid(grant_row.grantee)
                 ) END
        );
    END LOOP;
END;
$stage4_view_acl_cleanup$;

GRANT SELECT ON TABLE public.research_formula_exploration_stage4_v1
    TO research_formula_exploration_reader_v1;

REVOKE ALL ON TABLE
    public.research_events,
    public.research_max_pain_snapshot_sets,
    public.research_max_pain_snapshot_symbols,
    public.research_max_pain_snapshot_rows
    FROM research_formula_exploration_reader_v1;
REVOKE ALL ON SEQUENCE
    public.research_events_event_id_seq,
    public.research_max_pain_snapshot_sets_snapshot_set_id_seq,
    public.research_max_pain_snapshot_rows_snapshot_row_id_seq
    FROM research_formula_exploration_reader_v1;

-- Table-level REVOKE does not remove per-column privileges.  Clean every
-- explicit reader column ACL on source relations and both views.
DO $column_acl_cleanup$
DECLARE
    relation_name TEXT;
    grant_row RECORD;
BEGIN
    FOREACH relation_name IN ARRAY ARRAY[
        'research_events',
        'research_max_pain_snapshot_sets',
        'research_max_pain_snapshot_symbols',
        'research_max_pain_snapshot_rows',
        'research_formula_exploration_stage4_v1'
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
                      'research_formula_exploration_stage4_v1'
                  OR acl.grantee = (
                      SELECT role_row.oid FROM pg_catalog.pg_roles role_row
                      WHERE role_row.rolname =
                          'research_formula_exploration_reader_v1'
                  )
              )
        LOOP
            EXECUTE pg_catalog.format(
                'REVOKE %s (%I) ON TABLE public.%I FROM %s CASCADE',
                grant_row.privilege_type,
                grant_row.attname,
                relation_name,
                CASE
                    WHEN grant_row.grantee = 0 THEN 'PUBLIC'
                    ELSE pg_catalog.quote_ident(
                        pg_catalog.pg_get_userbyid(grant_row.grantee)
                    )
                END
            );
        END LOOP;
    END LOOP;
END;
$column_acl_cleanup$;

-- Re-grant SELECT after column cleanup; the intended privilege is relation
-- level only and never grantable.
GRANT SELECT ON TABLE public.research_formula_exploration_stage4_v1
    TO research_formula_exploration_reader_v1;
SET LOCAL ROLE research_market_movement_owner;
GRANT SELECT ON TABLE public.research_formula_exploration_wave_v5_v1
    TO research_formula_exploration_reader_v1;
RESET ROLE;

DO $final_catalog_assertions$
DECLARE
    reader_oid OID := (
        SELECT role_row.oid FROM pg_catalog.pg_roles role_row
        WHERE role_row.rolname = 'research_formula_exploration_reader_v1'
    );
    stage4_owner_oid OID := (
        SELECT relation_row.relowner FROM pg_catalog.pg_class relation_row
        WHERE relation_row.oid = 'public.research_events'::REGCLASS
    );
    wave_owner_oid OID := (
        SELECT role_row.oid FROM pg_catalog.pg_roles role_row
        WHERE role_row.rolname = 'research_market_movement_owner'
    );
    spec RECORD;
    source_name TEXT;
BEGIN
    FOR spec IN
        SELECT * FROM (VALUES
            ('research_formula_exploration_stage4_v1', stage4_owner_oid, 35),
            ('research_formula_exploration_wave_v5_v1', wave_owner_oid, 59)
        ) AS expected(view_name, owner_oid, column_count)
    LOOP
        IF NOT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_class view_row
            WHERE view_row.oid = pg_catalog.to_regclass(
                      'public.' || spec.view_name
                  )
              AND view_row.relkind = 'v'
              AND view_row.relowner = spec.owner_oid
              AND view_row.reloptions @> ARRAY[
                    'security_barrier=true', 'security_invoker=false'
                  ]::TEXT[]
              AND CARDINALITY(view_row.reloptions) = 2
        ) OR (
            SELECT COUNT(*)
            FROM pg_catalog.pg_attribute attribute
            WHERE attribute.attrelid = pg_catalog.to_regclass(
                      'public.' || spec.view_name
                  )
              AND attribute.attnum > 0
              AND NOT attribute.attisdropped
        ) <> spec.column_count THEN
            RAISE EXCEPTION
                'Formula exploration view public.% has an unsafe shape',
                spec.view_name;
        END IF;
        IF NOT pg_catalog.has_table_privilege(
            'research_formula_exploration_reader_v1',
            'public.' || spec.view_name,
            'SELECT'
        ) OR pg_catalog.has_table_privilege(
            'research_formula_exploration_reader_v1',
            'public.' || spec.view_name,
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
            WHERE view_row.oid = pg_catalog.to_regclass(
                      'public.' || spec.view_name
                  )
              AND acl.grantee NOT IN (spec.owner_oid, reader_oid)
        ) OR EXISTS (
            SELECT 1
            FROM pg_catalog.pg_class view_row
            CROSS JOIN LATERAL pg_catalog.aclexplode(
                COALESCE(
                    view_row.relacl,
                    pg_catalog.acldefault('r', view_row.relowner)
                )
            ) acl
            WHERE view_row.oid = pg_catalog.to_regclass(
                      'public.' || spec.view_name
                  )
              AND acl.grantee = reader_oid
              AND (
                  acl.privilege_type <> 'SELECT'
                  OR acl.is_grantable
              )
        ) OR EXISTS (
            SELECT 1
            FROM pg_catalog.pg_attribute attribute
            WHERE attribute.attrelid = pg_catalog.to_regclass(
                      'public.' || spec.view_name
                  )
              AND attribute.attnum > 0
              AND NOT attribute.attisdropped
              AND COALESCE(CARDINALITY(attribute.attacl), 0) <> 0
        ) THEN
            RAISE EXCEPTION
                'Formula exploration view public.% has an unsafe ACL',
                spec.view_name;
        END IF;
    END LOOP;

    FOR spec IN
        SELECT * FROM (VALUES
            ('research_formula_exploration_stage4_v1', ARRAY[
                'public.research_events',
                'public.research_max_pain_snapshot_sets'
            ]::TEXT[]),
            ('research_formula_exploration_wave_v5_v1', ARRAY[
                'public.research_market_movement_memberships',
                'public.research_market_movement_transitions',
                'public.research_neutral_price_anchors'
            ]::TEXT[])
        ) AS expected(view_name, dependencies)
    LOOP
        IF (
            SELECT ARRAY_AGG(
                DISTINCT dependency_namespace.nspname || '.' ||
                         dependency_relation.relname
                ORDER BY dependency_namespace.nspname || '.' ||
                         dependency_relation.relname
            )
            FROM pg_catalog.pg_rewrite rule_row
            JOIN pg_catalog.pg_depend dependency_row
              ON dependency_row.classid = 'pg_catalog.pg_rewrite'::REGCLASS
             AND dependency_row.objid = rule_row.oid
            JOIN pg_catalog.pg_class dependency_relation
              ON dependency_relation.oid = dependency_row.refobjid
            JOIN pg_catalog.pg_namespace dependency_namespace
              ON dependency_namespace.oid = dependency_relation.relnamespace
            WHERE rule_row.ev_class = pg_catalog.to_regclass(
                      'public.' || spec.view_name
                  )
              AND rule_row.rulename = '_RETURN'
              AND dependency_relation.oid <> rule_row.ev_class
              AND dependency_relation.relkind IN ('r', 'p')
        ) IS DISTINCT FROM spec.dependencies THEN
            RAISE EXCEPTION
                'Formula exploration view public.% has unsafe dependencies',
                spec.view_name;
        END IF;
    END LOOP;

    FOREACH source_name IN ARRAY ARRAY[
        'research_events',
        'research_max_pain_snapshot_sets',
        'research_max_pain_snapshot_symbols',
        'research_max_pain_snapshot_rows',
        'research_price_collection_attempts',
        'research_neutral_price_anchors',
        'research_market_movement_transitions',
        'research_market_movement_memberships'
    ] LOOP
        IF pg_catalog.has_table_privilege(
            'research_formula_exploration_reader_v1',
            'public.' || source_name,
            'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER'
        ) OR pg_catalog.has_any_column_privilege(
            'research_formula_exploration_reader_v1',
            'public.' || source_name,
            'SELECT, INSERT, UPDATE, REFERENCES'
        ) THEN
            RAISE EXCEPTION
                'Formula exploration reader can bypass view through public.%',
                source_name;
        END IF;
    END LOOP;

    IF NOT pg_catalog.has_schema_privilege(
        'research_formula_exploration_reader_v1', 'public', 'USAGE'
    ) OR pg_catalog.has_schema_privilege(
        'research_formula_exploration_reader_v1', 'public', 'CREATE'
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_auth_members membership
        WHERE membership.member = reader_oid OR membership.roleid = reader_oid
    ) THEN
        RAISE EXCEPTION
            'Formula exploration reader role boundary is not intact';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM (VALUES
            ('research_events_event_id_seq'),
            ('research_max_pain_snapshot_sets_snapshot_set_id_seq'),
            ('research_max_pain_snapshot_rows_snapshot_row_id_seq')
        ) AS source(sequence_name)
        WHERE pg_catalog.has_sequence_privilege(
            'research_formula_exploration_reader_v1',
            'public.' || source.sequence_name,
            'USAGE, SELECT, UPDATE'
        )
    ) THEN
        RAISE EXCEPTION
            'Formula exploration reader has a source-sequence privilege';
    END IF;
END;
$final_catalog_assertions$;

-- Seal the attested catalog state without catalog OIDs, timestamps or the
-- comments that carry the receipt.  Every nested array has an explicit order;
-- JSONB's canonical object representation supplies deterministic key order.
-- The runtime reader pins this same environment before using PostgreSQL
-- deparsers; keeping it identical here makes every pg_get_* rendering
-- deterministic across installer and reader sessions.
SET LOCAL search_path = pg_catalog;
SET LOCAL TIME ZONE 'UTC';
SET LOCAL DateStyle = 'ISO, YMD';
SET LOCAL IntervalStyle = 'postgres';
SET LOCAL extra_float_digits = 3;
SET LOCAL quote_all_identifiers = off;

DO $source_catalog_receipt$
DECLARE
    deparser_gucs_payload JSONB;
    roles_payload JSONB;
    schema_payload JSONB;
    relations_payload JSONB;
    sequences_payload JSONB;
    constraints_payload JSONB;
    indexes_payload JSONB;
    triggers_payload JSONB;
    functions_payload JSONB;
    views_payload JSONB;
    catalog_payload JSONB;
    source_catalog_sha256 TEXT;
    stage4_view_definition_sha256 TEXT;
BEGIN
    deparser_gucs_payload := JSONB_BUILD_OBJECT(
        'date_style', pg_catalog.current_setting('DateStyle'),
        'interval_style', pg_catalog.current_setting('IntervalStyle'),
        'extra_float_digits',
            pg_catalog.current_setting('extra_float_digits'),
        'quote_all_identifiers',
            pg_catalog.current_setting('quote_all_identifiers'),
        'search_path', pg_catalog.current_setting('search_path'),
        'time_zone', pg_catalog.current_setting('TimeZone')
    );
    IF deparser_gucs_payload IS DISTINCT FROM JSONB_BUILD_OBJECT(
        'date_style', 'ISO, YMD',
        'interval_style', 'postgres',
        'extra_float_digits', '3',
        'quote_all_identifiers', 'off',
        'search_path', 'pg_catalog',
        'time_zone', 'UTC'
    ) THEN
        RAISE EXCEPTION
            'Formula exploration deparser GUC environment is not canonical';
    END IF;

    -- `pg_auth_members` gained per-edge inherit/set options in PostgreSQL 16.
    -- Reading the row through to_jsonb keeps this receipt runnable on PG15;
    -- pre-PG16 membership edges canonically map both historical options to
    -- TRUE, which is their effective legacy behavior.
    WITH RECURSIVE
    authority_roots(authority, role_oid) AS (
        VALUES
            ('stage4'::TEXT, (
                SELECT relation_row.relowner
                FROM pg_catalog.pg_class relation_row
                WHERE relation_row.oid =
                      'public.research_events'::REGCLASS
            )),
            ('wave_v5'::TEXT, (
                SELECT role_row.oid
                FROM pg_catalog.pg_roles role_row
                WHERE role_row.rolname = 'research_market_movement_owner'
            ))
    ),
    authority_reachable(role_oid) AS (
        SELECT root.role_oid
        FROM authority_roots root
        UNION
        SELECT membership.member
        FROM authority_reachable reachable
        JOIN pg_catalog.pg_auth_members membership
          ON membership.roleid = reachable.role_oid
    ),
    required_role_ids(role_oid) AS (
        SELECT role_row.oid
        FROM pg_catalog.pg_roles role_row
        WHERE role_row.rolname IN (
            'research_signal_snapshot_writer_v1',
            'research_market_movement_owner',
            'research_market_movement_writer_v5',
            'research_formula_exploration_reader_v1'
        )
        UNION
        SELECT root.role_oid
        FROM authority_roots root
    ),
    authority_membership_edges AS (
        SELECT membership.roleid,
               membership.member,
               membership.grantor,
               membership.admin_option,
               COALESCE(
                   (pg_catalog.to_jsonb(membership) ->>
                       'inherit_option')::BOOLEAN,
                   TRUE
               ) AS inherit_option,
               COALESCE(
                   (pg_catalog.to_jsonb(membership) ->>
                       'set_option')::BOOLEAN,
                   TRUE
               ) AS set_option
        FROM pg_catalog.pg_auth_members membership
        WHERE membership.roleid IN (
            SELECT reachable.role_oid FROM authority_reachable reachable
        )
           OR membership.roleid IN (
                SELECT required.role_oid FROM required_role_ids required
           )
           OR membership.member IN (
                SELECT required.role_oid FROM required_role_ids required
           )
    ),
    graph_role_ids(role_oid) AS (
        SELECT required.role_oid FROM required_role_ids required
        UNION
        SELECT reachable.role_oid FROM authority_reachable reachable
        UNION
        SELECT edge.roleid FROM authority_membership_edges edge
        UNION
        SELECT edge.member FROM authority_membership_edges edge
        UNION
        SELECT edge.grantor FROM authority_membership_edges edge
    )
    SELECT JSONB_BUILD_OBJECT(
        'authority_roots', COALESCE((
            SELECT JSONB_AGG(
                JSONB_BUILD_OBJECT(
                    'authority', root.authority,
                    'role', pg_catalog.pg_get_userbyid(root.role_oid)
                ) ORDER BY root.authority
            )
            FROM authority_roots root
        ), '[]'::JSONB),
        'required_roles', COALESCE((
            SELECT JSONB_AGG(
                pg_catalog.pg_get_userbyid(required.role_oid)
                ORDER BY pg_catalog.pg_get_userbyid(required.role_oid)
            )
            FROM required_role_ids required
        ), '[]'::JSONB),
        'nodes', COALESCE((
            SELECT JSONB_AGG(
                JSONB_BUILD_OBJECT(
                    'name', role_row.rolname,
                    'can_login', role_row.rolcanlogin,
                    'inherit', role_row.rolinherit,
                    'superuser', role_row.rolsuper,
                    'create_db', role_row.rolcreatedb,
                    'create_role', role_row.rolcreaterole,
                    'replication', role_row.rolreplication,
                    'bypass_rls', role_row.rolbypassrls
                ) ORDER BY role_row.rolname
            )
            FROM graph_role_ids graph_role
            JOIN pg_catalog.pg_roles role_row
              ON role_row.oid = graph_role.role_oid
        ), '[]'::JSONB),
        'membership_edges', COALESCE((
            SELECT JSONB_AGG(
                JSONB_BUILD_OBJECT(
                    'role', pg_catalog.pg_get_userbyid(edge.roleid),
                    'member', pg_catalog.pg_get_userbyid(edge.member),
                    'grantor', pg_catalog.pg_get_userbyid(edge.grantor),
                    'admin_option', edge.admin_option,
                    'inherit_option', edge.inherit_option,
                    'set_option', edge.set_option
                ) ORDER BY
                    pg_catalog.pg_get_userbyid(edge.roleid),
                    pg_catalog.pg_get_userbyid(edge.member),
                    pg_catalog.pg_get_userbyid(edge.grantor)
            )
            FROM authority_membership_edges edge
        ), '[]'::JSONB)
    ) INTO roles_payload;

    SELECT JSONB_BUILD_OBJECT(
        'name', namespace_row.nspname,
        'owner', pg_catalog.pg_get_userbyid(namespace_row.nspowner),
        'acl', COALESCE((
            SELECT JSONB_AGG(
                JSONB_BUILD_OBJECT(
                    'grantee', CASE WHEN acl.grantee = 0 THEN 'PUBLIC'
                                    ELSE pg_catalog.pg_get_userbyid(acl.grantee)
                               END,
                    'privilege', acl.privilege_type,
                    'grantable', acl.is_grantable
                ) ORDER BY
                    CASE WHEN acl.grantee = 0 THEN 'PUBLIC'
                         ELSE pg_catalog.pg_get_userbyid(acl.grantee) END,
                    acl.privilege_type
            )
            FROM pg_catalog.aclexplode(
                COALESCE(
                    namespace_row.nspacl,
                    pg_catalog.acldefault('n', namespace_row.nspowner)
                )
            ) acl
            -- Migration 026 owns and independently attests this one delegated
            -- schema ACL.  Normalize only its exact non-grantable shape so
            -- full migration reapplication preserves this canonical receipt.
            WHERE NOT (
                acl.grantee <> 0
                AND pg_catalog.pg_get_userbyid(acl.grantee) =
                    'research_stage4_no_signal_outcome_writer_v1'
                AND acl.privilege_type = 'USAGE'
                AND NOT acl.is_grantable
            )
        ), '[]'::JSONB)
    ) INTO schema_payload
    FROM pg_catalog.pg_namespace namespace_row
    WHERE namespace_row.nspname = 'public';

    SELECT COALESCE(JSONB_AGG(entry ORDER BY relation_name), '[]'::JSONB)
    INTO relations_payload
    FROM (
        SELECT relation_row.relname::TEXT AS relation_name,
               JSONB_BUILD_OBJECT(
                   'schema', namespace_row.nspname,
                   'name', relation_row.relname,
                   'owner', pg_catalog.pg_get_userbyid(relation_row.relowner),
                   'kind', relation_row.relkind,
                   'persistence', relation_row.relpersistence,
                   'is_partition', relation_row.relispartition,
                   'rls', relation_row.relrowsecurity,
                   'force_rls', relation_row.relforcerowsecurity,
                   'columns', COALESCE((
                       SELECT JSONB_AGG(
                           JSONB_BUILD_OBJECT(
                               'name', attribute.attname,
                               'type', pg_catalog.format_type(
                                   attribute.atttypid, attribute.atttypmod
                               ),
                               'not_null', attribute.attnotnull,
                               'collation', CASE
                                   WHEN attribute.attcollation = 0 THEN NULL
                                   ELSE attribute.attcollation::REGCOLLATION::TEXT
                               END,
                               'identity', attribute.attidentity,
                               'generated', attribute.attgenerated,
                               'default', pg_catalog.pg_get_expr(
                                   default_value.adbin, default_value.adrelid
                               ),
                               'acl', COALESCE((
                                   SELECT JSONB_AGG(
                                       JSONB_BUILD_OBJECT(
                                           'grantee', CASE
                                               WHEN column_acl.grantee = 0
                                                   THEN 'PUBLIC'
                                               ELSE pg_catalog.pg_get_userbyid(
                                                   column_acl.grantee
                                               ) END,
                                           'privilege',
                                               column_acl.privilege_type,
                                           'grantable', column_acl.is_grantable
                                       ) ORDER BY
                                           CASE WHEN column_acl.grantee = 0
                                               THEN 'PUBLIC'
                                               ELSE pg_catalog.pg_get_userbyid(
                                                   column_acl.grantee
                                               ) END,
                                           column_acl.privilege_type
                                   )
                                   FROM pg_catalog.aclexplode(
                                       attribute.attacl
                                   ) column_acl
                               ), '[]'::JSONB)
                           ) ORDER BY attribute.attnum
                       )
                       FROM pg_catalog.pg_attribute attribute
                       LEFT JOIN pg_catalog.pg_attrdef default_value
                         ON default_value.adrelid = attribute.attrelid
                        AND default_value.adnum = attribute.attnum
                       WHERE attribute.attrelid = relation_row.oid
                         AND attribute.attnum > 0
                         AND NOT attribute.attisdropped
                   ), '[]'::JSONB),
                   'acl', COALESCE((
                       SELECT JSONB_AGG(
                           JSONB_BUILD_OBJECT(
                               'grantee', CASE WHEN acl.grantee = 0 THEN 'PUBLIC'
                                               ELSE pg_catalog.pg_get_userbyid(
                                                   acl.grantee
                                               ) END,
                               'privilege', acl.privilege_type,
                               'grantable', acl.is_grantable
                           ) ORDER BY
                               CASE WHEN acl.grantee = 0 THEN 'PUBLIC'
                                    ELSE pg_catalog.pg_get_userbyid(acl.grantee)
                               END,
                               acl.privilege_type
                       )
                       FROM pg_catalog.aclexplode(
                           COALESCE(
                               relation_row.relacl,
                               pg_catalog.acldefault(
                                   'r', relation_row.relowner
                               )
                           )
                       ) acl
                       -- Migration 026 separately attests these four exact
                       -- non-grantable SELECT entries.  They are the only
                       -- later relation ACLs normalized from this receipt.
                       WHERE NOT (
                           acl.grantee <> 0
                           AND pg_catalog.pg_get_userbyid(acl.grantee) =
                               'research_stage4_no_signal_outcome_writer_v1'
                           AND relation_row.relname IN (
                               'research_events',
                               'research_max_pain_snapshot_sets',
                               'research_max_pain_snapshot_symbols',
                               'research_max_pain_snapshot_rows'
                           )
                           AND acl.privilege_type = 'SELECT'
                           AND NOT acl.is_grantable
                       )
                   ), '[]'::JSONB),
                   'policies', COALESCE((
                       SELECT JSONB_AGG(
                           JSONB_BUILD_OBJECT(
                               'name', policy_row.polname,
                               'command', policy_row.polcmd,
                               'permissive', policy_row.polpermissive,
                               'roles', COALESCE((
                                   SELECT JSONB_AGG(
                                       pg_catalog.pg_get_userbyid(role_id)
                                       ORDER BY pg_catalog.pg_get_userbyid(
                                           role_id
                                       )
                                   )
                                   FROM UNNEST(policy_row.polroles) role_id
                               ), '[]'::JSONB),
                               'using', pg_catalog.pg_get_expr(
                                   policy_row.polqual,
                                   policy_row.polrelid,
                                   FALSE
                               ),
                               'check', pg_catalog.pg_get_expr(
                                   policy_row.polwithcheck,
                                   policy_row.polrelid,
                                   FALSE
                               )
                           ) ORDER BY policy_row.polname
                       )
                       FROM pg_catalog.pg_policy policy_row
                       WHERE policy_row.polrelid = relation_row.oid
                   ), '[]'::JSONB),
                   'rules', COALESCE((
                       SELECT JSONB_AGG(
                           JSONB_BUILD_OBJECT(
                               'name', rule_row.rulename,
                               'event', rule_row.ev_type,
                               'enabled', rule_row.ev_enabled,
                               'instead', rule_row.is_instead,
                               'definition', pg_catalog.pg_get_ruledef(
                                   rule_row.oid, FALSE
                               )
                           ) ORDER BY rule_row.rulename
                       )
                       FROM pg_catalog.pg_rewrite rule_row
                       WHERE rule_row.ev_class = relation_row.oid
                   ), '[]'::JSONB)
               ) AS entry
        FROM pg_catalog.pg_class relation_row
        JOIN pg_catalog.pg_namespace namespace_row
          ON namespace_row.oid = relation_row.relnamespace
        WHERE namespace_row.nspname = 'public'
          AND relation_row.relname IN (
              'research_events',
              'research_max_pain_snapshot_sets',
              'research_max_pain_snapshot_symbols',
              'research_max_pain_snapshot_rows',
              'research_price_collection_attempts',
              'research_neutral_price_anchors',
              'research_market_movement_transitions',
              'research_market_movement_memberships'
          )
    ) ordered_relations;

    SELECT COALESCE(JSONB_AGG(entry ORDER BY sequence_name), '[]'::JSONB)
    INTO sequences_payload
    FROM (
        SELECT sequence_row.relname::TEXT AS sequence_name,
               JSONB_BUILD_OBJECT(
                   'schema', namespace_row.nspname,
                   'name', sequence_row.relname,
                   'owner', pg_catalog.pg_get_userbyid(sequence_row.relowner),
                   'persistence', sequence_row.relpersistence,
                   'acl', COALESCE((
                       SELECT JSONB_AGG(
                           JSONB_BUILD_OBJECT(
                               'grantee', CASE WHEN acl.grantee = 0 THEN 'PUBLIC'
                                               ELSE pg_catalog.pg_get_userbyid(
                                                   acl.grantee
                                               ) END,
                               'privilege', acl.privilege_type,
                               'grantable', acl.is_grantable
                           ) ORDER BY
                               CASE WHEN acl.grantee = 0 THEN 'PUBLIC'
                                    ELSE pg_catalog.pg_get_userbyid(acl.grantee)
                               END,
                               acl.privilege_type
                       )
                       FROM pg_catalog.aclexplode(
                           COALESCE(
                               sequence_row.relacl,
                               pg_catalog.acldefault(
                                   's', sequence_row.relowner
                               )
                           )
                       ) acl
                   ), '[]'::JSONB)
               ) AS entry
        FROM pg_catalog.pg_class sequence_row
        JOIN pg_catalog.pg_namespace namespace_row
          ON namespace_row.oid = sequence_row.relnamespace
        WHERE namespace_row.nspname = 'public'
          AND sequence_row.relkind = 'S'
          AND sequence_row.relname IN (
              'research_events_event_id_seq',
              'research_max_pain_snapshot_sets_snapshot_set_id_seq',
              'research_max_pain_snapshot_rows_snapshot_row_id_seq'
          )
    ) ordered_sequences;

    SELECT COALESCE(JSONB_AGG(entry ORDER BY relation_name, definition), '[]'::JSONB)
    INTO constraints_payload
    FROM (
        SELECT relation_row.relname::TEXT AS relation_name,
               pg_catalog.pg_get_constraintdef(
                   constraint_row.oid, FALSE
               ) AS definition,
               JSONB_BUILD_OBJECT(
                   'relation', relation_row.relname,
                   'type', constraint_row.contype,
                   'local_columns', COALESCE((
                       SELECT JSONB_AGG(
                           attribute.attname ORDER BY key_column.ordinality
                       )
                       FROM UNNEST(constraint_row.conkey)
                            WITH ORDINALITY AS key_column(attnum, ordinality)
                       JOIN pg_catalog.pg_attribute attribute
                         ON attribute.attrelid = constraint_row.conrelid
                        AND attribute.attnum = key_column.attnum
                   ), '[]'::JSONB),
                   'reference_relation', CASE
                       WHEN constraint_row.confrelid = 0 THEN NULL
                       ELSE reference_namespace.nspname || '.' ||
                            reference_relation.relname
                   END,
                   'reference_columns', COALESCE((
                       SELECT JSONB_AGG(
                           attribute.attname ORDER BY key_column.ordinality
                       )
                       FROM UNNEST(constraint_row.confkey)
                            WITH ORDINALITY AS key_column(attnum, ordinality)
                       JOIN pg_catalog.pg_attribute attribute
                         ON attribute.attrelid = constraint_row.confrelid
                        AND attribute.attnum = key_column.attnum
                   ), '[]'::JSONB),
                   'deferrable', constraint_row.condeferrable,
                   'deferred', constraint_row.condeferred,
                   'validated', constraint_row.convalidated,
                   'local', constraint_row.conislocal,
                   'inherit_count', constraint_row.coninhcount,
                   'no_inherit', constraint_row.connoinherit,
                   'definition', pg_catalog.pg_get_constraintdef(
                       constraint_row.oid, FALSE
                   )
               ) AS entry
        FROM pg_catalog.pg_constraint constraint_row
        JOIN pg_catalog.pg_class relation_row
          ON relation_row.oid = constraint_row.conrelid
        JOIN pg_catalog.pg_namespace relation_namespace
          ON relation_namespace.oid = relation_row.relnamespace
        LEFT JOIN pg_catalog.pg_class reference_relation
          ON reference_relation.oid = constraint_row.confrelid
        LEFT JOIN pg_catalog.pg_namespace reference_namespace
          ON reference_namespace.oid = reference_relation.relnamespace
        WHERE relation_namespace.nspname = 'public'
          AND relation_row.relname IN (
              'research_events',
              'research_max_pain_snapshot_sets',
              'research_max_pain_snapshot_symbols',
              'research_max_pain_snapshot_rows',
              'research_price_collection_attempts',
              'research_neutral_price_anchors',
              'research_market_movement_transitions',
              'research_market_movement_memberships'
          )
    ) ordered_constraints;

    SELECT COALESCE(JSONB_AGG(entry ORDER BY relation_name, index_name), '[]'::JSONB)
    INTO indexes_payload
    FROM (
        SELECT relation_row.relname::TEXT AS relation_name,
               index_relation.relname::TEXT AS index_name,
               JSONB_BUILD_OBJECT(
                   'relation', relation_row.relname,
                   'name', index_relation.relname,
                   'owner', pg_catalog.pg_get_userbyid(index_relation.relowner),
                   'access_method', access_method.amname,
                   'unique', index_row.indisunique,
                   'primary', index_row.indisprimary,
                   'valid', index_row.indisvalid,
                   'ready', index_row.indisready,
                   'live', index_row.indislive,
                   'immediate', index_row.indimmediate,
                   'definition', pg_catalog.pg_get_indexdef(
                       index_row.indexrelid
                   ),
                   'predicate', pg_catalog.pg_get_expr(
                       index_row.indpred, index_row.indrelid, FALSE
                   )
               ) AS entry
        FROM pg_catalog.pg_index index_row
        JOIN pg_catalog.pg_class relation_row
          ON relation_row.oid = index_row.indrelid
        JOIN pg_catalog.pg_namespace relation_namespace
          ON relation_namespace.oid = relation_row.relnamespace
        JOIN pg_catalog.pg_class index_relation
          ON index_relation.oid = index_row.indexrelid
        JOIN pg_catalog.pg_am access_method
          ON access_method.oid = index_relation.relam
        WHERE relation_namespace.nspname = 'public'
          AND relation_row.relname IN (
              'research_events',
              'research_max_pain_snapshot_sets',
              'research_max_pain_snapshot_symbols',
              'research_max_pain_snapshot_rows',
              'research_price_collection_attempts',
              'research_neutral_price_anchors',
              'research_market_movement_transitions',
              'research_market_movement_memberships'
          )
    ) ordered_indexes;

    SELECT COALESCE(JSONB_AGG(entry ORDER BY relation_name, trigger_name), '[]'::JSONB)
    INTO triggers_payload
    FROM (
        SELECT relation_row.relname::TEXT AS relation_name,
               trigger_row.tgname::TEXT AS trigger_name,
               JSONB_BUILD_OBJECT(
                   'relation', relation_row.relname,
                   'name', trigger_row.tgname,
                   'enabled', trigger_row.tgenabled,
                   'type', trigger_row.tgtype,
                   'deferrable', trigger_row.tgdeferrable,
                   'deferred', trigger_row.tginitdeferred,
                   'constraint_trigger', trigger_row.tgconstraint <> 0,
                   'when', pg_catalog.pg_get_expr(
                       trigger_row.tgqual, trigger_row.tgrelid, FALSE
                   ),
                   'definition', pg_catalog.pg_get_triggerdef(
                       trigger_row.oid, FALSE
                   ),
                   'function_schema', function_namespace.nspname,
                   'function_name', function_row.proname,
                   'function_owner', pg_catalog.pg_get_userbyid(
                       function_row.proowner
                   ),
                   'function_security_definer', function_row.prosecdef,
                   'function_config', COALESCE(
                       pg_catalog.to_jsonb(function_row.proconfig),
                       '[]'::JSONB
                   )
               ) AS entry
        FROM pg_catalog.pg_trigger trigger_row
        JOIN pg_catalog.pg_class relation_row
          ON relation_row.oid = trigger_row.tgrelid
        JOIN pg_catalog.pg_namespace relation_namespace
          ON relation_namespace.oid = relation_row.relnamespace
        JOIN pg_catalog.pg_proc function_row
          ON function_row.oid = trigger_row.tgfoid
        JOIN pg_catalog.pg_namespace function_namespace
          ON function_namespace.oid = function_row.pronamespace
        WHERE relation_namespace.nspname = 'public'
          AND NOT trigger_row.tgisinternal
          AND relation_row.relname IN (
              'research_events',
              'research_max_pain_snapshot_sets',
              'research_max_pain_snapshot_symbols',
              'research_max_pain_snapshot_rows',
              'research_price_collection_attempts',
              'research_neutral_price_anchors',
              'research_market_movement_transitions',
              'research_market_movement_memberships'
          )
    ) ordered_triggers;

    SELECT COALESCE(JSONB_AGG(entry ORDER BY function_name, identity_args), '[]'::JSONB)
    INTO functions_payload
    FROM (
        SELECT function_row.proname::TEXT AS function_name,
               pg_catalog.pg_get_function_identity_arguments(
                   function_row.oid
               ) AS identity_args,
               JSONB_BUILD_OBJECT(
                   'schema', function_namespace.nspname,
                   'name', function_row.proname,
                   'identity_arguments',
                       pg_catalog.pg_get_function_identity_arguments(
                           function_row.oid
                       ),
                   'result', pg_catalog.pg_get_function_result(
                       function_row.oid
                   ),
                   'owner', pg_catalog.pg_get_userbyid(function_row.proowner),
                   'language', language_row.lanname,
                   'security_definer', function_row.prosecdef,
                   'volatility', function_row.provolatile,
                   'strict', function_row.proisstrict,
                   'parallel', function_row.proparallel,
                   'config', COALESCE(
                       pg_catalog.to_jsonb(function_row.proconfig),
                       '[]'::JSONB
                   ),
                   'acl', COALESCE((
                       SELECT JSONB_AGG(
                           JSONB_BUILD_OBJECT(
                               'grantee', CASE WHEN acl.grantee = 0 THEN 'PUBLIC'
                                               ELSE pg_catalog.pg_get_userbyid(
                                                   acl.grantee
                                               ) END,
                               'privilege', acl.privilege_type,
                               'grantable', acl.is_grantable
                           ) ORDER BY
                               CASE WHEN acl.grantee = 0 THEN 'PUBLIC'
                                    ELSE pg_catalog.pg_get_userbyid(acl.grantee)
                               END,
                               acl.privilege_type
                       )
                       FROM pg_catalog.aclexplode(
                           COALESCE(
                               function_row.proacl,
                               pg_catalog.acldefault(
                                   'f', function_row.proowner
                               )
                           )
                       ) acl
                   ), '[]'::JSONB),
                   'definition', pg_catalog.pg_get_functiondef(
                       function_row.oid
                   )
               ) AS entry
        FROM pg_catalog.pg_proc function_row
        JOIN pg_catalog.pg_namespace function_namespace
          ON function_namespace.oid = function_row.pronamespace
        JOIN pg_catalog.pg_language language_row
          ON language_row.oid = function_row.prolang
        WHERE function_namespace.nspname = 'public'
          AND function_row.proname = ANY(ARRAY[
              'assert_research_max_pain_snapshot_complete',
              'prevent_research_max_pain_archive_mutation',
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
              'prevent_research_signal_snapshot_v1_truncate',
              'assert_market_movement_writer_v5',
              'prevent_market_movement_archive_mutation',
              'assert_neutral_price_attempt_anchor_complete',
              'validate_market_movement_transition_insert',
              'assert_market_movement_receipt_complete'
          ]::NAME[])
    ) ordered_functions;

    SELECT COALESCE(JSONB_AGG(entry ORDER BY view_name), '[]'::JSONB)
    INTO views_payload
    FROM (
        SELECT view_row.relname::TEXT AS view_name,
               JSONB_BUILD_OBJECT(
                   'schema', view_namespace.nspname,
                   'name', view_row.relname,
                   'owner', pg_catalog.pg_get_userbyid(view_row.relowner),
                   'kind', view_row.relkind,
                   'options', COALESCE((
                       SELECT JSONB_AGG(option_value ORDER BY option_value)
                       FROM UNNEST(view_row.reloptions) option_value
                   ), '[]'::JSONB),
                   'columns', COALESCE((
                       SELECT JSONB_AGG(
                           JSONB_BUILD_OBJECT(
                               'name', attribute.attname,
                               'type', pg_catalog.format_type(
                                   attribute.atttypid, attribute.atttypmod
                               )
                           ) ORDER BY attribute.attnum
                       )
                       FROM pg_catalog.pg_attribute attribute
                       WHERE attribute.attrelid = view_row.oid
                         AND attribute.attnum > 0
                         AND NOT attribute.attisdropped
                   ), '[]'::JSONB),
                   'acl', COALESCE((
                       SELECT JSONB_AGG(
                           JSONB_BUILD_OBJECT(
                               'grantee', CASE WHEN acl.grantee = 0 THEN 'PUBLIC'
                                               ELSE pg_catalog.pg_get_userbyid(
                                                   acl.grantee
                                               ) END,
                               'privilege', acl.privilege_type,
                               'grantable', acl.is_grantable
                           ) ORDER BY
                               CASE WHEN acl.grantee = 0 THEN 'PUBLIC'
                                    ELSE pg_catalog.pg_get_userbyid(acl.grantee)
                               END,
                               acl.privilege_type
                       )
                       FROM pg_catalog.aclexplode(
                           COALESCE(
                               view_row.relacl,
                               pg_catalog.acldefault('r', view_row.relowner)
                           )
                       ) acl
                   ), '[]'::JSONB),
                   'definition', pg_catalog.pg_get_viewdef(
                       view_row.oid, FALSE
                   ),
                   'dependencies', COALESCE((
                       SELECT JSONB_AGG(
                           DISTINCT dependency_namespace.nspname || '.' ||
                                    dependency_relation.relname
                           ORDER BY dependency_namespace.nspname || '.' ||
                                    dependency_relation.relname
                       )
                       FROM pg_catalog.pg_rewrite rule_row
                       JOIN pg_catalog.pg_depend dependency_row
                         ON dependency_row.classid =
                            'pg_catalog.pg_rewrite'::REGCLASS
                        AND dependency_row.objid = rule_row.oid
                       JOIN pg_catalog.pg_class dependency_relation
                         ON dependency_relation.oid = dependency_row.refobjid
                       JOIN pg_catalog.pg_namespace dependency_namespace
                         ON dependency_namespace.oid =
                            dependency_relation.relnamespace
                       WHERE rule_row.ev_class = view_row.oid
                         AND rule_row.rulename = '_RETURN'
                         AND dependency_relation.oid <> view_row.oid
                         AND dependency_relation.relkind IN ('r', 'p')
                   ), '[]'::JSONB)
               ) AS entry
        FROM pg_catalog.pg_class view_row
        JOIN pg_catalog.pg_namespace view_namespace
          ON view_namespace.oid = view_row.relnamespace
        WHERE view_namespace.nspname = 'public'
          AND view_row.relname IN (
              'research_formula_exploration_stage4_v1',
              'research_formula_exploration_wave_v5_v1'
          )
    ) ordered_views;

    catalog_payload := JSONB_BUILD_OBJECT(
        'contract_version', 'stage4-wave-v5-authoritative-source-v1',
        'deparser_gucs', deparser_gucs_payload,
        'roles', roles_payload,
        'schema', schema_payload,
        'relations', relations_payload,
        'sequences', sequences_payload,
        'constraints', constraints_payload,
        'indexes', indexes_payload,
        'triggers', triggers_payload,
        'functions', functions_payload,
        'views', views_payload
    );
    source_catalog_sha256 := ENCODE(
        SHA256(CONVERT_TO(catalog_payload::TEXT, 'UTF8')),
        'hex'
    );
    IF source_catalog_sha256 !~ '^[0-9a-f]{64}$'
       OR JSONB_TYPEOF(roles_payload) <> 'object'
       OR JSONB_ARRAY_LENGTH(roles_payload -> 'authority_roots') <> 2
       OR JSONB_ARRAY_LENGTH(roles_payload -> 'required_roles') <> 5
       OR JSONB_ARRAY_LENGTH(roles_payload -> 'nodes') < 5
       OR JSONB_TYPEOF(roles_payload -> 'membership_edges') <> 'array'
       OR JSONB_ARRAY_LENGTH(relations_payload) <> 8
       OR JSONB_ARRAY_LENGTH(sequences_payload) <> 3
       OR JSONB_ARRAY_LENGTH(functions_payload) <> 28
       OR JSONB_ARRAY_LENGTH(views_payload) <> 2 THEN
        RAISE EXCEPTION
            'Formula exploration source catalog receipt is incomplete';
    END IF;

    SELECT ENCODE(SHA256(CONVERT_TO(
        pg_catalog.pg_get_viewdef(view_row.oid, FALSE), 'UTF8'
    )), 'hex')
    INTO stage4_view_definition_sha256
    FROM pg_catalog.pg_class view_row
    WHERE view_row.oid =
          'public.research_formula_exploration_stage4_v1'::REGCLASS;
    EXECUTE pg_catalog.format(
        'COMMENT ON VIEW public.%I IS %L',
        'research_formula_exploration_stage4_v1',
        'stage4-wave-v5-authoritative-source-v1; read-only Stage-4 cohort '
        || 'source; no Formula, outcome, delivery, LIVE or trading authority; '
        || 'view_definition_sha256=' || stage4_view_definition_sha256
        || '; source_catalog_sha256=' || source_catalog_sha256
    );
    PERFORM pg_catalog.set_config(
        'research.formula_exploration_source_catalog_sha256',
        source_catalog_sha256,
        TRUE
    );
END;
$source_catalog_receipt$;

SET LOCAL ROLE research_market_movement_owner;
DO $wave_source_catalog_comment$
DECLARE
    source_catalog_sha256 TEXT := pg_catalog.current_setting(
        'research.formula_exploration_source_catalog_sha256'
    );
    view_definition_sha256 TEXT;
BEGIN
    SELECT ENCODE(SHA256(CONVERT_TO(
        pg_catalog.pg_get_viewdef(view_row.oid, FALSE), 'UTF8'
    )), 'hex')
    INTO view_definition_sha256
    FROM pg_catalog.pg_class view_row
    WHERE view_row.oid =
          'public.research_formula_exploration_wave_v5_v1'::REGCLASS;
    IF source_catalog_sha256 !~ '^[0-9a-f]{64}$'
       OR view_definition_sha256 !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION 'Wave source catalog comment receipt is incomplete';
    END IF;
    EXECUTE pg_catalog.format(
        'COMMENT ON VIEW public.%I IS %L',
        'research_formula_exploration_wave_v5_v1',
        'stage4-wave-v5-authoritative-source-v1; read-only Wave-v5 grouping '
        || 'source; no Formula, outcome, delivery, LIVE or trading authority; '
        || 'view_definition_sha256=' || view_definition_sha256
        || '; source_catalog_sha256=' || source_catalog_sha256
    );
END;
$wave_source_catalog_comment$;
RESET ROLE;

RESET search_path;
RESET TIME ZONE;
RESET DateStyle;
RESET IntervalStyle;
RESET extra_float_digits;
RESET quote_all_identifiers;
