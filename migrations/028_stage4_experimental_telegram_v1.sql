-- Stage-4 experimental Telegram persistence and outbox v1
--
-- This is a separate, explicitly experimental delivery authority.  It never
-- writes the Formula registry, reuses LIVE subscriptions, satisfies a LIVE
-- approval, or grants trading authority.  Search runs, alert occurrences and
-- delivery-attempt events are immutable.  Subscriptions are explicit opt-ins;
-- this migration deliberately performs no backfill from legacy/LIVE chats.
-- Roles and passwords are provisioned out of band.

SET LOCAL search_path = pg_catalog;
SET LOCAL TIME ZONE 'UTC';
SET LOCAL DateStyle = 'ISO, YMD';
SET LOCAL IntervalStyle = 'postgres';
SET LOCAL extra_float_digits = 3;
SET LOCAL quote_all_identifiers = off;
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '30s';

DO $preflight$
DECLARE
    dispatcher RECORD;
    trusted_owner OID;
    relation_name TEXT;
    relation_oid OID;
BEGIN
    IF pg_catalog.current_setting('server_version_num')::INTEGER < 150000 THEN
        RAISE EXCEPTION
            'Stage-4 experimental Telegram requires PostgreSQL 15 or newer';
    END IF;

    SELECT * INTO dispatcher
      FROM pg_catalog.pg_roles
     WHERE rolname = 'research_formula_experimental_dispatcher_v1';
    IF NOT FOUND
       OR NOT dispatcher.rolcanlogin
       OR dispatcher.rolinherit
       OR dispatcher.rolsuper
       OR dispatcher.rolcreatedb
       OR dispatcher.rolcreaterole
       OR dispatcher.rolreplication
       OR dispatcher.rolbypassrls THEN
        RAISE EXCEPTION
            'research_formula_experimental_dispatcher_v1 must be an unprivileged NOINHERIT LOGIN role';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM pg_catalog.pg_auth_members membership
         WHERE membership.member = dispatcher.oid
            OR membership.roleid = dispatcher.oid
            OR membership.grantor = dispatcher.oid
    ) THEN
        RAISE EXCEPTION
            'Experimental dispatcher cannot participate in role membership';
    END IF;
    IF pg_catalog.has_database_privilege(
           'research_formula_experimental_dispatcher_v1',
           pg_catalog.current_database(), 'CREATE'
       ) OR pg_catalog.has_schema_privilege(
           'research_formula_experimental_dispatcher_v1', 'public', 'CREATE'
       ) OR EXISTS (
           SELECT 1 FROM pg_catalog.pg_database database_row
            WHERE database_row.datname = pg_catalog.current_database()
              AND database_row.datdba = dispatcher.oid
       ) OR EXISTS (
           SELECT 1 FROM pg_catalog.pg_namespace namespace_row
            WHERE namespace_row.nspname = 'public'
              AND namespace_row.nspowner = dispatcher.oid
       ) THEN
        RAISE EXCEPTION
            'Experimental dispatcher cannot create or own database/schema objects';
    END IF;

    FOREACH relation_name IN ARRAY ARRAY[
        'research_events',
        'research_formulas',
        'research_formula_live_approvals',
        'research_formula_live_deliveries',
        'research_formula_alert_subscriptions',
        'research_formula_exploration_stage4_v1'
    ] LOOP
        relation_oid := pg_catalog.to_regclass('public.' || relation_name);
        IF relation_oid IS NULL THEN
            RAISE EXCEPTION 'Required relation public.% is missing', relation_name;
        END IF;
    END LOOP;

    SELECT relation_row.relowner
      INTO trusted_owner
      FROM pg_catalog.pg_class relation_row
     WHERE relation_row.oid = 'public.research_events'::REGCLASS;
    IF trusted_owner IS NULL
       OR pg_catalog.pg_get_userbyid(trusted_owner) <> SESSION_USER
       OR trusted_owner = dispatcher.oid
       OR EXISTS (
           SELECT 1
             FROM pg_catalog.pg_class relation_row
            WHERE relation_row.oid IN (
                'public.research_formulas'::REGCLASS,
                'public.research_formula_live_approvals'::REGCLASS,
                'public.research_formula_live_deliveries'::REGCLASS,
                'public.research_formula_alert_subscriptions'::REGCLASS,
                'public.research_formula_exploration_stage4_v1'::REGCLASS
            )
              AND relation_row.relowner <> trusted_owner
       ) THEN
        RAISE EXCEPTION
            'Experimental migration requires one trusted source/session owner';
    END IF;

    -- Fail before mutation if this role has ever been mixed into the existing
    -- Formula, LIVE, subscription, source or trading-adjacent boundary.
    IF EXISTS (
        SELECT 1
          FROM (VALUES
              ('public.research_events'),
              ('public.research_formulas'),
              ('public.research_formula_live_approvals'),
              ('public.research_formula_live_deliveries'),
              ('public.research_formula_alert_subscriptions'),
              ('public.research_formula_exploration_stage4_v1')
          ) protected(relation_name)
          CROSS JOIN (VALUES
              ('SELECT'), ('INSERT'), ('UPDATE'), ('DELETE'), ('TRUNCATE'),
              ('REFERENCES'), ('TRIGGER')
          ) privilege(privilege_name)
         WHERE pg_catalog.has_table_privilege(
             'research_formula_experimental_dispatcher_v1',
             protected.relation_name,
             privilege.privilege_name
         )
    ) THEN
        RAISE EXCEPTION
            'Experimental dispatcher already has forbidden existing authority';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM (VALUES
              ('public.research_events'),
              ('public.research_formulas'),
              ('public.research_formula_live_approvals'),
              ('public.research_formula_live_deliveries'),
              ('public.research_formula_alert_subscriptions'),
              ('public.research_formula_exploration_stage4_v1')
          ) protected(relation_name)
          CROSS JOIN (VALUES
              ('SELECT'), ('INSERT'), ('UPDATE'), ('REFERENCES')
          ) privilege(privilege_name)
         WHERE pg_catalog.has_any_column_privilege(
             'research_formula_experimental_dispatcher_v1',
             protected.relation_name,
             privilege.privilege_name
         )
    ) THEN
        RAISE EXCEPTION
            'Experimental dispatcher already has forbidden column authority';
    END IF;

    FOREACH relation_name IN ARRAY ARRAY[
        'research_formula_experimental_search_runs_v1',
        'research_formula_experimental_alerts_v1',
        'research_formula_experimental_subscriptions_v1',
        'research_formula_experimental_deliveries_v1',
        'research_formula_experimental_delivery_attempt_events_v1'
    ] LOOP
        relation_oid := pg_catalog.to_regclass('public.' || relation_name);
        IF relation_oid IS NOT NULL AND NOT EXISTS (
            SELECT 1
              FROM pg_catalog.pg_class relation_row
             WHERE relation_row.oid = relation_oid
               AND relation_row.relkind = 'r'
               AND relation_row.relpersistence = 'p'
               AND relation_row.relowner = trusted_owner
        ) THEN
            RAISE EXCEPTION
                'Existing experimental relation public.% has an unsafe kind or owner',
                relation_name;
        END IF;
    END LOOP;
END;
$preflight$;

LOCK TABLE public.research_events IN ACCESS SHARE MODE;
LOCK TABLE public.research_formulas IN ACCESS SHARE MODE;
LOCK TABLE public.research_formula_live_approvals IN ACCESS SHARE MODE;
LOCK TABLE public.research_formula_live_deliveries IN ACCESS SHARE MODE;
LOCK TABLE public.research_formula_alert_subscriptions IN ACCESS SHARE MODE;
LOCK TABLE public.research_formula_exploration_stage4_v1 IN ACCESS SHARE MODE;

SET LOCAL search_path = public;

CREATE TABLE IF NOT EXISTS public.research_formula_experimental_search_runs_v1 (
    search_run_id CHAR(64) NOT NULL,
    search_receipt_sha256 CHAR(64) NOT NULL,
    source_corpus_receipt_sha256 CHAR(64) NOT NULL,
    input_observation_chain_sha256 CHAR(64) NOT NULL,
    engine_version TEXT NOT NULL,
    candidate_schema_version TEXT NOT NULL,
    feature_schema_version TEXT NOT NULL,
    label_policy_version TEXT NOT NULL,
    independence_policy_version TEXT NOT NULL,
    multiple_testing_policy_version TEXT NOT NULL,
    schedule_slot_utc TIMESTAMPTZ NOT NULL,
    analysis_as_of_utc TIMESTAMPTZ NOT NULL,
    horizon_minutes INTEGER NOT NULL,
    input_observation_count INTEGER NOT NULL,
    eligible_candidate_count INTEGER NOT NULL,
    search_status TEXT NOT NULL,
    search_payload JSONB NOT NULL,
    search_payload_sha256 CHAR(64) NOT NULL,
    formula_registry_effect TEXT NOT NULL DEFAULT 'NONE',
    delivery_channel TEXT NOT NULL DEFAULT 'NONE',
    live_eligible BOOLEAN NOT NULL DEFAULT FALSE,
    telegram_delivery_allowed BOOLEAN NOT NULL DEFAULT FALSE,
    trade_execution_allowed BOOLEAN NOT NULL DEFAULT FALSE,
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT research_stage4_experimental_search_run_pk PRIMARY KEY (
        search_run_id
    ),
    CONSTRAINT research_stage4_experimental_search_receipt_uk UNIQUE (
        search_receipt_sha256
    ),
    CONSTRAINT research_stage4_experimental_search_schedule_slot_uk UNIQUE (
        horizon_minutes, schedule_slot_utc
    ),
    CONSTRAINT research_stage4_experimental_search_identity_uk UNIQUE (
        search_run_id, search_receipt_sha256, horizon_minutes
    ),
    CONSTRAINT research_stage4_experimental_search_run_id_ck CHECK (
        BTRIM(search_run_id) ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT research_stage4_experimental_search_receipt_ck CHECK (
        BTRIM(search_receipt_sha256) ~ '^[0-9a-f]{64}$'
        AND BTRIM(source_corpus_receipt_sha256) ~ '^[0-9a-f]{64}$'
        AND BTRIM(input_observation_chain_sha256) ~ '^[0-9a-f]{64}$'
        AND BTRIM(search_payload_sha256) ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT research_stage4_experimental_search_versions_ck CHECK (
        BTRIM(engine_version) <> ''
        AND BTRIM(candidate_schema_version) <> ''
        AND BTRIM(feature_schema_version) <> ''
        AND BTRIM(label_policy_version) <> ''
        AND BTRIM(independence_policy_version) <> ''
        AND BTRIM(multiple_testing_policy_version) <> ''
    ),
    CONSTRAINT research_stage4_experimental_search_horizon_ck CHECK (
        horizon_minutes IN (60, 240, 720, 1440)
    ),
    CONSTRAINT research_stage4_experimental_search_counts_ck CHECK (
        input_observation_count BETWEEN 0 AND 131072
        AND eligible_candidate_count BETWEEN 0 AND 4096
    ),
    CONSTRAINT research_stage4_experimental_search_status_ck CHECK (
        (
            search_status = 'EMPTY_CORPUS'
            AND input_observation_count = 0
            AND eligible_candidate_count = 0
        ) OR (
            search_status = 'NO_ELIGIBLE_EXPERIMENTAL_CANDIDATES'
            AND input_observation_count > 0
            AND eligible_candidate_count = 0
        ) OR (
            search_status = 'ELIGIBLE_EXPERIMENTAL_CANDIDATES_FOUND'
            AND input_observation_count > 0
            AND eligible_candidate_count > 0
        )
    ),
    CONSTRAINT research_stage4_experimental_search_time_ck CHECK (
        analysis_as_of_utc >= schedule_slot_utc
    ),
    CONSTRAINT research_stage4_experimental_search_payload_ck CHECK (
        JSONB_TYPEOF(search_payload) = 'object'
        AND search_payload ->> 'search_receipt_sha256' IS NOT DISTINCT FROM
            search_receipt_sha256
        AND search_payload ->> 'engine_version' IS NOT DISTINCT FROM
            engine_version
        AND search_payload ->> 'candidate_schema_version' IS NOT DISTINCT FROM
            candidate_schema_version
        AND search_payload ->> 'feature_schema_version' IS NOT DISTINCT FROM
            feature_schema_version
        AND search_payload ->> 'label_policy_version' IS NOT DISTINCT FROM
            label_policy_version
        AND search_payload ->> 'independence_policy_version'
            IS NOT DISTINCT FROM independence_policy_version
        AND search_payload ->> 'multiple_testing_policy_version'
            IS NOT DISTINCT FROM multiple_testing_policy_version
        AND (search_payload ->> 'horizon_minutes')::INTEGER
            IS NOT DISTINCT FROM horizon_minutes
        AND (search_payload ->> 'input_observation_count')::INTEGER
            IS NOT DISTINCT FROM input_observation_count
        AND search_payload ->> 'input_observation_chain_sha256'
            IS NOT DISTINCT FROM input_observation_chain_sha256
        AND (search_payload #>> '{counts,observations}')::INTEGER
            IS NOT DISTINCT FROM input_observation_count
        AND (search_payload #>> '{counts,eligible_candidate_variants}')::INTEGER
            IS NOT DISTINCT FROM eligible_candidate_count
        AND search_payload ->> 'status' IS NOT DISTINCT FROM search_status
        AND search_payload -> 'ready_for_candidate_search'
            IS NOT DISTINCT FROM 'true'::JSONB
        AND search_payload ->> 'formula_registry_effect'
            IS NOT DISTINCT FROM 'NONE'
        AND search_payload ->> 'delivery_channel' IS NOT DISTINCT FROM 'NONE'
        AND search_payload -> 'live_eligible'
            IS NOT DISTINCT FROM 'false'::JSONB
        AND search_payload -> 'telegram_delivery_allowed'
            IS NOT DISTINCT FROM 'false'::JSONB
        AND search_payload -> 'trade_execution_allowed'
            IS NOT DISTINCT FROM 'false'::JSONB
    ),
    CONSTRAINT research_stage4_experimental_search_authority_ck CHECK (
        formula_registry_effect = 'NONE'
        AND delivery_channel = 'NONE'
        AND live_eligible = FALSE
        AND telegram_delivery_allowed = FALSE
        AND trade_execution_allowed = FALSE
    )
);

CREATE INDEX IF NOT EXISTS idx_stage4_experimental_search_time_v1
    ON public.research_formula_experimental_search_runs_v1 (
        horizon_minutes, analysis_as_of_utc DESC, search_run_id
    );

CREATE TABLE IF NOT EXISTS
    public.research_formula_experimental_alerts_v1 (
        alert_occurrence_id CHAR(64) NOT NULL,
        search_run_id CHAR(64) NOT NULL,
        candidate_key CHAR(64) NOT NULL,
        search_receipt_sha256 CHAR(64) NOT NULL,
        candidate_snapshot JSONB NOT NULL,
        trigger_key CHAR(64) NOT NULL,
        trigger_observation_id CHAR(64) NOT NULL,
        projection_event_id BIGINT NOT NULL,
        projection_event_fingerprint CHAR(64) NOT NULL,
        btc_parent_movement_id CHAR(64) NOT NULL,
        symbol TEXT NOT NULL,
        direction TEXT NOT NULL,
        horizon_minutes INTEGER NOT NULL,
        decision_time_utc TIMESTAMPTZ NOT NULL,
        expires_at_utc TIMESTAMPTZ NOT NULL,
        trigger_snapshot JSONB NOT NULL,
        trigger_snapshot_sha256 CHAR(64) NOT NULL,
        current_trigger_receipt_sha256 CHAR(64) NOT NULL,
        current_trigger_policy_version TEXT NOT NULL,
        formula_text TEXT NOT NULL,
        conditions JSONB NOT NULL,
        independent_movement_count INTEGER NOT NULL,
        accepted_paths JSONB NOT NULL,
        metrics JSONB NOT NULL,
        experimental_reasons JSONB NOT NULL,
        renderer_version TEXT NOT NULL,
        rendered_message TEXT NOT NULL,
        rendered_message_sha256 CHAR(64) NOT NULL,
        disclaimer TEXT NOT NULL,
        delivery_channel TEXT NOT NULL,
        formula_registry_effect TEXT NOT NULL DEFAULT 'NONE',
        human_formula_approval_required BOOLEAN NOT NULL DEFAULT FALSE,
        live_eligible BOOLEAN NOT NULL DEFAULT FALSE,
        trade_execution_allowed BOOLEAN NOT NULL DEFAULT FALSE,
        telegram_delivery_allowed BOOLEAN NOT NULL DEFAULT TRUE,
        created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT research_stage4_experimental_alert_pk PRIMARY KEY (
            alert_occurrence_id
        ),
        CONSTRAINT research_stage4_experimental_alert_search_fk
            FOREIGN KEY (
                search_run_id, search_receipt_sha256, horizon_minutes
            ) REFERENCES
                public.research_formula_experimental_search_runs_v1(
                    search_run_id, search_receipt_sha256, horizon_minutes
                ) ON DELETE RESTRICT,
        CONSTRAINT research_stage4_experimental_alert_projection_fk
            FOREIGN KEY (projection_event_id) REFERENCES
                public.research_events(event_id) ON DELETE RESTRICT,
        CONSTRAINT research_stage4_experimental_alert_id_ck CHECK (
            BTRIM(alert_occurrence_id) ~ '^[0-9a-f]{64}$'
            AND BTRIM(candidate_key) ~ '^[0-9a-f]{64}$'
            AND BTRIM(trigger_key) ~ '^[0-9a-f]{64}$'
            AND BTRIM(trigger_observation_id) ~ '^[0-9a-f]{64}$'
            AND BTRIM(projection_event_fingerprint) ~ '^[0-9a-f]{64}$'
            AND BTRIM(btc_parent_movement_id) ~ '^[0-9a-f]{64}$'
            AND BTRIM(trigger_snapshot_sha256) ~ '^[0-9a-f]{64}$'
            AND BTRIM(current_trigger_receipt_sha256) ~ '^[0-9a-f]{64}$'
            AND BTRIM(rendered_message_sha256) ~ '^[0-9a-f]{64}$'
        ),
        CONSTRAINT research_stage4_experimental_alert_symbol_ck CHECK (
            BTRIM(symbol) ~ '^[A-Z0-9-]{1,20}$'
        ),
        CONSTRAINT research_stage4_experimental_alert_direction_ck CHECK (
            direction IN ('LONG', 'SHORT')
        ),
        CONSTRAINT research_stage4_experimental_alert_horizon_ck CHECK (
            horizon_minutes IN (60, 240, 720, 1440)
        ),
        CONSTRAINT research_stage4_experimental_alert_freshness_ck CHECK (
            expires_at_utc > decision_time_utc
            AND expires_at_utc <= decision_time_utc + INTERVAL '60 minutes'
            AND created_at_utc >= decision_time_utc
            AND created_at_utc < expires_at_utc
        ),
        CONSTRAINT research_stage4_experimental_alert_snapshot_ck CHECK (
            JSONB_TYPEOF(trigger_snapshot) = 'object'
            AND NOT trigger_snapshot ? 'outcome'
            AND NOT trigger_snapshot ? 'path'
            AND trigger_snapshot ->> 'status' IS NOT DISTINCT FROM
                'FROZEN_BOUND_FRESH'
            AND trigger_snapshot ->> 'contract_version' IS NOT DISTINCT FROM
                'stage4-experimental-current-snapshot-no-outcome-v1'
            AND trigger_snapshot ->> 'observation_id' IS NOT DISTINCT FROM
                trigger_observation_id
            AND (trigger_snapshot ->> 'projection_event_id')::BIGINT
                IS NOT DISTINCT FROM projection_event_id
            AND trigger_snapshot ->> 'projection_event_fingerprint'
                IS NOT DISTINCT FROM projection_event_fingerprint
            AND trigger_snapshot ->> 'btc_parent_movement_id'
                IS NOT DISTINCT FROM btc_parent_movement_id
            AND trigger_snapshot ->> 'symbol' IS NOT DISTINCT FROM symbol
            AND trigger_snapshot ->> 'direction' IS NOT DISTINCT FROM direction
            AND (trigger_snapshot ->> 'projection_decision_time_utc')::TIMESTAMPTZ
                IS NOT DISTINCT FROM decision_time_utc
            AND trigger_snapshot ->> 'trigger_snapshot_sha256'
                IS NOT DISTINCT FROM trigger_snapshot_sha256
            AND trigger_snapshot ->> 'current_snapshot_sha256'
                IS NOT DISTINCT FROM trigger_snapshot_sha256
        ),
        CONSTRAINT research_stage4_experimental_alert_candidate_ck CHECK (
            JSONB_TYPEOF(candidate_snapshot) = 'object'
            AND candidate_snapshot ->> 'candidate_key' IS NOT DISTINCT FROM
                candidate_key
            AND candidate_snapshot -> 'experimental_formula_eligible'
                IS NOT DISTINCT FROM 'true'::JSONB
            AND candidate_snapshot ->> 'direction' IS NOT DISTINCT FROM direction
            AND (candidate_snapshot ->> 'horizon_minutes')::INTEGER
                IS NOT DISTINCT FROM horizon_minutes
            AND candidate_snapshot ->> 'formula_text' IS NOT DISTINCT FROM
                formula_text
            AND candidate_snapshot -> 'conditions' IS NOT DISTINCT FROM
                conditions
            AND candidate_snapshot -> 'accepted_paths' IS NOT DISTINCT FROM
                accepted_paths
            AND candidate_snapshot -> 'metrics' IS NOT DISTINCT FROM metrics
            AND (candidate_snapshot #>> '{metrics,sample_size}')::INTEGER
                IS NOT DISTINCT FROM independent_movement_count
            AND candidate_snapshot ->> 'formula_registry_effect'
                IS NOT DISTINCT FROM 'NONE'
            AND candidate_snapshot ->> 'delivery_channel'
                IS NOT DISTINCT FROM 'NONE'
            AND candidate_snapshot -> 'live_eligible'
                IS NOT DISTINCT FROM 'false'::JSONB
            AND candidate_snapshot -> 'telegram_delivery_allowed'
                IS NOT DISTINCT FROM 'false'::JSONB
            AND candidate_snapshot -> 'trade_execution_allowed'
                IS NOT DISTINCT FROM 'false'::JSONB
        ),
        CONSTRAINT research_stage4_experimental_alert_formula_ck CHECK (
            BTRIM(formula_text) <> ''
            AND JSONB_TYPEOF(conditions) = 'array'
            AND JSONB_ARRAY_LENGTH(conditions) BETWEEN 1 AND 3
            AND independent_movement_count >= 5
            AND JSONB_TYPEOF(accepted_paths) = 'array'
            AND JSONB_ARRAY_LENGTH(accepted_paths) BETWEEN 1 AND 2
            AND accepted_paths <@ '["PROBABILITY","ASYMMETRY"]'::JSONB
            AND JSONB_TYPEOF(metrics) = 'object'
            AND JSONB_TYPEOF(experimental_reasons) = 'array'
            AND JSONB_ARRAY_LENGTH(experimental_reasons) > 0
        ),
        CONSTRAINT research_stage4_experimental_alert_message_ck CHECK (
            current_trigger_policy_version =
                'stage4-experimental-current-trigger-v1'
            AND renderer_version = 'stage4-experimental-telegram-renderer-v1'
            AND CHAR_LENGTH(rendered_message) BETWEEN 1 AND 4096
            AND disclaimer = 'ניסיוני, לא מאושר למסחר'
            AND POSITION(disclaimer IN rendered_message) > 0
        ),
        CONSTRAINT research_stage4_experimental_alert_authority_ck CHECK (
            delivery_channel = 'TELEGRAM_EXPERIMENTAL_ONLY'
            AND formula_registry_effect = 'NONE'
            AND human_formula_approval_required = FALSE
            AND live_eligible = FALSE
            AND trade_execution_allowed = FALSE
            AND telegram_delivery_allowed = TRUE
        ),
        CONSTRAINT research_stage4_experimental_alert_candidate_cell_uk UNIQUE (
            candidate_key, trigger_key
        )
    );

CREATE INDEX IF NOT EXISTS idx_stage4_experimental_alert_time_v1
    ON public.research_formula_experimental_alerts_v1 (
        decision_time_utc DESC, alert_occurrence_id
    );

CREATE TABLE IF NOT EXISTS
    public.research_formula_experimental_subscriptions_v1 (
        chat_id BIGINT NOT NULL,
        active BOOLEAN NOT NULL DEFAULT TRUE,
        requested_by_user_id BIGINT NOT NULL,
        subscription_policy_version TEXT NOT NULL,
        consent_source TEXT NOT NULL,
        delivery_scope TEXT NOT NULL,
        disclaimer_acknowledged TEXT NOT NULL,
        disclaimer_acknowledged_at_utc TIMESTAMPTZ NOT NULL,
        subscribed_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT research_stage4_experimental_subscription_pk PRIMARY KEY (
            chat_id
        ),
        CONSTRAINT research_stage4_experimental_subscription_user_ck CHECK (
            requested_by_user_id > 0
        ),
        CONSTRAINT research_stage4_experimental_subscription_policy_ck CHECK (
            subscription_policy_version =
                'stage4-experimental-telegram-subscription-v1'
            AND consent_source = 'EXPLICIT_TELEGRAM_COMMAND'
            AND delivery_scope = 'TELEGRAM_EXPERIMENTAL_ONLY'
            AND disclaimer_acknowledged = 'ניסיוני, לא מאושר למסחר'
        ),
        CONSTRAINT research_stage4_experimental_subscription_time_ck CHECK (
            disclaimer_acknowledged_at_utc <= subscribed_at_utc
            AND subscribed_at_utc <= updated_at_utc
        )
    );

CREATE INDEX IF NOT EXISTS idx_stage4_experimental_subscription_active_v1
    ON public.research_formula_experimental_subscriptions_v1 (
        active, updated_at_utc DESC, chat_id
    );

CREATE TABLE IF NOT EXISTS public.research_formula_experimental_deliveries_v1 (
    delivery_key CHAR(64) NOT NULL,
    alert_occurrence_id CHAR(64) NOT NULL,
    chat_id BIGINT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    available_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    claim_token CHAR(64),
    claimed_at_utc TIMESTAMPTZ,
    claim_expires_at_utc TIMESTAMPTZ,
    sent_at_utc TIMESTAMPTZ,
    telegram_message_id BIGINT,
    last_failure_kind TEXT,
    last_error TEXT,
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT research_stage4_experimental_delivery_pk PRIMARY KEY (
        delivery_key
    ),
    CONSTRAINT research_stage4_experimental_delivery_alert_fk FOREIGN KEY (
        alert_occurrence_id
    ) REFERENCES public.research_formula_experimental_alerts_v1(
        alert_occurrence_id
    ) ON DELETE RESTRICT,
    CONSTRAINT research_stage4_experimental_delivery_subscription_fk FOREIGN KEY (
        chat_id
    ) REFERENCES public.research_formula_experimental_subscriptions_v1(chat_id)
        ON DELETE RESTRICT,
    CONSTRAINT research_stage4_experimental_delivery_key_ck CHECK (
        BTRIM(delivery_key) ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT research_stage4_experimental_delivery_status_ck CHECK (
        status IN (
            'PENDING', 'IN_FLIGHT', 'RETRYABLE', 'SENT',
            'FAILED_FINAL', 'AMBIGUOUS', 'EXPIRED'
        )
    ),
    CONSTRAINT research_stage4_experimental_delivery_attempt_ck CHECK (
        attempt_count BETWEEN 0 AND 10
        AND (claim_token IS NULL) = (claimed_at_utc IS NULL)
        AND (claim_token IS NULL) = (claim_expires_at_utc IS NULL)
        AND (
            claim_token IS NULL
            OR (
                BTRIM(claim_token) ~ '^[0-9a-f]{64}$'
                AND claim_expires_at_utc > claimed_at_utc
                AND claim_expires_at_utc <=
                    claimed_at_utc + INTERVAL '10 minutes'
            )
        )
        AND (attempt_count = 0) = (claim_token IS NULL)
    ),
    CONSTRAINT research_stage4_experimental_delivery_state_ck CHECK (
        (
            status = 'PENDING'
            AND attempt_count = 0
            AND sent_at_utc IS NULL
            AND telegram_message_id IS NULL
            AND last_failure_kind IS NULL
            AND last_error IS NULL
        ) OR (
            status = 'IN_FLIGHT'
            AND attempt_count > 0
            AND claim_token IS NOT NULL
            AND sent_at_utc IS NULL
            AND telegram_message_id IS NULL
            AND last_failure_kind IS NULL
            AND last_error IS NULL
        ) OR (
            status = 'RETRYABLE'
            AND attempt_count > 0
            AND claim_token IS NOT NULL
            AND sent_at_utc IS NULL
            AND telegram_message_id IS NULL
            AND last_failure_kind = 'DEFINITE_NOT_SENT'
            AND BTRIM(COALESCE(last_error, '')) <> ''
        ) OR (
            status = 'SENT'
            AND attempt_count > 0
            AND claim_token IS NOT NULL
            AND sent_at_utc IS NOT NULL
            AND telegram_message_id IS NOT NULL
            AND last_failure_kind IS NULL
            AND last_error IS NULL
        ) OR (
            status = 'FAILED_FINAL'
            AND attempt_count > 0
            AND claim_token IS NOT NULL
            AND sent_at_utc IS NULL
            AND telegram_message_id IS NULL
            AND last_failure_kind IN (
                'DEFINITE_NOT_SENT', 'ATTEMPTS_EXHAUSTED'
            )
            AND BTRIM(COALESCE(last_error, '')) <> ''
        ) OR (
            status = 'AMBIGUOUS'
            AND attempt_count > 0
            AND claim_token IS NOT NULL
            AND sent_at_utc IS NULL
            AND telegram_message_id IS NULL
            AND last_failure_kind = 'AMBIGUOUS_SEND'
            AND BTRIM(COALESCE(last_error, '')) <> ''
        ) OR (
            status = 'EXPIRED'
            AND sent_at_utc IS NULL
            AND telegram_message_id IS NULL
            AND last_failure_kind = 'EXPIRED_BEFORE_SEND'
            AND BTRIM(COALESCE(last_error, '')) <> ''
        )
    ),
    CONSTRAINT research_stage4_experimental_delivery_time_ck CHECK (
        updated_at_utc >= created_at_utc
        AND (sent_at_utc IS NULL OR sent_at_utc >= claimed_at_utc)
    ),
    CONSTRAINT research_stage4_experimental_delivery_occurrence_chat_uk UNIQUE (
        alert_occurrence_id, chat_id
    )
);

CREATE INDEX IF NOT EXISTS idx_stage4_experimental_delivery_due_v1
    ON public.research_formula_experimental_deliveries_v1 (
        available_at_utc, created_at_utc, delivery_key
    ) WHERE status IN ('PENDING', 'RETRYABLE');

CREATE INDEX IF NOT EXISTS idx_stage4_experimental_delivery_inflight_v1
    ON public.research_formula_experimental_deliveries_v1 (
        claim_expires_at_utc, delivery_key
    ) WHERE status = 'IN_FLIGHT';

CREATE TABLE IF NOT EXISTS
    public.research_formula_experimental_delivery_attempt_events_v1 (
        attempt_event_key CHAR(64) NOT NULL,
        delivery_key CHAR(64) NOT NULL,
        attempt_number INTEGER NOT NULL,
        event_phase TEXT NOT NULL,
        terminal_result TEXT,
        claim_token CHAR(64) NOT NULL,
        event_time_utc TIMESTAMPTZ NOT NULL,
        telegram_message_id BIGINT,
        error_text TEXT,
        event_payload JSONB NOT NULL,
        created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT research_stage4_experimental_attempt_event_pk PRIMARY KEY (
            attempt_event_key
        ),
        CONSTRAINT research_stage4_experimental_attempt_delivery_fk FOREIGN KEY (
            delivery_key
        ) REFERENCES public.research_formula_experimental_deliveries_v1(
            delivery_key
        ) ON DELETE RESTRICT,
        CONSTRAINT research_stage4_experimental_attempt_event_key_ck CHECK (
            BTRIM(attempt_event_key) ~ '^[0-9a-f]{64}$'
            AND BTRIM(claim_token) ~ '^[0-9a-f]{64}$'
        ),
        CONSTRAINT research_stage4_experimental_attempt_number_ck CHECK (
            attempt_number BETWEEN 1 AND 10
        ),
        CONSTRAINT research_stage4_experimental_attempt_result_ck CHECK (
            (
                event_phase = 'CLAIMED'
                AND terminal_result IS NULL
                AND telegram_message_id IS NULL
                AND error_text IS NULL
            ) OR (
                event_phase = 'TERMINAL'
                AND terminal_result = 'SENT'
                AND telegram_message_id IS NOT NULL
                AND error_text IS NULL
            ) OR (
                event_phase = 'TERMINAL'
                AND terminal_result IN ('DEFINITE_FAILURE', 'AMBIGUOUS')
                AND telegram_message_id IS NULL
                AND BTRIM(COALESCE(error_text, '')) <> ''
            )
        ),
        CONSTRAINT research_stage4_experimental_attempt_time_ck CHECK (
            event_time_utc = created_at_utc
        ),
        CONSTRAINT research_stage4_experimental_attempt_payload_ck CHECK (
            JSONB_TYPEOF(event_payload) = 'object'
            AND event_payload ->> 'attempt_event_key' IS NOT DISTINCT FROM
                attempt_event_key
            AND event_payload ->> 'delivery_key' IS NOT DISTINCT FROM
                delivery_key
            AND (event_payload ->> 'attempt_number')::INTEGER
                IS NOT DISTINCT FROM attempt_number
            AND event_payload ->> 'event_phase' IS NOT DISTINCT FROM event_phase
            AND event_payload ->> 'claim_token' IS NOT DISTINCT FROM claim_token
            AND event_payload ->> 'terminal_result'
                IS NOT DISTINCT FROM terminal_result
            AND (event_payload ->> 'telegram_message_id')::BIGINT
                IS NOT DISTINCT FROM telegram_message_id
            AND event_payload ->> 'error_text' IS NOT DISTINCT FROM error_text
        ),
        CONSTRAINT research_stage4_experimental_attempt_delivery_phase_uk UNIQUE (
            delivery_key, attempt_number, event_phase
        )
    );

CREATE INDEX IF NOT EXISTS idx_stage4_experimental_attempt_delivery_v1
    ON public.research_formula_experimental_delivery_attempt_events_v1 (
        delivery_key, attempt_number, event_time_utc, attempt_event_key
    );

CREATE OR REPLACE FUNCTION
    public.prevent_research_stage4_experimental_immutable_v1()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog, public
AS $function$
BEGIN
    RAISE EXCEPTION 'Stage-4 experimental evidence is append-only';
END;
$function$;

CREATE OR REPLACE FUNCTION
    public.validate_research_stage4_experimental_subscription_v1()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog, public
AS $function$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'Stage-4 experimental subscriptions cannot be deleted';
    END IF;
    IF TG_OP = 'UPDATE' AND (
        NEW.chat_id IS DISTINCT FROM OLD.chat_id
        OR NEW.requested_by_user_id IS DISTINCT FROM OLD.requested_by_user_id
        OR NEW.subscription_policy_version IS DISTINCT FROM
            OLD.subscription_policy_version
        OR NEW.consent_source IS DISTINCT FROM OLD.consent_source
        OR NEW.delivery_scope IS DISTINCT FROM OLD.delivery_scope
        OR NEW.disclaimer_acknowledged IS DISTINCT FROM
            OLD.disclaimer_acknowledged
        OR NEW.disclaimer_acknowledged_at_utc IS DISTINCT FROM
            OLD.disclaimer_acknowledged_at_utc
        OR NEW.subscribed_at_utc IS DISTINCT FROM OLD.subscribed_at_utc
    ) THEN
        RAISE EXCEPTION
            'Only active may change on an experimental subscription';
    END IF;
    NEW.updated_at_utc := pg_catalog.transaction_timestamp();
    RETURN NEW;
END;
$function$;

CREATE OR REPLACE FUNCTION
    public.validate_research_stage4_experimental_delivery_v1()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog, public
AS $function$
DECLARE
    occurrence_expiry TIMESTAMPTZ;
    occurrence_decision_time TIMESTAMPTZ;
    subscription_active BOOLEAN;
    subscription_updated_at TIMESTAMPTZ;
    now_utc TIMESTAMPTZ := pg_catalog.transaction_timestamp();
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'Stage-4 experimental deliveries cannot be deleted';
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF NEW.status IS DISTINCT FROM 'PENDING'
           OR NEW.attempt_count IS DISTINCT FROM 0
           OR NEW.claim_token IS NOT NULL
           OR NEW.claimed_at_utc IS NOT NULL
           OR NEW.claim_expires_at_utc IS NOT NULL
           OR NEW.sent_at_utc IS NOT NULL
           OR NEW.telegram_message_id IS NOT NULL
           OR NEW.last_failure_kind IS NOT NULL
           OR NEW.last_error IS NOT NULL THEN
            RAISE EXCEPTION
                'New experimental delivery must be pristine PENDING';
        END IF;
        SELECT occurrence.expires_at_utc, occurrence.decision_time_utc,
               subscription.active, subscription.updated_at_utc
          INTO occurrence_expiry, occurrence_decision_time,
               subscription_active, subscription_updated_at
          FROM public.research_formula_experimental_alerts_v1 occurrence
         JOIN public.research_formula_experimental_subscriptions_v1 subscription
            ON subscription.chat_id = NEW.chat_id
         WHERE occurrence.alert_occurrence_id = NEW.alert_occurrence_id
         FOR SHARE OF subscription;
        IF NOT FOUND
           OR subscription_active IS NOT TRUE
           OR subscription_updated_at > occurrence_decision_time
           OR occurrence_expiry <= now_utc
           OR NEW.available_at_utc >= occurrence_expiry THEN
            RAISE EXCEPTION
                'New experimental delivery requires a current explicit opt-in';
        END IF;
        NEW.created_at_utc := now_utc;
        NEW.updated_at_utc := now_utc;
        RETURN NEW;
    END IF;

    IF NEW.delivery_key IS DISTINCT FROM OLD.delivery_key
       OR NEW.alert_occurrence_id IS DISTINCT FROM OLD.alert_occurrence_id
       OR NEW.chat_id IS DISTINCT FROM OLD.chat_id
       OR NEW.created_at_utc IS DISTINCT FROM OLD.created_at_utc THEN
        RAISE EXCEPTION 'Experimental delivery identity is immutable';
    END IF;
    IF OLD.status IN ('SENT', 'FAILED_FINAL', 'AMBIGUOUS', 'EXPIRED') THEN
        RAISE EXCEPTION 'Terminal experimental delivery cannot change';
    END IF;

    SELECT occurrence.expires_at_utc, occurrence.decision_time_utc
      INTO occurrence_expiry, occurrence_decision_time
      FROM public.research_formula_experimental_alerts_v1 occurrence
     WHERE occurrence.alert_occurrence_id = OLD.alert_occurrence_id;
    IF occurrence_expiry IS NULL THEN
        RAISE EXCEPTION 'Experimental delivery occurrence is missing';
    END IF;

    IF OLD.status = 'IN_FLIGHT'
       AND OLD.claim_expires_at_utc <= now_utc
       AND NEW.status NOT IN ('SENT', 'AMBIGUOUS') THEN
        RAISE EXCEPTION
            'Stale IN_FLIGHT delivery must become AMBIGUOUS, never retry';
    END IF;

    IF OLD.status IN ('PENDING', 'RETRYABLE') AND NEW.status = 'IN_FLIGHT' THEN
        SELECT subscription.active, subscription.updated_at_utc
          INTO subscription_active, subscription_updated_at
          FROM public.research_formula_experimental_subscriptions_v1 subscription
         WHERE subscription.chat_id = OLD.chat_id
         FOR SHARE;
        IF occurrence_expiry <= now_utc OR OLD.available_at_utc > now_utc THEN
            RAISE EXCEPTION 'Experimental delivery is not currently claimable';
        END IF;
        IF subscription_active IS NOT TRUE
           OR subscription_updated_at > occurrence_decision_time
           OR NEW.attempt_count IS DISTINCT FROM OLD.attempt_count + 1
           OR NEW.available_at_utc IS DISTINCT FROM OLD.available_at_utc
           OR NEW.claim_token IS NULL
           OR NEW.claim_token IS NOT DISTINCT FROM OLD.claim_token
           OR NEW.claimed_at_utc IS NULL
           OR NEW.claim_expires_at_utc IS NULL
           OR NEW.claimed_at_utc IS DISTINCT FROM now_utc
           OR NEW.claim_expires_at_utc <= NEW.claimed_at_utc
           OR NEW.claim_expires_at_utc >
                NEW.claimed_at_utc + INTERVAL '10 minutes'
           OR NEW.sent_at_utc IS NOT NULL
           OR NEW.telegram_message_id IS NOT NULL
           OR NEW.last_failure_kind IS NOT NULL
           OR NEW.last_error IS NOT NULL THEN
            RAISE EXCEPTION 'Experimental delivery claim is invalid';
        END IF;
    ELSIF OLD.status = 'IN_FLIGHT' AND NEW.status = 'SENT' THEN
        IF NEW.attempt_count IS DISTINCT FROM OLD.attempt_count
           OR NEW.available_at_utc IS DISTINCT FROM OLD.available_at_utc
           OR NEW.claim_token IS DISTINCT FROM OLD.claim_token
           OR NEW.claimed_at_utc IS DISTINCT FROM OLD.claimed_at_utc
           OR NEW.claim_expires_at_utc IS DISTINCT FROM OLD.claim_expires_at_utc
           OR NEW.sent_at_utc IS DISTINCT FROM now_utc
           OR NEW.telegram_message_id IS NULL
           OR NEW.last_failure_kind IS NOT NULL
           OR NEW.last_error IS NOT NULL THEN
            RAISE EXCEPTION 'Experimental SENT transition is invalid';
        END IF;
    ELSIF OLD.status = 'IN_FLIGHT' AND NEW.status = 'RETRYABLE' THEN
        IF OLD.claim_expires_at_utc <= now_utc
           OR occurrence_expiry <= now_utc
           OR NEW.attempt_count IS DISTINCT FROM OLD.attempt_count
           OR NEW.claim_token IS DISTINCT FROM OLD.claim_token
           OR NEW.claimed_at_utc IS DISTINCT FROM OLD.claimed_at_utc
           OR NEW.claim_expires_at_utc IS DISTINCT FROM OLD.claim_expires_at_utc
           OR NEW.available_at_utc <= now_utc
           OR NEW.sent_at_utc IS NOT NULL
           OR NEW.telegram_message_id IS NOT NULL
           OR NEW.last_failure_kind IS DISTINCT FROM 'DEFINITE_NOT_SENT'
           OR BTRIM(COALESCE(NEW.last_error, '')) = '' THEN
            RAISE EXCEPTION 'Experimental RETRYABLE transition is invalid';
        END IF;
    ELSIF OLD.status = 'IN_FLIGHT' AND NEW.status = 'FAILED_FINAL' THEN
        IF OLD.claim_expires_at_utc <= now_utc
           OR NEW.attempt_count IS DISTINCT FROM OLD.attempt_count
           OR NEW.available_at_utc IS DISTINCT FROM OLD.available_at_utc
           OR NEW.claim_token IS DISTINCT FROM OLD.claim_token
           OR NEW.claimed_at_utc IS DISTINCT FROM OLD.claimed_at_utc
           OR NEW.claim_expires_at_utc IS DISTINCT FROM OLD.claim_expires_at_utc
           OR NEW.sent_at_utc IS NOT NULL
           OR NEW.telegram_message_id IS NOT NULL
           OR NEW.last_failure_kind NOT IN (
                'DEFINITE_NOT_SENT', 'ATTEMPTS_EXHAUSTED'
           )
           OR BTRIM(COALESCE(NEW.last_error, '')) = '' THEN
            RAISE EXCEPTION 'Experimental FAILED_FINAL transition is invalid';
        END IF;
    ELSIF OLD.status = 'IN_FLIGHT' AND NEW.status = 'AMBIGUOUS' THEN
        IF NEW.attempt_count IS DISTINCT FROM OLD.attempt_count
           OR NEW.available_at_utc IS DISTINCT FROM OLD.available_at_utc
           OR NEW.claim_token IS DISTINCT FROM OLD.claim_token
           OR NEW.claimed_at_utc IS DISTINCT FROM OLD.claimed_at_utc
           OR NEW.claim_expires_at_utc IS DISTINCT FROM OLD.claim_expires_at_utc
           OR NEW.sent_at_utc IS NOT NULL
           OR NEW.telegram_message_id IS NOT NULL
           OR NEW.last_failure_kind IS DISTINCT FROM 'AMBIGUOUS_SEND'
           OR BTRIM(COALESCE(NEW.last_error, '')) = '' THEN
            RAISE EXCEPTION 'Experimental AMBIGUOUS transition is invalid';
        END IF;
    ELSIF OLD.status IN ('PENDING', 'RETRYABLE')
          AND NEW.status = 'EXPIRED' THEN
        IF occurrence_expiry > now_utc
           OR NEW.attempt_count IS DISTINCT FROM OLD.attempt_count
           OR NEW.available_at_utc IS DISTINCT FROM OLD.available_at_utc
           OR NEW.claim_token IS DISTINCT FROM OLD.claim_token
           OR NEW.claimed_at_utc IS DISTINCT FROM OLD.claimed_at_utc
           OR NEW.claim_expires_at_utc IS DISTINCT FROM OLD.claim_expires_at_utc
           OR NEW.sent_at_utc IS NOT NULL
           OR NEW.telegram_message_id IS NOT NULL
           OR NEW.last_failure_kind IS DISTINCT FROM 'EXPIRED_BEFORE_SEND'
           OR BTRIM(COALESCE(NEW.last_error, '')) = '' THEN
            RAISE EXCEPTION 'Experimental EXPIRED transition is invalid';
        END IF;
    ELSE
        RAISE EXCEPTION 'Experimental delivery status transition is forbidden';
    END IF;

    NEW.updated_at_utc := now_utc;
    RETURN NEW;
END;
$function$;

-- Every transition that starts or resolves a Telegram attempt must carry its
-- immutable audit event in the same transaction.  This is deferred so the
-- dispatcher may update the outbox row first and append the event second.
CREATE OR REPLACE FUNCTION
    public.require_research_stage4_experimental_attempt_audit_v1()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog, public
AS $function$
DECLARE
    expected_phase TEXT;
    expected_result TEXT;
BEGIN
    IF NEW.status IS NOT DISTINCT FROM OLD.status THEN
        RETURN NULL;
    END IF;

    IF NEW.status = 'IN_FLIGHT' THEN
        expected_phase := 'CLAIMED';
        expected_result := NULL;
    ELSIF NEW.status = 'SENT' THEN
        expected_phase := 'TERMINAL';
        expected_result := 'SENT';
    ELSIF NEW.status IN ('RETRYABLE', 'FAILED_FINAL') THEN
        expected_phase := 'TERMINAL';
        expected_result := 'DEFINITE_FAILURE';
    ELSIF NEW.status = 'AMBIGUOUS' THEN
        expected_phase := 'TERMINAL';
        expected_result := 'AMBIGUOUS';
    ELSE
        -- PENDING has no attempt.  EXPIRED adds no attempt either: a RETRYABLE
        -- row already has the previous TERMINAL event, while a pristine
        -- PENDING row has attempt_count zero.
        RETURN NULL;
    END IF;

    IF NOT EXISTS (
        SELECT 1
          FROM public.research_formula_experimental_delivery_attempt_events_v1
               event_row
         WHERE event_row.delivery_key = NEW.delivery_key
           AND event_row.attempt_number = NEW.attempt_count
           AND event_row.event_phase = expected_phase
           AND event_row.claim_token = NEW.claim_token
           AND event_row.terminal_result IS NOT DISTINCT FROM expected_result
           AND event_row.event_time_utc = pg_catalog.transaction_timestamp()
           AND event_row.created_at_utc = pg_catalog.transaction_timestamp()
           AND (
               NEW.status <> 'SENT'
               OR event_row.telegram_message_id IS NOT DISTINCT FROM
                    NEW.telegram_message_id
           )
           AND (
               NEW.status NOT IN ('RETRYABLE', 'FAILED_FINAL', 'AMBIGUOUS')
               OR event_row.error_text IS NOT DISTINCT FROM NEW.last_error
           )
    ) THEN
        RAISE EXCEPTION
            'Experimental delivery transition requires same-transaction attempt audit';
    END IF;
    RETURN NULL;
END;
$function$;

CREATE OR REPLACE FUNCTION
    public.validate_research_stage4_experimental_attempt_v1()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog, public
AS $function$
DECLARE
    delivery_status TEXT;
    delivery_attempt_count INTEGER;
    delivery_claim_token CHAR(64);
    delivery_telegram_message_id BIGINT;
    delivery_last_error TEXT;
    now_utc TIMESTAMPTZ := pg_catalog.transaction_timestamp();
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Stage-4 experimental attempt audit is append-only';
    END IF;
    IF NEW.event_time_utc IS DISTINCT FROM now_utc
       OR NEW.created_at_utc IS DISTINCT FROM now_utc THEN
        RAISE EXCEPTION
            'Experimental attempt audit timestamps must bind its transaction';
    END IF;

    SELECT delivery.status, delivery.attempt_count, delivery.claim_token,
           delivery.telegram_message_id, delivery.last_error
      INTO delivery_status, delivery_attempt_count, delivery_claim_token,
           delivery_telegram_message_id, delivery_last_error
      FROM public.research_formula_experimental_deliveries_v1 delivery
     WHERE delivery.delivery_key = NEW.delivery_key
    FOR KEY SHARE;
    IF NOT FOUND
       OR delivery_attempt_count IS DISTINCT FROM NEW.attempt_number
       OR delivery_claim_token IS DISTINCT FROM NEW.claim_token THEN
        RAISE EXCEPTION
            'Experimental attempt audit does not bind the current delivery claim';
    END IF;

    IF NEW.event_phase = 'CLAIMED' THEN
        IF delivery_status <> 'IN_FLIGHT' THEN
            RAISE EXCEPTION
                'Experimental CLAIMED audit requires an IN_FLIGHT delivery';
        END IF;
    ELSIF NEW.terminal_result = 'SENT' THEN
        IF delivery_status <> 'SENT'
           OR delivery_telegram_message_id IS DISTINCT FROM
                NEW.telegram_message_id THEN
            RAISE EXCEPTION
                'Experimental SENT audit does not bind a SENT delivery';
        END IF;
    ELSIF NEW.terminal_result = 'DEFINITE_FAILURE' THEN
        IF delivery_status NOT IN ('RETRYABLE', 'FAILED_FINAL')
           OR delivery_last_error IS DISTINCT FROM NEW.error_text THEN
            RAISE EXCEPTION
                'Experimental failure audit does not bind its delivery state';
        END IF;
    ELSIF NEW.terminal_result = 'AMBIGUOUS' THEN
        IF delivery_status <> 'AMBIGUOUS'
           OR delivery_last_error IS DISTINCT FROM NEW.error_text THEN
            RAISE EXCEPTION
                'Experimental ambiguous audit does not bind its delivery state';
        END IF;
    ELSE
        RAISE EXCEPTION 'Experimental attempt audit phase/result is invalid';
    END IF;
    RETURN NEW;
END;
$function$;

-- Immutable evidence and audit rows.
DROP TRIGGER IF EXISTS trg_stage4_experimental_search_runs_immutable_v1
    ON public.research_formula_experimental_search_runs_v1;
CREATE TRIGGER trg_stage4_experimental_search_runs_immutable_v1
BEFORE UPDATE OR DELETE
ON public.research_formula_experimental_search_runs_v1
FOR EACH ROW EXECUTE FUNCTION
    public.prevent_research_stage4_experimental_immutable_v1();
ALTER TABLE public.research_formula_experimental_search_runs_v1
    ENABLE ALWAYS TRIGGER trg_stage4_experimental_search_runs_immutable_v1;

DROP TRIGGER IF EXISTS trg_stage4_experimental_search_runs_no_truncate_v1
    ON public.research_formula_experimental_search_runs_v1;
CREATE TRIGGER trg_stage4_experimental_search_runs_no_truncate_v1
BEFORE TRUNCATE
ON public.research_formula_experimental_search_runs_v1
FOR EACH STATEMENT EXECUTE FUNCTION
    public.prevent_research_stage4_experimental_immutable_v1();
ALTER TABLE public.research_formula_experimental_search_runs_v1
    ENABLE ALWAYS TRIGGER trg_stage4_experimental_search_runs_no_truncate_v1;

DROP TRIGGER IF EXISTS trg_stage4_experimental_alerts_immutable_v1
    ON public.research_formula_experimental_alerts_v1;
CREATE TRIGGER trg_stage4_experimental_alerts_immutable_v1
BEFORE UPDATE OR DELETE
ON public.research_formula_experimental_alerts_v1
FOR EACH ROW EXECUTE FUNCTION
    public.prevent_research_stage4_experimental_immutable_v1();
ALTER TABLE public.research_formula_experimental_alerts_v1
    ENABLE ALWAYS TRIGGER trg_stage4_experimental_alerts_immutable_v1;

DROP TRIGGER IF EXISTS trg_stage4_experimental_alerts_no_truncate_v1
    ON public.research_formula_experimental_alerts_v1;
CREATE TRIGGER trg_stage4_experimental_alerts_no_truncate_v1
BEFORE TRUNCATE
ON public.research_formula_experimental_alerts_v1
FOR EACH STATEMENT EXECUTE FUNCTION
    public.prevent_research_stage4_experimental_immutable_v1();
ALTER TABLE public.research_formula_experimental_alerts_v1
    ENABLE ALWAYS TRIGGER trg_stage4_experimental_alerts_no_truncate_v1;

DROP TRIGGER IF EXISTS trg_stage4_experimental_subscriptions_validate_v1
    ON public.research_formula_experimental_subscriptions_v1;
CREATE TRIGGER trg_stage4_experimental_subscriptions_validate_v1
BEFORE INSERT OR UPDATE OR DELETE
ON public.research_formula_experimental_subscriptions_v1
FOR EACH ROW EXECUTE FUNCTION
    public.validate_research_stage4_experimental_subscription_v1();
ALTER TABLE public.research_formula_experimental_subscriptions_v1
    ENABLE ALWAYS TRIGGER trg_stage4_experimental_subscriptions_validate_v1;

DROP TRIGGER IF EXISTS trg_stage4_experimental_subscriptions_no_truncate_v1
    ON public.research_formula_experimental_subscriptions_v1;
CREATE TRIGGER trg_stage4_experimental_subscriptions_no_truncate_v1
BEFORE TRUNCATE
ON public.research_formula_experimental_subscriptions_v1
FOR EACH STATEMENT EXECUTE FUNCTION
    public.prevent_research_stage4_experimental_immutable_v1();
ALTER TABLE public.research_formula_experimental_subscriptions_v1
    ENABLE ALWAYS TRIGGER trg_stage4_experimental_subscriptions_no_truncate_v1;

DROP TRIGGER IF EXISTS trg_stage4_experimental_deliveries_validate_v1
    ON public.research_formula_experimental_deliveries_v1;
CREATE TRIGGER trg_stage4_experimental_deliveries_validate_v1
BEFORE INSERT OR UPDATE OR DELETE
ON public.research_formula_experimental_deliveries_v1
FOR EACH ROW EXECUTE FUNCTION
    public.validate_research_stage4_experimental_delivery_v1();
ALTER TABLE public.research_formula_experimental_deliveries_v1
    ENABLE ALWAYS TRIGGER trg_stage4_experimental_deliveries_validate_v1;

DROP TRIGGER IF EXISTS trg_stage4_experimental_delivery_attempt_audit_v1
    ON public.research_formula_experimental_deliveries_v1;
CREATE CONSTRAINT TRIGGER trg_stage4_experimental_delivery_attempt_audit_v1
AFTER UPDATE
ON public.research_formula_experimental_deliveries_v1
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION
    public.require_research_stage4_experimental_attempt_audit_v1();
ALTER TABLE public.research_formula_experimental_deliveries_v1
    ENABLE ALWAYS TRIGGER trg_stage4_experimental_delivery_attempt_audit_v1;

DROP TRIGGER IF EXISTS trg_stage4_experimental_deliveries_no_truncate_v1
    ON public.research_formula_experimental_deliveries_v1;
CREATE TRIGGER trg_stage4_experimental_deliveries_no_truncate_v1
BEFORE TRUNCATE
ON public.research_formula_experimental_deliveries_v1
FOR EACH STATEMENT EXECUTE FUNCTION
    public.prevent_research_stage4_experimental_immutable_v1();
ALTER TABLE public.research_formula_experimental_deliveries_v1
    ENABLE ALWAYS TRIGGER trg_stage4_experimental_deliveries_no_truncate_v1;

DROP TRIGGER IF EXISTS trg_stage4_experimental_attempts_immutable_v1
    ON public.research_formula_experimental_delivery_attempt_events_v1;
CREATE TRIGGER trg_stage4_experimental_attempts_immutable_v1
BEFORE INSERT OR UPDATE OR DELETE
ON public.research_formula_experimental_delivery_attempt_events_v1
FOR EACH ROW EXECUTE FUNCTION
    public.validate_research_stage4_experimental_attempt_v1();
ALTER TABLE public.research_formula_experimental_delivery_attempt_events_v1
    ENABLE ALWAYS TRIGGER trg_stage4_experimental_attempts_immutable_v1;

DROP TRIGGER IF EXISTS trg_stage4_experimental_attempts_no_truncate_v1
    ON public.research_formula_experimental_delivery_attempt_events_v1;
CREATE TRIGGER trg_stage4_experimental_attempts_no_truncate_v1
BEFORE TRUNCATE
ON public.research_formula_experimental_delivery_attempt_events_v1
FOR EACH STATEMENT EXECUTE FUNCTION
    public.prevent_research_stage4_experimental_immutable_v1();
ALTER TABLE public.research_formula_experimental_delivery_attempt_events_v1
    ENABLE ALWAYS TRIGGER trg_stage4_experimental_attempts_no_truncate_v1;

COMMENT ON TABLE public.research_formula_experimental_search_runs_v1 IS
    'contract=stage4-experimental-search-run-v1;immutable=true;registry=none;delivery=none;live=false;trading=false';
COMMENT ON TABLE public.research_formula_experimental_alerts_v1 IS
    'contract=stage4-experimental-alert-occurrence-v1;immutable=true;channel=telegram-experimental-only;live=false;trading=false';
COMMENT ON TABLE public.research_formula_experimental_subscriptions_v1 IS
    'contract=stage4-experimental-subscription-v1;explicit-opt-in=true;live-subscription-backfill=false';
COMMENT ON TABLE public.research_formula_experimental_deliveries_v1 IS
    'contract=stage4-experimental-delivery-outbox-v1;stale-in-flight=ambiguous;automatic-live=false;trading=false';
COMMENT ON TABLE
    public.research_formula_experimental_delivery_attempt_events_v1 IS
    'contract=stage4-experimental-delivery-attempt-event-v1;immutable=true;two-phase=claimed-terminal';

-- Normalize every target ACL before granting the one dedicated dispatcher
-- its exact, non-grantable capabilities.
DO $target_acl_cleanup$
DECLARE
    relation_name TEXT;
    relation_oid OID;
    owner_oid OID;
    grant_row RECORD;
    grantee_sql TEXT;
BEGIN
    FOREACH relation_name IN ARRAY ARRAY[
        'research_formula_experimental_search_runs_v1',
        'research_formula_experimental_alerts_v1',
        'research_formula_experimental_subscriptions_v1',
        'research_formula_experimental_deliveries_v1',
        'research_formula_experimental_delivery_attempt_events_v1'
    ] LOOP
        relation_oid := pg_catalog.to_regclass('public.' || relation_name);
        SELECT relation_row.relowner INTO owner_oid
          FROM pg_catalog.pg_class relation_row
         WHERE relation_row.oid = relation_oid;
        FOR grant_row IN
            SELECT DISTINCT acl.grantee
              FROM pg_catalog.pg_class relation_row
              CROSS JOIN LATERAL pg_catalog.aclexplode(
                  COALESCE(
                      relation_row.relacl,
                      pg_catalog.acldefault('r', relation_row.relowner)
                  )
              ) acl
             WHERE relation_row.oid = relation_oid
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
                relation_name, grantee_sql
            );
        END LOOP;
        FOR grant_row IN
            SELECT attribute.attname, acl.grantee, acl.privilege_type
              FROM pg_catalog.pg_attribute attribute
              CROSS JOIN LATERAL pg_catalog.aclexplode(attribute.attacl) acl
             WHERE attribute.attrelid = relation_oid
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
                grant_row.privilege_type, grant_row.attname,
                relation_name, grantee_sql
            );
        END LOOP;
    END LOOP;
END;
$target_acl_cleanup$;

REVOKE CREATE ON SCHEMA public
    FROM research_formula_experimental_dispatcher_v1;
GRANT USAGE ON SCHEMA public
    TO research_formula_experimental_dispatcher_v1;

GRANT SELECT, INSERT ON TABLE
    public.research_formula_experimental_search_runs_v1,
    public.research_formula_experimental_alerts_v1,
    public.research_formula_experimental_subscriptions_v1,
    public.research_formula_experimental_deliveries_v1,
    public.research_formula_experimental_delivery_attempt_events_v1
    TO research_formula_experimental_dispatcher_v1;

GRANT UPDATE (active) ON TABLE
    public.research_formula_experimental_subscriptions_v1
    TO research_formula_experimental_dispatcher_v1;

GRANT UPDATE (
    status, attempt_count, available_at_utc, claim_token,
    claimed_at_utc, claim_expires_at_utc, sent_at_utc,
    telegram_message_id, last_failure_kind, last_error
) ON TABLE public.research_formula_experimental_deliveries_v1
    TO research_formula_experimental_dispatcher_v1;

REVOKE ALL ON FUNCTION
    public.prevent_research_stage4_experimental_immutable_v1(),
    public.validate_research_stage4_experimental_subscription_v1(),
    public.validate_research_stage4_experimental_delivery_v1(),
    public.require_research_stage4_experimental_attempt_audit_v1(),
    public.validate_research_stage4_experimental_attempt_v1()
    FROM PUBLIC, research_formula_experimental_dispatcher_v1;

SET LOCAL search_path = pg_catalog;

DO $postflight$
DECLARE
    dispatcher_oid OID := (
        SELECT oid FROM pg_catalog.pg_roles
         WHERE rolname = 'research_formula_experimental_dispatcher_v1'
    );
    trusted_owner OID := (
        SELECT relowner FROM pg_catalog.pg_class
         WHERE oid = 'public.research_events'::REGCLASS
    );
    spec RECORD;
    actual_columns TEXT[];
    actual_constraints TEXT[];
    actual_indexes TEXT[];
    actual_triggers TEXT[];
    expected_columns TEXT[];
    expected_constraints TEXT[];
    expected_indexes TEXT[];
    expected_triggers TEXT[];
    expected_comment TEXT;
    relation_oid OID;
BEGIN
    FOR spec IN
        SELECT * FROM (VALUES
            (
                'research_formula_experimental_search_runs_v1',
                ARRAY[
                    'search_run_id','search_receipt_sha256',
                    'source_corpus_receipt_sha256',
                    'input_observation_chain_sha256','engine_version',
                    'candidate_schema_version','feature_schema_version',
                    'label_policy_version','independence_policy_version',
                    'multiple_testing_policy_version','schedule_slot_utc',
                    'analysis_as_of_utc','horizon_minutes',
                    'input_observation_count','eligible_candidate_count',
                    'search_status','search_payload','search_payload_sha256',
                    'formula_registry_effect',
                    'delivery_channel','live_eligible',
                    'telegram_delivery_allowed','trade_execution_allowed',
                    'created_at_utc'
                ]::TEXT[],
                ARRAY[
                    'research_stage4_experimental_search_authority_ck',
                    'research_stage4_experimental_search_counts_ck',
                    'research_stage4_experimental_search_horizon_ck',
                    'research_stage4_experimental_search_identity_uk',
                    'research_stage4_experimental_search_payload_ck',
                    'research_stage4_experimental_search_receipt_ck',
                    'research_stage4_experimental_search_receipt_uk',
                    'research_stage4_experimental_search_run_id_ck',
                    'research_stage4_experimental_search_run_pk',
                    'research_stage4_experimental_search_schedule_slot_uk',
                    'research_stage4_experimental_search_status_ck',
                    'research_stage4_experimental_search_time_ck',
                    'research_stage4_experimental_search_versions_ck'
                ]::TEXT[],
                ARRAY[
                    'idx_stage4_experimental_search_time_v1',
                    'research_stage4_experimental_search_identity_uk',
                    'research_stage4_experimental_search_receipt_uk',
                    'research_stage4_experimental_search_run_pk',
                    'research_stage4_experimental_search_schedule_slot_uk'
                ]::TEXT[],
                ARRAY[
                    'trg_stage4_experimental_search_runs_immutable_v1',
                    'trg_stage4_experimental_search_runs_no_truncate_v1'
                ]::TEXT[],
                'contract=stage4-experimental-search-run-v1;immutable=true;registry=none;delivery=none;live=false;trading=false'
            ),
            (
                'research_formula_experimental_alerts_v1',
                ARRAY[
                    'alert_occurrence_id','search_run_id','candidate_key',
                    'search_receipt_sha256','candidate_snapshot','trigger_key',
                    'trigger_observation_id','projection_event_id',
                    'projection_event_fingerprint','btc_parent_movement_id',
                    'symbol','direction',
                    'horizon_minutes','decision_time_utc','expires_at_utc',
                    'trigger_snapshot','trigger_snapshot_sha256',
                    'current_trigger_receipt_sha256',
                    'current_trigger_policy_version','formula_text','conditions',
                    'independent_movement_count','accepted_paths','metrics',
                    'experimental_reasons','renderer_version','rendered_message',
                    'rendered_message_sha256','disclaimer','delivery_channel',
                    'formula_registry_effect','human_formula_approval_required',
                    'live_eligible','trade_execution_allowed',
                    'telegram_delivery_allowed','created_at_utc'
                ]::TEXT[],
                ARRAY[
                    'research_stage4_experimental_alert_authority_ck',
                    'research_stage4_experimental_alert_candidate_cell_uk',
                    'research_stage4_experimental_alert_candidate_ck',
                    'research_stage4_experimental_alert_direction_ck',
                    'research_stage4_experimental_alert_formula_ck',
                    'research_stage4_experimental_alert_freshness_ck',
                    'research_stage4_experimental_alert_horizon_ck',
                    'research_stage4_experimental_alert_id_ck',
                    'research_stage4_experimental_alert_message_ck',
                    'research_stage4_experimental_alert_pk',
                    'research_stage4_experimental_alert_projection_fk',
                    'research_stage4_experimental_alert_search_fk',
                    'research_stage4_experimental_alert_snapshot_ck',
                    'research_stage4_experimental_alert_symbol_ck'
                ]::TEXT[],
                ARRAY[
                    'idx_stage4_experimental_alert_time_v1',
                    'research_stage4_experimental_alert_candidate_cell_uk',
                    'research_stage4_experimental_alert_pk'
                ]::TEXT[],
                ARRAY[
                    'trg_stage4_experimental_alerts_immutable_v1',
                    'trg_stage4_experimental_alerts_no_truncate_v1'
                ]::TEXT[],
                'contract=stage4-experimental-alert-occurrence-v1;immutable=true;channel=telegram-experimental-only;live=false;trading=false'
            ),
            (
                'research_formula_experimental_subscriptions_v1',
                ARRAY[
                    'chat_id','active','requested_by_user_id',
                    'subscription_policy_version','consent_source',
                    'delivery_scope','disclaimer_acknowledged',
                    'disclaimer_acknowledged_at_utc','subscribed_at_utc',
                    'updated_at_utc'
                ]::TEXT[],
                ARRAY[
                    'research_stage4_experimental_subscription_pk',
                    'research_stage4_experimental_subscription_policy_ck',
                    'research_stage4_experimental_subscription_time_ck',
                    'research_stage4_experimental_subscription_user_ck'
                ]::TEXT[],
                ARRAY[
                    'idx_stage4_experimental_subscription_active_v1',
                    'research_stage4_experimental_subscription_pk'
                ]::TEXT[],
                ARRAY[
                    'trg_stage4_experimental_subscriptions_no_truncate_v1',
                    'trg_stage4_experimental_subscriptions_validate_v1'
                ]::TEXT[],
                'contract=stage4-experimental-subscription-v1;explicit-opt-in=true;live-subscription-backfill=false'
            ),
            (
                'research_formula_experimental_deliveries_v1',
                ARRAY[
                    'delivery_key','alert_occurrence_id','chat_id','status',
                    'attempt_count','available_at_utc','claim_token',
                    'claimed_at_utc','claim_expires_at_utc','sent_at_utc',
                    'telegram_message_id','last_failure_kind','last_error',
                    'created_at_utc','updated_at_utc'
                ]::TEXT[],
                ARRAY[
                    'research_stage4_experimental_delivery_alert_fk',
                    'research_stage4_experimental_delivery_attempt_ck',
                    'research_stage4_experimental_delivery_key_ck',
                    'research_stage4_experimental_delivery_occurrence_chat_uk',
                    'research_stage4_experimental_delivery_pk',
                    'research_stage4_experimental_delivery_state_ck',
                    'research_stage4_experimental_delivery_status_ck',
                    'research_stage4_experimental_delivery_subscription_fk',
                    'research_stage4_experimental_delivery_time_ck'
                ]::TEXT[],
                ARRAY[
                    'idx_stage4_experimental_delivery_due_v1',
                    'idx_stage4_experimental_delivery_inflight_v1',
                    'research_stage4_experimental_delivery_occurrence_chat_uk',
                    'research_stage4_experimental_delivery_pk'
                ]::TEXT[],
                ARRAY[
                    'trg_stage4_experimental_deliveries_no_truncate_v1',
                    'trg_stage4_experimental_deliveries_validate_v1',
                    'trg_stage4_experimental_delivery_attempt_audit_v1'
                ]::TEXT[],
                'contract=stage4-experimental-delivery-outbox-v1;stale-in-flight=ambiguous;automatic-live=false;trading=false'
            ),
            (
                'research_formula_experimental_delivery_attempt_events_v1',
                ARRAY[
                    'attempt_event_key','delivery_key','attempt_number',
                    'event_phase','terminal_result','claim_token',
                    'event_time_utc','telegram_message_id','error_text',
                    'event_payload','created_at_utc'
                ]::TEXT[],
                ARRAY[
                    'research_stage4_experimental_attempt_delivery_fk',
                    'research_stage4_experimental_attempt_delivery_phase_uk',
                    'research_stage4_experimental_attempt_event_key_ck',
                    'research_stage4_experimental_attempt_event_pk',
                    'research_stage4_experimental_attempt_number_ck',
                    'research_stage4_experimental_attempt_payload_ck',
                    'research_stage4_experimental_attempt_result_ck',
                    'research_stage4_experimental_attempt_time_ck'
                ]::TEXT[],
                ARRAY[
                    'idx_stage4_experimental_attempt_delivery_v1',
                    'research_stage4_experimental_attempt_delivery_phase_uk',
                    'research_stage4_experimental_attempt_event_pk'
                ]::TEXT[],
                ARRAY[
                    'trg_stage4_experimental_attempts_immutable_v1',
                    'trg_stage4_experimental_attempts_no_truncate_v1'
                ]::TEXT[],
                'contract=stage4-experimental-delivery-attempt-event-v1;immutable=true;two-phase=claimed-terminal'
            )
        ) contract(
            relation_name, columns, constraints, indexes, triggers,
            table_comment
        )
    LOOP
        relation_oid := pg_catalog.to_regclass(
            'public.' || spec.relation_name
        );
        IF relation_oid IS NULL OR NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_class relation_row
             WHERE relation_row.oid = relation_oid
               AND relation_row.relkind = 'r'
               AND relation_row.relpersistence = 'p'
               AND relation_row.relowner = trusted_owner
               AND NOT relation_row.relrowsecurity
               AND NOT relation_row.relforcerowsecurity
        ) THEN
            RAISE EXCEPTION
                'Experimental table public.% has an unsafe catalog shape',
                spec.relation_name;
        END IF;

        expected_columns := spec.columns;
        SELECT ARRAY_AGG(attribute.attname::TEXT ORDER BY attribute.attnum)
          INTO actual_columns
          FROM pg_catalog.pg_attribute attribute
         WHERE attribute.attrelid = relation_oid
           AND attribute.attnum > 0
           AND NOT attribute.attisdropped;
        IF actual_columns IS DISTINCT FROM expected_columns THEN
            RAISE EXCEPTION
                'Experimental table public.% columns changed',
                spec.relation_name;
        END IF;

        expected_constraints := spec.constraints;
        SELECT ARRAY_AGG(constraint_row.conname::TEXT ORDER BY constraint_row.conname)
          INTO actual_constraints
          FROM pg_catalog.pg_constraint constraint_row
         WHERE constraint_row.conrelid = relation_oid
           AND constraint_row.contype IN ('c', 'p', 'u', 'f');
        IF actual_constraints IS DISTINCT FROM expected_constraints THEN
            RAISE EXCEPTION
                'Experimental table public.% constraints changed',
                spec.relation_name;
        END IF;
        IF EXISTS (
            SELECT 1
              FROM pg_catalog.pg_constraint constraint_row
             WHERE constraint_row.conrelid = relation_oid
               AND constraint_row.contype IN ('c', 'p', 'u', 'f')
               AND (
                   NOT constraint_row.convalidated
                   OR constraint_row.condeferrable
               )
        ) THEN
            RAISE EXCEPTION
                'Experimental table public.% has a weak constraint',
                spec.relation_name;
        END IF;

        expected_indexes := spec.indexes;
        SELECT ARRAY_AGG(index_relation.relname::TEXT ORDER BY index_relation.relname)
          INTO actual_indexes
          FROM pg_catalog.pg_index index_row
          JOIN pg_catalog.pg_class index_relation
            ON index_relation.oid = index_row.indexrelid
         WHERE index_row.indrelid = relation_oid
           AND index_row.indisvalid
           AND index_row.indisready;
        IF actual_indexes IS DISTINCT FROM expected_indexes THEN
            RAISE EXCEPTION
                'Experimental table public.% indexes changed',
                spec.relation_name;
        END IF;

        expected_triggers := spec.triggers;
        SELECT ARRAY_AGG(trigger_row.tgname::TEXT ORDER BY trigger_row.tgname)
          INTO actual_triggers
          FROM pg_catalog.pg_trigger trigger_row
         WHERE trigger_row.tgrelid = relation_oid
           AND NOT trigger_row.tgisinternal
           AND trigger_row.tgenabled = 'A';
        IF actual_triggers IS DISTINCT FROM expected_triggers THEN
            RAISE EXCEPTION
                'Experimental table public.% triggers changed',
                spec.relation_name;
        END IF;

        expected_comment := spec.table_comment;
        IF pg_catalog.obj_description(relation_oid, 'pg_class') IS DISTINCT FROM
           expected_comment THEN
            RAISE EXCEPTION
                'Experimental table public.% contract receipt changed',
                spec.relation_name;
        END IF;
        IF EXISTS (
            SELECT 1 FROM pg_catalog.pg_policy policy_row
             WHERE policy_row.polrelid = relation_oid
        ) OR EXISTS (
            SELECT 1 FROM pg_catalog.pg_rewrite rewrite_row
             WHERE rewrite_row.ev_class = relation_oid
               AND rewrite_row.rulename <> '_RETURN'
        ) THEN
            RAISE EXCEPTION
                'Experimental table public.% has unsafe enforcement',
                spec.relation_name;
        END IF;
    END LOOP;

    -- Exact relation grants: SELECT+INSERT, non-grantable, for the dispatcher;
    -- owner grants are ignored.  Column UPDATE grants are checked separately.
    IF EXISTS (
        SELECT 1
          FROM pg_catalog.pg_class relation_row
          CROSS JOIN LATERAL pg_catalog.aclexplode(
              COALESCE(
                  relation_row.relacl,
                  pg_catalog.acldefault('r', relation_row.relowner)
              )
          ) acl
         WHERE relation_row.relname IN (
             'research_formula_experimental_search_runs_v1',
             'research_formula_experimental_alerts_v1',
             'research_formula_experimental_subscriptions_v1',
             'research_formula_experimental_deliveries_v1',
             'research_formula_experimental_delivery_attempt_events_v1'
         )
           AND relation_row.relnamespace = 'public'::REGNAMESPACE
           AND acl.grantee <> trusted_owner
           AND NOT (
               acl.grantee = dispatcher_oid
               AND acl.privilege_type IN ('SELECT', 'INSERT')
               AND NOT acl.is_grantable
           )
    ) OR EXISTS (
        SELECT 1
          FROM pg_catalog.pg_class relation_row
         WHERE relation_row.relname IN (
             'research_formula_experimental_search_runs_v1',
             'research_formula_experimental_alerts_v1',
             'research_formula_experimental_subscriptions_v1',
             'research_formula_experimental_deliveries_v1',
             'research_formula_experimental_delivery_attempt_events_v1'
         )
           AND relation_row.relnamespace = 'public'::REGNAMESPACE
           AND (
               SELECT COUNT(*)
                 FROM pg_catalog.aclexplode(COALESCE(
                     relation_row.relacl,
                     pg_catalog.acldefault('r', relation_row.relowner)
                 )) acl
                WHERE acl.grantee = dispatcher_oid
                  AND acl.privilege_type IN ('SELECT', 'INSERT')
                  AND NOT acl.is_grantable
           ) <> 2
    ) THEN
        RAISE EXCEPTION 'Experimental relation ACL is not exact';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM pg_catalog.pg_attribute attribute
          CROSS JOIN LATERAL pg_catalog.aclexplode(attribute.attacl) acl
         WHERE attribute.attrelid IN (
             'public.research_formula_experimental_subscriptions_v1'::REGCLASS,
             'public.research_formula_experimental_deliveries_v1'::REGCLASS
         )
           AND attribute.attnum > 0
           AND NOT attribute.attisdropped
           AND NOT (
               acl.grantee = dispatcher_oid
               AND acl.privilege_type = 'UPDATE'
               AND NOT acl.is_grantable
               AND (
                   (
                       attribute.attrelid =
                           'public.research_formula_experimental_subscriptions_v1'::REGCLASS
                       AND attribute.attname = 'active'
                   ) OR (
                       attribute.attrelid =
                           'public.research_formula_experimental_deliveries_v1'::REGCLASS
                       AND attribute.attname IN (
                           'status','attempt_count','available_at_utc',
                           'claim_token','claimed_at_utc',
                           'claim_expires_at_utc','sent_at_utc',
                           'telegram_message_id','last_failure_kind','last_error'
                       )
                   )
               )
           )
    ) OR (
        SELECT COUNT(*)
          FROM pg_catalog.pg_attribute attribute
          CROSS JOIN LATERAL pg_catalog.aclexplode(attribute.attacl) acl
         WHERE attribute.attrelid =
               'public.research_formula_experimental_subscriptions_v1'::REGCLASS
           AND attribute.attnum > 0
           AND NOT attribute.attisdropped
           AND acl.grantee = dispatcher_oid
           AND acl.privilege_type = 'UPDATE'
           AND NOT acl.is_grantable
    ) <> 1 OR (
        SELECT COUNT(*)
          FROM pg_catalog.pg_attribute attribute
          CROSS JOIN LATERAL pg_catalog.aclexplode(attribute.attacl) acl
         WHERE attribute.attrelid =
               'public.research_formula_experimental_deliveries_v1'::REGCLASS
           AND attribute.attnum > 0
           AND NOT attribute.attisdropped
           AND acl.grantee = dispatcher_oid
           AND acl.privilege_type = 'UPDATE'
           AND NOT acl.is_grantable
    ) <> 10 THEN
        RAISE EXCEPTION 'Experimental column UPDATE ACL is not exact';
    END IF;

    IF NOT pg_catalog.has_schema_privilege(
           'research_formula_experimental_dispatcher_v1', 'public', 'USAGE'
       ) OR pg_catalog.has_schema_privilege(
           'research_formula_experimental_dispatcher_v1', 'public', 'CREATE'
       ) OR pg_catalog.has_database_privilege(
           'research_formula_experimental_dispatcher_v1',
           pg_catalog.current_database(), 'CREATE'
       ) OR EXISTS (
           SELECT 1
             FROM (VALUES
                 ('public.research_events'),
                 ('public.research_formulas'),
                 ('public.research_formula_live_approvals'),
                 ('public.research_formula_live_deliveries'),
                 ('public.research_formula_alert_subscriptions'),
                 ('public.research_formula_exploration_stage4_v1')
             ) protected(relation_name)
             CROSS JOIN (VALUES
                 ('SELECT'), ('INSERT'), ('UPDATE'), ('DELETE'), ('TRUNCATE'),
                 ('REFERENCES'), ('TRIGGER')
             ) privilege(privilege_name)
            WHERE pg_catalog.has_table_privilege(
                'research_formula_experimental_dispatcher_v1',
                protected.relation_name,
                privilege.privilege_name
            )
       ) OR EXISTS (
           SELECT 1
             FROM (VALUES
                 ('public.research_events'),
                 ('public.research_formulas'),
                 ('public.research_formula_live_approvals'),
                 ('public.research_formula_live_deliveries'),
                 ('public.research_formula_alert_subscriptions'),
                 ('public.research_formula_exploration_stage4_v1')
             ) protected(relation_name)
             CROSS JOIN (VALUES
                 ('SELECT'), ('INSERT'), ('UPDATE'), ('REFERENCES')
             ) privilege(privilege_name)
            WHERE pg_catalog.has_any_column_privilege(
                'research_formula_experimental_dispatcher_v1',
                protected.relation_name,
                privilege.privilege_name
            )
       ) THEN
        RAISE EXCEPTION
            'Experimental dispatcher escaped its isolated authority boundary';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM pg_catalog.pg_proc function_row
         WHERE function_row.oid IN (
             'public.prevent_research_stage4_experimental_immutable_v1()'::REGPROCEDURE,
             'public.validate_research_stage4_experimental_subscription_v1()'::REGPROCEDURE,
             'public.validate_research_stage4_experimental_delivery_v1()'::REGPROCEDURE,
             'public.require_research_stage4_experimental_attempt_audit_v1()'::REGPROCEDURE,
             'public.validate_research_stage4_experimental_attempt_v1()'::REGPROCEDURE
         )
           AND (
               function_row.proowner <> trusted_owner
               OR function_row.prosecdef
               OR function_row.provolatile <> 'v'
               OR function_row.proconfig IS DISTINCT FROM
                    ARRAY['search_path=pg_catalog, public']::TEXT[]
           )
    ) OR EXISTS (
        SELECT 1
          FROM pg_catalog.pg_proc function_row
          CROSS JOIN LATERAL pg_catalog.aclexplode(
              COALESCE(
                  function_row.proacl,
                  pg_catalog.acldefault('f', function_row.proowner)
              )
          ) acl
         WHERE function_row.oid IN (
             'public.prevent_research_stage4_experimental_immutable_v1()'::REGPROCEDURE,
             'public.validate_research_stage4_experimental_subscription_v1()'::REGPROCEDURE,
             'public.validate_research_stage4_experimental_delivery_v1()'::REGPROCEDURE,
             'public.require_research_stage4_experimental_attempt_audit_v1()'::REGPROCEDURE,
             'public.validate_research_stage4_experimental_attempt_v1()'::REGPROCEDURE
         )
           AND acl.grantee <> function_row.proowner
    ) THEN
        RAISE EXCEPTION 'Experimental trigger-function contract is unsafe';
    END IF;
END;
$postflight$;

-- Explicit manual rollback.  First set the experimental Telegram runtime flag
-- false, stop every dispatcher replica and preserve any desired audit export.
-- Drop only these new objects, in dependency order; never touch LIVE Formula
-- approvals, LIVE deliveries, legacy subscriptions or source evidence.
-- DROP TABLE IF EXISTS
--     public.research_formula_experimental_delivery_attempt_events_v1;
-- DROP TABLE IF EXISTS
--     public.research_formula_experimental_deliveries_v1;
-- DROP TABLE IF EXISTS
--     public.research_formula_experimental_subscriptions_v1;
-- DROP TABLE IF EXISTS
--     public.research_formula_experimental_alerts_v1;
-- DROP TABLE IF EXISTS
--     public.research_formula_experimental_search_runs_v1;
-- DROP FUNCTION IF EXISTS
--     public.validate_research_stage4_experimental_attempt_v1();
-- DROP FUNCTION IF EXISTS
--     public.require_research_stage4_experimental_attempt_audit_v1();
-- DROP FUNCTION IF EXISTS
--     public.validate_research_stage4_experimental_delivery_v1();
-- DROP FUNCTION IF EXISTS
--     public.validate_research_stage4_experimental_subscription_v1();
-- DROP FUNCTION IF EXISTS
--     public.prevent_research_stage4_experimental_immutable_v1();
-- REVOKE USAGE ON SCHEMA public
--     FROM research_formula_experimental_dispatcher_v1;
