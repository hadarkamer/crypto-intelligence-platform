-- Causal neutral-price Market Movements (Wave v5)
--
-- Research-only, additive, append-only storage for the pure Wave v5 contract.
-- This migration deliberately does not wire a collector, worker, Formula,
-- Telegram, LIVE delivery, or trading path.
--
-- Trust boundary: PostgreSQL verifies immutable projections, graph shape,
-- ordering, and receipt relationships.  The complete Wave algorithm and its
-- SHA-256 receipts are recomputed by the one trusted Python writer.  That
-- writer is therefore a fixed LOGIN identity, not a grantable capability
-- group.  Roles and passwords must be provisioned out of band.

-- The supported installer executes this file inside one transaction. Secure
-- catalog lookup before even inspecting the pre-provisioned trust roles.
SET LOCAL search_path = pg_catalog;

DO $roles$
DECLARE
    owner_row RECORD;
    writer_row RECORD;
BEGIN
    SELECT * INTO owner_row
      FROM pg_roles
     WHERE rolname = 'research_market_movement_owner';
    IF NOT FOUND THEN
        RAISE EXCEPTION
            'Required NOLOGIN role research_market_movement_owner is missing';
    END IF;
    IF owner_row.rolcanlogin
       OR owner_row.rolinherit
       OR owner_row.rolsuper
       OR owner_row.rolcreatedb
       OR owner_row.rolcreaterole
       OR owner_row.rolreplication
       OR owner_row.rolbypassrls THEN
        RAISE EXCEPTION
            'research_market_movement_owner must be an unprivileged NOLOGIN role';
    END IF;

    SELECT * INTO writer_row
      FROM pg_roles
     WHERE rolname = 'research_market_movement_writer_v5';
    IF NOT FOUND THEN
        RAISE EXCEPTION
            'Required LOGIN role research_market_movement_writer_v5 is missing';
    END IF;
    IF NOT writer_row.rolcanlogin
       OR writer_row.rolinherit
       OR writer_row.rolsuper
       OR writer_row.rolcreatedb
       OR writer_row.rolcreaterole
       OR writer_row.rolreplication
       OR writer_row.rolbypassrls THEN
        RAISE EXCEPTION
            'research_market_movement_writer_v5 must be an unprivileged LOGIN role';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM pg_catalog.pg_auth_members membership
          JOIN pg_catalog.pg_roles writer
            ON writer.rolname = 'research_market_movement_writer_v5'
         WHERE membership.member = writer.oid
            OR membership.roleid = writer.oid
    ) THEN
        RAISE EXCEPTION
            'Wave v5 writer must not participate in any role membership';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM pg_catalog.pg_database database_row
          JOIN pg_catalog.pg_roles writer
            ON writer.oid = database_row.datdba
         WHERE database_row.datname = pg_catalog.current_database()
           AND writer.rolname = 'research_market_movement_writer_v5'
    ) THEN
        RAISE EXCEPTION
            'Wave v5 writer must not own the current database';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM pg_catalog.pg_namespace namespace_row
          JOIN pg_catalog.pg_roles writer
            ON writer.oid = namespace_row.nspowner
         WHERE namespace_row.nspname = 'public'
           AND writer.rolname = 'research_market_movement_writer_v5'
    ) THEN
        RAISE EXCEPTION
            'Wave v5 writer must not own schema public';
    END IF;
    IF has_schema_privilege(
        'research_market_movement_writer_v5', 'public', 'CREATE'
    ) THEN
        RAISE EXCEPTION
            'Wave v5 writer must not have CREATE on schema public';
    END IF;
    IF NOT has_schema_privilege(
        'research_market_movement_writer_v5', 'public', 'USAGE'
    ) THEN
        RAISE EXCEPTION
            'Wave v5 writer needs USAGE on schema public';
    END IF;
    IF NOT has_schema_privilege(
        'research_market_movement_owner', 'public', 'USAGE'
    ) OR NOT has_schema_privilege(
        'research_market_movement_owner', 'public', 'CREATE'
    ) THEN
        RAISE EXCEPTION
            'Wave v5 owner needs USAGE and CREATE on schema public';
    END IF;
    IF NOT pg_has_role(
        session_user, 'research_market_movement_owner', 'MEMBER'
    ) THEN
        RAISE EXCEPTION
            'Migration session must be able to SET ROLE research_market_movement_owner';
    END IF;
END;
$roles$;

SET LOCAL ROLE research_market_movement_owner;
-- Keep public as the creation schema while omitting pg_catalog deliberately:
-- PostgreSQL then searches pg_catalog implicitly *before* public, preventing
-- public functions/operators from shadowing built-ins used in DDL.
SET LOCAL search_path = public;
-- pg_get_constraintdef() renders timestamptz constants through the active
-- timezone and date/interval output GUCs. Pin them so the replay
-- fingerprints below are deterministic across sessions.
SET LOCAL TIME ZONE 'UTC';
SET LOCAL DateStyle = 'ISO, YMD';
SET LOCAL IntervalStyle = 'postgres';

CREATE TABLE IF NOT EXISTS research_price_collection_attempts (
    attempt_receipt_sha256 CHAR(64) COLLATE pg_catalog."C" PRIMARY KEY CHECK (
        BTRIM(attempt_receipt_sha256) ~ '^[0-9a-f]{64}$'
    ),
    contract_version TEXT COLLATE pg_catalog."C" NOT NULL CHECK (
        contract_version = 'market-movement-v5-causal-neutral-price-wave'
    ),
    symbol TEXT COLLATE pg_catalog."C" NOT NULL CHECK (
        BTRIM(symbol) ~ '^[A-Z0-9-]{1,20}$'
    ),
    eligible_at_utc TIMESTAMPTZ NOT NULL CHECK (
        eligible_at_utc = DATE_BIN(
            INTERVAL '30 minutes',
            eligible_at_utc,
            '1970-01-01 00:02:00+00'::timestamptz
        )
    ),
    decision_time_utc TIMESTAMPTZ NOT NULL,
    evaluation_status TEXT COLLATE pg_catalog."C" NOT NULL CHECK (
        evaluation_status IN ('EVALUABLE', 'UNEVALUABLE')
    ),
    evaluation_reason TEXT COLLATE pg_catalog."C" NOT NULL CHECK (BTRIM(evaluation_reason) <> ''),
    anchor_id CHAR(64) COLLATE pg_catalog."C" CHECK (
        anchor_id IS NULL OR BTRIM(anchor_id) ~ '^[0-9a-f]{64}$'
    ),
    anchor_receipt_sha256 CHAR(64) COLLATE pg_catalog."C" CHECK (
        anchor_receipt_sha256 IS NULL
        OR BTRIM(anchor_receipt_sha256) ~ '^[0-9a-f]{64}$'
    ),
    attempt_receipt JSONB NOT NULL CHECK (
        JSONB_TYPEOF(attempt_receipt) = 'object'
    ),
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT CLOCK_TIMESTAMP(),
    -- A provider call that began inside the capture window can complete after
    -- it.  Preserve that real decision time as an auditable UNEVALUABLE
    -- attempt; only an EVALUABLE result may create an in-window anchor.
    CHECK (
        decision_time_utc >= eligible_at_utc
        AND (
            evaluation_status = 'UNEVALUABLE'
            OR decision_time_utc < eligible_at_utc + INTERVAL '30 minutes'
        )
    ),
    CHECK (
        (evaluation_status = 'EVALUABLE'
         AND anchor_id IS NOT NULL
         AND anchor_receipt_sha256 IS NOT NULL)
        OR
        (evaluation_status = 'UNEVALUABLE'
         AND anchor_id IS NULL
         AND anchor_receipt_sha256 IS NULL)
    ),
    CHECK (
        attempt_receipt ?& ARRAY[
            'contract_version', 'attempt_receipt_sha256', 'symbol',
            'eligible_at_utc', 'decision_time_utc', 'evaluation_status',
            'evaluation_reason', 'anchor_id', 'anchor_receipt_sha256'
        ]
    ),
    CHECK ((
        attempt_receipt->>'contract_version' = contract_version
        AND attempt_receipt->>'attempt_receipt_sha256' =
            BTRIM(attempt_receipt_sha256)
        AND attempt_receipt->>'symbol' = symbol
        AND (attempt_receipt->>'eligible_at_utc')::timestamptz =
            eligible_at_utc
        AND (attempt_receipt->>'decision_time_utc')::timestamptz =
            decision_time_utc
        AND attempt_receipt->>'evaluation_status' = evaluation_status
        AND attempt_receipt->>'evaluation_reason' = evaluation_reason
        AND attempt_receipt->>'anchor_id' IS NOT DISTINCT FROM
            BTRIM(anchor_id)
        AND attempt_receipt->>'anchor_receipt_sha256' IS NOT DISTINCT FROM
            BTRIM(anchor_receipt_sha256)
    ) IS TRUE)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_neutral_price_attempt_evaluable_anchor
    ON research_price_collection_attempts (anchor_id)
    WHERE evaluation_status = 'EVALUABLE';

CREATE INDEX IF NOT EXISTS idx_neutral_price_attempt_slot
    ON research_price_collection_attempts (
        symbol, eligible_at_utc, decision_time_utc
    );

CREATE TABLE IF NOT EXISTS research_neutral_price_anchors (
    anchor_id CHAR(64) COLLATE pg_catalog."C" PRIMARY KEY CHECK (
        BTRIM(anchor_id) ~ '^[0-9a-f]{64}$'
    ),
    anchor_receipt_sha256 CHAR(64) COLLATE pg_catalog."C" NOT NULL UNIQUE CHECK (
        BTRIM(anchor_receipt_sha256) ~ '^[0-9a-f]{64}$'
    ),
    contract_version TEXT COLLATE pg_catalog."C" NOT NULL CHECK (
        contract_version = 'market-movement-v5-causal-neutral-price-wave'
    ),
    symbol TEXT COLLATE pg_catalog."C" NOT NULL CHECK (
        BTRIM(symbol) ~ '^[A-Z0-9-]{1,20}$'
    ),
    origin TEXT COLLATE pg_catalog."C" NOT NULL CHECK (
        origin IN ('PROSPECTIVE_V5', 'AUTHORIZED_LEGACY_V3_V4')
    ),
    sampler_version TEXT COLLATE pg_catalog."C" NOT NULL CHECK (BTRIM(sampler_version) <> ''),
    eligible_at_utc TIMESTAMPTZ NOT NULL CHECK (
        eligible_at_utc = DATE_BIN(
            INTERVAL '30 minutes',
            eligible_at_utc,
            '1970-01-01 00:02:00+00'::timestamptz
        )
    ),
    decision_time_utc TIMESTAMPTZ NOT NULL,
    source_price_candle_open_utc TIMESTAMPTZ NOT NULL,
    source_price_candle_close_utc TIMESTAMPTZ NOT NULL,
    observed_at_utc TIMESTAMPTZ NOT NULL,
    refresh_completed_at_utc TIMESTAMPTZ NOT NULL,
    price NUMERIC NOT NULL CHECK (
        price > 0 AND price < 'Infinity'::numeric
    ),
    source TEXT COLLATE pg_catalog."C" NOT NULL CHECK (BTRIM(source) <> ''),
    upstream_source TEXT COLLATE pg_catalog."C" NOT NULL CHECK (BTRIM(upstream_source) <> ''),
    price_exchange TEXT COLLATE pg_catalog."C" NOT NULL CHECK (BTRIM(price_exchange) <> ''),
    price_market TEXT COLLATE pg_catalog."C" NOT NULL CHECK (BTRIM(price_market) <> ''),
    price_pair TEXT COLLATE pg_catalog."C" NOT NULL CHECK (BTRIM(price_pair) <> ''),
    price_instrument_id TEXT COLLATE pg_catalog."C" NOT NULL CHECK (BTRIM(price_instrument_id) <> ''),
    price_timeframe TEXT COLLATE pg_catalog."C" NOT NULL CHECK (LOWER(price_timeframe) = '1m'),
    quality_status TEXT COLLATE pg_catalog."C" NOT NULL CHECK (UPPER(quality_status) = 'PASS'),
    fallback_used BOOLEAN NOT NULL CHECK (fallback_used = FALSE),
    fallback_policy TEXT COLLATE pg_catalog."C" NOT NULL CHECK (
        UPPER(fallback_policy) = 'PROVIDER_ATTESTED_NO_FALLBACK'
    ),
    price_candle_identity_basis TEXT COLLATE pg_catalog."C" NOT NULL,
    source_input_fingerprint CHAR(64) COLLATE pg_catalog."C" CHECK (
        source_input_fingerprint IS NULL
        OR BTRIM(source_input_fingerprint) ~ '^[0-9a-f]{64}$'
    ),
    source_record_created_at_utc TIMESTAMPTZ,
    anchor_receipt JSONB NOT NULL CHECK (
        JSONB_TYPEOF(anchor_receipt) = 'object'
    ),
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT CLOCK_TIMESTAMP(),
    UNIQUE (contract_version, symbol, eligible_at_utc),
    UNIQUE (anchor_id, anchor_receipt_sha256),
    CHECK (
        source_price_candle_open_utc =
            eligible_at_utc - INTERVAL '1 minute'
    ),
    CHECK (
        source_price_candle_close_utc >=
            eligible_at_utc - INTERVAL '1 second'
        AND source_price_candle_close_utc < eligible_at_utc
        AND observed_at_utc = source_price_candle_close_utc
    ),
    CHECK (
        refresh_completed_at_utc >= eligible_at_utc
        AND refresh_completed_at_utc <= decision_time_utc
        AND decision_time_utc >= eligible_at_utc
        AND decision_time_utc < eligible_at_utc + INTERVAL '30 minutes'
    ),
    CHECK (
        (symbol = 'HYPE'
         AND LOWER(source) = 'hyperliquid_spot_@107'
         AND LOWER(upstream_source) = 'hyperliquid'
         AND UPPER(price_exchange) = 'HYPERLIQUID'
         AND UPPER(price_market) = 'SPOT'
         AND REGEXP_REPLACE(
             UPPER(price_pair), '[^A-Z0-9]', '', 'g'
         ) = 'HYPEUSDT'
         AND UPPER(price_instrument_id) = '@107')
        OR
        (symbol <> 'HYPE'
         AND LOWER(source) = 'binance_spot'
         AND LOWER(upstream_source) = 'binance_spot'
         AND UPPER(price_exchange) = 'BINANCE'
         AND UPPER(price_market) = 'SPOT'
         AND REGEXP_REPLACE(
             UPPER(price_pair), '[^A-Z0-9]', '', 'g'
         ) = symbol || 'USDT'
         AND UPPER(price_instrument_id) = symbol || 'USDT')
    ),
    CHECK (
        (origin = 'PROSPECTIVE_V5'
         AND sampler_version = 'neutral-price-anchor-v5-exact-closed-1m'
         AND price_candle_identity_basis =
             'FROZEN_EXACT_CLOSED_1M_CANDLE'
         AND source_record_created_at_utc IS NULL)
        OR
        (origin = 'AUTHORIZED_LEGACY_V3_V4'
         AND sampler_version IN (
             'prospective-neutral-anchor-v3-max-pain-frozen',
             'prospective-neutral-anchor-v4-decision-features-frozen'
         )
         AND eligible_at_utc >=
             '2026-08-29 00:00:00+00'::timestamptz
         AND price_candle_identity_basis =
             'DERIVED_FROM_FROZEN_CLOSE_AND_AUTHORIZED_1M_SAMPLER_CONTRACT'
         AND source_input_fingerprint IS NOT NULL
         AND source_record_created_at_utc IS NOT NULL
         AND source_record_created_at_utc >= decision_time_utc
         AND source_record_created_at_utc <=
             decision_time_utc + INTERVAL '5 minutes'
         AND source_record_created_at_utc <
             eligible_at_utc + INTERVAL '30 minutes')
    ),
    CHECK (
        anchor_receipt ?& ARRAY[
            'contract_version', 'anchor_id', 'anchor_receipt_sha256',
            'symbol', 'origin', 'sampler_version', 'eligible_at_utc',
            'decision_time_utc', 'source_price_candle_open_utc',
            'source_price_candle_close_utc', 'observed_at_utc',
            'refresh_completed_at_utc', 'price', 'source',
            'upstream_source', 'price_exchange', 'price_market', 'price_pair',
            'price_instrument_id', 'price_timeframe', 'quality_status',
            'fallback_used', 'fallback_policy',
            'price_candle_identity_basis', 'source_input_fingerprint',
            'source_record_created_at_utc'
        ]
    ),
    CHECK ((
        anchor_receipt->>'contract_version' = contract_version
        AND anchor_receipt->>'anchor_id' = BTRIM(anchor_id)
        AND anchor_receipt->>'anchor_receipt_sha256' =
            BTRIM(anchor_receipt_sha256)
        AND anchor_receipt->>'symbol' = symbol
        AND anchor_receipt->>'origin' = origin
        AND anchor_receipt->>'sampler_version' = sampler_version
        AND (anchor_receipt->>'eligible_at_utc')::timestamptz =
            eligible_at_utc
        AND (anchor_receipt->>'decision_time_utc')::timestamptz =
            decision_time_utc
        AND (anchor_receipt->>'source_price_candle_open_utc')::timestamptz =
            source_price_candle_open_utc
        AND (anchor_receipt->>'source_price_candle_close_utc')::timestamptz =
            source_price_candle_close_utc
        AND (anchor_receipt->>'observed_at_utc')::timestamptz =
            observed_at_utc
        AND (anchor_receipt->>'refresh_completed_at_utc')::timestamptz =
            refresh_completed_at_utc
        AND (anchor_receipt->>'price')::numeric = price
        AND anchor_receipt->>'source' = source
        AND anchor_receipt->>'upstream_source' = upstream_source
        AND anchor_receipt->>'price_exchange' = price_exchange
        AND anchor_receipt->>'price_market' = price_market
        AND anchor_receipt->>'price_pair' = price_pair
        AND anchor_receipt->>'price_instrument_id' IS NOT DISTINCT FROM
            price_instrument_id
        AND anchor_receipt->>'price_timeframe' = price_timeframe
        AND anchor_receipt->>'quality_status' = quality_status
        AND (anchor_receipt->>'fallback_used')::boolean = fallback_used
        AND anchor_receipt->>'fallback_policy' = fallback_policy
        AND anchor_receipt->>'price_candle_identity_basis' =
            price_candle_identity_basis
        AND anchor_receipt->>'source_input_fingerprint' IS NOT DISTINCT FROM
            BTRIM(source_input_fingerprint)
        AND (anchor_receipt->>'source_record_created_at_utc')::timestamptz
            IS NOT DISTINCT FROM source_record_created_at_utc
    ) IS TRUE)
);

CREATE INDEX IF NOT EXISTS idx_neutral_price_anchor_slot
    ON research_neutral_price_anchors (symbol, eligible_at_utc, anchor_id);

DO $constraint$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conrelid =
               'research_price_collection_attempts'::regclass
           AND conname = 'fk_neutral_price_attempt_anchor_receipt'
    ) THEN
        ALTER TABLE research_price_collection_attempts
            ADD CONSTRAINT fk_neutral_price_attempt_anchor_receipt
            FOREIGN KEY (anchor_id, anchor_receipt_sha256)
            REFERENCES research_neutral_price_anchors (
                anchor_id, anchor_receipt_sha256
            )
            ON DELETE RESTRICT
            DEFERRABLE INITIALLY DEFERRED;
    END IF;
END;
$constraint$;

CREATE TABLE IF NOT EXISTS research_market_movement_transitions (
    transition_receipt_sha256 CHAR(64) COLLATE pg_catalog."C" PRIMARY KEY CHECK (
        BTRIM(transition_receipt_sha256) ~ '^[0-9a-f]{64}$'
    ),
    contract_version TEXT COLLATE pg_catalog."C" NOT NULL CHECK (
        contract_version = 'market-movement-v5-causal-neutral-price-wave'
    ),
    previous_transition_receipt_sha256 CHAR(64) COLLATE pg_catalog."C" CHECK (
        previous_transition_receipt_sha256 IS NULL
        OR BTRIM(previous_transition_receipt_sha256) ~ '^[0-9a-f]{64}$'
    ),
    chain_ordinal BIGINT NOT NULL CHECK (chain_ordinal > 0),
    transition_type TEXT COLLATE pg_catalog."C" NOT NULL CHECK (
        transition_type IN (
            'OPENED', 'OPENED_AFTER_DATA_GAP',
            'OPENED_AFTER_DIRECTION_END', 'DIRECTION_ESTABLISHED',
            'EXTREME_EXTENDED', 'NON_EXTREME_OBSERVED',
            'MOVEMENT_CLOSED'
        )
    ),
    stream_id CHAR(64) COLLATE pg_catalog."C" NOT NULL CHECK (
        BTRIM(stream_id) ~ '^[0-9a-f]{64}$'
    ),
    namespace TEXT COLLATE pg_catalog."C" NOT NULL CHECK (namespace IN ('SYMBOL', 'BTC_PARENT')),
    symbol TEXT COLLATE pg_catalog."C" NOT NULL CHECK (
        BTRIM(symbol) ~ '^[A-Z0-9-]{1,20}$'
        AND (namespace <> 'BTC_PARENT' OR symbol = 'BTC')
    ),
    movement_id CHAR(64) COLLATE pg_catalog."C" NOT NULL CHECK (
        BTRIM(movement_id) ~ '^[0-9a-f]{64}$'
    ),
    trigger_anchor_id CHAR(64) COLLATE pg_catalog."C" NOT NULL REFERENCES
        research_neutral_price_anchors(anchor_id) ON DELETE RESTRICT,
    trigger_eligible_at_utc TIMESTAMPTZ NOT NULL,
    trigger_decision_time_utc TIMESTAMPTZ NOT NULL,
    pre_state_sha256 CHAR(64) COLLATE pg_catalog."C" CHECK (
        pre_state_sha256 IS NULL
        OR BTRIM(pre_state_sha256) ~ '^[0-9a-f]{64}$'
    ),
    post_state_sha256 CHAR(64) COLLATE pg_catalog."C" NOT NULL CHECK (
        BTRIM(post_state_sha256) ~ '^[0-9a-f]{64}$'
    ),
    post_state JSONB NOT NULL CHECK (JSONB_TYPEOF(post_state) = 'object'),
    transition_receipt JSONB NOT NULL CHECK (
        JSONB_TYPEOF(transition_receipt) = 'object'
    ),
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT CLOCK_TIMESTAMP(),
    FOREIGN KEY (previous_transition_receipt_sha256)
        REFERENCES research_market_movement_transitions(
            transition_receipt_sha256
        )
        ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (stream_id, chain_ordinal),
    UNIQUE (stream_id, trigger_anchor_id, transition_type),
    CHECK (
        transition_receipt ?& ARRAY[
            'contract_version', 'transition_receipt_sha256',
            'previous_transition_receipt_sha256', 'transition_type',
            'stream_id', 'movement_id', 'trigger_anchor_id',
            'trigger_eligible_at_utc', 'trigger_decision_time_utc',
            'pre_state_sha256', 'post_state'
        ]
    ),
    CHECK ((
        transition_receipt->>'contract_version' = contract_version
        AND transition_receipt->>'transition_receipt_sha256' =
            BTRIM(transition_receipt_sha256)
        AND transition_receipt->>'previous_transition_receipt_sha256'
            IS NOT DISTINCT FROM
            BTRIM(previous_transition_receipt_sha256)
        AND transition_receipt->>'transition_type' = transition_type
        AND transition_receipt->>'stream_id' = BTRIM(stream_id)
        AND transition_receipt->>'movement_id' = BTRIM(movement_id)
        AND transition_receipt->>'trigger_anchor_id' =
            BTRIM(trigger_anchor_id)
        AND (transition_receipt->>'trigger_eligible_at_utc')::timestamptz =
            trigger_eligible_at_utc
        AND (transition_receipt->>'trigger_decision_time_utc')::timestamptz =
            trigger_decision_time_utc
        AND transition_receipt->>'pre_state_sha256' IS NOT DISTINCT FROM
            BTRIM(pre_state_sha256)
        AND transition_receipt->'post_state' = post_state
    ) IS TRUE),
    CHECK ((
        post_state ?& ARRAY[
            'contract_version', 'state_sha256', 'stream_id', 'namespace',
            'symbol', 'movement_id', 'status', 'direction',
            'started_anchor_id', 'started_eligible_at_utc',
            'started_decision_time_utc', 'start_price',
            'extreme_anchor_id', 'extreme_eligible_at_utc', 'extreme_price',
            'last_member_anchor_id', 'last_member_eligible_at_utc',
            'last_member_decision_time_utc', 'last_member_price',
            'member_count', 'consecutive_non_extremes', 'closed_at_utc',
            'close_boundary_eligible_at_utc', 'close_reason'
        ]
        AND post_state->>'contract_version' = contract_version
        AND post_state->>'state_sha256' = BTRIM(post_state_sha256)
        AND post_state->>'stream_id' = BTRIM(stream_id)
        AND post_state->>'namespace' = namespace
        AND post_state->>'symbol' = symbol
        AND post_state->>'movement_id' = BTRIM(movement_id)
        AND post_state->>'status' IN ('OPEN', 'CLOSED')
        AND post_state->>'direction' IN ('PENDING', 'UP', 'DOWN')
        AND (post_state->>'member_count')::bigint > 0
        AND (post_state->>'consecutive_non_extremes')::integer BETWEEN 0 AND 1
    ) IS TRUE),
    CHECK ((
        BTRIM(post_state->>'started_anchor_id') ~ '^[0-9a-f]{64}$'
        AND BTRIM(post_state->>'extreme_anchor_id') ~ '^[0-9a-f]{64}$'
        AND BTRIM(post_state->>'last_member_anchor_id') ~ '^[0-9a-f]{64}$'
        AND (post_state->>'started_eligible_at_utc')::timestamptz =
            DATE_BIN(
                INTERVAL '30 minutes',
                (post_state->>'started_eligible_at_utc')::timestamptz,
                '1970-01-01 00:02:00+00'::timestamptz
            )
        AND (post_state->>'extreme_eligible_at_utc')::timestamptz =
            DATE_BIN(
                INTERVAL '30 minutes',
                (post_state->>'extreme_eligible_at_utc')::timestamptz,
                '1970-01-01 00:02:00+00'::timestamptz
            )
        AND (post_state->>'last_member_eligible_at_utc')::timestamptz =
            DATE_BIN(
                INTERVAL '30 minutes',
                (post_state->>'last_member_eligible_at_utc')::timestamptz,
                '1970-01-01 00:02:00+00'::timestamptz
            )
        AND (post_state->>'started_eligible_at_utc')::timestamptz <=
            (post_state->>'extreme_eligible_at_utc')::timestamptz
        AND (post_state->>'extreme_eligible_at_utc')::timestamptz <=
            (post_state->>'last_member_eligible_at_utc')::timestamptz
        AND (post_state->>'started_decision_time_utc')::timestamptz >=
            (post_state->>'started_eligible_at_utc')::timestamptz
        AND (post_state->>'started_decision_time_utc')::timestamptz <
            (post_state->>'started_eligible_at_utc')::timestamptz
                + INTERVAL '30 minutes'
        AND (post_state->>'last_member_decision_time_utc')::timestamptz >=
            (post_state->>'last_member_eligible_at_utc')::timestamptz
        AND (post_state->>'last_member_decision_time_utc')::timestamptz <
            (post_state->>'last_member_eligible_at_utc')::timestamptz
                + INTERVAL '30 minutes'
        AND (post_state->>'last_member_eligible_at_utc')::timestamptz =
            (post_state->>'started_eligible_at_utc')::timestamptz
                + INTERVAL '30 minutes'
                    * ((post_state->>'member_count')::bigint - 1)
        AND (post_state->>'start_price')::numeric > 0
        AND (post_state->>'start_price')::numeric < 'Infinity'::numeric
        AND (post_state->>'extreme_price')::numeric > 0
        AND (post_state->>'extreme_price')::numeric < 'Infinity'::numeric
        AND (post_state->>'last_member_price')::numeric > 0
        AND (post_state->>'last_member_price')::numeric < 'Infinity'::numeric
    ) IS TRUE),
    CHECK ((
        ((post_state->>'member_count')::bigint <> 1)
        OR (
            post_state->>'started_anchor_id' =
                post_state->>'extreme_anchor_id'
            AND post_state->>'started_anchor_id' =
                post_state->>'last_member_anchor_id'
            AND (post_state->>'start_price')::numeric =
                (post_state->>'extreme_price')::numeric
            AND (post_state->>'start_price')::numeric =
                (post_state->>'last_member_price')::numeric
            AND post_state->>'direction' = 'PENDING'
            AND (post_state->>'consecutive_non_extremes')::integer = 0
        )
    ) IS TRUE),
    CHECK ((
        (post_state->>'direction' <> 'PENDING')
        OR (
            post_state->>'extreme_anchor_id' =
                post_state->>'started_anchor_id'
            AND (post_state->>'extreme_eligible_at_utc')::timestamptz =
                (post_state->>'started_eligible_at_utc')::timestamptz
            AND (post_state->>'extreme_price')::numeric =
                (post_state->>'start_price')::numeric
        )
    ) IS TRUE),
    CHECK ((
        (post_state->>'direction' <> 'UP'
         OR (post_state->>'extreme_price')::numeric >
            (post_state->>'start_price')::numeric)
        AND
        (post_state->>'direction' <> 'DOWN'
         OR (post_state->>'extreme_price')::numeric <
            (post_state->>'start_price')::numeric)
        AND
        ((post_state->>'consecutive_non_extremes')::integer <> 0
         OR (
            post_state->>'last_member_anchor_id' =
                post_state->>'extreme_anchor_id'
            AND (post_state->>'last_member_eligible_at_utc')::timestamptz =
                (post_state->>'extreme_eligible_at_utc')::timestamptz
            AND (post_state->>'last_member_price')::numeric =
                (post_state->>'extreme_price')::numeric
         ))
    ) IS TRUE),
    CHECK ((
        (post_state->>'status' = 'OPEN'
         AND post_state->>'closed_at_utc' IS NULL
         AND post_state->>'close_boundary_eligible_at_utc' IS NULL
         AND post_state->>'close_reason' IS NULL)
        OR
        (post_state->>'status' = 'CLOSED'
         AND post_state->>'closed_at_utc' IS NOT NULL
         AND post_state->>'close_boundary_eligible_at_utc' IS NOT NULL
         AND (post_state->>'close_boundary_eligible_at_utc')::timestamptz =
            DATE_BIN(
                INTERVAL '30 minutes',
                (post_state->>'close_boundary_eligible_at_utc')::timestamptz,
                '1970-01-01 00:02:00+00'::timestamptz
            )
         AND (post_state->>'closed_at_utc')::timestamptz >=
            (post_state->>'close_boundary_eligible_at_utc')::timestamptz
         AND post_state->>'close_reason' IN (
            'DATA_GAP_CENSORED', 'TWO_CONSECUTIVE_NON_EXTREMES'
         ))
    ) IS TRUE),
    CHECK ((
        (transition_type = 'MOVEMENT_CLOSED'
         AND post_state->>'status' = 'CLOSED'
         AND post_state->>'close_reason' IN (
             'DATA_GAP_CENSORED', 'TWO_CONSECUTIVE_NON_EXTREMES'
         ))
        OR
        (transition_type <> 'MOVEMENT_CLOSED'
         AND post_state->>'status' = 'OPEN'
         AND post_state->>'close_reason' IS NULL)
    ) IS TRUE)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_market_movement_transition_root
    ON research_market_movement_transitions (stream_id)
    WHERE previous_transition_receipt_sha256 IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_market_movement_transition_successor
    ON research_market_movement_transitions (
        previous_transition_receipt_sha256
    )
    WHERE previous_transition_receipt_sha256 IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_market_movement_transition_head
    ON research_market_movement_transitions (
        stream_id, chain_ordinal DESC
    );

CREATE TABLE IF NOT EXISTS research_market_movement_memberships (
    membership_receipt_sha256 CHAR(64) COLLATE pg_catalog."C" PRIMARY KEY CHECK (
        BTRIM(membership_receipt_sha256) ~ '^[0-9a-f]{64}$'
    ),
    emitted_by_transition_receipt_sha256 CHAR(64) COLLATE pg_catalog."C" NOT NULL UNIQUE
        REFERENCES research_market_movement_transitions(
            transition_receipt_sha256
        )
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    contract_version TEXT COLLATE pg_catalog."C" NOT NULL CHECK (
        contract_version = 'market-movement-v5-causal-neutral-price-wave'
    ),
    stream_id CHAR(64) COLLATE pg_catalog."C" NOT NULL CHECK (
        BTRIM(stream_id) ~ '^[0-9a-f]{64}$'
    ),
    movement_id CHAR(64) COLLATE pg_catalog."C" NOT NULL CHECK (
        BTRIM(movement_id) ~ '^[0-9a-f]{64}$'
    ),
    anchor_id CHAR(64) COLLATE pg_catalog."C" NOT NULL,
    anchor_receipt_sha256 CHAR(64) COLLATE pg_catalog."C" NOT NULL,
    ordinal BIGINT NOT NULL CHECK (ordinal > 0),
    classification TEXT COLLATE pg_catalog."C" NOT NULL CHECK (
        classification IN (
            'START', 'DIRECTIONAL_EXTREME',
            'EXTREME_EXTENSION', 'NON_EXTREME'
        )
    ),
    eligible_at_utc TIMESTAMPTZ NOT NULL,
    decision_time_utc TIMESTAMPTZ NOT NULL,
    price NUMERIC NOT NULL CHECK (
        price > 0 AND price < 'Infinity'::numeric
    ),
    membership_receipt JSONB NOT NULL CHECK (
        JSONB_TYPEOF(membership_receipt) = 'object'
    ),
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT CLOCK_TIMESTAMP(),
    FOREIGN KEY (anchor_id, anchor_receipt_sha256)
        REFERENCES research_neutral_price_anchors(
            anchor_id, anchor_receipt_sha256
        )
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (stream_id, anchor_id),
    UNIQUE (stream_id, movement_id, ordinal),
    CHECK (
        membership_receipt ?& ARRAY[
            'contract_version', 'membership_receipt_sha256', 'stream_id',
            'movement_id', 'anchor_id', 'anchor_receipt_sha256', 'ordinal',
            'classification', 'eligible_at_utc', 'decision_time_utc', 'price'
        ]
    ),
    CHECK ((
        membership_receipt->>'contract_version' = contract_version
        AND membership_receipt->>'membership_receipt_sha256' =
            BTRIM(membership_receipt_sha256)
        AND membership_receipt->>'stream_id' = BTRIM(stream_id)
        AND membership_receipt->>'movement_id' = BTRIM(movement_id)
        AND membership_receipt->>'anchor_id' = BTRIM(anchor_id)
        AND membership_receipt->>'anchor_receipt_sha256' =
            BTRIM(anchor_receipt_sha256)
        AND (membership_receipt->>'ordinal')::bigint = ordinal
        AND membership_receipt->>'classification' = classification
        AND (membership_receipt->>'eligible_at_utc')::timestamptz =
            eligible_at_utc
        AND (membership_receipt->>'decision_time_utc')::timestamptz =
            decision_time_utc
        AND (membership_receipt->>'price')::numeric = price
    ) IS TRUE)
);

CREATE INDEX IF NOT EXISTS idx_market_movement_membership_movement
    ON research_market_movement_memberships (
        stream_id, movement_id, ordinal
    );

CREATE INDEX IF NOT EXISTS idx_market_movement_membership_slot
    ON research_market_movement_memberships (eligible_at_utc, stream_id);

DO $shape$
DECLARE
    spec RECORD;
    actual_columns TEXT[];
    actual_check_fingerprints TEXT[];
    actual_index_signatures TEXT[];
    actual_count BIGINT;
    matched_count BIGINT;
    created_default TEXT;
BEGIN
    -- CREATE ... IF NOT EXISTS is safe only when a pre-existing relation is
    -- exactly the archive shape this writer expects.  Column names alone are
    -- insufficient: a stale nullable key or a widened receipt column would
    -- silently weaken the append-only projection.
    FOR spec IN
        SELECT *
          FROM (VALUES
            (
                'research_price_collection_attempts',
                ARRAY[
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
                ]::text[]
            ),
            (
                'research_neutral_price_anchors',
                ARRAY[
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
                ]::text[]
            ),
            (
                'research_market_movement_transitions',
                ARRAY[
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
                ]::text[]
            ),
            (
                'research_market_movement_memberships',
                ARRAY[
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
                ]::text[]
            )
          ) AS expected(relation_name, column_shape)
    LOOP
        IF NOT EXISTS (
            SELECT 1
              FROM pg_catalog.pg_class relation
             WHERE relation.oid =
                    pg_catalog.to_regclass('public.' || spec.relation_name)
               AND relation.relkind = 'r'
               AND relation.relpersistence = 'p'
        ) THEN
            RAISE EXCEPTION
                'Wave v5 replay requires permanent ordinary table public.%',
                spec.relation_name;
        END IF;

        IF EXISTS (
            SELECT 1
              FROM pg_catalog.pg_class relation
             WHERE relation.oid =
                    pg_catalog.to_regclass('public.' || spec.relation_name)
               AND (
                    relation.relispartition
                    OR EXISTS (
                        SELECT 1
                          FROM pg_catalog.pg_inherits inheritance
                         WHERE inheritance.inhrelid = relation.oid
                            OR inheritance.inhparent = relation.oid
                    )
               )
        ) THEN
            RAISE EXCEPTION
                'Wave v5 replay rejects inheritance/partitioning on public.%',
                spec.relation_name;
        END IF;

        SELECT pg_catalog.array_agg(
                   attribute.attname::text || '|' ||
                   pg_catalog.format_type(
                       attribute.atttypid, attribute.atttypmod
                   ) || '|' || attribute.attnotnull::text
                   ORDER BY attribute.attnum
               )
          INTO actual_columns
          FROM pg_catalog.pg_attribute attribute
         WHERE attribute.attrelid =
                pg_catalog.to_regclass('public.' || spec.relation_name)
           AND attribute.attnum > 0
           AND NOT attribute.attisdropped;
        IF actual_columns IS DISTINCT FROM spec.column_shape THEN
            RAISE EXCEPTION
                'Wave v5 replay found incompatible columns for public.%: expected %, got %',
                spec.relation_name,
                spec.column_shape,
                actual_columns;
        END IF;

        IF EXISTS (
            SELECT 1
              FROM pg_catalog.pg_attribute attribute
             WHERE attribute.attrelid =
                    pg_catalog.to_regclass('public.' || spec.relation_name)
               AND attribute.attnum > 0
               AND NOT attribute.attisdropped
               AND attribute.attcollation <> 0
               AND attribute.attcollation <>
                    'pg_catalog."C"'::pg_catalog.regcollation
        ) THEN
            RAISE EXCEPTION
                'Wave v5 replay requires byte-exact C collation on public.% text identities',
                spec.relation_name;
        END IF;

        IF EXISTS (
            SELECT 1
              FROM pg_catalog.pg_attribute attribute
             WHERE attribute.attrelid =
                    pg_catalog.to_regclass('public.' || spec.relation_name)
               AND attribute.attnum > 0
               AND NOT attribute.attisdropped
               AND (
                    attribute.attidentity <> ''
                    OR attribute.attgenerated <> ''
               )
        ) THEN
            RAISE EXCEPTION
                'Wave v5 replay rejects generated/identity columns on public.%',
                spec.relation_name;
        END IF;

        SELECT pg_catalog.count(*),
               pg_catalog.max(
                   CASE WHEN attribute.attname = 'created_at_utc'
                        THEN pg_catalog.pg_get_expr(
                            default_value.adbin, default_value.adrelid
                        )
                   END
               )
          INTO actual_count, created_default
          FROM pg_catalog.pg_attrdef default_value
          JOIN pg_catalog.pg_attribute attribute
            ON attribute.attrelid = default_value.adrelid
           AND attribute.attnum = default_value.adnum
         WHERE default_value.adrelid =
                pg_catalog.to_regclass('public.' || spec.relation_name);
        IF actual_count <> 1
           OR created_default IS DISTINCT FROM 'clock_timestamp()' THEN
            RAISE EXCEPTION
                'Wave v5 replay found incompatible defaults for public.%',
                spec.relation_name;
        END IF;
    END LOOP;

    -- CHECK constraints carry most of this archive's semantic integrity.
    -- Compare a deterministic multiset of normalized definition
    -- fingerprints, not generated constraint names, so CREATE TABLE IF NOT
    -- EXISTS cannot silently accept a stale or weakened pre-existing table.
    -- Include normalized length as an independent guard around core md5().
    FOR spec IN
        SELECT *
          FROM (VALUES
            (
                'research_price_collection_attempts',
                ARRAY[
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
                ]::text[]
            ),
            (
                'research_neutral_price_anchors',
                ARRAY[
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
                ]::text[]
            ),
            (
                'research_market_movement_transitions',
                ARRAY[
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
                ]::text[]
            ),
            (
                'research_market_movement_memberships',
                ARRAY[
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
                ]::text[]
            )
          ) AS expected(relation_name, check_fingerprints)
    LOOP
        SELECT pg_catalog.array_agg(
                   fingerprint ORDER BY fingerprint COLLATE pg_catalog."C"
               ),
               pg_catalog.count(*),
               pg_catalog.count(*) FILTER (
                   WHERE constraint_row.convalidated
                     AND constraint_row.conislocal
                     AND constraint_row.coninhcount = 0
                     AND NOT constraint_row.connoinherit
                     AND COALESCE(
                         (
                             pg_catalog.to_jsonb(constraint_row)
                                 ->> 'conenforced'
                         )::boolean,
                         TRUE
                     )
               )
          INTO actual_check_fingerprints, actual_count, matched_count
          FROM pg_catalog.pg_constraint constraint_row
          CROSS JOIN LATERAL (
              SELECT
                  pg_catalog.length(normalized_definition)::text || ':' ||
                  pg_catalog.md5(normalized_definition) AS fingerprint
                FROM (
                    SELECT pg_catalog.regexp_replace(
                               pg_catalog.pg_get_constraintdef(
                                   constraint_row.oid, FALSE
                               ),
                               '[[:space:]]+', '', 'g'
                           ) AS normalized_definition
                ) normalized
          ) fingerprint_row
         WHERE constraint_row.conrelid =
                pg_catalog.to_regclass('public.' || spec.relation_name)
           AND constraint_row.contype::text = 'c';
        IF actual_count <> pg_catalog.cardinality(spec.check_fingerprints)
           OR matched_count <> actual_count
           OR actual_check_fingerprints IS DISTINCT FROM
                spec.check_fingerprints THEN
            RAISE EXCEPTION
                'Wave v5 replay found incompatible CHECK constraints on public.%: expected %, got %',
                spec.relation_name,
                spec.check_fingerprints,
                actual_check_fingerprints;
        END IF;
    END LOOP;

    -- Compare key/FK semantics through catalog attnums, not names.  Constraint
    -- names can differ on an equivalent restored schema, but missing or extra
    -- PK/UQ/FK behavior must fail before the writer is granted access.
    FOR spec IN
        SELECT *
          FROM (VALUES
            ('research_price_collection_attempts', 'p',
             ARRAY['attempt_receipt_sha256']::text[], NULL::text,
             NULL::text[], FALSE, FALSE, NULL::text),
            ('research_price_collection_attempts', 'f',
             ARRAY['anchor_id', 'anchor_receipt_sha256']::text[],
             'research_neutral_price_anchors',
             ARRAY['anchor_id', 'anchor_receipt_sha256']::text[],
             TRUE, TRUE, 'r'),

            ('research_neutral_price_anchors', 'p',
             ARRAY['anchor_id']::text[], NULL::text,
             NULL::text[], FALSE, FALSE, NULL::text),
            ('research_neutral_price_anchors', 'u',
             ARRAY['anchor_receipt_sha256']::text[], NULL::text,
             NULL::text[], FALSE, FALSE, NULL::text),
            ('research_neutral_price_anchors', 'u',
             ARRAY['contract_version', 'symbol', 'eligible_at_utc']::text[],
             NULL::text, NULL::text[], FALSE, FALSE, NULL::text),
            ('research_neutral_price_anchors', 'u',
             ARRAY['anchor_id', 'anchor_receipt_sha256']::text[],
             NULL::text, NULL::text[], FALSE, FALSE, NULL::text),

            ('research_market_movement_transitions', 'p',
             ARRAY['transition_receipt_sha256']::text[], NULL::text,
             NULL::text[], FALSE, FALSE, NULL::text),
            ('research_market_movement_transitions', 'u',
             ARRAY['stream_id', 'chain_ordinal']::text[], NULL::text,
             NULL::text[], FALSE, FALSE, NULL::text),
            ('research_market_movement_transitions', 'u',
             ARRAY['stream_id', 'trigger_anchor_id', 'transition_type']::text[],
             NULL::text, NULL::text[], FALSE, FALSE, NULL::text),
            ('research_market_movement_transitions', 'f',
             ARRAY['previous_transition_receipt_sha256']::text[],
             'research_market_movement_transitions',
             ARRAY['transition_receipt_sha256']::text[], TRUE, TRUE, 'r'),
            ('research_market_movement_transitions', 'f',
             ARRAY['trigger_anchor_id']::text[],
             'research_neutral_price_anchors', ARRAY['anchor_id']::text[],
             FALSE, FALSE, 'r'),

            ('research_market_movement_memberships', 'p',
             ARRAY['membership_receipt_sha256']::text[], NULL::text,
             NULL::text[], FALSE, FALSE, NULL::text),
            ('research_market_movement_memberships', 'u',
             ARRAY['emitted_by_transition_receipt_sha256']::text[],
             NULL::text, NULL::text[], FALSE, FALSE, NULL::text),
            ('research_market_movement_memberships', 'u',
             ARRAY['stream_id', 'anchor_id']::text[], NULL::text,
             NULL::text[], FALSE, FALSE, NULL::text),
            ('research_market_movement_memberships', 'u',
             ARRAY['stream_id', 'movement_id', 'ordinal']::text[],
             NULL::text, NULL::text[], FALSE, FALSE, NULL::text),
            ('research_market_movement_memberships', 'f',
             ARRAY['emitted_by_transition_receipt_sha256']::text[],
             'research_market_movement_transitions',
             ARRAY['transition_receipt_sha256']::text[], TRUE, TRUE, 'r'),
            ('research_market_movement_memberships', 'f',
             ARRAY['anchor_id', 'anchor_receipt_sha256']::text[],
             'research_neutral_price_anchors',
             ARRAY['anchor_id', 'anchor_receipt_sha256']::text[],
             TRUE, TRUE, 'r')
          ) AS expected(
              relation_name, constraint_type, local_columns,
              reference_relation, reference_columns,
              is_deferrable, is_deferred, delete_action
          )
    LOOP
        SELECT pg_catalog.count(*)
          INTO matched_count
          FROM pg_catalog.pg_constraint constraint_row
         WHERE constraint_row.conrelid =
                pg_catalog.to_regclass('public.' || spec.relation_name)
           AND constraint_row.contype::text = spec.constraint_type
           AND constraint_row.convalidated
           AND COALESCE(
               (
                   pg_catalog.to_jsonb(constraint_row)
                       ->> 'conenforced'
               )::boolean,
               TRUE
           )
           AND constraint_row.condeferrable = spec.is_deferrable
           AND constraint_row.condeferred = spec.is_deferred
           AND (
                SELECT pg_catalog.array_agg(
                           attribute.attname::text
                           ORDER BY key_column.ordinality
                       )
                  FROM pg_catalog.unnest(constraint_row.conkey)
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
                        SELECT pg_catalog.array_agg(
                                   attribute.attname::text
                                   ORDER BY key_column.ordinality
                               )
                          FROM pg_catalog.unnest(constraint_row.confkey)
                               WITH ORDINALITY AS key_column(
                                   attnum, ordinality
                               )
                          JOIN pg_catalog.pg_attribute attribute
                            ON attribute.attrelid = constraint_row.confrelid
                           AND attribute.attnum = key_column.attnum
                       ) = spec.reference_columns
                    AND constraint_row.confdeltype::text = spec.delete_action
                    AND constraint_row.confupdtype::text = 'a'
                    AND constraint_row.confmatchtype::text = 's'
                )
           );
        IF matched_count <> 1 THEN
            RAISE EXCEPTION
                'Wave v5 replay found missing/ambiguous % constraint on public.% (%)',
                spec.constraint_type,
                spec.relation_name,
                spec.local_columns;
        END IF;
    END LOOP;

    FOR spec IN
        SELECT *
          FROM (VALUES
            ('research_price_collection_attempts', 2::bigint),
            ('research_neutral_price_anchors', 4::bigint),
            ('research_market_movement_transitions', 5::bigint),
            ('research_market_movement_memberships', 6::bigint)
          ) AS expected(relation_name, constraint_count)
    LOOP
        SELECT pg_catalog.count(*)
          INTO actual_count
          FROM pg_catalog.pg_constraint constraint_row
         WHERE constraint_row.conrelid =
                pg_catalog.to_regclass('public.' || spec.relation_name)
           AND constraint_row.contype::text IN ('p', 'u', 'f');
        IF actual_count <> spec.constraint_count THEN
            RAISE EXCEPTION
                'Wave v5 replay found unexpected PK/UQ/FK count on public.%: expected %, got %',
                spec.relation_name,
                spec.constraint_count,
                actual_count;
        END IF;
    END LOOP;

    -- The three partial unique indexes carry integrity semantics that ordinary
    -- constraints cannot express.  A same-named stale index must not make an
    -- IF NOT EXISTS replay look successful.
    FOR spec IN
        SELECT *
          FROM (VALUES
            ('research_price_collection_attempts',
             'uq_neutral_price_attempt_evaluable_anchor',
             ARRAY['anchor_id']::text[],
             'evaluation_status=''EVALUABLE''::text'),
            ('research_market_movement_transitions',
             'uq_market_movement_transition_root',
             ARRAY['stream_id']::text[],
             'previous_transition_receipt_sha256ISNULL'),
            ('research_market_movement_transitions',
             'uq_market_movement_transition_successor',
             ARRAY['previous_transition_receipt_sha256']::text[],
             'previous_transition_receipt_sha256ISNOTNULL')
          ) AS expected(
              relation_name, index_name, key_columns, normalized_predicate
          )
    LOOP
        SELECT pg_catalog.count(*)
          INTO matched_count
          FROM pg_catalog.pg_index index_row
          JOIN pg_catalog.pg_class index_relation
            ON index_relation.oid = index_row.indexrelid
          JOIN pg_catalog.pg_am access_method
            ON access_method.oid = index_relation.relam
         WHERE index_row.indexrelid =
                pg_catalog.to_regclass('public.' || spec.index_name)
           AND index_row.indrelid =
                pg_catalog.to_regclass('public.' || spec.relation_name)
           AND index_relation.relkind = 'i'
           AND index_relation.relpersistence = 'p'
           AND access_method.amname = 'btree'
           AND index_row.indisunique
           AND index_row.indisvalid
           AND index_row.indisready
           AND index_row.indexprs IS NULL
           AND index_row.indnatts = index_row.indnkeyatts
           AND index_row.indnkeyatts =
                pg_catalog.cardinality(spec.key_columns)
           AND (
                SELECT pg_catalog.array_agg(
                           attribute.attname::text
                           ORDER BY key_column.ordinality
                       )
                  FROM pg_catalog.unnest(index_row.indkey::smallint[])
                       WITH ORDINALITY AS key_column(attnum, ordinality)
                  JOIN pg_catalog.pg_attribute attribute
                    ON attribute.attrelid = index_row.indrelid
                   AND attribute.attnum = key_column.attnum
               ) = spec.key_columns
           AND pg_catalog.btrim(
                   pg_catalog.regexp_replace(
                       pg_catalog.pg_get_expr(
                           index_row.indpred, index_row.indrelid, FALSE
                       ),
                       '[[:space:]]+', '', 'g'
                   ),
                   '()'
               ) = spec.normalized_predicate;
        IF matched_count <> 1 THEN
            RAISE EXCEPTION
                'Wave v5 replay found incompatible partial unique index public.%',
                spec.index_name;
        END IF;
    END LOOP;

    -- Validate the complete index inventory independently of generated index
    -- names. This covers constraint-backing indexes and the four supporting
    -- indexes, and rejects stale expression/unique/partial indexes that could
    -- execute code or change admissible archive writes.
    FOR spec IN
        SELECT *
          FROM (VALUES
            (
                'research_price_collection_attempts',
                ARRAY[
                    'N|N|symbol:0,eligible_at_utc:0,decision_time_utc:0|-',
                    'N|U|anchor_id:0|(evaluation_status=''EVALUABLE''::text)',
                    'P|U|attempt_receipt_sha256:0|-'
                ]::text[]
            ),
            (
                'research_neutral_price_anchors',
                ARRAY[
                    'N|N|symbol:0,eligible_at_utc:0,anchor_id:0|-',
                    'N|U|anchor_id:0,anchor_receipt_sha256:0|-',
                    'N|U|anchor_receipt_sha256:0|-',
                    'N|U|contract_version:0,symbol:0,eligible_at_utc:0|-',
                    'P|U|anchor_id:0|-'
                ]::text[]
            ),
            (
                'research_market_movement_transitions',
                ARRAY[
                    'N|N|stream_id:0,chain_ordinal:3|-',
                    'N|U|previous_transition_receipt_sha256:0|(previous_transition_receipt_sha256ISNOTNULL)',
                    'N|U|stream_id:0,chain_ordinal:0|-',
                    'N|U|stream_id:0,trigger_anchor_id:0,transition_type:0|-',
                    'N|U|stream_id:0|(previous_transition_receipt_sha256ISNULL)',
                    'P|U|transition_receipt_sha256:0|-'
                ]::text[]
            ),
            (
                'research_market_movement_memberships',
                ARRAY[
                    'N|N|eligible_at_utc:0,stream_id:0|-',
                    'N|N|stream_id:0,movement_id:0,ordinal:0|-',
                    'N|U|emitted_by_transition_receipt_sha256:0|-',
                    'N|U|stream_id:0,anchor_id:0|-',
                    'N|U|stream_id:0,movement_id:0,ordinal:0|-',
                    'P|U|membership_receipt_sha256:0|-'
                ]::text[]
            )
          ) AS expected(relation_name, index_signatures)
    LOOP
        IF EXISTS (
            SELECT 1
              FROM pg_catalog.pg_index index_row
              JOIN pg_catalog.pg_class index_relation
                ON index_relation.oid = index_row.indexrelid
              JOIN pg_catalog.pg_am access_method
                ON access_method.oid = index_relation.relam
             WHERE index_row.indrelid = pg_catalog.to_regclass(
                       'public.' || spec.relation_name
                   )
               AND (
                    index_relation.relkind <> 'i'
                    OR index_relation.relpersistence <> 'p'
                    OR access_method.amname <> 'btree'
                    OR NOT index_row.indisvalid
                    OR NOT index_row.indisready
                    OR NOT index_row.indislive
                    OR NOT index_row.indimmediate
                    OR index_row.indisexclusion
                    OR index_row.indcheckxmin
                    OR index_row.indisreplident
                    OR index_row.indexprs IS NOT NULL
                    OR index_row.indnatts <> index_row.indnkeyatts
                    OR COALESCE(
                        (
                            pg_catalog.to_jsonb(index_row)
                                ->> 'indnullsnotdistinct'
                        )::boolean,
                        FALSE
                    )
                    OR EXISTS (
                        SELECT 1
                          FROM pg_catalog.unnest(
                                   index_row.indkey::smallint[]
                               ) WITH ORDINALITY AS key_column(
                                   attnum, ordinality
                               )
                          LEFT JOIN pg_catalog.pg_attribute attribute
                            ON attribute.attrelid = index_row.indrelid
                           AND attribute.attnum = key_column.attnum
                          LEFT JOIN pg_catalog.pg_opclass operator_class
                            ON operator_class.oid =
                               (index_row.indclass::oid[])[
                                   key_column.ordinality - 1
                               ]
                         WHERE key_column.attnum <= 0
                            OR attribute.attnum IS NULL
                            OR operator_class.oid IS NULL
                            OR NOT operator_class.opcdefault
                            OR operator_class.opcmethod <>
                                 index_relation.relam
                            OR (index_row.indcollation::oid[])[
                                   key_column.ordinality - 1
                               ] <>
                                 attribute.attcollation
                    )
               )
        ) THEN
            RAISE EXCEPTION
                'Wave v5 replay found unsafe index structure on public.%',
                spec.relation_name;
        END IF;

        SELECT pg_catalog.array_agg(
                   signature ORDER BY signature COLLATE pg_catalog."C"
               )
          INTO actual_index_signatures
          FROM pg_catalog.pg_index index_row
          CROSS JOIN LATERAL (
              SELECT pg_catalog.array_to_string(
                         pg_catalog.array_agg(
                             attribute.attname::text || ':' ||
                             (index_row.indoption::smallint[])[
                                 key_column.ordinality - 1
                             ]::text
                             ORDER BY key_column.ordinality
                         ),
                         ','
                     ) AS key_signature
                FROM pg_catalog.unnest(
                         index_row.indkey::smallint[]
                     ) WITH ORDINALITY AS key_column(
                         attnum, ordinality
                     )
                JOIN pg_catalog.pg_attribute attribute
                  ON attribute.attrelid = index_row.indrelid
                 AND attribute.attnum = key_column.attnum
          ) key_columns
          CROSS JOIN LATERAL (
              SELECT
                  (CASE WHEN index_row.indisprimary THEN 'P' ELSE 'N' END)
                  || '|' ||
                  (CASE WHEN index_row.indisunique THEN 'U' ELSE 'N' END)
                  || '|' || key_columns.key_signature || '|' ||
                  COALESCE(
                      pg_catalog.regexp_replace(
                          pg_catalog.pg_get_expr(
                              index_row.indpred,
                              index_row.indrelid,
                              FALSE
                          ),
                          '[[:space:]]+', '', 'g'
                      ),
                      '-'
                  ) AS signature
          ) signature_row
         WHERE index_row.indrelid = pg_catalog.to_regclass(
                   'public.' || spec.relation_name
               );
        IF actual_index_signatures IS DISTINCT FROM spec.index_signatures THEN
            RAISE EXCEPTION
                'Wave v5 replay found incompatible index inventory on public.%: expected %, got %',
                spec.relation_name,
                spec.index_signatures,
                actual_index_signatures;
        END IF;
    END LOOP;
END;
$shape$;

CREATE OR REPLACE FUNCTION assert_market_movement_writer_v5()
RETURNS TRIGGER AS $$
BEGIN
    IF session_user <> 'research_market_movement_writer_v5'
       OR current_user <> 'research_market_movement_writer_v5' THEN
        RAISE EXCEPTION
            'Wave v5 INSERT requires dedicated session research_market_movement_writer_v5'
            USING ERRCODE = '42501';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog;

CREATE OR REPLACE FUNCTION prevent_market_movement_archive_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME;
END;
$$ LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog;

CREATE OR REPLACE FUNCTION assert_neutral_price_attempt_anchor_complete()
RETURNS TRIGGER AS $$
DECLARE
    target_anchor_id CHAR(64);
    target_anchor_receipt CHAR(64);
    target_origin TEXT;
    anchor_row public.research_neutral_price_anchors%ROWTYPE;
    attempt_row public.research_price_collection_attempts%ROWTYPE;
BEGIN
    IF TG_TABLE_NAME = 'research_price_collection_attempts' THEN
        IF NEW.evaluation_status <> 'EVALUABLE' THEN
            RETURN NULL;
        END IF;
        target_anchor_id := NEW.anchor_id;
        target_anchor_receipt := NEW.anchor_receipt_sha256;
        SELECT * INTO anchor_row
          FROM public.research_neutral_price_anchors anchor
         WHERE anchor.anchor_id = target_anchor_id
           AND anchor.anchor_receipt_sha256 = target_anchor_receipt;
        IF anchor_row.anchor_id IS NULL
           OR anchor_row.origin IS DISTINCT FROM 'PROSPECTIVE_V5'
           OR anchor_row.symbol IS DISTINCT FROM NEW.symbol
           OR anchor_row.eligible_at_utc IS DISTINCT FROM NEW.eligible_at_utc
           OR anchor_row.decision_time_utc IS DISTINCT FROM
                NEW.decision_time_utc THEN
            RAISE EXCEPTION
                'EVALUABLE neutral-price attempt % lacks its exact anchor projection',
                NEW.attempt_receipt_sha256;
        END IF;
    ELSE
        target_anchor_id := NEW.anchor_id;
        target_anchor_receipt := NEW.anchor_receipt_sha256;
        target_origin := NEW.origin;
        IF target_origin = 'PROSPECTIVE_V5' THEN
            SELECT * INTO attempt_row
              FROM public.research_price_collection_attempts attempt
             WHERE attempt.evaluation_status = 'EVALUABLE'
               AND attempt.anchor_id = target_anchor_id
               AND attempt.anchor_receipt_sha256 = target_anchor_receipt;
            IF attempt_row.attempt_receipt_sha256 IS NULL
               OR attempt_row.symbol IS DISTINCT FROM NEW.symbol
               OR attempt_row.eligible_at_utc IS DISTINCT FROM
                    NEW.eligible_at_utc
               OR attempt_row.decision_time_utc IS DISTINCT FROM
                    NEW.decision_time_utc THEN
                RAISE EXCEPTION
                    'Prospective neutral-price anchor % lacks its exact EVALUABLE attempt',
                    target_anchor_id;
            END IF;
        END IF;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog;

CREATE OR REPLACE FUNCTION validate_market_movement_transition_insert()
RETURNS TRIGGER AS $$
DECLARE
    anchor_row public.research_neutral_price_anchors%ROWTYPE;
    existing_row public.research_market_movement_transitions%ROWTYPE;
    previous_row public.research_market_movement_transitions%ROWTYPE;
    expected_anchor_id CHAR(64);
    expected_open_type TEXT;
BEGIN
    SELECT * INTO existing_row
      FROM public.research_market_movement_transitions
     WHERE transition_receipt_sha256 = NEW.transition_receipt_sha256;
    IF FOUND THEN
        IF existing_row.contract_version IS NOT DISTINCT FROM
                NEW.contract_version
           AND existing_row.previous_transition_receipt_sha256
                IS NOT DISTINCT FROM
                NEW.previous_transition_receipt_sha256
           AND existing_row.chain_ordinal IS NOT DISTINCT FROM
                NEW.chain_ordinal
           AND existing_row.transition_type IS NOT DISTINCT FROM
                NEW.transition_type
           AND existing_row.stream_id IS NOT DISTINCT FROM NEW.stream_id
           AND existing_row.namespace IS NOT DISTINCT FROM NEW.namespace
           AND existing_row.symbol IS NOT DISTINCT FROM NEW.symbol
           AND existing_row.movement_id IS NOT DISTINCT FROM NEW.movement_id
           AND existing_row.trigger_anchor_id IS NOT DISTINCT FROM
                NEW.trigger_anchor_id
           AND existing_row.trigger_eligible_at_utc IS NOT DISTINCT FROM
                NEW.trigger_eligible_at_utc
           AND existing_row.trigger_decision_time_utc IS NOT DISTINCT FROM
                NEW.trigger_decision_time_utc
           AND existing_row.pre_state_sha256 IS NOT DISTINCT FROM
                NEW.pre_state_sha256
           AND existing_row.post_state_sha256 IS NOT DISTINCT FROM
                NEW.post_state_sha256
           AND existing_row.post_state IS NOT DISTINCT FROM NEW.post_state
           AND existing_row.transition_receipt IS NOT DISTINCT FROM
                NEW.transition_receipt THEN
            RETURN NEW;
        END IF;
        RAISE EXCEPTION
            'Wave transition receipt % conflicts with its stored row',
            NEW.transition_receipt_sha256;
    END IF;

    SELECT * INTO STRICT anchor_row
      FROM public.research_neutral_price_anchors
     WHERE anchor_id = NEW.trigger_anchor_id;
    IF anchor_row.symbol IS DISTINCT FROM NEW.symbol
       OR anchor_row.eligible_at_utc IS DISTINCT FROM
            NEW.trigger_eligible_at_utc
       OR anchor_row.decision_time_utc IS DISTINCT FROM
            NEW.trigger_decision_time_utc THEN
        RAISE EXCEPTION
            'Wave transition % does not project its trigger anchor',
            NEW.transition_receipt_sha256;
    END IF;
    IF NEW.namespace = 'BTC_PARENT' AND NEW.symbol <> 'BTC' THEN
        RAISE EXCEPTION 'BTC_PARENT accepts only BTC anchors';
    END IF;
    IF NEW.transition_type <> 'MOVEMENT_CLOSED' AND (
        NEW.post_state->>'last_member_anchor_id' IS DISTINCT FROM
            BTRIM(NEW.trigger_anchor_id)
        OR (NEW.post_state->>'last_member_eligible_at_utc')::timestamptz
            IS DISTINCT FROM NEW.trigger_eligible_at_utc
        OR (NEW.post_state->>'last_member_decision_time_utc')::timestamptz
            IS DISTINCT FROM NEW.trigger_decision_time_utc
        OR (NEW.post_state->>'last_member_price')::numeric
            IS DISTINCT FROM anchor_row.price
    ) THEN
        RAISE EXCEPTION
            'Wave transition % does not make its trigger the last member',
            NEW.transition_receipt_sha256;
    END IF;
    IF NEW.transition_type IN (
        'OPENED', 'OPENED_AFTER_DATA_GAP', 'OPENED_AFTER_DIRECTION_END'
    ) AND (
        (NEW.post_state->>'member_count')::bigint IS DISTINCT FROM 1::bigint
        OR NEW.post_state->>'started_anchor_id' IS DISTINCT FROM
            BTRIM(NEW.trigger_anchor_id)
        OR (NEW.post_state->>'started_eligible_at_utc')::timestamptz
            IS DISTINCT FROM NEW.trigger_eligible_at_utc
        OR (NEW.post_state->>'started_decision_time_utc')::timestamptz
            IS DISTINCT FROM NEW.trigger_decision_time_utc
        OR (NEW.post_state->>'start_price')::numeric IS DISTINCT FROM
            anchor_row.price
    ) THEN
        RAISE EXCEPTION
            'Wave opening % is not seeded exactly by its trigger anchor',
            NEW.transition_receipt_sha256;
    END IF;

    SELECT anchor.anchor_id INTO expected_anchor_id
      FROM public.research_neutral_price_anchors anchor
     WHERE anchor.symbol = NEW.symbol
       AND NOT EXISTS (
           SELECT 1
             FROM public.research_market_movement_memberships membership
            WHERE membership.stream_id = NEW.stream_id
              AND membership.anchor_id = anchor.anchor_id
       )
     ORDER BY anchor.eligible_at_utc, anchor.anchor_id
     LIMIT 1;
    IF expected_anchor_id IS NULL
       OR expected_anchor_id IS DISTINCT FROM NEW.trigger_anchor_id THEN
        RAISE EXCEPTION
            'Wave transition % skipped earliest pending anchor (expected %, got %)',
            NEW.transition_receipt_sha256,
            expected_anchor_id,
            NEW.trigger_anchor_id;
    END IF;

    IF NEW.previous_transition_receipt_sha256 IS NULL THEN
        IF NEW.chain_ordinal <> 1
           OR NEW.transition_type <> 'OPENED'
           OR NEW.pre_state_sha256 IS NOT NULL
           OR EXISTS (
               SELECT 1
                 FROM public.research_market_movement_transitions existing
                WHERE existing.stream_id = NEW.stream_id
           ) THEN
            RAISE EXCEPTION 'Invalid Wave v5 root transition';
        END IF;
        RETURN NEW;
    END IF;

    SELECT * INTO STRICT previous_row
      FROM public.research_market_movement_transitions
     WHERE transition_receipt_sha256 =
           NEW.previous_transition_receipt_sha256
     FOR KEY SHARE;
    IF previous_row.stream_id IS DISTINCT FROM NEW.stream_id
       OR previous_row.namespace IS DISTINCT FROM NEW.namespace
       OR previous_row.symbol IS DISTINCT FROM NEW.symbol
       OR NEW.chain_ordinal <> previous_row.chain_ordinal + 1 THEN
        RAISE EXCEPTION 'Wave v5 transition predecessor crosses its stream';
    END IF;

    IF NEW.transition_type IN (
        'OPENED_AFTER_DATA_GAP', 'OPENED_AFTER_DIRECTION_END'
    ) THEN
        expected_open_type := CASE
            WHEN previous_row.post_state->>'close_reason' =
                 'DATA_GAP_CENSORED'
                THEN 'OPENED_AFTER_DATA_GAP'
            WHEN previous_row.post_state->>'close_reason' =
                 'TWO_CONSECUTIVE_NON_EXTREMES'
                THEN 'OPENED_AFTER_DIRECTION_END'
            ELSE NULL
        END;
        IF previous_row.transition_type <> 'MOVEMENT_CLOSED'
           OR previous_row.post_state->>'status' <> 'CLOSED'
           OR expected_open_type IS DISTINCT FROM NEW.transition_type
           OR previous_row.trigger_anchor_id IS DISTINCT FROM
                NEW.trigger_anchor_id
           OR previous_row.trigger_eligible_at_utc IS DISTINCT FROM
                NEW.trigger_eligible_at_utc
           OR NEW.pre_state_sha256 IS NOT NULL
           OR NEW.movement_id = previous_row.movement_id THEN
            RAISE EXCEPTION 'Invalid Wave v5 close/open rollover';
        END IF;
    ELSE
        IF NEW.pre_state_sha256 IS DISTINCT FROM
                previous_row.post_state_sha256
           OR previous_row.post_state->>'status' <> 'OPEN'
           OR NEW.movement_id IS DISTINCT FROM previous_row.movement_id
           OR NEW.trigger_eligible_at_utc <=
                previous_row.trigger_eligible_at_utc THEN
            RAISE EXCEPTION 'Invalid Wave v5 state-chain projection';
        END IF;
        IF NEW.transition_type = 'MOVEMENT_CLOSED' THEN
            IF (NEW.post_state->>'closed_at_utc')::timestamptz
                    IS DISTINCT FROM NEW.trigger_decision_time_utc
               OR (
                    NEW.post_state - ARRAY[
                        'status', 'closed_at_utc',
                        'close_boundary_eligible_at_utc', 'close_reason',
                        'state_sha256'
                    ]::text[]
                  ) IS DISTINCT FROM (
                    previous_row.post_state - ARRAY[
                        'status', 'closed_at_utc',
                        'close_boundary_eligible_at_utc', 'close_reason',
                        'state_sha256'
                    ]::text[]
                  ) THEN
                RAISE EXCEPTION
                    'Wave close % rewrites pre-boundary movement state',
                    NEW.transition_receipt_sha256;
            END IF;
            IF NEW.post_state->>'close_reason' = 'DATA_GAP_CENSORED' THEN
                IF (NEW.post_state->>'close_boundary_eligible_at_utc')::timestamptz
                        IS DISTINCT FROM
                        (previous_row.post_state->>'last_member_eligible_at_utc')::timestamptz
                            + INTERVAL '30 minutes'
                   OR NEW.trigger_eligible_at_utc <=
                        (NEW.post_state->>'close_boundary_eligible_at_utc')::timestamptz
                THEN
                    RAISE EXCEPTION 'Invalid Wave v5 data-gap boundary';
                END IF;
            ELSIF NEW.post_state->>'close_reason' =
                    'TWO_CONSECUTIVE_NON_EXTREMES' THEN
                IF (NEW.post_state->>'close_boundary_eligible_at_utc')::timestamptz
                        IS DISTINCT FROM NEW.trigger_eligible_at_utc
                   OR NEW.trigger_eligible_at_utc IS DISTINCT FROM
                        (previous_row.post_state->>'last_member_eligible_at_utc')::timestamptz
                            + INTERVAL '30 minutes'
                THEN
                    RAISE EXCEPTION 'Invalid Wave v5 direction-end boundary';
                END IF;
            END IF;
        ELSIF NEW.trigger_eligible_at_utc IS DISTINCT FROM
                (previous_row.post_state->>'last_member_eligible_at_utc')::timestamptz
                    + INTERVAL '30 minutes' THEN
            RAISE EXCEPTION
                'Wave continuation is not the next eligibility lattice point';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog;

CREATE OR REPLACE FUNCTION assert_market_movement_receipt_complete()
RETURNS TRIGGER AS $$
DECLARE
    target_transition_sha CHAR(64);
    transition_row public.research_market_movement_transitions%ROWTYPE;
    membership_row public.research_market_movement_memberships%ROWTYPE;
    anchor_row public.research_neutral_price_anchors%ROWTYPE;
    successor_row public.research_market_movement_transitions%ROWTYPE;
    transition_namespace TEXT;
    expected_classification TEXT;
    expected_successor_type TEXT;
BEGIN
    target_transition_sha := CASE
        WHEN TG_TABLE_NAME = 'research_market_movement_memberships'
            THEN BTRIM(
                TO_JSONB(NEW)->>'emitted_by_transition_receipt_sha256'
            )::CHAR(64)
        ELSE BTRIM(
            TO_JSONB(NEW)->>'transition_receipt_sha256'
        )::CHAR(64)
    END;
    SELECT * INTO STRICT transition_row
      FROM public.research_market_movement_transitions
     WHERE transition_receipt_sha256 = target_transition_sha;

    IF transition_row.transition_type = 'MOVEMENT_CLOSED' THEN
        IF EXISTS (
            SELECT 1
              FROM public.research_market_movement_memberships membership
             WHERE membership.emitted_by_transition_receipt_sha256 =
                   target_transition_sha
        ) THEN
            RAISE EXCEPTION 'MOVEMENT_CLOSED cannot emit a membership';
        END IF;
        SELECT * INTO successor_row
          FROM public.research_market_movement_transitions successor
         WHERE successor.previous_transition_receipt_sha256 =
               target_transition_sha;
        expected_successor_type := CASE
            WHEN transition_row.post_state->>'close_reason' =
                 'DATA_GAP_CENSORED'
                THEN 'OPENED_AFTER_DATA_GAP'
            WHEN transition_row.post_state->>'close_reason' =
                 'TWO_CONSECUTIVE_NON_EXTREMES'
                THEN 'OPENED_AFTER_DIRECTION_END'
            ELSE NULL
        END;
        IF successor_row.transition_receipt_sha256 IS NULL
           OR successor_row.transition_type IS DISTINCT FROM
                expected_successor_type
           OR successor_row.trigger_anchor_id IS DISTINCT FROM
                transition_row.trigger_anchor_id THEN
            RAISE EXCEPTION
                'MOVEMENT_CLOSED % lacks its atomic opening successor',
                target_transition_sha;
        END IF;
        RETURN NULL;
    END IF;

    SELECT * INTO membership_row
      FROM public.research_market_movement_memberships membership
     WHERE membership.emitted_by_transition_receipt_sha256 =
           target_transition_sha;
    expected_classification := CASE transition_row.transition_type
        WHEN 'OPENED' THEN 'START'
        WHEN 'OPENED_AFTER_DATA_GAP' THEN 'START'
        WHEN 'OPENED_AFTER_DIRECTION_END' THEN 'START'
        WHEN 'DIRECTION_ESTABLISHED' THEN 'DIRECTIONAL_EXTREME'
        WHEN 'EXTREME_EXTENDED' THEN 'EXTREME_EXTENSION'
        WHEN 'NON_EXTREME_OBSERVED' THEN 'NON_EXTREME'
        ELSE NULL
    END;
    IF membership_row.membership_receipt_sha256 IS NULL
       OR membership_row.stream_id IS DISTINCT FROM transition_row.stream_id
       OR membership_row.movement_id IS DISTINCT FROM
            transition_row.movement_id
       OR membership_row.anchor_id IS DISTINCT FROM
            transition_row.trigger_anchor_id
       OR membership_row.eligible_at_utc IS DISTINCT FROM
            transition_row.trigger_eligible_at_utc
       OR membership_row.decision_time_utc IS DISTINCT FROM
            transition_row.trigger_decision_time_utc
       OR membership_row.ordinal IS DISTINCT FROM
            (transition_row.post_state->>'member_count')::bigint
       OR membership_row.classification IS DISTINCT FROM
            expected_classification THEN
        RAISE EXCEPTION
            'Wave transition % lacks its exact membership receipt',
            target_transition_sha;
    END IF;
    SELECT * INTO STRICT anchor_row
      FROM public.research_neutral_price_anchors anchor
     WHERE anchor.anchor_id = membership_row.anchor_id
       AND anchor.anchor_receipt_sha256 =
            membership_row.anchor_receipt_sha256;
    IF membership_row.eligible_at_utc IS DISTINCT FROM
            anchor_row.eligible_at_utc
       OR membership_row.decision_time_utc IS DISTINCT FROM
            anchor_row.decision_time_utc
       OR membership_row.price IS DISTINCT FROM anchor_row.price
       OR transition_row.post_state->>'last_member_anchor_id'
            IS DISTINCT FROM BTRIM(membership_row.anchor_id)
       OR (transition_row.post_state->>'last_member_eligible_at_utc')::timestamptz
            IS DISTINCT FROM membership_row.eligible_at_utc
       OR (transition_row.post_state->>'last_member_decision_time_utc')::timestamptz
            IS DISTINCT FROM membership_row.decision_time_utc
       OR (transition_row.post_state->>'last_member_price')::numeric
            IS DISTINCT FROM membership_row.price THEN
        RAISE EXCEPTION
            'Membership % conflicts with its anchor or post-state projection',
            membership_row.membership_receipt_sha256;
    END IF;

    transition_namespace := transition_row.namespace;
    -- BTC's local and Parent projections are an atomic pair. Other symbols
    -- remain independent local waves: a missing same-slot BTC anchor must not
    -- block their canonical chronology, and future consumers can join the
    -- optional Parent by the exact eligibility slot.
    IF transition_namespace = 'SYMBOL'
       AND transition_row.symbol = 'BTC'
       AND NOT EXISTS (
        SELECT 1
          FROM public.research_market_movement_memberships parent_membership
          JOIN public.research_market_movement_transitions parent_transition
            ON parent_transition.transition_receipt_sha256 =
               parent_membership.emitted_by_transition_receipt_sha256
         WHERE parent_transition.namespace = 'BTC_PARENT'
           AND parent_transition.symbol = 'BTC'
           AND parent_membership.eligible_at_utc =
               membership_row.eligible_at_utc
    ) THEN
        RAISE EXCEPTION
            'SYMBOL membership % lacks its same-slot BTC Parent',
            membership_row.membership_receipt_sha256;
    END IF;
    IF transition_namespace = 'BTC_PARENT' AND NOT EXISTS (
        SELECT 1
          FROM public.research_market_movement_memberships btc_membership
          JOIN public.research_market_movement_transitions btc_transition
            ON btc_transition.transition_receipt_sha256 =
               btc_membership.emitted_by_transition_receipt_sha256
         WHERE btc_transition.namespace = 'SYMBOL'
           AND btc_transition.symbol = 'BTC'
           AND btc_membership.eligible_at_utc =
               membership_row.eligible_at_utc
    ) THEN
        RAISE EXCEPTION
            'BTC Parent membership % lacks its separate same-slot BTC identity',
            membership_row.membership_receipt_sha256;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog;

DROP TRIGGER IF EXISTS trg_market_movement_attempt_writer
    ON research_price_collection_attempts;
CREATE TRIGGER trg_market_movement_attempt_writer
BEFORE INSERT ON research_price_collection_attempts
FOR EACH ROW EXECUTE FUNCTION assert_market_movement_writer_v5();
ALTER TABLE research_price_collection_attempts
    ENABLE ALWAYS TRIGGER trg_market_movement_attempt_writer;

DROP TRIGGER IF EXISTS trg_market_movement_anchor_writer
    ON research_neutral_price_anchors;
CREATE TRIGGER trg_market_movement_anchor_writer
BEFORE INSERT ON research_neutral_price_anchors
FOR EACH ROW EXECUTE FUNCTION assert_market_movement_writer_v5();
ALTER TABLE research_neutral_price_anchors
    ENABLE ALWAYS TRIGGER trg_market_movement_anchor_writer;

DROP TRIGGER IF EXISTS trg_market_movement_transition_writer
    ON research_market_movement_transitions;
CREATE TRIGGER trg_market_movement_transition_writer
BEFORE INSERT ON research_market_movement_transitions
FOR EACH ROW EXECUTE FUNCTION assert_market_movement_writer_v5();
ALTER TABLE research_market_movement_transitions
    ENABLE ALWAYS TRIGGER trg_market_movement_transition_writer;

DROP TRIGGER IF EXISTS trg_market_movement_membership_writer
    ON research_market_movement_memberships;
CREATE TRIGGER trg_market_movement_membership_writer
BEFORE INSERT ON research_market_movement_memberships
FOR EACH ROW EXECUTE FUNCTION assert_market_movement_writer_v5();
ALTER TABLE research_market_movement_memberships
    ENABLE ALWAYS TRIGGER trg_market_movement_membership_writer;

DROP TRIGGER IF EXISTS trg_validate_market_movement_transition_insert
    ON research_market_movement_transitions;
CREATE TRIGGER trg_validate_market_movement_transition_insert
BEFORE INSERT ON research_market_movement_transitions
FOR EACH ROW EXECUTE FUNCTION validate_market_movement_transition_insert();
ALTER TABLE research_market_movement_transitions
    ENABLE ALWAYS TRIGGER trg_validate_market_movement_transition_insert;

DROP TRIGGER IF EXISTS trg_neutral_price_attempt_complete
    ON research_price_collection_attempts;
CREATE CONSTRAINT TRIGGER trg_neutral_price_attempt_complete
AFTER INSERT ON research_price_collection_attempts
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION assert_neutral_price_attempt_anchor_complete();
ALTER TABLE research_price_collection_attempts
    ENABLE ALWAYS TRIGGER trg_neutral_price_attempt_complete;

DROP TRIGGER IF EXISTS trg_neutral_price_anchor_complete
    ON research_neutral_price_anchors;
CREATE CONSTRAINT TRIGGER trg_neutral_price_anchor_complete
AFTER INSERT ON research_neutral_price_anchors
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION assert_neutral_price_attempt_anchor_complete();
ALTER TABLE research_neutral_price_anchors
    ENABLE ALWAYS TRIGGER trg_neutral_price_anchor_complete;

DROP TRIGGER IF EXISTS trg_market_movement_transition_complete
    ON research_market_movement_transitions;
CREATE CONSTRAINT TRIGGER trg_market_movement_transition_complete
AFTER INSERT ON research_market_movement_transitions
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION assert_market_movement_receipt_complete();
ALTER TABLE research_market_movement_transitions
    ENABLE ALWAYS TRIGGER trg_market_movement_transition_complete;

DROP TRIGGER IF EXISTS trg_market_movement_membership_complete
    ON research_market_movement_memberships;
CREATE CONSTRAINT TRIGGER trg_market_movement_membership_complete
AFTER INSERT ON research_market_movement_memberships
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION assert_market_movement_receipt_complete();
ALTER TABLE research_market_movement_memberships
    ENABLE ALWAYS TRIGGER trg_market_movement_membership_complete;

DO $triggers$
DECLARE
    relation_name TEXT;
BEGIN
    FOREACH relation_name IN ARRAY ARRAY[
        'research_price_collection_attempts',
        'research_neutral_price_anchors',
        'research_market_movement_transitions',
        'research_market_movement_memberships'
    ] LOOP
        EXECUTE FORMAT(
            'DROP TRIGGER IF EXISTS %I ON public.%I',
            'trg_' || relation_name || '_append_only',
            relation_name
        );
        EXECUTE FORMAT(
            'CREATE TRIGGER %I BEFORE UPDATE OR DELETE ON public.%I '
            || 'FOR EACH ROW EXECUTE FUNCTION '
            || 'public.prevent_market_movement_archive_mutation()',
            'trg_' || relation_name || '_append_only',
            relation_name
        );
        EXECUTE FORMAT(
            'ALTER TABLE public.%I ENABLE ALWAYS TRIGGER %I',
            relation_name,
            'trg_' || relation_name || '_append_only'
        );
        EXECUTE FORMAT(
            'DROP TRIGGER IF EXISTS %I ON public.%I',
            'trg_' || relation_name || '_no_truncate',
            relation_name
        );
        EXECUTE FORMAT(
            'CREATE TRIGGER %I BEFORE TRUNCATE ON public.%I '
            || 'FOR EACH STATEMENT EXECUTE FUNCTION '
            || 'public.prevent_market_movement_archive_mutation()',
            'trg_' || relation_name || '_no_truncate',
            relation_name
        );
        EXECUTE FORMAT(
            'ALTER TABLE public.%I ENABLE ALWAYS TRIGGER %I',
            relation_name,
            'trg_' || relation_name || '_no_truncate'
        );
    END LOOP;
END;
$triggers$;

COMMENT ON TABLE research_price_collection_attempts IS
    'Append-only Wave v5 price-collection audit; UNEVALUABLE attempts never create anchors.';
COMMENT ON TABLE research_neutral_price_anchors IS
    'Immutable exact closed-Spot-1m neutral prices on the :02/:32 eligibility lattice.';
COMMENT ON TABLE research_market_movement_transitions IS
    'Append-only chained Wave v5 state receipts; a closing boundary and its new opening are separate transitions.';
COMMENT ON TABLE research_market_movement_memberships IS
    'One immutable Wave v5 movement membership per stream and neutral-price anchor.';

ALTER TABLE research_price_collection_attempts
    OWNER TO research_market_movement_owner;
ALTER TABLE research_neutral_price_anchors
    OWNER TO research_market_movement_owner;
ALTER TABLE research_market_movement_transitions
    OWNER TO research_market_movement_owner;
ALTER TABLE research_market_movement_memberships
    OWNER TO research_market_movement_owner;

ALTER FUNCTION assert_market_movement_writer_v5()
    OWNER TO research_market_movement_owner;
ALTER FUNCTION prevent_market_movement_archive_mutation()
    OWNER TO research_market_movement_owner;
ALTER FUNCTION assert_neutral_price_attempt_anchor_complete()
    OWNER TO research_market_movement_owner;
ALTER FUNCTION validate_market_movement_transition_insert()
    OWNER TO research_market_movement_owner;
ALTER FUNCTION assert_market_movement_receipt_complete()
    OWNER TO research_market_movement_owner;

REVOKE ALL ON TABLE research_price_collection_attempts FROM PUBLIC;
REVOKE ALL ON TABLE research_neutral_price_anchors FROM PUBLIC;
REVOKE ALL ON TABLE research_market_movement_transitions FROM PUBLIC;
REVOKE ALL ON TABLE research_market_movement_memberships FROM PUBLIC;

REVOKE ALL ON TABLE research_price_collection_attempts
    FROM research_market_movement_writer_v5;
REVOKE ALL ON TABLE research_neutral_price_anchors
    FROM research_market_movement_writer_v5;
REVOKE ALL ON TABLE research_market_movement_transitions
    FROM research_market_movement_writer_v5;
REVOKE ALL ON TABLE research_market_movement_memberships
    FROM research_market_movement_writer_v5;

-- ALTER DEFAULT PRIVILEGES or stale grants must not create a second writer.
-- Remove every non-owner table ACL before granting the dedicated writer its
-- exact SELECT/INSERT surface, and remove all explicit column ACLs.
DO $table_acl_cleanup$
DECLARE
    relation_name TEXT;
    grant_row RECORD;
    grantee_sql TEXT;
    owner_oid OID := (
        SELECT oid FROM pg_catalog.pg_roles
         WHERE rolname = 'research_market_movement_owner'
    );
BEGIN
    FOREACH relation_name IN ARRAY ARRAY[
        'research_price_collection_attempts',
        'research_neutral_price_anchors',
        'research_market_movement_transitions',
        'research_market_movement_memberships'
    ] LOOP
        FOR grant_row IN
            SELECT DISTINCT acl.grantee
              FROM pg_catalog.pg_class relation
              CROSS JOIN LATERAL pg_catalog.aclexplode(
                  COALESCE(
                      relation.relacl,
                      pg_catalog.acldefault('r', relation.relowner)
                  )
              ) acl
             WHERE relation.oid = pg_catalog.to_regclass(
                       'public.' || relation_name
                   )
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
                relation_name,
                grantee_sql
            );
        END LOOP;

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
        LOOP
            IF grant_row.privilege_type NOT IN (
                'SELECT', 'INSERT', 'UPDATE', 'REFERENCES'
            ) THEN
                RAISE EXCEPTION
                    'Unexpected column privilege % on public.%.%',
                    grant_row.privilege_type,
                    relation_name,
                    grant_row.attname;
            END IF;
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
                relation_name,
                grantee_sql
            );
        END LOOP;
        EXECUTE pg_catalog.format(
            'GRANT ALL PRIVILEGES ON TABLE public.%I TO %I',
            relation_name,
            'research_market_movement_owner'
        );
    END LOOP;
END;
$table_acl_cleanup$;

GRANT SELECT, INSERT ON TABLE research_price_collection_attempts
    TO research_market_movement_writer_v5;
GRANT SELECT, INSERT ON TABLE research_neutral_price_anchors
    TO research_market_movement_writer_v5;
GRANT SELECT, INSERT ON TABLE research_market_movement_transitions
    TO research_market_movement_writer_v5;
GRANT SELECT, INSERT ON TABLE research_market_movement_memberships
    TO research_market_movement_writer_v5;

REVOKE ALL ON FUNCTION assert_market_movement_writer_v5()
    FROM PUBLIC, research_market_movement_writer_v5;
REVOKE ALL ON FUNCTION prevent_market_movement_archive_mutation()
    FROM PUBLIC, research_market_movement_writer_v5;
REVOKE ALL ON FUNCTION assert_neutral_price_attempt_anchor_complete()
    FROM PUBLIC, research_market_movement_writer_v5;
REVOKE ALL ON FUNCTION validate_market_movement_transition_insert()
    FROM PUBLIC, research_market_movement_writer_v5;
REVOKE ALL ON FUNCTION assert_market_movement_receipt_complete()
    FROM PUBLIC, research_market_movement_writer_v5;

DO $function_acl_cleanup$
DECLARE
    function_name TEXT;
    grant_row RECORD;
    grantee_sql TEXT;
    function_oid REGPROCEDURE;
    owner_oid OID := (
        SELECT oid FROM pg_catalog.pg_roles
         WHERE rolname = 'research_market_movement_owner'
    );
BEGIN
    FOREACH function_name IN ARRAY ARRAY[
        'assert_market_movement_writer_v5',
        'prevent_market_movement_archive_mutation',
        'assert_neutral_price_attempt_anchor_complete',
        'validate_market_movement_transition_insert',
        'assert_market_movement_receipt_complete'
    ] LOOP
        function_oid := pg_catalog.to_regprocedure(
            'public.' || function_name || '()'
        );
        FOR grant_row IN
            SELECT DISTINCT acl.grantee
              FROM pg_catalog.pg_proc procedure
              CROSS JOIN LATERAL pg_catalog.aclexplode(
                  COALESCE(
                      procedure.proacl,
                      pg_catalog.acldefault('f', procedure.proowner)
                  )
              ) acl
             WHERE procedure.oid = function_oid
               AND acl.grantee <> owner_oid
        LOOP
            grantee_sql := CASE
                WHEN grant_row.grantee = 0 THEN 'PUBLIC'
                ELSE pg_catalog.quote_ident(
                    pg_catalog.pg_get_userbyid(grant_row.grantee)
                )
            END;
            EXECUTE pg_catalog.format(
                'REVOKE ALL PRIVILEGES ON FUNCTION public.%I() FROM %s CASCADE',
                function_name,
                grantee_sql
            );
        END LOOP;
        EXECUTE pg_catalog.format(
            'GRANT ALL PRIVILEGES ON FUNCTION public.%I() TO %I',
            function_name,
            'research_market_movement_owner'
        );
    END LOOP;
END;
$function_acl_cleanup$;

DO $catalog_assertions$
DECLARE
    relation_name TEXT;
    forbidden_privilege TEXT;
    function_spec RECORD;
    trigger_spec RECORD;
    relation_oid REGCLASS;
    function_oid REGPROCEDURE;
    actual_trigger_names TEXT[];
    owner_oid OID := (
        SELECT oid FROM pg_roles
         WHERE rolname = 'research_market_movement_owner'
    );
    writer_oid OID := (
        SELECT oid FROM pg_roles
         WHERE rolname = 'research_market_movement_writer_v5'
    );
BEGIN
    FOREACH relation_name IN ARRAY ARRAY[
        'research_price_collection_attempts',
        'research_neutral_price_anchors',
        'research_market_movement_transitions',
        'research_market_movement_memberships'
    ] LOOP
        relation_oid := TO_REGCLASS('public.' || relation_name);
        IF relation_oid IS NULL OR (
            SELECT relowner FROM pg_class WHERE oid = relation_oid
        ) IS DISTINCT FROM owner_oid THEN
            RAISE EXCEPTION
                'public.% is missing or is not owned by the Wave v5 NOLOGIN owner',
                relation_name;
        END IF;
        IF NOT HAS_TABLE_PRIVILEGE(
            'research_market_movement_writer_v5', relation_oid, 'SELECT'
        ) OR NOT HAS_TABLE_PRIVILEGE(
            'research_market_movement_writer_v5', relation_oid, 'INSERT'
        ) THEN
            RAISE EXCEPTION
                'Wave v5 writer lacks SELECT/INSERT on public.%', relation_name;
        END IF;
        IF NOT HAS_TABLE_PRIVILEGE(
            'research_market_movement_owner', relation_oid, 'SELECT'
        ) THEN
            RAISE EXCEPTION
                'Wave v5 owner lacks SELECT on public.%', relation_name;
        END IF;
        FOREACH forbidden_privilege IN ARRAY ARRAY[
            'UPDATE', 'DELETE', 'TRUNCATE', 'REFERENCES', 'TRIGGER'
        ] LOOP
            IF HAS_TABLE_PRIVILEGE(
                'research_market_movement_writer_v5',
                relation_oid,
                forbidden_privilege
            ) THEN
                RAISE EXCEPTION
                    'Wave v5 writer unexpectedly has % on public.%',
                    forbidden_privilege,
                    relation_name;
            END IF;
        END LOOP;
        IF EXISTS (
            SELECT 1
              FROM ACLEXPLODE(
                    COALESCE(
                        (SELECT relacl FROM pg_class WHERE oid = relation_oid),
                        ACLDEFAULT('r', owner_oid)
                    )
              ) acl
             WHERE acl.grantee <> owner_oid
               AND NOT (
                   acl.grantee = writer_oid
                   AND acl.privilege_type IN ('SELECT', 'INSERT')
                   AND NOT acl.is_grantable
               )
        ) THEN
            RAISE EXCEPTION
                'Unexpected non-owner ACL remains on public.%', relation_name;
        END IF;
        IF EXISTS (
            SELECT 1
              FROM pg_attribute attribute
             WHERE attribute.attrelid = relation_oid
               AND attribute.attnum > 0
               AND NOT attribute.attisdropped
               AND COALESCE(CARDINALITY(attribute.attacl), 0) <> 0
        ) THEN
            RAISE EXCEPTION
                'Column ACL remains on public.%', relation_name;
        END IF;
        IF (
            SELECT relation.relrowsecurity OR relation.relforcerowsecurity
              FROM pg_class relation
             WHERE relation.oid = relation_oid
        ) OR EXISTS (
            SELECT 1 FROM pg_policy policy
             WHERE policy.polrelid = relation_oid
        ) THEN
            RAISE EXCEPTION
                'RLS is not part of the Wave v5 archive shape on public.%',
                relation_name;
        END IF;
        IF EXISTS (
            SELECT 1 FROM pg_rewrite rewrite_rule
             WHERE rewrite_rule.ev_class = relation_oid
        ) THEN
            RAISE EXCEPTION
                'Rules are not part of the Wave v5 archive shape on public.%',
                relation_name;
        END IF;
        IF NOT EXISTS (
            SELECT 1
              FROM pg_trigger trg
             WHERE trg.tgrelid = relation_oid
               AND trg.tgname =
                    'trg_' || relation_name || '_append_only'
               AND NOT trg.tgisinternal
               AND trg.tgenabled = 'A'
        ) OR NOT EXISTS (
            SELECT 1
              FROM pg_trigger trg
             WHERE trg.tgrelid = relation_oid
               AND trg.tgname =
                    'trg_' || relation_name || '_no_truncate'
               AND NOT trg.tgisinternal
               AND trg.tgenabled = 'A'
        ) THEN
            RAISE EXCEPTION
                'Append-only triggers are missing or not ENABLE ALWAYS on public.%',
                relation_name;
        END IF;
    END LOOP;

    FOR function_spec IN
        SELECT *
          FROM (VALUES
            ('assert_market_movement_writer_v5()', FALSE),
            ('prevent_market_movement_archive_mutation()', FALSE),
            ('assert_neutral_price_attempt_anchor_complete()', TRUE),
            ('validate_market_movement_transition_insert()', TRUE),
            ('assert_market_movement_receipt_complete()', TRUE)
          ) AS expected(signature, security_definer)
    LOOP
        function_oid := TO_REGPROCEDURE(
            'public.' || function_spec.signature
        );
        IF function_oid IS NULL OR NOT EXISTS (
            SELECT 1
             FROM pg_proc procedure
             WHERE procedure.oid = function_oid
               AND procedure.proowner = owner_oid
               AND procedure.prosecdef = function_spec.security_definer
               AND procedure.proconfig IS NOT DISTINCT FROM
                    ARRAY['search_path=pg_catalog']::text[]
        ) THEN
            RAISE EXCEPTION
                'Wave v5 function % has unsafe owner/security mode',
                function_spec.signature;
        END IF;
        IF HAS_FUNCTION_PRIVILEGE(
            'research_market_movement_writer_v5', function_oid, 'EXECUTE'
        ) THEN
            RAISE EXCEPTION
                'Wave v5 writer must not directly execute function %',
                function_spec.signature;
        END IF;
        IF NOT HAS_FUNCTION_PRIVILEGE(
            'research_market_movement_owner', function_oid, 'EXECUTE'
        ) THEN
            RAISE EXCEPTION
                'Wave v5 owner must execute trigger function %',
                function_spec.signature;
        END IF;
        IF EXISTS (
            SELECT 1
              FROM pg_proc procedure
              CROSS JOIN LATERAL ACLEXPLODE(
                  COALESCE(
                      procedure.proacl,
                      ACLDEFAULT('f', procedure.proowner)
                  )
              ) acl
             WHERE procedure.oid = function_oid
               AND acl.grantee <> owner_oid
        ) THEN
            RAISE EXCEPTION
                'Unexpected non-owner EXECUTE ACL remains on function %',
                function_spec.signature;
        END IF;
    END LOOP;

    FOR trigger_spec IN
        SELECT *
          FROM (VALUES
            ('research_price_collection_attempts',
             'trg_market_movement_attempt_writer'),
            ('research_price_collection_attempts',
             'trg_neutral_price_attempt_complete'),
            ('research_neutral_price_anchors',
             'trg_market_movement_anchor_writer'),
            ('research_neutral_price_anchors',
             'trg_neutral_price_anchor_complete'),
            ('research_market_movement_transitions',
             'trg_market_movement_transition_writer'),
            ('research_market_movement_transitions',
             'trg_validate_market_movement_transition_insert'),
            ('research_market_movement_transitions',
             'trg_market_movement_transition_complete'),
            ('research_market_movement_memberships',
             'trg_market_movement_membership_writer'),
            ('research_market_movement_memberships',
             'trg_market_movement_membership_complete')
          ) AS expected(relation_name, trigger_name)
    LOOP
        IF NOT EXISTS (
            SELECT 1
              FROM pg_trigger trg
             WHERE trg.tgrelid =
                    TO_REGCLASS('public.' || trigger_spec.relation_name)
               AND trg.tgname = trigger_spec.trigger_name
               AND NOT trg.tgisinternal
               AND trg.tgenabled = 'A'
        ) THEN
            RAISE EXCEPTION
                'Wave v5 trigger %.% is missing or not ENABLE ALWAYS',
                trigger_spec.relation_name,
                trigger_spec.trigger_name;
        END IF;
    END LOOP;

    FOR trigger_spec IN
        SELECT *
          FROM (VALUES
            (
                'research_price_collection_attempts',
                ARRAY[
                    'trg_market_movement_attempt_writer',
                    'trg_neutral_price_attempt_complete',
                    'trg_research_price_collection_attempts_append_only',
                    'trg_research_price_collection_attempts_no_truncate'
                ]::text[]
            ),
            (
                'research_neutral_price_anchors',
                ARRAY[
                    'trg_market_movement_anchor_writer',
                    'trg_neutral_price_anchor_complete',
                    'trg_research_neutral_price_anchors_append_only',
                    'trg_research_neutral_price_anchors_no_truncate'
                ]::text[]
            ),
            (
                'research_market_movement_transitions',
                ARRAY[
                    'trg_market_movement_transition_complete',
                    'trg_market_movement_transition_writer',
                    'trg_research_market_movement_transitions_append_only',
                    'trg_research_market_movement_transitions_no_truncate',
                    'trg_validate_market_movement_transition_insert'
                ]::text[]
            ),
            (
                'research_market_movement_memberships',
                ARRAY[
                    'trg_market_movement_membership_complete',
                    'trg_market_movement_membership_writer',
                    'trg_research_market_movement_memberships_append_only',
                    'trg_research_market_movement_memberships_no_truncate'
                ]::text[]
            )
          ) AS expected(relation_name, trigger_names)
    LOOP
        SELECT ARRAY_AGG(
                   trigger_row.tgname::text
                   ORDER BY trigger_row.tgname::text COLLATE pg_catalog."C"
               )
          INTO actual_trigger_names
          FROM pg_trigger trigger_row
         WHERE trigger_row.tgrelid =
                TO_REGCLASS('public.' || trigger_spec.relation_name)
           AND NOT trigger_row.tgisinternal;
        IF actual_trigger_names IS DISTINCT FROM trigger_spec.trigger_names
           OR EXISTS (
               SELECT 1
                 FROM pg_trigger trigger_row
                WHERE trigger_row.tgrelid =
                       TO_REGCLASS('public.' || trigger_spec.relation_name)
                  AND NOT trigger_row.tgisinternal
                  AND trigger_row.tgenabled <> 'A'
           ) THEN
            RAISE EXCEPTION
                'Unexpected or disabled user trigger on public.%: expected %, got %',
                trigger_spec.relation_name,
                trigger_spec.trigger_names,
                actual_trigger_names;
        END IF;
    END LOOP;
END;
$catalog_assertions$;

-- research_formula_schema_admin applies every migration in one transaction.
-- Do not leak this migration's owner identity or deterministic catalog GUCs
-- into a future migration appended after 022.
RESET ROLE;
RESET search_path;
RESET TIME ZONE;
RESET DateStyle;
RESET IntervalStyle;
