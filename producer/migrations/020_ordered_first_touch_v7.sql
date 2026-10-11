-- Ordered two-barrier First Touch outcomes v7
--
-- Additive only.  The one-sided no-dwell v6 table remains unchanged for
-- historical audit and for formula versions that explicitly bind to it.
-- This table stores the independent, ordered result for every event/window/
-- threshold combination.  Operational delivery state is normalized into a
-- separate durable outbox so a Sheets failure cannot alter research evidence.

-- event_id already is globally unique.  This redundant unique index permits a
-- composite foreign key below, making it impossible to persist a v7 direction
-- that disagrees with the immutable direction of its source event.
CREATE UNIQUE INDEX IF NOT EXISTS idx_research_events_event_direction
    ON research_events (event_id, direction);

CREATE TABLE IF NOT EXISTS research_ordered_first_touch_outcomes (
    event_id BIGINT NOT NULL,
    window_minutes INTEGER NOT NULL CHECK (
        window_minutes IN (60, 240, 720, 1440)
    ),
    threshold_bps SMALLINT NOT NULL CHECK (
        threshold_bps IN (25, 50, 75, 100, 125, 150, 175, 200)
    ),
    method_version TEXT NOT NULL CHECK (
        method_version = 'ordered-first-touch-v7'
    ),

    direction TEXT NOT NULL CHECK (direction IN ('LONG', 'SHORT')),
    status TEXT NOT NULL CHECK (
        status IN (
            'OPEN', 'SUCCESS', 'FAILURE', 'UNRESOLVED', 'DATA_MISSING'
        )
    ),
    first_touch_side TEXT NOT NULL CHECK (
        first_touch_side IN ('NONE', 'FAVORABLE', 'ADVERSE', 'AMBIGUOUS')
    ),
    terminal_reason TEXT CHECK (
        terminal_reason IN (
            'FAVORABLE_FIRST',
            'ADVERSE_FIRST',
            'SAME_CANDLE_BOTH',
            'OBSERVATION_WINDOW_CLOSED_NO_TOUCH',
            'INCOMPLETE_PATH'
        )
    ),
    success BOOLEAN,

    measurement_start_utc TIMESTAMPTZ NOT NULL,
    first_observed_open_utc TIMESTAMPTZ,
    observed_through_utc TIMESTAMPTZ NOT NULL,
    decision_time_utc TIMESTAMPTZ,
    time_to_decision_seconds INTEGER CHECK (
        time_to_decision_seconds IS NULL
        OR time_to_decision_seconds >= 0
    ),
    initial_gap_seconds INTEGER NOT NULL DEFAULT 0 CHECK (
        initial_gap_seconds >= 0 AND initial_gap_seconds < 60
    ),
    initial_gap_unobserved BOOLEAN NOT NULL DEFAULT FALSE,

    reference_price DOUBLE PRECISION NOT NULL CHECK (
        reference_price > 0
        AND reference_price NOT IN (
            'Infinity'::DOUBLE PRECISION,
            '-Infinity'::DOUBLE PRECISION,
            'NaN'::DOUBLE PRECISION
        )
    ),
    favorable_barrier_price DOUBLE PRECISION NOT NULL CHECK (
        favorable_barrier_price > 0
        AND favorable_barrier_price NOT IN (
            'Infinity'::DOUBLE PRECISION,
            '-Infinity'::DOUBLE PRECISION,
            'NaN'::DOUBLE PRECISION
        )
    ),
    adverse_barrier_price DOUBLE PRECISION NOT NULL CHECK (
        adverse_barrier_price > 0
        AND adverse_barrier_price NOT IN (
            'Infinity'::DOUBLE PRECISION,
            '-Infinity'::DOUBLE PRECISION,
            'NaN'::DOUBLE PRECISION
        )
    ),
    favorable_touch_price DOUBLE PRECISION CHECK (
        favorable_touch_price IS NULL
        OR (favorable_touch_price > 0
            AND favorable_touch_price NOT IN (
                'Infinity'::DOUBLE PRECISION,
                '-Infinity'::DOUBLE PRECISION,
                'NaN'::DOUBLE PRECISION
            ))
    ),
    adverse_touch_price DOUBLE PRECISION CHECK (
        adverse_touch_price IS NULL
        OR (adverse_touch_price > 0
            AND adverse_touch_price NOT IN (
                'Infinity'::DOUBLE PRECISION,
                '-Infinity'::DOUBLE PRECISION,
                'NaN'::DOUBLE PRECISION
            ))
    ),
    max_favorable_price DOUBLE PRECISION NOT NULL CHECK (
        max_favorable_price > 0
        AND max_favorable_price NOT IN (
            'Infinity'::DOUBLE PRECISION,
            '-Infinity'::DOUBLE PRECISION,
            'NaN'::DOUBLE PRECISION
        )
    ),
    max_adverse_price DOUBLE PRECISION NOT NULL CHECK (
        max_adverse_price > 0
        AND max_adverse_price NOT IN (
            'Infinity'::DOUBLE PRECISION,
            '-Infinity'::DOUBLE PRECISION,
            'NaN'::DOUBLE PRECISION
        )
    ),
    mfe_pct DOUBLE PRECISION NOT NULL CHECK (
        mfe_pct >= 0
        AND mfe_pct NOT IN (
            'Infinity'::DOUBLE PRECISION,
            '-Infinity'::DOUBLE PRECISION,
            'NaN'::DOUBLE PRECISION
        )
    ),
    mae_pct DOUBLE PRECISION NOT NULL CHECK (
        mae_pct >= 0
        AND mae_pct NOT IN (
            'Infinity'::DOUBLE PRECISION,
            '-Infinity'::DOUBLE PRECISION,
            'NaN'::DOUBLE PRECISION
        )
    ),

    candle_interval_seconds INTEGER NOT NULL CHECK (
        candle_interval_seconds = 60
    ),
    path_samples INTEGER NOT NULL CHECK (
        path_samples >= 0 AND path_samples <= window_minutes
    ),
    observation_closed BOOLEAN NOT NULL,
    input_path_complete BOOLEAN NOT NULL,
    path_complete BOOLEAN NOT NULL,
    price_source TEXT NOT NULL CHECK (BTRIM(price_source) <> ''),
    market_pair TEXT CHECK (market_pair IS NULL OR BTRIM(market_pair) <> ''),
    data_quality_status TEXT NOT NULL CHECK (
        data_quality_status IN (
            'VERIFIED_BINANCE_SPOT_1M_CLOSED_CANDLES',
            'PARTIAL_BINANCE_SPOT_1M_CLOSED_CANDLES',
            'VERIFIED_HYPERLIQUID_SPOT_1M_CLOSED_CANDLES',
            'PARTIAL_HYPERLIQUID_SPOT_1M_CLOSED_CANDLES'
        )
    ),
    data_quality_note TEXT NOT NULL CHECK (
        data_quality_note IN (
            'COMPLETE_CLOSED_1M_PATH',
            'COMPLETE_POST_GAP_CLOSED_1M_PATH',
            'INCOMPLETE_CLOSED_1M_PATH'
        )
    ),
    threshold_policy JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (
        JSONB_TYPEOF(threshold_policy) = 'object'
    ),
    calculation_audit JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (
        JSONB_TYPEOF(calculation_audit) = 'object'
    ),

    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (
        event_id, window_minutes, threshold_bps, method_version
    ),
    FOREIGN KEY (event_id, direction)
        REFERENCES research_events (event_id, direction)
        ON DELETE CASCADE,

    CHECK (
        (direction = 'LONG'
         AND favorable_barrier_price > reference_price
         AND adverse_barrier_price < reference_price)
        OR
        (direction = 'SHORT'
         AND favorable_barrier_price < reference_price
         AND adverse_barrier_price > reference_price)
    ),
    CHECK (
        (direction = 'LONG'
         AND ABS(
             favorable_barrier_price / reference_price
             - (1.0 + threshold_bps / 10000.0)
         ) <= 1e-12
         AND ABS(
             adverse_barrier_price / reference_price
             - (1.0 - threshold_bps / 10000.0)
         ) <= 1e-12)
        OR
        (direction = 'SHORT'
         AND ABS(
             favorable_barrier_price / reference_price
             - (1.0 - threshold_bps / 10000.0)
         ) <= 1e-12
         AND ABS(
             adverse_barrier_price / reference_price
             - (1.0 + threshold_bps / 10000.0)
         ) <= 1e-12)
    ),
    CHECK (
        (direction = 'LONG'
         AND max_favorable_price >= reference_price
         AND max_adverse_price <= reference_price)
        OR
        (direction = 'SHORT'
         AND max_favorable_price <= reference_price
         AND max_adverse_price >= reference_price)
    ),
    CHECK (
        observed_through_utc >= measurement_start_utc
        AND (
            first_observed_open_utc IS NULL
            OR (first_observed_open_utc >= measurement_start_utc
                AND observed_through_utc > first_observed_open_utc)
        )
    ),
    CHECK (
        (decision_time_utc IS NULL
         AND time_to_decision_seconds IS NULL)
        OR
        (decision_time_utc IS NOT NULL
         AND time_to_decision_seconds IS NOT NULL
         AND path_samples > 0
         AND decision_time_utc >= measurement_start_utc
         AND decision_time_utc <= observed_through_utc
         AND time_to_decision_seconds = FLOOR(EXTRACT(
             EPOCH FROM decision_time_utc - measurement_start_utc
         ))::INTEGER)
    ),
    CHECK (
        (initial_gap_unobserved AND initial_gap_seconds > 0)
        OR (NOT initial_gap_unobserved AND initial_gap_seconds = 0)
    ),
    CHECK ((path_samples = 0) = (first_observed_open_utc IS NULL)),
    CHECK (path_complete = (status <> 'DATA_MISSING')),
    CHECK (NOT path_complete OR input_path_complete),
    CHECK (
        (path_complete AND data_quality_status IN (
            'VERIFIED_BINANCE_SPOT_1M_CLOSED_CANDLES',
            'VERIFIED_HYPERLIQUID_SPOT_1M_CLOSED_CANDLES'
        ))
        OR
        (NOT path_complete AND data_quality_status IN (
            'PARTIAL_BINANCE_SPOT_1M_CLOSED_CANDLES',
            'PARTIAL_HYPERLIQUID_SPOT_1M_CLOSED_CANDLES'
        ))
    ),
    CHECK (
        (NOT path_complete
         AND data_quality_note = 'INCOMPLETE_CLOSED_1M_PATH')
        OR
        (path_complete
         AND initial_gap_unobserved
         AND data_quality_note = 'COMPLETE_POST_GAP_CLOSED_1M_PATH')
        OR
        (path_complete
         AND NOT initial_gap_unobserved
         AND data_quality_note = 'COMPLETE_CLOSED_1M_PATH')
    ),
    -- CHECK normally permits NULL.  Require TRUE so nullable touch prices or
    -- terminal reasons cannot bypass the complete status-specific contract.
    CHECK ((
        (status = 'OPEN'
         AND observation_closed IS FALSE
         AND first_touch_side = 'NONE'
         AND terminal_reason IS NULL
         AND success IS NULL
         AND decision_time_utc IS NULL
         AND favorable_touch_price IS NULL
         AND adverse_touch_price IS NULL)
        OR
        (status = 'SUCCESS'
         AND first_touch_side = 'FAVORABLE'
         AND terminal_reason = 'FAVORABLE_FIRST'
         AND success IS TRUE
         AND decision_time_utc IS NOT NULL
         AND favorable_touch_price = favorable_barrier_price
         AND adverse_touch_price IS NULL)
        OR
        (status = 'FAILURE'
         AND success IS FALSE
         AND decision_time_utc IS NOT NULL
         AND first_touch_side = 'ADVERSE'
         AND terminal_reason = 'ADVERSE_FIRST'
         AND favorable_touch_price IS NULL
         AND adverse_touch_price = adverse_barrier_price)
        OR
        (status = 'UNRESOLVED'
         AND success IS NULL
         AND (
             (first_touch_side = 'AMBIGUOUS'
              AND terminal_reason = 'SAME_CANDLE_BOTH'
              AND decision_time_utc IS NOT NULL
              AND favorable_touch_price = favorable_barrier_price
              AND adverse_touch_price = adverse_barrier_price)
             OR
             (first_touch_side = 'NONE'
              AND observation_closed IS TRUE
              AND terminal_reason = 'OBSERVATION_WINDOW_CLOSED_NO_TOUCH'
              AND decision_time_utc IS NULL
              AND favorable_touch_price IS NULL
              AND adverse_touch_price IS NULL)
         ))
        OR
        (status = 'DATA_MISSING'
         AND first_touch_side = 'NONE'
         AND terminal_reason = 'INCOMPLETE_PATH'
         AND success IS NULL
         AND decision_time_utc IS NULL
         AND favorable_touch_price IS NULL
         AND adverse_touch_price IS NULL)
    ) IS TRUE)
);

CREATE INDEX IF NOT EXISTS idx_ordered_first_touch_open_queue
    ON research_ordered_first_touch_outcomes (
        observed_through_utc ASC, event_id ASC, window_minutes, threshold_bps
    )
    WHERE method_version = 'ordered-first-touch-v7'
      AND status = 'OPEN';

CREATE INDEX IF NOT EXISTS idx_ordered_first_touch_terminal_scope
    ON research_ordered_first_touch_outcomes (
        method_version, window_minutes, threshold_bps, direction,
        decision_time_utc DESC, event_id
    )
    WHERE status IN ('SUCCESS', 'FAILURE');

CREATE TABLE IF NOT EXISTS research_ordered_first_touch_sync_outbox (
    event_id BIGINT NOT NULL,
    window_minutes INTEGER NOT NULL,
    threshold_bps SMALLINT NOT NULL,
    method_version TEXT NOT NULL,
    destination TEXT NOT NULL DEFAULT 'GOOGLE_SHEETS' CHECK (
        BTRIM(destination) <> ''
    ),
    sync_status TEXT NOT NULL DEFAULT 'PENDING' CHECK (
        sync_status IN (
            'PENDING', 'IN_FLIGHT', 'RETRY', 'SYNCED', 'DEAD_LETTER'
        )
    ),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_attempt_at_utc TIMESTAMPTZ,
    claim_token UUID,
    claimed_at_utc TIMESTAMPTZ,
    lease_expires_at_utc TIMESTAMPTZ,
    claimed_payload_sha256 CHAR(64) CHECK (
        claimed_payload_sha256 IS NULL
        OR BTRIM(claimed_payload_sha256) ~ '^[0-9a-f]{64}$'
    ),
    synced_at_utc TIMESTAMPTZ,
    last_error TEXT,
    remote_row_key TEXT NOT NULL CHECK (BTRIM(remote_row_key) <> ''),
    payload JSONB NOT NULL CHECK (JSONB_TYPEOF(payload) = 'object'),
    payload_sha256 CHAR(64) NOT NULL CHECK (
        BTRIM(payload_sha256) ~ '^[0-9a-f]{64}$'
    ),
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (
        event_id, window_minutes, threshold_bps, method_version, destination
    ),
    FOREIGN KEY (event_id, window_minutes, threshold_bps, method_version)
        REFERENCES research_ordered_first_touch_outcomes (
            event_id, window_minutes, threshold_bps, method_version
        ) ON DELETE CASCADE,
    CHECK (
        (sync_status = 'SYNCED' AND synced_at_utc IS NOT NULL)
        OR (sync_status <> 'SYNCED' AND synced_at_utc IS NULL)
    ),
    CHECK (
        (sync_status IN ('RETRY', 'DEAD_LETTER')
         AND last_error IS NOT NULL
         AND BTRIM(last_error) <> '')
        OR
        (sync_status NOT IN ('RETRY', 'DEAD_LETTER')
         AND last_error IS NULL)
    ),
    CHECK (
        (attempts = 0 AND last_attempt_at_utc IS NULL)
        OR (attempts > 0 AND last_attempt_at_utc IS NOT NULL)
    ),
    CHECK (
        (sync_status = 'IN_FLIGHT'
         AND attempts > 0
         AND claim_token IS NOT NULL
         AND claimed_at_utc IS NOT NULL
         AND lease_expires_at_utc IS NOT NULL
         AND lease_expires_at_utc > claimed_at_utc
         AND claimed_payload_sha256 = payload_sha256)
        OR
        (sync_status <> 'IN_FLIGHT'
         AND claim_token IS NULL
         AND claimed_at_utc IS NULL
         AND lease_expires_at_utc IS NULL
         AND claimed_payload_sha256 IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_ordered_first_touch_sync_queue
    ON research_ordered_first_touch_sync_outbox (
        destination, next_attempt_at_utc ASC, created_at_utc ASC,
        event_id, window_minutes, threshold_bps
    )
    WHERE sync_status IN ('PENDING', 'RETRY');

CREATE INDEX IF NOT EXISTS idx_ordered_first_touch_expired_lease
    ON research_ordered_first_touch_sync_outbox (
        destination, lease_expires_at_utc ASC, event_id,
        window_minutes, threshold_bps
    )
    WHERE sync_status = 'IN_FLIGHT';

CREATE UNIQUE INDEX IF NOT EXISTS idx_ordered_first_touch_remote_row
    ON research_ordered_first_touch_sync_outbox (
        destination, remote_row_key
    );

COMMENT ON TABLE research_ordered_first_touch_outcomes IS
    'Normalized v7 ordered two-barrier outcomes. v6 one-sided labels remain untouched and must not be silently reinterpreted.';

COMMENT ON COLUMN research_ordered_first_touch_outcomes.decision_time_utc IS
    'Close timestamp of the first decisive 1m candle; exact intrabar touch time remains unknowable from OHLC.';

COMMENT ON COLUMN research_ordered_first_touch_outcomes.initial_gap_unobserved IS
    'True when a non-minute-aligned alert leaves an intentionally unobserved initial partial minute; disclosed but not forced to DATA_MISSING.';

COMMENT ON TABLE research_ordered_first_touch_sync_outbox IS
    'Durable, retryable delivery state and exact payload for observational exports such as Google Sheets; research outcomes remain source-of-truth.';

COMMENT ON COLUMN research_ordered_first_touch_sync_outbox.claimed_payload_sha256 IS
    'Optimistic acknowledgement guard: a sender may mark SYNCED only when its claim token and this hash still match the current payload.';
