-- No-dwell first-touch outcomes v6
--
-- Additive migration only.  Legacy fixed-horizon endpoint/MFE/MAE rows and
-- their method versions remain unchanged for audit.  This migration is never
-- applied automatically by Watch or a recurring worker.

CREATE TABLE IF NOT EXISTS research_first_touch_outcomes (
    event_id BIGINT NOT NULL REFERENCES research_events(event_id) ON DELETE CASCADE,
    horizon_minutes INTEGER NOT NULL CHECK (
        horizon_minutes IN (60, 240, 720, 1440)
    ),
    method_version TEXT NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('LONG', 'SHORT')),
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'HIT', 'MISS')),
    success BOOLEAN,
    failure_final BOOLEAN NOT NULL,
    observed_through_utc TIMESTAMPTZ NOT NULL,
    reference_price DOUBLE PRECISION NOT NULL CHECK (reference_price > 0),
    qualifying_move_price DOUBLE PRECISION NOT NULL CHECK (qualifying_move_price > 0),
    qualifying_move_threshold_pct DOUBLE PRECISION NOT NULL CHECK (
        qualifying_move_threshold_pct > 0
    ),
    threshold_scale_factor DOUBLE PRECISION NOT NULL CHECK (
        threshold_scale_factor BETWEEN 0.50 AND 1.00
    ),
    threshold_source_kind TEXT NOT NULL CHECK (
        threshold_source_kind IN (
            'STATIC_HORIZON_FLOOR',
            'PRIOR_ONLY_SESSION_CALIBRATION'
        )
    ),
    threshold_source TEXT NOT NULL,
    threshold_policy JSONB NOT NULL,
    first_qualifying_move_time_utc TIMESTAMPTZ,
    time_to_first_qualifying_move_seconds INTEGER CHECK (
        time_to_first_qualifying_move_seconds IS NULL
        OR time_to_first_qualifying_move_seconds >= 0
    ),
    pre_qualifying_mae_pct DOUBLE PRECISION NOT NULL CHECK (
        pre_qualifying_mae_pct >= 0
    ),
    qualifying_candle_adverse_excursion_pct DOUBLE PRECISION CHECK (
        qualifying_candle_adverse_excursion_pct IS NULL
        OR qualifying_candle_adverse_excursion_pct >= 0
    ),
    qualifying_candle_order_ambiguous BOOLEAN NOT NULL DEFAULT FALSE,
    dwell_required_seconds INTEGER NOT NULL DEFAULT 0 CHECK (
        dwell_required_seconds = 0
    ),
    path_resolution_seconds INTEGER NOT NULL CHECK (path_resolution_seconds = 60),
    path_samples INTEGER NOT NULL CHECK (path_samples > 0),
    price_source TEXT NOT NULL,
    data_quality_status TEXT NOT NULL,
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, horizon_minutes, method_version),
    CHECK (
        (
            status = 'HIT'
            AND success IS TRUE
            AND failure_final IS FALSE
            AND first_qualifying_move_time_utc IS NOT NULL
            AND time_to_first_qualifying_move_seconds IS NOT NULL
        )
        OR (
            status = 'MISS'
            AND success IS FALSE
            AND failure_final IS TRUE
            AND first_qualifying_move_time_utc IS NULL
            AND time_to_first_qualifying_move_seconds IS NULL
        )
        OR (
            status = 'PENDING'
            AND success IS NULL
            AND failure_final IS FALSE
            AND first_qualifying_move_time_utc IS NULL
            AND time_to_first_qualifying_move_seconds IS NULL
        )
    )
);

CREATE INDEX IF NOT EXISTS idx_research_first_touch_status
    ON research_first_touch_outcomes (
        method_version, status, horizon_minutes, observed_through_utc DESC
    );

ALTER TABLE research_historical_opportunity_outcomes
    ADD COLUMN IF NOT EXISTS long_first_touch_metrics JSONB,
    ADD COLUMN IF NOT EXISTS short_first_touch_metrics JSONB,
    ADD COLUMN IF NOT EXISTS first_touch_method_version TEXT,
    ADD COLUMN IF NOT EXISTS first_touch_path_samples INTEGER,
    ADD COLUMN IF NOT EXISTS first_touch_data_quality_status TEXT,
    ADD COLUMN IF NOT EXISTS first_touch_replay_run_id BIGINT REFERENCES research_historical_replay_runs(replay_run_id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_historical_first_touch_method
    ON research_historical_opportunity_outcomes (
        first_touch_method_version, horizon_minutes, observation_time_utc
    );

COMMENT ON TABLE research_first_touch_outcomes IS
    'Versioned zero-dwell first-touch labels. HIT is final on touch; MISS only after the horizon closes.';
COMMENT ON COLUMN research_first_touch_outcomes.pre_qualifying_mae_pct IS
    'Conservative adverse excursion through and including the qualifying 1m candle.';
COMMENT ON COLUMN research_first_touch_outcomes.qualifying_candle_order_ambiguous IS
    'True when favorable qualification and an adverse-side excursion share one 1m OHLC candle, whose intrabar order is unknowable.';
