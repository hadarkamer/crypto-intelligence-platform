-- Historical raw-opportunity replay v1
--
-- Additive only.  The replay stores compact outcome summaries for archived
-- Price/OI/CVD observation times.  One-minute exchange candles are used to
-- calculate labels and are not copied into PostgreSQL.

CREATE TABLE IF NOT EXISTS research_historical_replay_runs (
    replay_run_id BIGSERIAL PRIMARY KEY,
    replay_version TEXT NOT NULL,
    outcome_method_version TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED')),
    requested_start_utc TIMESTAMPTZ,
    requested_end_utc TIMESTAMPTZ,
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    coverage JSONB NOT NULL DEFAULT '{}'::jsonb,
    anchors_seen INTEGER NOT NULL DEFAULT 0,
    outcomes_written INTEGER NOT NULL DEFAULT 0,
    outcomes_skipped INTEGER NOT NULL DEFAULT 0,
    failures INTEGER NOT NULL DEFAULT 0,
    error_text TEXT,
    started_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at_utc TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_historical_replay_runs_time
    ON research_historical_replay_runs (started_at_utc DESC);

CREATE TABLE IF NOT EXISTS research_historical_opportunity_outcomes (
    opportunity_id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    observation_time_utc TIMESTAMPTZ NOT NULL,
    source_observation_time_utc TIMESTAMPTZ NOT NULL,
    horizon_minutes INTEGER NOT NULL CHECK (
        horizon_minutes IN (60, 240, 720, 1440)
    ),
    reference_time_utc TIMESTAMPTZ NOT NULL,
    reference_price DOUBLE PRECISION NOT NULL CHECK (reference_price > 0),
    price_at_horizon DOUBLE PRECISION NOT NULL CHECK (price_at_horizon > 0),
    raw_return_pct DOUBLE PRECISION NOT NULL,
    long_metrics JSONB NOT NULL,
    short_metrics JSONB NOT NULL,
    path_samples INTEGER NOT NULL CHECK (path_samples > 0),
    outcome_method_version TEXT NOT NULL,
    exchange TEXT NOT NULL,
    market TEXT NOT NULL,
    pair TEXT NOT NULL,
    interval_seconds INTEGER NOT NULL CHECK (interval_seconds = 60),
    provenance TEXT NOT NULL,
    data_quality_status TEXT NOT NULL,
    replay_version TEXT NOT NULL,
    replay_run_id BIGINT REFERENCES research_historical_replay_runs(replay_run_id)
        ON DELETE SET NULL,
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (symbol, observation_time_utc, horizon_minutes)
);

CREATE INDEX IF NOT EXISTS idx_historical_opportunity_horizon_time
    ON research_historical_opportunity_outcomes (
        horizon_minutes, observation_time_utc, symbol
    );

CREATE INDEX IF NOT EXISTS idx_historical_opportunity_symbol_time
    ON research_historical_opportunity_outcomes (
        symbol, observation_time_utc, horizon_minutes
    );
