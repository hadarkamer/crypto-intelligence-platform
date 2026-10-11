-- Historical Replay v2 bounded-streaming support
--
-- This additive index matches the exact-version resume and coherent-coverage
-- scans: first-touch method, replay version, symbol, decision time, horizon.

CREATE INDEX IF NOT EXISTS idx_historical_replay_exact_method_version_symbol_time
    ON research_historical_opportunity_outcomes (
        first_touch_method_version,
        replay_version,
        symbol,
        observation_time_utc,
        horizon_minutes
    );

COMMENT ON INDEX idx_historical_replay_exact_method_version_symbol_time IS
    'Supports bounded Replay v2 resume and streaming coherence scans by exact method/version and symbol/time/horizon.';
