-- Separate derived-perpetual full-wave reports; canonical labels remain untouched.
CREATE TABLE IF NOT EXISTS research_btc_wave_report_state (
    worker_key text PRIMARY KEY,
    pending_job jsonb,
    report jsonb,
    source jsonb,
    report_observed_at_utc timestamptz,
    last_completed_at_utc timestamptz,
    next_report_at_utc timestamptz,
    last_error text,
    updated_at_utc timestamptz NOT NULL DEFAULT NOW()
);
INSERT INTO research_btc_wave_report_state(worker_key)
VALUES ('native-maxpain-fullwave-archive-perp-v1') ON CONFLICT DO NOTHING;
