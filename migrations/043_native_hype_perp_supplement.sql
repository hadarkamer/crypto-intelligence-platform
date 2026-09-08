-- Explicit derived PERP measurements. Never a native Spot outcome or a new wave.
CREATE TABLE IF NOT EXISTS research_native_hype_perp_measurements (
    event_id BIGINT NOT NULL REFERENCES research_events(event_id),
    adapter_version TEXT NOT NULL,
    source_sha256 TEXT NOT NULL CHECK(source_sha256 ~ '^[0-9a-f]{64}$'),
    source_event JSONB NOT NULL,
    btc_membership JSONB,
    observed_at_utc TIMESTAMPTZ NOT NULL,
    measurement_payload JSONB NOT NULL,
    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(event_id,adapter_version),
    CHECK(measurement_payload->>'source_scope'='DERIVED_NATIVE_HYPE_PERP'),
    CHECK(measurement_payload->>'live_union_eligible'='false')
);
CREATE TABLE IF NOT EXISTS research_native_hype_perp_outcomes (
    event_id BIGINT NOT NULL,
    adapter_version TEXT NOT NULL,
    window_minutes INTEGER NOT NULL CHECK(window_minutes IN(60,240,720,1440)),
    threshold_bps INTEGER NOT NULL CHECK(threshold_bps IN(25,50,75,100,125,150,175,200)),
    status TEXT NOT NULL,
    outcome_payload JSONB NOT NULL,
    PRIMARY KEY(event_id,adapter_version,window_minutes,threshold_bps),
    FOREIGN KEY(event_id,adapter_version) REFERENCES research_native_hype_perp_measurements(event_id,adapter_version)
);
CREATE TABLE IF NOT EXISTS research_native_hype_perp_metrics (
    event_id BIGINT NOT NULL,
    adapter_version TEXT NOT NULL,
    window_minutes INTEGER NOT NULL CHECK(window_minutes IN(60,240,720,1440)),
    status TEXT NOT NULL,
    metrics_payload JSONB NOT NULL,
    PRIMARY KEY(event_id,adapter_version,window_minutes),
    FOREIGN KEY(event_id,adapter_version) REFERENCES research_native_hype_perp_measurements(event_id,adapter_version)
);
