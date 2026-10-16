-- Versioned full-history reports, persisted one complete BTC parent at a time.
-- Existing report state and published legacy generations remain untouched.
CREATE TABLE IF NOT EXISTS research_btc_wave_report_generations (
    generation_id UUID PRIMARY KEY,
    worker_key TEXT NOT NULL,
    observed_at_utc TIMESTAMPTZ NOT NULL,
    parent_count INTEGER NOT NULL CHECK (parent_count >= 0),
    status TEXT NOT NULL CHECK (status IN ('CAPTURED','PUBLISHED','ABANDONED')),
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_btc_wave_generation_worker_created
    ON research_btc_wave_report_generations(worker_key,created_at_utc DESC,generation_id);

CREATE TABLE IF NOT EXISTS research_btc_wave_report_parts (
    generation_id UUID NOT NULL REFERENCES research_btc_wave_report_generations(generation_id),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    parent_id TEXT NOT NULL,
    parent_start_utc TIMESTAMPTZ NOT NULL,
    source JSONB,
    source_digest TEXT,
    source_bytes INTEGER,
    job JSONB,
    report JSONB,
    report_digest TEXT,
    report_bytes INTEGER,
    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(generation_id,ordinal),
    UNIQUE(generation_id,parent_id),
    CHECK ((source IS NULL AND source_digest IS NULL AND source_bytes IS NULL)
        OR (source IS NOT NULL AND source_digest IS NOT NULL AND source_bytes IS NOT NULL
            AND jsonb_typeof(source)='object' AND source_digest ~ '^[0-9a-f]{64}$'
            AND source_bytes BETWEEN 1 AND 16777216)),
    CHECK ((report IS NULL AND report_digest IS NULL AND report_bytes IS NULL)
        OR (report IS NOT NULL AND report_digest IS NOT NULL AND report_bytes IS NOT NULL
            AND jsonb_typeof(report)='object' AND report_digest ~ '^[0-9a-f]{64}$'
            AND report_bytes BETWEEN 1 AND 16777216)),
    CHECK (job IS NULL OR (jsonb_typeof(job)='object' AND NOT (job ? 'source'))),
    CHECK ((job IS NULL AND report IS NULL) OR source IS NOT NULL)
);
CREATE INDEX IF NOT EXISTS idx_btc_wave_unfinished_parts
    ON research_btc_wave_report_parts(generation_id,ordinal) WHERE report IS NULL;
