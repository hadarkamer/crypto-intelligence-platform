-- Formula Discovery horizon scheduler v1
--
-- Additive operational metadata only.  The recurring worker never executes
-- DDL: this migration is applied once by research_formula_schema_admin.py
-- before the Formula workers start.

ALTER TABLE research_formula_runs
    ADD COLUMN IF NOT EXISTS scheduler_version TEXT,
    ADD COLUMN IF NOT EXISTS schedule_slot_utc TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS source_watermark JSONB,
    ADD COLUMN IF NOT EXISTS source_watermark_sha256 CHAR(64),
    ADD COLUMN IF NOT EXISTS walk_forward_policy_version TEXT,
    ADD COLUMN IF NOT EXISTS purge_policy_version TEXT,
    ADD COLUMN IF NOT EXISTS embargo_policy_version TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_formula_runs_scheduler_slot
    ON research_formula_runs (
        scheduler_version, engine_version, feature_schema_version,
        outcome_method_version, horizon_minutes, schedule_slot_utc
    )
    WHERE scheduler_version IS NOT NULL AND schedule_slot_utc IS NOT NULL;

CREATE TABLE IF NOT EXISTS research_formula_discovery_schedule_state (
    scheduler_version TEXT NOT NULL,
    engine_version TEXT NOT NULL,
    feature_schema_version TEXT NOT NULL,
    outcome_method_version TEXT NOT NULL,
    horizon_minutes INTEGER NOT NULL CHECK (
        horizon_minutes IN (60, 240, 720, 1440)
    ),
    last_slot_utc TIMESTAMPTZ NOT NULL,
    last_due_at_utc TIMESTAMPTZ NOT NULL,
    last_analysis_as_of_utc TIMESTAMPTZ NOT NULL,
    last_status TEXT NOT NULL CHECK (
        last_status IN (
            'COMPLETED', 'SKIPPED_UNCHANGED', 'SKIPPED_UNAVAILABLE', 'FAILED'
        )
    ),
    last_source_watermark JSONB,
    last_source_watermark_sha256 CHAR(64),
    last_discovery_run_id BIGINT REFERENCES research_formula_runs(run_id)
        ON DELETE SET NULL,
    last_reason TEXT,
    checked_at_utc TIMESTAMPTZ NOT NULL,
    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (
        scheduler_version, engine_version, feature_schema_version,
        outcome_method_version, horizon_minutes
    ),
    CHECK (last_due_at_utc >= last_slot_utc),
    CHECK (last_analysis_as_of_utc = last_due_at_utc),
    CHECK (
        (last_source_watermark IS NULL) =
        (last_source_watermark_sha256 IS NULL)
    ),
    CHECK (
        last_source_watermark_sha256 IS NULL
        OR BTRIM(last_source_watermark_sha256) ~ '^[0-9a-f]{64}$'
    ),
    CHECK (
        last_status <> 'COMPLETED' OR last_discovery_run_id IS NOT NULL
    )
);

CREATE INDEX IF NOT EXISTS idx_formula_discovery_schedule_state_time
    ON research_formula_discovery_schedule_state (
        scheduler_version, engine_version, feature_schema_version,
        outcome_method_version, last_slot_utc DESC, horizon_minutes
    );

COMMENT ON TABLE research_formula_discovery_schedule_state IS
    'One operational watermark per horizon. PostgreSQL advisory locks serialize recurring Discovery; this table makes restart/slot skipping durable.';
