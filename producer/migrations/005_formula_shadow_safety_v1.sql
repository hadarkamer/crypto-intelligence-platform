-- Formula Shadow safety v1
-- Additive only. Shadow remains observational: these objects preserve the
-- decision-time evidence and require a separate, explicit human approval
-- record before any LIVE delivery can be queued or sent.

ALTER TABLE research_formula_shadow_checks
    ADD COLUMN IF NOT EXISTS evaluation_status TEXT NOT NULL DEFAULT 'UNMATCHED';

ALTER TABLE research_formula_shadow_checks
    ADD COLUMN IF NOT EXISTS evaluation_reason TEXT;

ALTER TABLE research_formula_shadow_checks
    ADD COLUMN IF NOT EXISTS input_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE research_formula_shadow_checks
    ADD COLUMN IF NOT EXISTS condition_results JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE research_formula_shadow_checks
    ADD COLUMN IF NOT EXISTS decision_cohort_key CHAR(64);

ALTER TABLE research_formula_shadow_checks
    ADD COLUMN IF NOT EXISTS decision_anchor_time_utc TIMESTAMPTZ;

-- This migration is replayed on every startup.  Normalize the one legacy
-- mismatch only on the first upgrade, before migration 013 installs the
-- append-only evidence guard.  Every later replay skips UPDATE altogether.
DO $migration$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM pg_trigger
         WHERE tgrelid = 'public.research_formula_shadow_checks'::regclass
           AND tgname = 'trg_formula_shadow_checks_append_only'
           AND tgisinternal = FALSE
    ) THEN
        UPDATE research_formula_shadow_checks
           SET evaluation_status = 'MATCHED'
         WHERE evaluation_status = 'UNMATCHED'
           AND matched IS TRUE;
    END IF;
END;
$migration$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'research_formula_shadow_checks_evaluation_status_check'
          AND conrelid = 'public.research_formula_shadow_checks'::regclass
    ) THEN
        ALTER TABLE research_formula_shadow_checks
            ADD CONSTRAINT research_formula_shadow_checks_evaluation_status_check
            CHECK (evaluation_status IN ('MATCHED', 'UNMATCHED', 'UNEVALUABLE'));
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx_formula_shadow_checks_cohort
    ON research_formula_shadow_checks (
        formula_id, decision_cohort_key, decision_anchor_time_utc, event_id
    );

CREATE TABLE IF NOT EXISTS research_formula_live_approvals (
    approval_id BIGSERIAL PRIMARY KEY,
    formula_id BIGINT NOT NULL REFERENCES research_formulas(formula_id) ON DELETE CASCADE,
    formula_version INTEGER NOT NULL CHECK (formula_version > 0),
    horizon_minutes INTEGER NOT NULL CHECK (horizon_minutes IN (60, 240, 720, 1440)),
    review_kind TEXT NOT NULL CHECK (review_kind='FROZEN_PROSPECTIVE'),
    validation_policy_version TEXT NOT NULL CHECK (BTRIM(validation_policy_version) <> ''),
    validation_started_at_utc TIMESTAMPTZ NOT NULL,
    validation_cutoff_event_id BIGINT NOT NULL CHECK (validation_cutoff_event_id > 0),
    validation_cutoff_time_utc TIMESTAMPTZ NOT NULL,
    validation_fingerprint CHAR(64) NOT NULL CHECK (
        BTRIM(validation_fingerprint) ~ '^[0-9a-f]{64}$'
    ),
    validated_future_matches INTEGER NOT NULL CHECK (validated_future_matches >= 12),
    validated_future_controls INTEGER NOT NULL CHECK (validated_future_controls >= 12),
    validated_span_hours DOUBLE PRECISION NOT NULL CHECK (validated_span_hours >= 72.0),
    validated_utc_dates INTEGER NOT NULL CHECK (validated_utc_dates >= 3),
    thresholds_met BOOLEAN NOT NULL CHECK (thresholds_met=TRUE),
    approved_by TEXT NOT NULL CHECK (BTRIM(approved_by) <> ''),
    approval_reason TEXT NOT NULL CHECK (BTRIM(approval_reason) <> ''),
    validation_snapshot JSONB NOT NULL CHECK (
        JSONB_TYPEOF(validation_snapshot)='object'
        AND validation_snapshot @> '{"thresholds_met": true}'::jsonb
    ),
    approved_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (validation_started_at_utc <= validation_cutoff_time_utc),
    CHECK (validation_cutoff_time_utc <= approved_at_utc),
    UNIQUE (formula_id, formula_version)
);

CREATE INDEX IF NOT EXISTS idx_formula_live_approvals_formula
    ON research_formula_live_approvals (formula_id, formula_version, approved_at_utc DESC);

CREATE OR REPLACE FUNCTION prevent_formula_live_approval_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'research_formula_live_approvals is append-only';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_formula_live_approvals_append_only
    ON research_formula_live_approvals;

CREATE TRIGGER trg_formula_live_approvals_append_only
BEFORE UPDATE OR DELETE ON research_formula_live_approvals
FOR EACH ROW EXECUTE FUNCTION prevent_formula_live_approval_mutation();
