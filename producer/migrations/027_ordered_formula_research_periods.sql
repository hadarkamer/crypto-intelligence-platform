-- Add two overlapping LIVE research periods without rewriting legacy trials.
-- Cutoffs are midnight Asia/Jerusalem (+03:00 on these concrete dates).
ALTER TABLE research_ordered_formula_scopes
    ADD COLUMN IF NOT EXISTS period_key TEXT NOT NULL DEFAULT 'LEGACY_UNSCOPED',
    ADD COLUMN IF NOT EXISTS period_start_utc TIMESTAMPTZ;

-- The original five-dimension unique constraint cannot hold two periods.
-- Locate that exact column set instead of assuming PostgreSQL's truncated name.
DO $$
DECLARE old_constraint RECORD;
BEGIN
    FOR old_constraint IN
        SELECT c.conname FROM pg_constraint c
        WHERE c.conrelid='research_ordered_formula_scopes'::regclass AND c.contype='u'
          AND (SELECT array_agg(a.attname::text ORDER BY a.attname)
               FROM unnest(c.conkey) AS k(attnum)
               JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=k.attnum)
            = ARRAY['candidate_key','direction','symbol','threshold_bps','window_minutes']::text[]
    LOOP
        EXECUTE format('ALTER TABLE research_ordered_formula_scopes DROP CONSTRAINT %I', old_constraint.conname);
    END LOOP;
    IF NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid='research_ordered_formula_scopes'::regclass AND conname='ordered_formula_period_contract') THEN
        ALTER TABLE research_ordered_formula_scopes ADD CONSTRAINT ordered_formula_period_contract CHECK (
            (period_key='LEGACY_UNSCOPED' AND period_start_utc IS NULL)
            OR (period_key='ALL_COMPATIBLE_SINCE_20260816' AND period_start_utc IS NOT NULL AND period_start_utc='2026-08-15T21:00:00Z'::timestamptz)
            OR (period_key='SINCE_20260904' AND period_start_utc IS NOT NULL AND period_start_utc='2026-09-03T21:00:00Z'::timestamptz)
        );
    END IF;
END $$;
CREATE UNIQUE INDEX IF NOT EXISTS idx_ordered_formula_scope_period_identity
    ON research_ordered_formula_scopes(candidate_key,symbol,direction,window_minutes,threshold_bps,period_key);
CREATE INDEX IF NOT EXISTS idx_ordered_formula_scope_period_schedule
    ON research_ordered_formula_scopes(last_evaluated_at_utc ASC NULLS FIRST,scope_key)
    WHERE period_key IN ('ALL_COMPATIBLE_SINCE_20260816','SINCE_20260904');
COMMENT ON COLUMN research_ordered_formula_scopes.period_key IS
    'Overlapping LIVE periods, never additive evidence. LEGACY_UNSCOPED results are retained for audit and no longer scheduled.';
COMMENT ON COLUMN research_ordered_formula_scopes.period_start_utc IS
    'Fixed source cutoff. Entire BTC parent waves starting before cutoff are excluded from period evidence, with explicit coverage counts. All individual source events and outcomes remain retained.';
