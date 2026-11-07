-- Phase 4B integrity hardening.
-- A liquidity level must belong to the same project as its parent timeframe row.
-- Written idempotently because the production database was hardened before this migration was committed.

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'project_liquidity_rows_id_project_key'
      AND conrelid = 'public.project_liquidity_rows'::regclass
  ) THEN
    ALTER TABLE public.project_liquidity_rows
      ADD CONSTRAINT project_liquidity_rows_id_project_key UNIQUE (id, project_id);
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'project_liquidity_levels_row_project_fkey'
      AND conrelid = 'public.project_liquidity_levels'::regclass
  ) THEN
    ALTER TABLE public.project_liquidity_levels
      ADD CONSTRAINT project_liquidity_levels_row_project_fkey
      FOREIGN KEY (row_id, project_id)
      REFERENCES public.project_liquidity_rows(id, project_id)
      ON DELETE CASCADE;
  END IF;
END
$$;
