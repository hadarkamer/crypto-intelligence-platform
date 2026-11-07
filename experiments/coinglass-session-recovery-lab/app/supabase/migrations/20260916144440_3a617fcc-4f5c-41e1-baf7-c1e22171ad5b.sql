-- 1) Trade effective/first-PnL timestamps
ALTER TABLE public.v2_trades
  ADD COLUMN IF NOT EXISTS first_pnl_at timestamptz,
  ADD COLUMN IF NOT EXISTS break_even boolean NOT NULL DEFAULT false;

CREATE OR REPLACE FUNCTION public.v2_trade_first_pnl_stamp()
RETURNS trigger
LANGUAGE plpgsql
SET search_path TO 'public','pg_temp'
AS $$
begin
  if tg_op = 'INSERT' then
    if new.pnl is not null and new.first_pnl_at is null then new.first_pnl_at := now(); end if;
    return new;
  end if;
  -- Earliest known PnL moment is immutable once recorded.
  if old.first_pnl_at is not null then
    new.first_pnl_at := old.first_pnl_at;
  elsif new.pnl is not null then
    new.first_pnl_at := now();
  else
    new.first_pnl_at := null;
  end if;
  return new;
end $$;

DROP TRIGGER IF EXISTS zz_v2_trade_first_pnl_stamp ON public.v2_trades;
CREATE TRIGGER zz_v2_trade_first_pnl_stamp
BEFORE INSERT OR UPDATE ON public.v2_trades
FOR EACH ROW EXECUTE FUNCTION public.v2_trade_first_pnl_stamp();

-- A PnL worksheet value is also a first-PnL event, at the DB write boundary.
CREATE OR REPLACE FUNCTION public.v2_trade_value_first_pnl_stamp()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public','pg_temp'
AS $$
declare kind text;
begin
  select field_type into kind from public.v2_template_fields where id = new.field_id;
  if kind = 'pnl' and nullif(trim(coalesce(new.value->>'text','')),'') is not null then
    update public.v2_trades set first_pnl_at = now()
    where id = new.trade_id and first_pnl_at is null;
  end if;
  return new;
end $$;

DROP TRIGGER IF EXISTS zz_v2_trade_value_first_pnl_stamp ON public.v2_trade_values;
CREATE TRIGGER zz_v2_trade_value_first_pnl_stamp
AFTER INSERT OR UPDATE ON public.v2_trade_values
FOR EACH ROW EXECUTE FUNCTION public.v2_trade_value_first_pnl_stamp();

-- 2) Manual journal / PnL entries
CREATE TABLE IF NOT EXISTS public.v2_manual_entries (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  workspace_id uuid NOT NULL REFERENCES public.v2_workspaces(id) ON DELETE CASCADE,
  occurred_at timestamptz NOT NULL DEFAULT now(),
  symbol text NOT NULL DEFAULT 'BTC',
  notes text NOT NULL DEFAULT '',
  conclusions text NOT NULL DEFAULT '',
  pnl numeric,
  break_even boolean NOT NULL DEFAULT false,
  source_kind text NOT NULL DEFAULT 'trade',
  batch_id uuid NOT NULL,
  row_key text NOT NULL,
  created_by uuid NOT NULL DEFAULT auth.uid(),
  updated_by uuid,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT v2_manual_entries_symbol_check CHECK (symbol ~ '^[A-Z0-9][A-Z0-9._-]{0,19}$'),
  CONSTRAINT v2_manual_entries_source_check CHECK (source_kind IN ('trade','bot_alert')),
  CONSTRAINT v2_manual_entries_batch_row_key UNIQUE (workspace_id, batch_id, row_key)
);

GRANT SELECT, INSERT, UPDATE, DELETE ON public.v2_manual_entries TO authenticated;
GRANT ALL ON public.v2_manual_entries TO service_role;
ALTER TABLE public.v2_manual_entries ENABLE ROW LEVEL SECURITY;

CREATE POLICY "manual entries readable by workspace members"
ON public.v2_manual_entries FOR SELECT TO authenticated
USING (public.v2_is_workspace_member(workspace_id));

CREATE POLICY "manual entries inserted by editors"
ON public.v2_manual_entries FOR INSERT TO authenticated
WITH CHECK (public.v2_can_edit_workspace(workspace_id));

CREATE POLICY "manual entries updated by editors"
ON public.v2_manual_entries FOR UPDATE TO authenticated
USING (public.v2_can_edit_workspace(workspace_id))
WITH CHECK (public.v2_can_edit_workspace(workspace_id));

CREATE POLICY "manual entries deleted by editors"
ON public.v2_manual_entries FOR DELETE TO authenticated
USING (public.v2_can_edit_workspace(workspace_id));

CREATE OR REPLACE FUNCTION public.v2_touch_manual_entry()
RETURNS trigger
LANGUAGE plpgsql
SET search_path TO 'public','pg_temp'
AS $$
begin
  new.updated_at := now();
  if tg_op = 'INSERT' then
    new.created_by := coalesce(auth.uid(), new.created_by);
  else
    new.created_by := old.created_by;
    new.created_at := old.created_at;
  end if;
  new.updated_by := coalesce(auth.uid(), new.updated_by);
  return new;
end $$;

DROP TRIGGER IF EXISTS v2_touch_manual_entry ON public.v2_manual_entries;
CREATE TRIGGER v2_touch_manual_entry
BEFORE INSERT OR UPDATE ON public.v2_manual_entries
FOR EACH ROW EXECUTE FUNCTION public.v2_touch_manual_entry();

CREATE INDEX IF NOT EXISTS v2_manual_entries_workspace_time
  ON public.v2_manual_entries (workspace_id, occurred_at DESC);

-- 3) Per user + workspace report settings
CREATE TABLE IF NOT EXISTS public.v2_report_settings (
  user_id uuid NOT NULL,
  workspace_id uuid NOT NULL REFERENCES public.v2_workspaces(id) ON DELETE CASCADE,
  be_min numeric NOT NULL DEFAULT 0,
  be_max numeric NOT NULL DEFAULT 0,
  stats_mode text NOT NULL DEFAULT 'A',
  next_entry_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, workspace_id),
  CONSTRAINT v2_report_settings_range CHECK (be_min <= be_max),
  CONSTRAINT v2_report_settings_mode CHECK (stats_mode IN ('A','B','C'))
);

GRANT SELECT, INSERT, UPDATE, DELETE ON public.v2_report_settings TO authenticated;
GRANT ALL ON public.v2_report_settings TO service_role;
ALTER TABLE public.v2_report_settings ENABLE ROW LEVEL SECURITY;

CREATE POLICY "own report settings"
ON public.v2_report_settings FOR ALL TO authenticated
USING (user_id = auth.uid() AND public.v2_is_workspace_member(workspace_id))
WITH CHECK (user_id = auth.uid() AND public.v2_is_workspace_member(workspace_id));

CREATE OR REPLACE FUNCTION public.v2_touch_report_settings()
RETURNS trigger LANGUAGE plpgsql SET search_path TO 'public','pg_temp'
AS $$ begin new.updated_at := now(); return new; end $$;

DROP TRIGGER IF EXISTS v2_touch_report_settings ON public.v2_report_settings;
CREATE TRIGGER v2_touch_report_settings
BEFORE UPDATE ON public.v2_report_settings
FOR EACH ROW EXECUTE FUNCTION public.v2_touch_report_settings();

-- 4) Atomic, idempotent manual batch save
CREATE OR REPLACE FUNCTION public.v2_save_manual_entries(
  p_workspace_id uuid,
  p_batch_id uuid,
  p_occurred_at timestamptz,
  p_notes text,
  p_conclusions text,
  p_source_kind text,
  p_rows jsonb
) RETURNS SETOF public.v2_manual_entries
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public','pg_temp'
AS $$
declare item jsonb; sym text; amount numeric;
begin
  if auth.uid() is null or not coalesce(public.v2_can_edit_workspace(p_workspace_id), false) then
    raise exception 'Workspace edit permission required' using errcode='42501';
  end if;
  if p_batch_id is null or p_occurred_at is null or jsonb_typeof(p_rows) <> 'array'
     or jsonb_array_length(p_rows) = 0
     or coalesce(p_source_kind,'trade') not in ('trade','bot_alert') then
    raise exception 'Invalid manual entry batch' using errcode='23514';
  end if;
  -- One stable batch id serializes retries: already stored rows are never duplicated.
  perform pg_advisory_xact_lock(hashtextextended('v2-manual-entries:'||p_batch_id::text, 0));
  for item in select * from jsonb_array_elements(p_rows) loop
    sym := upper(btrim(coalesce(item->>'symbol','BTC')));
    if sym !~ '^[A-Z0-9][A-Z0-9._-]{0,19}$' or nullif(btrim(coalesce(item->>'row_key','')),'') is null then
      raise exception 'Invalid manual entry row' using errcode='23514';
    end if;
    amount := nullif(item->>'pnl','')::numeric;
    if amount is not null and not (amount = amount) then
      raise exception 'Invalid manual entry amount' using errcode='23514';
    end if;
    insert into public.v2_manual_entries(
      workspace_id, occurred_at, symbol, notes, conclusions, pnl, break_even, source_kind, batch_id, row_key, created_by, updated_by)
    values (p_workspace_id, p_occurred_at, sym, coalesce(p_notes,''), coalesce(p_conclusions,''),
      amount, coalesce((item->>'break_even')::boolean, false), coalesce(p_source_kind,'trade'),
      p_batch_id, btrim(item->>'row_key'), auth.uid(), auth.uid())
    on conflict (workspace_id, batch_id, row_key) do nothing;
  end loop;
  return query select * from public.v2_manual_entries
    where workspace_id = p_workspace_id and batch_id = p_batch_id order by row_key;
end $$;

REVOKE ALL ON FUNCTION public.v2_save_manual_entries(uuid,uuid,timestamptz,text,text,text,jsonb) FROM public;
GRANT EXECUTE ON FUNCTION public.v2_save_manual_entries(uuid,uuid,timestamptz,text,text,text,jsonb) TO authenticated;