-- 1) formula registry -------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.v2_formulas (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  workspace_id uuid NOT NULL REFERENCES public.v2_workspaces(id) ON DELETE CASCADE,
  name text NOT NULL,
  normalized_name text GENERATED ALWAYS AS (lower(btrim(name))) STORED,
  created_by uuid,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT v2_formulas_name_not_blank CHECK (btrim(name) <> '')
);
CREATE UNIQUE INDEX IF NOT EXISTS v2_formulas_workspace_name_key
  ON public.v2_formulas (workspace_id, normalized_name);

GRANT SELECT, INSERT, UPDATE, DELETE ON public.v2_formulas TO authenticated;
GRANT ALL ON public.v2_formulas TO service_role;
ALTER TABLE public.v2_formulas ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "v2_formulas_select_members" ON public.v2_formulas;
CREATE POLICY "v2_formulas_select_members" ON public.v2_formulas
  FOR SELECT TO authenticated USING (public.v2_is_workspace_member(workspace_id));
DROP POLICY IF EXISTS "v2_formulas_insert_editors" ON public.v2_formulas;
CREATE POLICY "v2_formulas_insert_editors" ON public.v2_formulas
  FOR INSERT TO authenticated WITH CHECK (public.v2_can_edit_workspace(workspace_id));
DROP POLICY IF EXISTS "v2_formulas_update_editors" ON public.v2_formulas;
CREATE POLICY "v2_formulas_update_editors" ON public.v2_formulas
  FOR UPDATE TO authenticated USING (public.v2_can_edit_workspace(workspace_id))
  WITH CHECK (public.v2_can_edit_workspace(workspace_id));

DROP TRIGGER IF EXISTS v2_formulas_touch ON public.v2_formulas;
CREATE TRIGGER v2_formulas_touch BEFORE UPDATE ON public.v2_formulas
  FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

-- 2) nullable formula name on the actual rows --------------------------------
ALTER TABLE public.v2_manual_entries ADD COLUMN IF NOT EXISTS formula_name text;
ALTER TABLE public.v2_trades ADD COLUMN IF NOT EXISTS formula_name text;
ALTER TABLE public.project_trade_cycles ADD COLUMN IF NOT EXISTS formula_name text;

-- 3) archive metadata --------------------------------------------------------
ALTER TABLE public.v2_manual_entries ADD COLUMN IF NOT EXISTS archived_at timestamptz;
ALTER TABLE public.v2_manual_entries ADD COLUMN IF NOT EXISTS archived_by uuid;

CREATE TABLE IF NOT EXISTS public.v2_report_archives (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  workspace_id uuid NOT NULL REFERENCES public.v2_workspaces(id) ON DELETE CASCADE,
  source_kind text NOT NULL CHECK (source_kind IN ('spot_lot','legacy_cycle')),
  source_key text NOT NULL,
  archived_at timestamptz NOT NULL DEFAULT now(),
  archived_by uuid,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (workspace_id, source_kind, source_key)
);
GRANT SELECT, INSERT, UPDATE, DELETE ON public.v2_report_archives TO authenticated;
GRANT ALL ON public.v2_report_archives TO service_role;
ALTER TABLE public.v2_report_archives ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "v2_report_archives_select_members" ON public.v2_report_archives;
CREATE POLICY "v2_report_archives_select_members" ON public.v2_report_archives
  FOR SELECT TO authenticated USING (public.v2_is_workspace_member(workspace_id));
DROP POLICY IF EXISTS "v2_report_archives_write_editors" ON public.v2_report_archives;
CREATE POLICY "v2_report_archives_write_editors" ON public.v2_report_archives
  FOR ALL TO authenticated USING (public.v2_can_edit_workspace(workspace_id))
  WITH CHECK (public.v2_can_edit_workspace(workspace_id));

DROP TRIGGER IF EXISTS v2_report_archives_touch ON public.v2_report_archives;
CREATE TRIGGER v2_report_archives_touch BEFORE UPDATE ON public.v2_report_archives
  FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

-- 4) archive / restore RPCs --------------------------------------------------
CREATE OR REPLACE FUNCTION public.v2_set_manual_entry_archived(p_entry_id uuid, p_archived boolean)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'public','pg_temp' AS $$
begin
  update public.v2_manual_entries
     set archived_at = case when p_archived then coalesce(archived_at, now()) else null end,
         archived_by = case when p_archived then auth.uid() else null end,
         updated_by  = auth.uid()
   where id = p_entry_id and public.v2_can_edit_workspace(workspace_id);
  if not found then
    raise exception 'Workspace edit permission required' using errcode='42501';
  end if;
end $$;

CREATE OR REPLACE FUNCTION public.v2_set_report_archive(
  p_workspace_id uuid, p_source_kind text, p_source_key text, p_archived boolean)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'public','pg_temp' AS $$
begin
  if auth.uid() is null or not coalesce(public.v2_can_edit_workspace(p_workspace_id), false) then
    raise exception 'Workspace edit permission required' using errcode='42501';
  end if;
  if p_source_kind not in ('spot_lot','legacy_cycle') or nullif(btrim(coalesce(p_source_key,'')),'') is null then
    raise exception 'Invalid report archive target' using errcode='23514';
  end if;
  if p_archived then
    insert into public.v2_report_archives(workspace_id, source_kind, source_key, archived_by)
    values (p_workspace_id, p_source_kind, btrim(p_source_key), auth.uid())
    on conflict (workspace_id, source_kind, source_key) do nothing;
  else
    delete from public.v2_report_archives
     where workspace_id = p_workspace_id and source_kind = p_source_kind
       and source_key = btrim(p_source_key);
  end if;
end $$;

CREATE OR REPLACE FUNCTION public.v2_add_formula(p_workspace_id uuid, p_name text)
RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'public','pg_temp' AS $$
declare clean text; canonical text;
begin
  if auth.uid() is null or not coalesce(public.v2_can_edit_workspace(p_workspace_id), false) then
    raise exception 'Workspace edit permission required' using errcode='42501';
  end if;
  clean := nullif(btrim(coalesce(p_name,'')),'');
  if clean is null then raise exception 'Invalid formula name' using errcode='23514'; end if;
  if length(clean) > 80 then raise exception 'Invalid formula name' using errcode='23514'; end if;
  insert into public.v2_formulas(workspace_id, name, created_by)
  values (p_workspace_id, clean, auth.uid())
  on conflict (workspace_id, normalized_name) do nothing;
  select name into canonical from public.v2_formulas
   where workspace_id = p_workspace_id and normalized_name = lower(clean);
  return canonical;
end $$;

-- 5) fingerprint keeps formula-less payloads byte-identical -------------------
CREATE OR REPLACE FUNCTION public.v2_manual_payload_fingerprint(
  p_workspace_id uuid, p_occurred_at timestamptz, p_notes text, p_conclusions text,
  p_source_kind text, p_rows jsonb, p_next_entry_at timestamptz)
RETURNS text LANGUAGE sql IMMUTABLE SET search_path TO 'public','pg_temp' AS $$
  select md5(jsonb_build_object(
    'workspace', p_workspace_id,
    'occurred_at', to_char(p_occurred_at at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MSZ'),
    'notes', coalesce(p_notes,''),
    'conclusions', coalesce(p_conclusions,''),
    'source_kind', coalesce(p_source_kind,'trade'),
    'next_entry_at', coalesce(to_char(p_next_entry_at at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MSZ'),''),
    'rows', (
      select coalesce(jsonb_agg(item order by item->>'row_key'), '[]'::jsonb)
      from (
        select jsonb_build_object(
          'row_key', coalesce(r->>'row_key',''),
          'symbol', upper(btrim(coalesce(r->>'symbol','BTC'))),
          'pnl', coalesce(nullif(btrim(coalesce(r->>'pnl','')),''),''),
          'break_even', coalesce((r->>'break_even')::boolean,false)
        )
        -- formula key is present only when supplied, so older payloads hash exactly as before
        || case when nullif(btrim(coalesce(r->>'formula_name','')),'') is null
                then '{}'::jsonb
                else jsonb_build_object('formula_name', btrim(r->>'formula_name')) end as item
        from jsonb_array_elements(p_rows) r
      ) normalized
    )
  )::text);
$$;

-- 6) manual save stores the per-row formula and registers new names atomically
CREATE OR REPLACE FUNCTION public.v2_save_manual_entries(
  p_workspace_id uuid, p_batch_id uuid, p_occurred_at timestamptz, p_notes text,
  p_conclusions text, p_source_kind text, p_rows jsonb,
  p_fingerprint text DEFAULT NULL, p_next_entry_at timestamptz DEFAULT NULL)
RETURNS SETOF public.v2_manual_entries
LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'public','pg_temp' AS $$
declare item jsonb; sym text; amount numeric; keys text[] := '{}'; row_key text;
        existing public.v2_manual_batches; legacy_fp text; server_fp text; formula text;
        has_amount boolean := false; has_text boolean;
begin
  if auth.uid() is null or not coalesce(public.v2_can_edit_workspace(p_workspace_id), false) then
    raise exception 'Workspace edit permission required' using errcode='42501';
  end if;
  if p_batch_id is null or p_occurred_at is null or jsonb_typeof(p_rows) <> 'array'
     or jsonb_array_length(p_rows) = 0
     or coalesce(p_source_kind,'trade') not in ('trade','bot_alert') then
    raise exception 'Invalid manual entry batch' using errcode='23514';
  end if;

  server_fp := public.v2_manual_payload_fingerprint(
    p_workspace_id, p_occurred_at, p_notes, p_conclusions, p_source_kind, p_rows, p_next_entry_at);
  legacy_fp := coalesce(nullif(btrim(p_fingerprint),''),
    md5(p_occurred_at::text || coalesce(p_notes,'') || coalesce(p_conclusions,'')
        || coalesce(p_source_kind,'trade') || p_rows::text));

  perform pg_advisory_xact_lock(hashtextextended('v2-manual-entries:'||p_batch_id::text, 0));
  select * into existing from public.v2_manual_batches where batch_id = p_batch_id;
  if existing.batch_id is not null then
    if existing.workspace_id <> p_workspace_id or existing.author_id <> auth.uid()
       or (existing.server_fingerprint is not null and existing.server_fingerprint <> server_fp)
       or (existing.server_fingerprint is null and existing.fingerprint <> legacy_fp) then
      raise exception 'Manual entry batch already saved with different content' using errcode='23505';
    end if;
    if existing.server_fingerprint is null then
      update public.v2_manual_batches set server_fingerprint = server_fp where batch_id = p_batch_id;
    end if;
    return query select * from public.v2_manual_entries
      where workspace_id = p_workspace_id and batch_id = p_batch_id order by row_key;
    return;
  end if;

  for item in select * from jsonb_array_elements(p_rows) loop
    sym := upper(btrim(coalesce(item->>'symbol','BTC')));
    row_key := nullif(btrim(coalesce(item->>'row_key','')),'');
    if sym !~ '^[A-Z0-9][A-Z0-9._-]{0,19}$' or row_key is null then
      raise exception 'Invalid manual entry row' using errcode='23514';
    end if;
    if row_key = any(keys) then
      raise exception 'Duplicate manual entry row key' using errcode='23505';
    end if;
    keys := keys || row_key;
    begin
      amount := nullif(btrim(coalesce(item->>'pnl','')),'')::numeric;
    exception when others then
      raise exception 'Invalid manual entry amount' using errcode='23514';
    end;
    if amount is not null and amount::text in ('NaN','Infinity','-Infinity') then
      raise exception 'Invalid manual entry amount' using errcode='23514';
    end if;
    if amount is null and coalesce((item->>'break_even')::boolean, false) then
      amount := 0;
    end if;
    if amount is not null then has_amount := true; end if;

    formula := nullif(btrim(coalesce(item->>'formula_name','')),'');
    if formula is not null then
      if length(formula) > 80 then
        raise exception 'Invalid formula name' using errcode='23514';
      end if;
      insert into public.v2_formulas(workspace_id, name, created_by)
      values (p_workspace_id, formula, auth.uid())
      on conflict (workspace_id, normalized_name) do nothing;
      -- the first human-readable spelling wins
      select name into formula from public.v2_formulas
       where workspace_id = p_workspace_id and normalized_name = lower(formula);
    end if;

    insert into public.v2_manual_entries(
      workspace_id, occurred_at, symbol, notes, conclusions, pnl, break_even, source_kind,
      batch_id, row_key, formula_name, created_by, updated_by)
    values (p_workspace_id, p_occurred_at, sym, coalesce(p_notes,''), coalesce(p_conclusions,''),
      amount, coalesce((item->>'break_even')::boolean, false), coalesce(p_source_kind,'trade'),
      p_batch_id, row_key, formula, auth.uid(), auth.uid());
  end loop;

  has_text := length(btrim(regexp_replace(
    coalesce(p_notes,'') || ' ' || coalesce(p_conclusions,''), '<[^>]*>|&nbsp;', ' ', 'g'))) > 0;
  if not has_amount and not has_text then
    raise exception 'Empty manual entry' using errcode='23514';
  end if;

  insert into public.v2_manual_batches(
    batch_id, workspace_id, author_id, fingerprint, server_fingerprint, occurred_at)
  values (p_batch_id, p_workspace_id, auth.uid(), legacy_fp, server_fp, p_occurred_at);
  if p_next_entry_at is not null then
    insert into public.v2_report_settings(user_id, workspace_id, next_entry_at)
    values (auth.uid(), p_workspace_id, p_next_entry_at)
    on conflict (user_id, workspace_id) do update set next_entry_at = excluded.next_entry_at;
  end if;
  return query select * from public.v2_manual_entries
    where workspace_id = p_workspace_id and batch_id = p_batch_id order by row_key;
end $$;