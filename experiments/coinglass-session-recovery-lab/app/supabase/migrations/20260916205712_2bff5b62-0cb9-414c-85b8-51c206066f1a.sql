-- שמירה אטומית של שם נוסחה על טרייד: רישום ברשימה + עדכון השורה בטרנזקציה אחת
create or replace function public.v2_set_trade_formula(p_trade_id uuid, p_name text)
returns text
language plpgsql
security definer
set search_path to 'public','pg_temp'
as $$
declare
  ws uuid;
  clean text;
  canonical text;
begin
  select workspace_id into ws from public.v2_trades where id = p_trade_id;
  if ws is null then
    raise exception 'Trade not found' using errcode='42501';
  end if;
  if auth.uid() is null or not coalesce(public.v2_can_edit_workspace(ws), false) then
    raise exception 'Workspace edit permission required' using errcode='42501';
  end if;
  clean := nullif(btrim(coalesce(p_name,'')),'');
  if clean is not null then
    if length(clean) > 80 then
      raise exception 'Invalid formula name' using errcode='23514';
    end if;
    insert into public.v2_formulas(workspace_id, name, created_by)
    values (ws, clean, auth.uid())
    on conflict (workspace_id, normalized_name) do nothing;
    select name into canonical from public.v2_formulas
     where workspace_id = ws and normalized_name = lower(clean);
  end if;
  update public.v2_trades
     set formula_name = canonical, updated_by = auth.uid()
   where id = p_trade_id;
  return canonical;
end $$;

grant execute on function public.v2_set_trade_formula(uuid, text) to authenticated;

-- מחזור ישן: אותה פעולה אטומית, ברמת המחזור הבודד בלבד
create or replace function public.v2_set_legacy_cycle_formula(
  p_workspace_id uuid, p_cycle_id uuid, p_name text
)
returns text
language plpgsql
security definer
set search_path to 'public','pg_temp'
as $$
declare
  clean text;
  canonical text;
  allowed boolean;
begin
  if auth.uid() is null or not coalesce(public.v2_can_edit_workspace(p_workspace_id), false) then
    raise exception 'Workspace edit permission required' using errcode='42501';
  end if;
  select exists (
    select 1
      from public.project_trade_cycles c
      join public.projects p on p.id = c.project_id
     where c.id = p_cycle_id
       and (p.user_id = auth.uid()
            or exists (select 1 from public.project_members m
                        where m.project_id = p.id and m.user_id = auth.uid()))
  ) into allowed;
  if not allowed then
    raise exception 'Legacy cycle not accessible' using errcode='42501';
  end if;
  clean := nullif(btrim(coalesce(p_name,'')),'');
  if clean is not null then
    if length(clean) > 80 then
      raise exception 'Invalid formula name' using errcode='23514';
    end if;
    insert into public.v2_formulas(workspace_id, name, created_by)
    values (p_workspace_id, clean, auth.uid())
    on conflict (workspace_id, normalized_name) do nothing;
    select name into canonical from public.v2_formulas
     where workspace_id = p_workspace_id and normalized_name = lower(clean);
  end if;
  update public.project_trade_cycles set formula_name = canonical where id = p_cycle_id;
  return canonical;
end $$;

grant execute on function public.v2_set_legacy_cycle_formula(uuid, uuid, text) to authenticated;

-- אימות זהות המקור לפני סימון הסתרה בדוח
create or replace function public.v2_set_report_archive(
  p_workspace_id uuid, p_source_kind text, p_source_key text, p_archived boolean
)
returns void
language plpgsql
security definer
set search_path to 'public','pg_temp'
as $$
declare
  key text;
  asset uuid;
  allowed boolean;
begin
  if auth.uid() is null or not coalesce(public.v2_can_edit_workspace(p_workspace_id), false) then
    raise exception 'Workspace edit permission required' using errcode='42501';
  end if;
  key := nullif(btrim(coalesce(p_source_key,'')),'');
  if p_source_kind not in ('spot_lot','legacy_cycle') or key is null then
    raise exception 'Invalid report archive target' using errcode='23514';
  end if;
  if p_source_kind = 'spot_lot' then
    -- מפתח יציב: <asset_id>:<lot_key>; הנכס חייב להיות בסביבת העבודה
    begin
      asset := split_part(key, ':', 1)::uuid;
    exception when others then
      raise exception 'Invalid report archive target' using errcode='23514';
    end;
    if not exists (select 1 from public.v2_spot_assets
                    where id = asset and workspace_id = p_workspace_id) then
      raise exception 'Spot asset not in workspace' using errcode='42501';
    end if;
  else
    select exists (
      select 1
        from public.project_trade_cycles c
        join public.projects p on p.id = c.project_id
       where c.id::text = key
         and (p.user_id = auth.uid()
              or exists (select 1 from public.project_members m
                          where m.project_id = p.id and m.user_id = auth.uid()))
    ) into allowed;
    if not allowed then
      raise exception 'Legacy cycle not accessible' using errcode='42501';
    end if;
  end if;
  if p_archived then
    insert into public.v2_report_archives(workspace_id, source_kind, source_key, archived_by)
    values (p_workspace_id, p_source_kind, key, auth.uid())
    on conflict (workspace_id, source_kind, source_key) do nothing;
  else
    delete from public.v2_report_archives
     where workspace_id = p_workspace_id and source_kind = p_source_kind and source_key = key;
  end if;
end $$;