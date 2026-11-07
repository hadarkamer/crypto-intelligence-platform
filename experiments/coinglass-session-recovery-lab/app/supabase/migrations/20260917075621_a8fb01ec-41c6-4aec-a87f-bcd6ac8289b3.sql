CREATE OR REPLACE FUNCTION public.v2_set_legacy_cycle_formula(p_workspace_id uuid, p_cycle_id uuid, p_name text)
 RETURNS text
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  clean text;
  canonical text;
  allowed boolean;
begin
  if auth.uid() is null or not coalesce(public.v2_can_edit_workspace(p_workspace_id), false) then
    raise exception 'Workspace edit permission required' using errcode='42501';
  end if;
  select coalesce(app_private.can_edit_project(c.project_id), false)
    into allowed
    from public.project_trade_cycles c
   where c.id = p_cycle_id;
  if not coalesce(allowed, false) then
    raise exception 'Legacy cycle not editable' using errcode='42501';
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
end $function$;

CREATE OR REPLACE FUNCTION public.v2_set_report_archive(p_workspace_id uuid, p_source_kind text, p_source_key text, p_archived boolean)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  key text;
  asset uuid;
  cycle uuid;
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
    begin
      cycle := key::uuid;
    exception when others then
      raise exception 'Invalid report archive target' using errcode='23514';
    end;
    select coalesce(app_private.can_edit_project(c.project_id), false)
      into allowed
      from public.project_trade_cycles c
     where c.id = cycle;
    if not coalesce(allowed, false) then
      raise exception 'Legacy cycle not editable' using errcode='42501';
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
end $function$;

CREATE OR REPLACE FUNCTION public.v2_editable_legacy_cycles(p_cycle_ids uuid[])
 RETURNS TABLE(cycle_id uuid)
 LANGUAGE sql
 STABLE
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
  select c.id
    from public.project_trade_cycles c
   where c.id = any(coalesce(p_cycle_ids, '{}'::uuid[]))
     and coalesce(app_private.can_edit_project(c.project_id), false)
$function$;

REVOKE ALL ON FUNCTION public.v2_set_legacy_cycle_formula(uuid, uuid, text) FROM PUBLIC, anon;
REVOKE ALL ON FUNCTION public.v2_set_report_archive(uuid, text, text, boolean) FROM PUBLIC, anon;
REVOKE ALL ON FUNCTION public.v2_set_trade_formula(uuid, text) FROM PUBLIC, anon;
REVOKE ALL ON FUNCTION public.v2_set_manual_entry_archived(uuid, boolean) FROM PUBLIC, anon;
REVOKE ALL ON FUNCTION public.v2_add_formula(uuid, text) FROM PUBLIC, anon;
REVOKE ALL ON FUNCTION public.v2_editable_legacy_cycles(uuid[]) FROM PUBLIC, anon;

GRANT EXECUTE ON FUNCTION public.v2_set_legacy_cycle_formula(uuid, uuid, text) TO authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.v2_set_report_archive(uuid, text, text, boolean) TO authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.v2_set_trade_formula(uuid, text) TO authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.v2_set_manual_entry_archived(uuid, boolean) TO authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.v2_add_formula(uuid, text) TO authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.v2_editable_legacy_cycles(uuid[]) TO authenticated, service_role;