-- The complete importer is an internal helper. Its authenticated EXECUTE grant
-- was correctly removed during security hardening, but the two public wrappers
-- still ran as SECURITY INVOKER and could no longer reach it.
-- Keep the helper private and expose only the fixed-kind, workspace-checked RPCs.
create or replace function public.v2_import_legacy_collection_template(p_workspace_id uuid)
returns uuid
language plpgsql
security definer
set search_path = public, pg_temp
as $$
begin
  if auth.uid() is null or not coalesce(public.v2_can_edit_workspace(p_workspace_id), false) then
    raise exception 'Workspace edit permission required' using errcode = '42501';
  end if;
  return public.v2_import_complete_legacy_template(p_workspace_id, 'collection');
end;
$$;

create or replace function public.v2_import_legacy_trade_template(p_workspace_id uuid)
returns uuid
language plpgsql
security definer
set search_path = public, pg_temp
as $$
begin
  if auth.uid() is null or not coalesce(public.v2_can_edit_workspace(p_workspace_id), false) then
    raise exception 'Workspace edit permission required' using errcode = '42501';
  end if;
  return public.v2_import_complete_legacy_template(p_workspace_id, 'trade');
end;
$$;

revoke all on function public.v2_import_legacy_collection_template(uuid) from public, anon;
revoke all on function public.v2_import_legacy_trade_template(uuid) from public, anon;
grant execute on function public.v2_import_legacy_collection_template(uuid) to authenticated;
grant execute on function public.v2_import_legacy_trade_template(uuid) to authenticated;
revoke all on function public.v2_import_complete_legacy_template(uuid, text) from public, anon, authenticated;
