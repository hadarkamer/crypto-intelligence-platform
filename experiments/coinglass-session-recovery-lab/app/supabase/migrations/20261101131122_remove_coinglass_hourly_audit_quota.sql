-- Remove the application-owned hourly audit quota for CoinGlass starts.
-- Keep the fourth argument (ignored) for clients already open during deployment.
-- Authorization, lease TTL, expiration cleanup and same-scope exclusion are unchanged.
create or replace function public.v2_acquire_ai_lease(
  p_scope_key text,
  p_workspace_id uuid,
  p_ttl_seconds integer default 300,
  p_hourly_limit integer default null
)
returns jsonb
language plpgsql
security definer
set search_path to 'public', 'pg_temp'
as $function$
declare inserted integer; ttl integer;
begin
  if auth.uid() is null then
    raise exception 'Authentication required' using errcode = '42501';
  end if;
  if nullif(btrim(coalesce(p_scope_key, '')), '') is null then
    raise exception 'Invalid lease key' using errcode = '22023';
  end if;
  if not coalesce(public.v2_can_edit_workspace(p_workspace_id), false) then
    raise exception 'Entity edit permission required' using errcode = '42501';
  end if;
  ttl := least(greatest(coalesce(p_ttl_seconds, 300), 30), 900);

  delete from public.v2_ai_run_leases where expires_at < now();

  insert into public.v2_ai_run_leases(scope_key, workspace_id, user_id, expires_at)
  values (p_scope_key, p_workspace_id, auth.uid(), now() + make_interval(secs => ttl))
  on conflict (scope_key) do nothing;
  get diagnostics inserted = row_count;

  return jsonb_build_object('acquired', inserted > 0, 'reason', case when inserted > 0 then 'ok' else 'busy' end);
end
$function$;

revoke all on function public.v2_acquire_ai_lease(text, uuid, integer, integer) from public, anon;
grant execute on function public.v2_acquire_ai_lease(text, uuid, integer, integer) to authenticated, service_role;

