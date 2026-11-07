-- ---------------------------------------------------------------------------
-- Atomic commit for the CoinGlass Model 1 AI liquidity fill.
-- ---------------------------------------------------------------------------

-- 1. Persistent run lease (server-side concurrency + rate protection).
create table if not exists public.v2_ai_run_leases (
  scope_key text primary key,
  workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
  user_id uuid not null,
  started_at timestamptz not null default now(),
  expires_at timestamptz not null
);

grant select on public.v2_ai_run_leases to authenticated;
grant all on public.v2_ai_run_leases to service_role;

alter table public.v2_ai_run_leases enable row level security;

drop policy if exists "members read ai run leases" on public.v2_ai_run_leases;
create policy "members read ai run leases" on public.v2_ai_run_leases
  for select to authenticated
  using (public.v2_is_workspace_member(workspace_id));

-- 2. Tighten the audit insert policy: session and field must belong to the
--    same workspace as the audit row itself.
drop policy if exists "editors insert liquidity ai runs" on public.v2_liquidity_ai_runs;
create policy "editors insert liquidity ai runs" on public.v2_liquidity_ai_runs
  for insert to authenticated
  with check (
    public.v2_can_edit_workspace(workspace_id)
    and created_by = auth.uid()
    and exists (
      select 1
      from public.v2_collection_sessions s
      join public.v2_template_fields f on f.id = v2_liquidity_ai_runs.field_id
      join public.v2_template_sections sec on sec.id = f.section_id
      where s.id = v2_liquidity_ai_runs.session_id
        and s.workspace_id = v2_liquidity_ai_runs.workspace_id
        and sec.template_id = s.template_id
    )
  );

-- 3. Level builder: strict validation of one timeframe's AI levels.
create or replace function public.v2_ai_liquidity_levels(p_row_id text, p_levels jsonb)
returns jsonb
language plpgsql
immutable
set search_path to 'public', 'pg_temp'
as $function$
declare lev jsonb; out_levels jsonb := '[]'::jsonb; n integer := 0; side text; amount text; price text;
begin
  if jsonb_typeof(p_levels) <> 'array' then
    raise exception 'AI levels must be an array' using errcode = '22023';
  end if;
  if jsonb_array_length(p_levels) > 12 then
    raise exception 'Too many AI levels' using errcode = '22023';
  end if;
  for lev in select value from jsonb_array_elements(p_levels) loop
    if jsonb_typeof(lev) <> 'object' then
      raise exception 'AI level must be an object' using errcode = '22023';
    end if;
    side := lev ->> 'side';
    amount := lev ->> 'amount';
    price := lev ->> 'price_text';
    if side is null or side not in ('above', 'below') then
      raise exception 'Invalid AI level side' using errcode = '22023';
    end if;
    if amount is null or amount not in ('many', 'normal', 'few') then
      raise exception 'Invalid AI level amount' using errcode = '22023';
    end if;
    if nullif(btrim(coalesce(price, '')), '') is null then
      raise exception 'Invalid AI level price' using errcode = '22023';
    end if;
    out_levels := out_levels || jsonb_build_array(jsonb_build_object(
      'id', p_row_id || ':ai:' || n::text,
      'row_id', p_row_id,
      'side', side,
      'price_text', price,
      'amount', amount,
      'taken', false,
      'position', n,
      'source', 'ai'
    ));
    n := n + 1;
  end loop;
  return out_levels;
end
$function$;

revoke all on function public.v2_ai_liquidity_levels(text, jsonb) from public;
grant execute on function public.v2_ai_liquidity_levels(text, jsonb) to authenticated, service_role;

-- 4. The single atomic commit. SECURITY INVOKER: every write stays under the
--    caller's own RLS, including the audit insert.
create or replace function public.v2_commit_liquidity_ai_run(
  p_session_id uuid,
  p_field_id uuid,
  p_baseline_updated_at timestamptz,
  p_results jsonb,
  p_audit jsonb
)
returns jsonb
language plpgsql
security invoker
set search_path to 'public', 'pg_temp'
as $function$
declare
  wid uuid; tid uuid; sstatus text; sdel timestamptz;
  ftype text; factive boolean;
  v jsonb; prev jsonb; cur timestamptz; newts timestamptz;
  normalized jsonb; result_rows jsonb := '[]'::jsonb;
  r jsonb; res jsonb; tf text; rid text; seen text[] := '{}';
begin
  if auth.uid() is null then
    raise exception 'Authentication required' using errcode = '42501';
  end if;
  if jsonb_typeof(p_results) <> 'array'
     or jsonb_array_length(p_results) = 0
     or jsonb_array_length(p_results) > 3 then
    raise exception 'Invalid results payload' using errcode = '22023';
  end if;
  if jsonb_typeof(p_audit) <> 'array'
     or jsonb_array_length(p_audit) <> jsonb_array_length(p_results) then
    raise exception 'Invalid audit payload' using errcode = '22023';
  end if;

  select workspace_id, template_id, status, deleted_at
    into wid, tid, sstatus, sdel
    from public.v2_collection_sessions
   where id = p_session_id
   for update;
  if wid is null then
    raise exception 'Collection not found' using errcode = '42704';
  end if;
  if sdel is not null then
    raise exception 'Archived collection is read only' using errcode = '42501';
  end if;
  if sstatus is distinct from 'draft' then
    raise exception 'Collection is not open for editing' using errcode = '42501';
  end if;
  if not coalesce(public.v2_can_edit_workspace(wid), false) then
    raise exception 'Entity edit permission required' using errcode = '42501';
  end if;

  select f.field_type, f.active
    into ftype, factive
    from public.v2_template_fields f
    join public.v2_template_sections sec on sec.id = f.section_id
    join public.v2_templates t on t.id = sec.template_id
   where f.id = p_field_id
     and sec.template_id = tid
     and t.workspace_id = wid;
  if ftype is distinct from 'liquidity_table' or coalesce(factive, false) is false then
    raise exception 'Field is not an active liquidity field of this collection' using errcode = '23514';
  end if;

  select value, updated_at into v, cur
    from public.v2_collection_values
   where session_id = p_session_id and field_id = p_field_id
   for update;

  if p_baseline_updated_at is distinct from cur then
    return jsonb_build_object('status', 'conflict', 'updated_at', cur);
  end if;

  v := coalesce(v, '{}'::jsonb);
  if jsonb_typeof(v) <> 'object' then
    raise exception 'Existing structured value has invalid shape' using errcode = '23514';
  end if;
  prev := v;
  normalized := public.v2_normalize_structured_rows(p_field_id, 'liquidity_table', v);

  -- Replace only the timeframes we actually captured; reuse the existing row id.
  for r in select value from jsonb_array_elements(normalized) loop
    tf := r ->> 'timeframe';
    res := null;
    if tf is not null and not (tf = any(seen)) then
      select value into res from jsonb_array_elements(p_results) where value ->> 'timeframe' = tf limit 1;
    end if;
    if res is not null then
      seen := seen || tf;
      r := r || jsonb_build_object('levels', public.v2_ai_liquidity_levels(r ->> 'id', res -> 'levels'));
    end if;
    result_rows := result_rows || jsonb_build_array(r);
  end loop;

  for res in select value from jsonb_array_elements(p_results) loop
    tf := res ->> 'timeframe';
    if tf is null or tf = any(seen) then
      continue;
    end if;
    seen := seen || tf;
    rid := p_field_id::text || ':' || tf;
    result_rows := result_rows || jsonb_build_array(jsonb_build_object(
      'id', rid,
      'timeframe', tf,
      'position', jsonb_array_length(result_rows),
      'levels', public.v2_ai_liquidity_levels(rid, res -> 'levels')
    ));
  end loop;

  v := v || jsonb_build_object('rows', result_rows);
  if jsonb_typeof(v #> '{structured,rows}') = 'array' then
    v := jsonb_set(v, '{structured,rows}', result_rows);
  end if;

  insert into public.v2_collection_values(session_id, field_id, value, updated_by, updated_at)
  values (p_session_id, p_field_id, v, auth.uid(), now())
  on conflict (session_id, field_id)
    do update set value = excluded.value, updated_by = auth.uid(), updated_at = now()
  returning updated_at into newts;

  -- Audit in the SAME transaction: an audit failure rolls the write back.
  insert into public.v2_liquidity_ai_runs(
    workspace_id, session_id, field_id, timeframe, source_url, provider, model,
    captured_at, observed_price, screenshot, analysis, previous_value, usage, created_by)
  select
    wid, p_session_id, p_field_id,
    a ->> 'timeframe', a ->> 'source_url', a ->> 'provider', a ->> 'model',
    (a ->> 'captured_at')::timestamptz, (a ->> 'observed_price')::numeric,
    a ->> 'screenshot', coalesce(a -> 'analysis', '{}'::jsonb), prev, a -> 'usage', auth.uid()
  from jsonb_array_elements(p_audit) a;

  return jsonb_build_object('status', 'ok', 'updated_at', newts, 'rows', jsonb_array_length(result_rows));
end
$function$;

revoke all on function public.v2_commit_liquidity_ai_run(uuid, uuid, timestamptz, jsonb, jsonb) from public;
grant execute on function public.v2_commit_liquidity_ai_run(uuid, uuid, timestamptz, jsonb, jsonb) to authenticated, service_role;

-- 5. Lease acquire / release.
create or replace function public.v2_acquire_ai_lease(
  p_scope_key text,
  p_workspace_id uuid,
  p_ttl_seconds integer default 300,
  p_hourly_limit integer default 20
)
returns jsonb
language plpgsql
security definer
set search_path to 'public', 'pg_temp'
as $function$
declare recent integer; inserted integer; ttl integer;
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

  select count(*) into recent
    from public.v2_liquidity_ai_runs
   where workspace_id = p_workspace_id
     and created_at > now() - interval '1 hour';
  if recent >= greatest(coalesce(p_hourly_limit, 20), 1) then
    return jsonb_build_object('acquired', false, 'reason', 'rate_limited');
  end if;

  insert into public.v2_ai_run_leases(scope_key, workspace_id, user_id, expires_at)
  values (p_scope_key, p_workspace_id, auth.uid(), now() + make_interval(secs => ttl))
  on conflict (scope_key) do nothing;
  get diagnostics inserted = row_count;

  return jsonb_build_object('acquired', inserted > 0, 'reason', case when inserted > 0 then 'ok' else 'busy' end);
end
$function$;

revoke all on function public.v2_acquire_ai_lease(text, uuid, integer, integer) from public;
grant execute on function public.v2_acquire_ai_lease(text, uuid, integer, integer) to authenticated, service_role;

create or replace function public.v2_release_ai_lease(p_scope_key text)
returns void
language plpgsql
security definer
set search_path to 'public', 'pg_temp'
as $function$
begin
  if auth.uid() is null then
    raise exception 'Authentication required' using errcode = '42501';
  end if;
  delete from public.v2_ai_run_leases
   where scope_key = p_scope_key
     and (user_id = auth.uid() or expires_at < now());
end
$function$;

revoke all on function public.v2_release_ai_lease(text) from public;
grant execute on function public.v2_release_ai_lease(text) to authenticated, service_role;