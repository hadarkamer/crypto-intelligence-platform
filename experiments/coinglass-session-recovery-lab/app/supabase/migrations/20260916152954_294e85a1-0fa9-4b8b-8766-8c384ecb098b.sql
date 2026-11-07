-- 1. first-event stamps for v2 trades: never invented, never cleared ---------
create or replace function public.v2_trade_first_pnl_stamp()
returns trigger
language plpgsql
set search_path to 'public','pg_temp'
as $function$
declare value_event boolean;
begin
  if tg_op = 'INSERT' then
    if new.pnl is not null and new.first_pnl_at is null then new.first_pnl_at := now(); end if;
    if new.status = 'closed' and new.first_close_at is null then
      new.first_close_at := coalesce(new.closed_at, now());
    end if;
    return new;
  end if;

  -- a stamp written by the validated value-event trigger is server-known and trusted
  value_event := coalesce(current_setting('v2.first_event_stamp', true), '') = 'on';

  if old.first_pnl_at is not null then
    new.first_pnl_at := old.first_pnl_at;                 -- recorded stamps are immutable
  elsif value_event and new.first_pnl_at is not null then
    new.first_pnl_at := new.first_pnl_at;                 -- accepted from the value event
  elsif old.pnl is null and new.pnl is not null then
    new.first_pnl_at := now();                            -- real null -> value transition
  else
    new.first_pnl_at := null;                             -- unknown history stays unknown
  end if;

  if old.first_close_at is not null then
    new.first_close_at := old.first_close_at;
  elsif value_event and new.first_close_at is not null then
    new.first_close_at := new.first_close_at;
  elsif new.status = 'closed' and coalesce(old.status, '') <> 'closed' then
    new.first_close_at := coalesce(new.closed_at, now());
  else
    new.first_close_at := null;
  end if;
  return new;
end $function$;

create or replace function public.v2_trade_value_first_pnl_stamp()
returns trigger
language plpgsql
security definer
set search_path to 'public','pg_temp'
as $function$
declare kind text; txt text;
begin
  select field_type into kind from public.v2_template_fields where id = new.field_id;
  txt := nullif(btrim(coalesce(new.value->>'text','')),'');
  if kind = 'pnl' and (public.v2_field_complete('pnl', jsonb_build_object('text', txt))
      or coalesce((new.value->>'break_even')::boolean,false)) then
    perform set_config('v2.first_event_stamp', 'on', true);
    update public.v2_trades set first_pnl_at = now()
    where id = new.trade_id and first_pnl_at is null;
    perform set_config('v2.first_event_stamp', 'off', true);
  end if;
  return new;
end $function$;

-- 2. legacy first close stamp on real closure --------------------------------
create or replace function public.tg_legacy_cycle_first_close()
returns trigger
language plpgsql
security definer
set search_path to 'public','pg_temp'
as $function$
begin
  if new.trade_cycle_id is null or new.closed_at is null then return new; end if;
  if tg_op = 'UPDATE' and old.closed_at is not null then return new; end if;
  update public.project_trade_cycles set first_close_at = new.closed_at
  where id = new.trade_cycle_id and first_close_at is null;
  return new;
end $function$;

drop trigger if exists tg_legacy_cycle_first_close on public.trades;
create trigger tg_legacy_cycle_first_close
  after insert or update of closed_at on public.trades
  for each row execute function public.tg_legacy_cycle_first_close();

-- 3. manual entries: server side fingerprint + stricter validation -----------
alter table public.v2_manual_batches add column if not exists server_fingerprint text;

alter table public.v2_manual_entries drop constraint if exists v2_manual_entries_pnl_finite;
alter table public.v2_manual_entries
  add constraint v2_manual_entries_pnl_finite check (
    pnl is null or (pnl::text not in ('NaN','Infinity','-Infinity'))
  );

create or replace function public.v2_manual_payload_fingerprint(
  p_workspace_id uuid, p_occurred_at timestamptz, p_notes text, p_conclusions text,
  p_source_kind text, p_rows jsonb, p_next_entry_at timestamptz
) returns text
language sql
immutable
set search_path to 'public','pg_temp'
as $function$
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
        ) as item
        from jsonb_array_elements(p_rows) r
      ) normalized
    )
  )::text);
$function$;

create or replace function public.v2_save_manual_entries(
  p_workspace_id uuid,
  p_batch_id uuid,
  p_occurred_at timestamptz,
  p_notes text,
  p_conclusions text,
  p_source_kind text,
  p_rows jsonb,
  p_fingerprint text default null,
  p_next_entry_at timestamptz default null
) returns setof public.v2_manual_entries
language plpgsql
security definer
set search_path to 'public','pg_temp'
as $function$
declare item jsonb; sym text; amount numeric; keys text[] := '{}'; row_key text;
        existing public.v2_manual_batches; legacy_fp text; server_fp text;
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

  -- the authoritative fingerprint is computed here from the full payload
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
    -- safe transition: backfill the server fingerprint for batches saved by older clients
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
    insert into public.v2_manual_entries(
      workspace_id, occurred_at, symbol, notes, conclusions, pnl, break_even, source_kind,
      batch_id, row_key, created_by, updated_by)
    values (p_workspace_id, p_occurred_at, sym, coalesce(p_notes,''), coalesce(p_conclusions,''),
      amount, coalesce((item->>'break_even')::boolean, false), coalesce(p_source_kind,'trade'),
      p_batch_id, row_key, auth.uid(), auth.uid());
  end loop;

  -- an entry with no amount, no break even flag and no meaningful text is not a record
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
end $function$;

revoke all on function public.v2_save_manual_entries(uuid,uuid,timestamptz,text,text,text,jsonb,text,timestamptz) from public;
grant execute on function public.v2_save_manual_entries(uuid,uuid,timestamptz,text,text,text,jsonb,text,timestamptz) to authenticated;