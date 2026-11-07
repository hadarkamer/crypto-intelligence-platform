-- 1. batch contract for manual entries -------------------------------------
create table if not exists public.v2_manual_batches (
  batch_id uuid primary key,
  workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
  author_id uuid not null,
  fingerprint text not null,
  occurred_at timestamptz not null,
  created_at timestamptz not null default now()
);
grant select on public.v2_manual_batches to authenticated;
grant all on public.v2_manual_batches to service_role;
alter table public.v2_manual_batches enable row level security;
drop policy if exists "manual batches readable by members" on public.v2_manual_batches;
create policy "manual batches readable by members" on public.v2_manual_batches
  for select to authenticated using (public.v2_is_workspace_member(workspace_id));

alter table public.v2_manual_entries
  drop constraint if exists v2_manual_entries_pnl_finite;
alter table public.v2_manual_entries
  add constraint v2_manual_entries_pnl_finite check (pnl is null or pnl::text <> 'NaN');

-- 2. atomic manual save with fingerprint + cursor ---------------------------
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
        existing public.v2_manual_batches; fingerprint text;
begin
  if auth.uid() is null or not coalesce(public.v2_can_edit_workspace(p_workspace_id), false) then
    raise exception 'Workspace edit permission required' using errcode='42501';
  end if;
  if p_batch_id is null or p_occurred_at is null or jsonb_typeof(p_rows) <> 'array'
     or jsonb_array_length(p_rows) = 0
     or coalesce(p_source_kind,'trade') not in ('trade','bot_alert') then
    raise exception 'Invalid manual entry batch' using errcode='23514';
  end if;
  fingerprint := coalesce(nullif(btrim(p_fingerprint),''),
    md5(p_occurred_at::text || coalesce(p_notes,'') || coalesce(p_conclusions,'')
        || coalesce(p_source_kind,'trade') || p_rows::text));
  -- one stable batch id serializes retries
  perform pg_advisory_xact_lock(hashtextextended('v2-manual-entries:'||p_batch_id::text, 0));
  select * into existing from public.v2_manual_batches where batch_id = p_batch_id;
  if existing.batch_id is not null then
    if existing.workspace_id <> p_workspace_id or existing.author_id <> auth.uid()
       or existing.fingerprint <> fingerprint then
      raise exception 'Manual entry batch already saved with different content' using errcode='23505';
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
    if amount is not null and (amount::text = 'NaN' or amount = 'Infinity'::numeric
        or amount = '-Infinity'::numeric) then
      raise exception 'Invalid manual entry amount' using errcode='23514';
    end if;
    if amount is null and coalesce((item->>'break_even')::boolean, false) then
      amount := 0;
    end if;
    insert into public.v2_manual_entries(
      workspace_id, occurred_at, symbol, notes, conclusions, pnl, break_even, source_kind,
      batch_id, row_key, created_by, updated_by)
    values (p_workspace_id, p_occurred_at, sym, coalesce(p_notes,''), coalesce(p_conclusions,''),
      amount, coalesce((item->>'break_even')::boolean, false), coalesce(p_source_kind,'trade'),
      p_batch_id, row_key, auth.uid(), auth.uid());
  end loop;
  insert into public.v2_manual_batches(batch_id, workspace_id, author_id, fingerprint, occurred_at)
  values (p_batch_id, p_workspace_id, auth.uid(), fingerprint, p_occurred_at);
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

-- 3. immutable first-event timestamps for v2 trades -------------------------
alter table public.v2_trades add column if not exists first_close_at timestamptz;

create or replace function public.v2_trade_first_pnl_stamp()
returns trigger
language plpgsql
set search_path to 'public','pg_temp'
as $function$
begin
  if tg_op = 'INSERT' then
    if new.pnl is not null and new.first_pnl_at is null then new.first_pnl_at := now(); end if;
    if new.status = 'closed' and new.first_close_at is null then new.first_close_at := coalesce(new.closed_at, now()); end if;
    return new;
  end if;
  -- earliest recorded moments are immutable; later edits never move them
  if old.first_pnl_at is not null then
    new.first_pnl_at := old.first_pnl_at;
  elsif new.pnl is not null then
    new.first_pnl_at := now();
  else
    new.first_pnl_at := null;
  end if;
  if old.first_close_at is not null then
    new.first_close_at := old.first_close_at;
  elsif new.status = 'closed' then
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
  -- only a valid finite number counts as a PnL event
  if kind = 'pnl' and (public.v2_field_complete('pnl', jsonb_build_object('text', txt))
      or coalesce((new.value->>'break_even')::boolean,false)) then
    update public.v2_trades set first_pnl_at = now()
    where id = new.trade_id and first_pnl_at is null;
  end if;
  return new;
end $function$;

-- break even at the PnL field itself, synced to the trade row
create or replace function public.v2_sync_trade_pnl_value()
returns trigger
language plpgsql
security definer
set search_path to 'public','pg_temp'
as $function$
declare old_kind text; new_kind text; replacement_pnl numeric; flagged boolean; parsed numeric;
begin
  if tg_op<>'INSERT' then
    select field_type into old_kind from public.v2_template_fields where id=old.field_id;
  end if;
  if tg_op<>'DELETE' then
    select field_type into new_kind from public.v2_template_fields where id=new.field_id;
  end if;
  if old_kind='pnl' and (tg_op='DELETE' or new_kind is distinct from 'pnl' or new.trade_id is distinct from old.trade_id) then
    select nullif(trim(v.value->>'text'),'')::numeric into replacement_pnl
    from public.v2_trade_values v join public.v2_template_fields f on f.id=v.field_id
    where v.trade_id=old.trade_id and f.field_type='pnl'
    order by v.updated_at desc,v.created_at desc,v.id desc limit 1;
    update public.v2_trades set pnl=replacement_pnl,updated_by=coalesce(auth.uid(),updated_by),updated_at=now()
    where id=old.trade_id and status='closed';
  end if;
  if new_kind='pnl' then
    flagged := coalesce((new.value->>'break_even')::boolean, false);
    parsed := nullif(trim(new.value->>'text'),'')::numeric;
    -- a checked break even with no amount means an explicit zero; a real amount is never overwritten
    if parsed is null and flagged then parsed := 0; end if;
    update public.v2_trades set pnl=parsed, break_even=flagged,
      updated_by=coalesce(auth.uid(),updated_by), updated_at=now()
    where id=new.trade_id and status='closed';
  end if;
  if tg_op='DELETE' then return old; end if;
  return new;
end $function$;

-- 4. legacy cycle chronology + break even -----------------------------------
alter table public.project_answers add column if not exists break_even boolean not null default false;
alter table public.project_trade_cycles add column if not exists first_pnl_at timestamptz;
alter table public.project_trade_cycles add column if not exists first_close_at timestamptz;

create or replace function public.tg_legacy_cycle_first_pnl()
returns trigger
language plpgsql
security definer
set search_path to 'public','pg_temp'
as $function$
declare is_pnl boolean; txt text;
begin
  if new.trade_cycle_id is null then return new; end if;
  select coalesce(q.field_key,'')='trade_pnl' or coalesce(q.question_type,'')='pnl'
    into is_pnl from public.template_questions q where q.id = new.question_id;
  if not coalesce(is_pnl,false) then return new; end if;
  txt := nullif(btrim(coalesce(new.number_value::text, new.text_value, '')),'');
  if txt is null and not coalesce(new.break_even,false) then return new; end if;
  if txt is not null and txt !~ '^\s*[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)([eE][+-]?[0-9]+)?\s*$'
     and not coalesce(new.break_even,false) then
    return new;
  end if;
  update public.project_trade_cycles set first_pnl_at = now()
  where id = new.trade_cycle_id and first_pnl_at is null;
  return new;
end $function$;

drop trigger if exists zz_legacy_cycle_first_pnl on public.project_answers;
create trigger zz_legacy_cycle_first_pnl
after insert or update on public.project_answers
for each row execute function public.tg_legacy_cycle_first_pnl();

-- 5. conservative historical backfill: only real recorded close times -------
update public.v2_trades set first_close_at = closed_at
where first_close_at is null and closed_at is not null;

update public.project_trade_cycles c set first_close_at = t.closed_at
from public.trades t
where t.trade_cycle_id = c.id and c.first_close_at is null and t.closed_at is not null;