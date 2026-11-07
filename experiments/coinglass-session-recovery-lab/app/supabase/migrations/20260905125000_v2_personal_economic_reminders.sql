-- Personal reminder choices. Old shared reminder rows remain unchanged and are
-- used only until that user explicitly chooses their own offsets (including none).
alter table public.economic_event_reminders add column if not exists user_id uuid references auth.users(id) on delete cascade;
alter table public.economic_event_reminders alter column user_id set default auth.uid();
alter table public.economic_event_reminders drop constraint if exists economic_event_reminders_event_id_offset_minutes_key;
create unique index if not exists economic_reminders_event_offset_user_idx
  on public.economic_event_reminders(event_id,offset_minutes,user_id) nulls not distinct;

create table if not exists public.v2_economic_reminder_preferences (
  id uuid primary key default gen_random_uuid(),
  event_id uuid references public.economic_events(id) on delete cascade,
  schedule_id uuid references public.v2_economic_schedules(id) on delete cascade,
  user_id uuid not null default auth.uid() references auth.users(id) on delete cascade,
  reminder_offsets integer[] not null default '{}',
  updated_at timestamptz not null default now(),
  check((event_id is not null)::integer + (schedule_id is not null)::integer = 1),
  unique(event_id,user_id),
  unique(schedule_id,user_id)
);
alter table public.v2_economic_reminder_preferences enable row level security;
revoke all on public.v2_economic_reminder_preferences from anon;
grant select,insert,update,delete on public.v2_economic_reminder_preferences to authenticated;

create or replace function public.v2_valid_reminder_offsets(p_offsets integer[])
returns integer[] language plpgsql immutable set search_path = public, pg_temp as $$
begin
  if p_offsets is null or cardinality(p_offsets) > 24 or exists(select 1 from unnest(p_offsets) m where m is null or m not between 1 and 10080) then
    raise exception 'invalid reminder offsets' using errcode = '23514';
  end if;
  return array(select distinct m from unnest(p_offsets) m order by m);
end $$;

create or replace function public.v2_can_configure_reminders(p_event_id uuid,p_schedule_id uuid)
returns boolean language sql stable security definer set search_path = public, pg_temp as $$
  select auth.uid() is not null and case
    when p_event_id is not null and p_schedule_id is null then exists(select 1 from public.economic_events e where e.id = p_event_id and public.v2_calendar_can_read(e.workspace_id))
    when p_schedule_id is not null and p_event_id is null then exists(select 1 from public.v2_economic_schedules s where s.id = p_schedule_id and public.v2_is_workspace_member(s.workspace_id))
    else false end;
$$;
revoke all on function public.v2_can_configure_reminders(uuid,uuid),public.v2_valid_reminder_offsets(integer[]) from public,anon;
grant execute on function public.v2_can_configure_reminders(uuid,uuid),public.v2_valid_reminder_offsets(integer[]) to authenticated;

drop policy if exists personal_reminders_read on public.v2_economic_reminder_preferences;
drop policy if exists personal_reminders_write on public.v2_economic_reminder_preferences;
create policy personal_reminders_read on public.v2_economic_reminder_preferences for select to authenticated
using(user_id = auth.uid() and public.v2_can_configure_reminders(event_id,schedule_id));
create policy personal_reminders_write on public.v2_economic_reminder_preferences for all to authenticated
using(user_id = auth.uid() and public.v2_can_configure_reminders(event_id,schedule_id))
with check(user_id = auth.uid() and public.v2_can_configure_reminders(event_id,schedule_id));

create or replace function public.v2_validate_reminder_preference()
returns trigger language plpgsql set search_path = public, pg_temp as $$
begin
  if tg_op = 'UPDATE' and (new.user_id is distinct from old.user_id or new.event_id is distinct from old.event_id or new.schedule_id is distinct from old.schedule_id) then
    raise exception 'reminder preference identity is immutable' using errcode = '23514';
  end if;
  new.reminder_offsets := public.v2_valid_reminder_offsets(new.reminder_offsets);
  new.updated_at := now();
  return new;
end $$;
drop trigger if exists v2_reminder_preferences_validate on public.v2_economic_reminder_preferences;
create trigger v2_reminder_preferences_validate before insert or update on public.v2_economic_reminder_preferences
for each row execute function public.v2_validate_reminder_preference();

create or replace function public.v2_has_personal_reminder_choice(p_event_id uuid)
returns boolean language sql stable security definer set search_path = public, pg_temp as $$
  select exists(select 1 from public.v2_economic_reminder_preferences p
    join public.economic_events e on e.id = p_event_id
    where p.user_id = auth.uid() and (p.event_id = e.id or p.schedule_id = e.recurring_schedule_id))
    or exists(select 1 from public.economic_event_reminders r where r.event_id = p_event_id and r.user_id = auth.uid());
$$;
revoke all on function public.v2_has_personal_reminder_choice(uuid) from public,anon;
grant execute on function public.v2_has_personal_reminder_choice(uuid) to authenticated;

drop policy if exists "economic reminders read" on public.economic_event_reminders;
drop policy if exists "economic reminders insert" on public.economic_event_reminders;
drop policy if exists "economic reminders update" on public.economic_event_reminders;
drop policy if exists "economic reminders delete" on public.economic_event_reminders;
create policy "economic reminders read" on public.economic_event_reminders for select to authenticated
using ((user_id = auth.uid() or (user_id is null and not public.v2_has_personal_reminder_choice(event_id)))
  and exists(select 1 from public.economic_events e where e.id = event_id and public.v2_calendar_can_read(e.workspace_id)));
create policy "economic reminders insert" on public.economic_event_reminders for insert to authenticated
with check(user_id = auth.uid() and public.v2_can_configure_reminders(event_id,null));
create policy "economic reminders update" on public.economic_event_reminders for update to authenticated
using(user_id = auth.uid() and public.v2_can_configure_reminders(event_id,null))
with check(user_id = auth.uid() and public.v2_can_configure_reminders(event_id,null));
create policy "economic reminders delete" on public.economic_event_reminders for delete to authenticated
using(user_id = auth.uid() and public.v2_can_configure_reminders(event_id,null));

-- Called only by trusted triggers/RPCs after their authorization checks.
create or replace function public.v2_sync_personal_event_reminders(p_event_id uuid)
returns void language plpgsql security definer set search_path = public, pg_temp as $$
declare e public.economic_events%rowtype; p record;
begin
  select * into e from public.economic_events where id = p_event_id;
  if not found then return; end if;
  -- Prefer this user's event-specific choice over their recurring-series choice.
  for p in select distinct on(user_id) user_id,reminder_offsets
    from public.v2_economic_reminder_preferences
    where event_id = e.id or schedule_id = e.recurring_schedule_id
    order by user_id,(event_id is not null) desc
  loop
    insert into public.economic_event_reminders(event_id,offset_minutes,due_at,user_id)
    select e.id,m,e.event_at - make_interval(mins => m),p.user_id from unnest(p.reminder_offsets) m
    on conflict(event_id,offset_minutes,user_id) do nothing;
    delete from public.economic_event_reminders r where r.event_id = e.id and r.user_id = p.user_id
      and not(r.offset_minutes = any(p.reminder_offsets));
  end loop;
  -- Direct personal reminder rows accepted by RLS remain valid even without
  -- a preferences row. Only explicit removal of a preference resets its rows.
end $$;

create or replace function public.v2_sync_reminder_preferences()
returns trigger language plpgsql security definer set search_path = public, pg_temp as $$
declare target record; e record;
begin
  if tg_op = 'DELETE' then target := old; else target := new; end if;
  for e in select id from public.economic_events
    where id = target.event_id or (recurring_schedule_id = target.schedule_id and event_at > now())
  loop
    if tg_op = 'DELETE' then
      delete from public.economic_event_reminders r where r.event_id = e.id and r.user_id = target.user_id
        and not exists(select 1 from public.v2_economic_reminder_preferences pref
          join public.economic_events ev on ev.id = e.id
          where pref.user_id = target.user_id and (pref.event_id = ev.id or pref.schedule_id = ev.recurring_schedule_id));
    end if;
    perform public.v2_sync_personal_event_reminders(e.id);
  end loop;
  if tg_op = 'DELETE' then return old; end if;
  return new;
end $$;
drop trigger if exists v2_reminder_preferences_sync on public.v2_economic_reminder_preferences;
create trigger v2_reminder_preferences_sync after insert or update or delete on public.v2_economic_reminder_preferences
for each row execute function public.v2_sync_reminder_preferences();

create or replace function public.v2_event_personal_reminders()
returns trigger language plpgsql security definer set search_path = public, pg_temp as $$
begin perform public.v2_sync_personal_event_reminders(new.id); return new; end $$;
drop trigger if exists v2_events_personal_reminders on public.economic_events;
create trigger v2_events_personal_reminders after insert or update on public.economic_events
for each row execute function public.v2_event_personal_reminders();

create or replace function public.v2_set_economic_reminders(p_event_id uuid,p_schedule_id uuid,p_offsets integer[])
returns void language plpgsql security definer set search_path = public, pg_temp as $$
begin
  if not public.v2_can_configure_reminders(p_event_id,p_schedule_id) then
    raise exception 'calendar access required' using errcode = '42501';
  end if;
  if p_event_id is not null then
    insert into public.v2_economic_reminder_preferences(event_id,user_id,reminder_offsets)
    values(p_event_id,auth.uid(),public.v2_valid_reminder_offsets(p_offsets))
    on conflict(event_id,user_id) do update set reminder_offsets = excluded.reminder_offsets;
  else
    insert into public.v2_economic_reminder_preferences(schedule_id,user_id,reminder_offsets)
    values(p_schedule_id,auth.uid(),public.v2_valid_reminder_offsets(p_offsets))
    on conflict(schedule_id,user_id) do update set reminder_offsets = excluded.reminder_offsets;
  end if;
end $$;

create or replace function public.v2_save_economic_event(p_id uuid,p_workspace_id uuid,p_name text,p_event_at timestamptz,p_importance integer,p_offsets integer[])
returns uuid language plpgsql security definer set search_path = public, pg_temp as $$
declare e public.economic_events%rowtype; result_id uuid;
begin
  perform public.v2_valid_reminder_offsets(p_offsets);
  if p_name is null or btrim(p_name) = '' or p_event_at is null or p_importance not between 1 and 3 then
    raise exception 'invalid calendar event' using errcode = '23514';
  end if;
  if p_id is null then
    if not public.v2_calendar_can_edit(p_workspace_id) then raise exception 'calendar edit permission required' using errcode = '42501'; end if;
    insert into public.economic_events(name,event_at,importance,workspace_id,created_by,updated_by)
    values(btrim(p_name),p_event_at,p_importance,p_workspace_id,auth.uid(),auth.uid()) returning id into result_id;
  else
    select * into e from public.economic_events where id = p_id for update;
    if not found or not public.v2_calendar_can_edit(e.workspace_id) then raise exception 'calendar edit permission required' using errcode = '42501'; end if;
    if p_workspace_id is distinct from e.workspace_id then raise exception 'calendar workspace is immutable' using errcode = '23514'; end if;
    update public.economic_events set name = btrim(p_name),event_at = p_event_at,importance = p_importance,updated_by = auth.uid() where id = p_id;
    result_id := p_id;
  end if;
  perform public.v2_set_economic_reminders(result_id,null,p_offsets);
  return result_id;
end $$;

create or replace function public.v2_save_economic_schedule(p_schedule jsonb,p_offsets integer[])
returns public.v2_economic_schedules language plpgsql security definer set search_path = public, pg_temp as $$
declare s public.v2_economic_schedules%rowtype; schedule_id uuid := nullif(p_schedule->>'id','')::uuid; wid uuid := (p_schedule->>'workspace_id')::uuid;
begin
  perform public.v2_valid_reminder_offsets(p_offsets);
  if not public.v2_can_edit_workspace(wid) then raise exception 'workspace edit permission required' using errcode = '42501'; end if;
  if schedule_id is null then
    insert into public.v2_economic_schedules(workspace_id,name,importance,weekdays,times,reminder_offsets,starts_on,ends_on,active,timezone,created_by,updated_by)
    values(wid,p_schedule->>'name',coalesce((p_schedule->>'importance')::smallint,1),
      array(select jsonb_array_elements_text(p_schedule->'weekdays')::smallint),
      array(select jsonb_array_elements_text(p_schedule->'times')),'{}',
      (p_schedule->>'starts_on')::date,nullif(p_schedule->>'ends_on','')::date,coalesce((p_schedule->>'active')::boolean,true),
      coalesce(p_schedule->>'timezone','Asia/Jerusalem'),auth.uid(),auth.uid()) returning * into s;
  else
    select * into s from public.v2_economic_schedules where id = schedule_id for update;
    if not found or s.workspace_id <> wid then raise exception 'schedule edit permission required' using errcode = '42501'; end if;
    update public.v2_economic_schedules set name = p_schedule->>'name',importance = (p_schedule->>'importance')::smallint,
      weekdays = array(select jsonb_array_elements_text(p_schedule->'weekdays')::smallint),
      times = array(select jsonb_array_elements_text(p_schedule->'times')),
      starts_on = (p_schedule->>'starts_on')::date,ends_on = nullif(p_schedule->>'ends_on','')::date,
      active = coalesce((p_schedule->>'active')::boolean,true),timezone = coalesce(p_schedule->>'timezone',s.timezone)
    where id = schedule_id returning * into s;
  end if;
  perform public.v2_set_economic_reminders(null,s.id,p_offsets);
  s.reminder_offsets := public.v2_valid_reminder_offsets(p_offsets);
  return s;
end $$;

revoke all on function public.v2_validate_reminder_preference(),public.v2_sync_personal_event_reminders(uuid),
  public.v2_sync_reminder_preferences(),public.v2_event_personal_reminders() from public,anon,authenticated;
revoke all on function public.v2_set_economic_reminders(uuid,uuid,integer[]),
  public.v2_save_economic_event(uuid,uuid,text,timestamptz,integer,integer[]),
  public.v2_save_economic_schedule(jsonb,integer[]) from public,anon;
grant execute on function public.v2_set_economic_reminders(uuid,uuid,integer[]),
  public.v2_save_economic_event(uuid,uuid,text,timestamptz,integer,integer[]),
  public.v2_save_economic_schedule(jsonb,integer[]) to authenticated;

-- Preserve shared schedule defaults separately from personal reminders.
create or replace function public.v2_materialize_economic_schedule(p_schedule_id uuid, p_horizon_days integer default 14)
returns integer language plpgsql security definer set search_path = public, pg_temp as $$
declare
  s public.v2_economic_schedules%rowtype;
  first_day date; last_day date; day_value date; clock_time text;
  event_time timestamptz; occurrence text; v_event_id uuid;
  desired text[] := '{}'::text[];
  total integer := 0;
begin
  select * into s from public.v2_economic_schedules where id = p_schedule_id for update;
  if not found or not public.v2_can_edit_workspace(s.workspace_id) then
    raise exception 'schedule edit permission required' using errcode = '42501';
  end if;
  if p_horizon_days is null or p_horizon_days not between 1 and 90 then
    raise exception 'calendar horizon must be between 1 and 90 days' using errcode = '22023';
  end if;
  first_day := greatest((now() at time zone s.timezone)::date, s.starts_on);
  last_day := least((now() at time zone s.timezone)::date + p_horizon_days, coalesce(s.ends_on, 'infinity'::date));
  -- Reconcile already materialized future dates when a caller requests a
  -- shorter refresh, so edited names/offsets stay consistent for the series.
  last_day := least(coalesce(s.ends_on, 'infinity'::date), greatest(last_day, coalesce(
    (select max((e.event_at at time zone s.timezone)::date) from public.economic_events e
      where e.recurring_schedule_id = s.id and e.event_at > now()
        and (e.event_at at time zone s.timezone)::date <= (now() at time zone s.timezone)::date + 90), last_day)));
  if s.active and first_day <= last_day then
    for day_value in select d::date from generate_series(first_day::timestamp, last_day::timestamp, interval '1 day') d loop
      if extract(dow from day_value)::smallint = any(s.weekdays) then
        foreach clock_time in array s.times loop
          occurrence := day_value::text || 'T' || clock_time;
          event_time := (day_value + clock_time::time) at time zone s.timezone;
          if event_time <= now() then continue; end if;
          desired := array_append(desired, occurrence);
          insert into public.economic_events(name,event_at,importance,workspace_id,recurring_schedule_id,occurrence_key,created_by,updated_by)
          values(s.name,event_time,s.importance,s.workspace_id,s.id,occurrence,auth.uid(),auth.uid())
          on conflict(recurring_schedule_id,occurrence_key) do update
          set name = excluded.name, event_at = excluded.event_at, importance = excluded.importance, updated_by = auth.uid()
          returning id into v_event_id;
          insert into public.economic_event_reminders(event_id,offset_minutes,due_at,user_id)
          select v_event_id, m, event_time - make_interval(mins => m), null::uuid from unnest(s.reminder_offsets) m
          on conflict(event_id,offset_minutes,user_id) do nothing;
          delete from public.economic_event_reminders r
          where r.event_id = v_event_id and r.user_id is null and not (r.offset_minutes = any(s.reminder_offsets));
          total := total + 1;
        end loop;
      end if;
    end loop;
  end if;
  -- A shorter refresh horizon must not erase valid occurrences outside it.
  -- Configuration changes are reconciled against their own date/day/time rules.
  delete from public.economic_events e
  where e.recurring_schedule_id = s.id and e.event_at > now()
    and (not s.active
      or (e.event_at at time zone s.timezone)::date < s.starts_on
      or (s.ends_on is not null and (e.event_at at time zone s.timezone)::date > s.ends_on)
      or not(extract(dow from e.event_at at time zone s.timezone)::smallint = any(s.weekdays))
      or not(to_char(e.event_at at time zone s.timezone,'HH24:MI') = any(s.times))
      or ((e.event_at at time zone s.timezone)::date <= last_day and not(e.occurrence_key = any(desired))));
  return total;
end $$;

-- The schedule column is a legacy shared fallback, not a writable personal
-- preference. Client saves always use the separate per-user preference API.
create or replace function public.v2_preserve_shared_schedule_reminders()
returns trigger language plpgsql set search_path = public, pg_temp as $$
begin
  if auth.uid() is not null then
    if tg_op = 'INSERT' then new.reminder_offsets := '{}'::integer[];
    else new.reminder_offsets := old.reminder_offsets; end if;
  end if;
  return new;
end $$;
drop trigger if exists v2_economic_schedules_personal_defaults on public.v2_economic_schedules;
create trigger v2_economic_schedules_personal_defaults before insert or update on public.v2_economic_schedules
for each row execute function public.v2_preserve_shared_schedule_reminders();
revoke all on function public.v2_preserve_shared_schedule_reminders() from public,anon,authenticated;
