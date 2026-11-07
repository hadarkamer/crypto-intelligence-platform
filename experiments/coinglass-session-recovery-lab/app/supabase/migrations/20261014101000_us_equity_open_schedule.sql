-- Regular U.S. equity market opening, using the official NYSE calendar verified
-- 2026-10-14: https://www.nyse.com/trade/hours-calendars (2026, 2027, 2028).
-- Nasdaq's 2026 calendar agrees: https://www.nasdaqtrader.com/Trader.aspx?id=Calendar.
-- Times are exchange-local America/New_York, never a fixed Israeli UTC offset.
-- No production schedule is created by this migration; creation is an explicit,
-- workspace-authorized operation through v2_create_us_equity_open_schedule.

begin;

alter table public.v2_economic_schedules
  add column if not exists market_calendar text;
do $$ begin
  if not exists(select 1 from pg_constraint where conrelid='public.v2_economic_schedules'::regclass
    and conname='v2_economic_schedule_market_calendar_check') then
    alter table public.v2_economic_schedules add constraint v2_economic_schedule_market_calendar_check
      check(market_calendar is null or market_calendar='us_equities');
  end if;
end $$;
create unique index if not exists v2_economic_schedules_us_equity_workspace_idx
  on public.v2_economic_schedules(workspace_id) where market_calendar='us_equities';

create table if not exists public.v2_us_equity_calendar_coverage (
  singleton boolean primary key default true check(singleton),
  verified_from date not null,
  verified_through date not null check(verified_through >= verified_from),
  source_url text not null check(source_url='https://www.nyse.com/trade/hours-calendars'),
  verified_on date not null
);
create table if not exists public.v2_us_equity_calendar_exceptions (
  market_date date primary key,
  closed boolean not null,
  close_time time,
  reason text not null check(length(reason) between 1 and 160),
  check((closed and close_time is null) or (not closed and close_time between '09:31'::time and '16:00'::time))
);
insert into public.v2_us_equity_calendar_coverage(singleton,verified_from,verified_through,source_url,verified_on)
values(true,'2026-01-01','2028-12-31','https://www.nyse.com/trade/hours-calendars','2026-10-14')
on conflict(singleton) do nothing;
-- Explicit published exchange dates; federal-holiday heuristics are incorrect
-- for Good Friday and for the unobserved New Year's Day on Saturday in 2028.
insert into public.v2_us_equity_calendar_exceptions(market_date,closed,close_time,reason) values
('2026-01-01',true,null,'New Year'),('2026-01-19',true,null,'Martin Luther King Jr.'),
('2026-02-16',true,null,'Washington birthday'),('2026-04-03',true,null,'Good Friday'),
('2026-05-25',true,null,'Memorial Day'),('2026-06-19',true,null,'Juneteenth'),
('2026-07-03',true,null,'Independence Day observed'),('2026-09-07',true,null,'Labor Day'),
('2026-11-26',true,null,'Thanksgiving'),('2026-12-25',true,null,'Christmas'),
('2027-01-01',true,null,'New Year'),('2027-01-18',true,null,'Martin Luther King Jr.'),
('2027-02-15',true,null,'Washington birthday'),('2027-03-26',true,null,'Good Friday'),
('2027-05-31',true,null,'Memorial Day'),('2027-06-18',true,null,'Juneteenth observed'),
('2027-07-05',true,null,'Independence Day observed'),('2027-09-06',true,null,'Labor Day'),
('2027-11-25',true,null,'Thanksgiving'),('2027-12-24',true,null,'Christmas observed'),
('2028-01-17',true,null,'Martin Luther King Jr.'),('2028-02-21',true,null,'Washington birthday'),
('2028-04-14',true,null,'Good Friday'),('2028-05-29',true,null,'Memorial Day'),
('2028-06-19',true,null,'Juneteenth'),('2028-07-04',true,null,'Independence Day'),
('2028-09-04',true,null,'Labor Day'),('2028-11-23',true,null,'Thanksgiving'),
('2028-12-25',true,null,'Christmas'),
('2026-11-27',false,'13:00','Day after Thanksgiving'),
('2026-12-24',false,'13:00','Christmas Eve'),
('2027-11-26',false,'13:00','Day after Thanksgiving'),
('2028-07-03',false,'13:00','Day before Independence Day'),
('2028-11-24',false,'13:00','Day after Thanksgiving')
on conflict(market_date) do nothing;
alter table public.v2_us_equity_calendar_coverage enable row level security;
alter table public.v2_us_equity_calendar_exceptions enable row level security;
revoke all on public.v2_us_equity_calendar_coverage,public.v2_us_equity_calendar_exceptions from public,anon,authenticated;
grant select on public.v2_us_equity_calendar_coverage,public.v2_us_equity_calendar_exceptions to authenticated;
grant select,insert,update,delete on public.v2_us_equity_calendar_coverage,public.v2_us_equity_calendar_exceptions to service_role;
drop policy if exists us_equity_coverage_read on public.v2_us_equity_calendar_coverage;
create policy us_equity_coverage_read on public.v2_us_equity_calendar_coverage for select to authenticated using(auth.uid() is not null);
drop policy if exists us_equity_exceptions_read on public.v2_us_equity_calendar_exceptions;
create policy us_equity_exceptions_read on public.v2_us_equity_calendar_exceptions for select to authenticated using(auth.uid() is not null);

-- Service-only exception updates can record an officially announced unscheduled
-- closure. Reapplying this migration never overwrites a newer calendar override.
create or replace function public.v2_us_equity_session(p_market_date date)
returns table(open_at timestamptz,close_at timestamptz)
language sql stable security invoker set search_path=public,pg_temp as $$
  select (p_market_date+'09:30'::time) at time zone 'America/New_York',
    (p_market_date+coalesce(x.close_time,'16:00'::time)) at time zone 'America/New_York'
  from public.v2_us_equity_calendar_coverage c
  left join public.v2_us_equity_calendar_exceptions x on x.market_date=p_market_date
  where c.singleton and p_market_date between c.verified_from and c.verified_through
    and extract(isodow from p_market_date) between 1 and 5 and not coalesce(x.closed,false);
$$;
revoke all on function public.v2_us_equity_session(date) from public,anon;
grant execute on function public.v2_us_equity_session(date) to authenticated,service_role;

-- A tombstone prevents a deleted occurrence returning at the next hourly refresh.
-- The source baseline also prevents a refresh undoing an event's manual edit.
create table if not exists public.v2_us_equity_open_occurrences (
  schedule_id uuid not null references public.v2_economic_schedules(id) on delete cascade,
  market_date date not null,
  event_id uuid references public.economic_events(id) on delete set null,
  source_baseline jsonb not null,
  primary key(schedule_id,market_date)
);
alter table public.v2_us_equity_open_occurrences enable row level security;
revoke all on public.v2_us_equity_open_occurrences from public,anon,authenticated;
grant select on public.v2_us_equity_open_occurrences to authenticated;
grant select,insert,update,delete on public.v2_us_equity_open_occurrences to service_role;
drop policy if exists us_equity_occurrences_read on public.v2_us_equity_open_occurrences;
create policy us_equity_occurrences_read on public.v2_us_equity_open_occurrences for select to authenticated
using(exists(select 1 from public.v2_economic_schedules s where s.id=schedule_id
  and public.v2_is_workspace_member(s.workspace_id)));

create or replace function public.v2_validate_us_equity_schedule()
returns trigger language plpgsql set search_path=public,pg_temp as $$
begin
  if tg_op='UPDATE' and new.market_calendar is distinct from old.market_calendar then
    raise exception 'market calendar identity is immutable' using errcode='23514';
  end if;
  if new.market_calendar='us_equities' and (new.timezone is distinct from 'America/New_York'
    or new.weekdays is distinct from array[1,2,3,4,5]::smallint[]
    or new.times is distinct from array['09:30']::text[]) then
    raise exception 'US equity opening uses weekdays at 09:30 America/New_York' using errcode='23514';
  end if;
  return new;
end $$;
drop trigger if exists v2_economic_schedules_validate_us_equity on public.v2_economic_schedules;
create trigger v2_economic_schedules_validate_us_equity before insert or update on public.v2_economic_schedules
for each row execute function public.v2_validate_us_equity_schedule();
revoke all on function public.v2_validate_us_equity_schedule() from public,anon,authenticated;

create or replace function public.v2_preserve_shared_schedule_reminders()
returns trigger language plpgsql set search_path=public,pg_temp as $$
begin
  if new.market_calendar='us_equities' then
    -- Future members who have not made a personal choice get one hour, not the
    -- generic two-star defaults. An explicit personal choice always takes priority.
    new.reminder_offsets := array[60]::integer[];
  elsif auth.uid() is not null then
    if tg_op='INSERT' then new.reminder_offsets := '{}'::integer[];
    else new.reminder_offsets := old.reminder_offsets; end if;
  end if;
  return new;
end $$;
revoke all on function public.v2_preserve_shared_schedule_reminders() from public,anon,authenticated;

create or replace function public.v2_materialize_economic_schedule(p_schedule_id uuid,p_horizon_days integer default 14)
returns integer language plpgsql security definer set search_path=public,pg_temp as $$
declare
  s public.v2_economic_schedules%rowtype;
  first_day date; last_day date; day_value date; clock_time text;
  event_time timestamptz; occurrence text; v_event_id uuid;
  desired text[] := '{}'::text[];
  total integer := 0;
  market_session record;
  mapped public.v2_us_equity_open_occurrences%rowtype;
  previous public.economic_events%rowtype;
  source_value jsonb; description_value text;
begin
  select * into s from public.v2_economic_schedules where id=p_schedule_id for update;
  if not found or not public.v2_can_edit_workspace(s.workspace_id) then
    raise exception 'schedule edit permission required' using errcode='42501';
  end if;
  if p_horizon_days is null or p_horizon_days not between 1 and 90 then
    raise exception 'calendar horizon must be between 1 and 90 days' using errcode='22023';
  end if;
  first_day := greatest((now() at time zone s.timezone)::date,s.starts_on);
  last_day := least((now() at time zone s.timezone)::date+p_horizon_days,coalesce(s.ends_on,'infinity'::date));
  last_day := least(coalesce(s.ends_on,'infinity'::date),greatest(last_day,coalesce(
    (select max((e.event_at at time zone s.timezone)::date) from public.economic_events e
      where e.recurring_schedule_id=s.id and e.event_at>now()
        and (e.event_at at time zone s.timezone)::date<=(now() at time zone s.timezone)::date+90),last_day)));

  if s.market_calendar='us_equities' then
    if s.active and first_day<=last_day then
      for day_value in select d::date from generate_series(first_day::timestamp,last_day::timestamp,interval '1 day') d loop
        select * into market_session from public.v2_us_equity_session(day_value);
        if not found or market_session.open_at<=now() then continue; end if;
        event_time := market_session.open_at;
        occurrence := day_value::text||'T09:30';
        description_value := 'NYSE / Nasdaq · '||to_char(event_time at time zone 'Asia/Jerusalem','HH24:MI')
          ||'–'||to_char(market_session.close_at at time zone 'Asia/Jerusalem','HH24:MI')||' (שעון ישראל)'
          ||case when (market_session.close_at at time zone 'America/New_York')::time='13:00'::time then ' · סגירה מוקדמת' else '' end;
        source_value := jsonb_build_object('name',s.name,'event_at',extract(epoch from event_time),'importance',s.importance,
          'description',description_value,'source_url','https://www.nyse.com/trade/hours-calendars');
        -- Native event first, then ledger: manual DELETE takes that same lock
        -- order through ON DELETE SET NULL. Never resurrect a deletion in flight.
        select * into mapped from public.v2_us_equity_open_occurrences
          where schedule_id=s.id and market_date=day_value;
        if found then
          if mapped.event_id is null then continue; end if;
          select * into previous from public.economic_events where id=mapped.event_id for update;
          select * into mapped from public.v2_us_equity_open_occurrences
            where schedule_id=s.id and market_date=day_value for update;
          if mapped.event_id is null or previous.id is null then continue; end if;
          if previous.workspace_id is distinct from s.workspace_id
            or previous.recurring_schedule_id is distinct from s.id
            or previous.occurrence_key is distinct from occurrence then
            raise exception 'market occurrence scope mismatch' using errcode='23514';
          end if;
          if jsonb_build_object('name',previous.name,'event_at',extract(epoch from previous.event_at),'importance',previous.importance,
            'description',previous.description,'source_url',previous.source_url) is distinct from mapped.source_baseline then
            continue;
          end if;
          v_event_id := previous.id;
          if source_value is distinct from mapped.source_baseline then
            update public.economic_events set name=s.name,event_at=event_time,importance=s.importance,
              description=description_value,source_url='https://www.nyse.com/trade/hours-calendars',updated_by=auth.uid()
              where id=v_event_id;
            update public.v2_us_equity_open_occurrences set source_baseline=source_value
              where schedule_id=s.id and market_date=day_value;
          end if;
        else
          insert into public.economic_events(name,event_at,importance,description,source_url,workspace_id,
            recurring_schedule_id,occurrence_key,created_by,updated_by)
          values(s.name,event_time,s.importance,description_value,'https://www.nyse.com/trade/hours-calendars',
            s.workspace_id,s.id,occurrence,auth.uid(),auth.uid()) returning id into v_event_id;
          insert into public.v2_us_equity_open_occurrences(schedule_id,market_date,event_id,source_baseline)
          values(s.id,day_value,v_event_id,source_value);
        end if;
        insert into public.economic_event_reminders(event_id,offset_minutes,due_at,user_id)
        values(v_event_id,60,event_time-interval '60 minutes',null)
        on conflict(event_id,offset_minutes,user_id) do nothing;
        total := total+1;
      end loop;
    end if;
    -- A closure override removes future notifications too. Edited individual
    -- events otherwise retain their time/content. A shorter refresh never
    -- erases valid events outside its horizon. No events beyond verified dates.
    for previous in select e.* from public.economic_events e
      join public.v2_us_equity_open_occurrences o on o.event_id=e.id and o.schedule_id=s.id
      where e.event_at>now()
        and (not s.active or o.market_date<s.starts_on or (s.ends_on is not null and o.market_date>s.ends_on)
          or not exists(select 1 from public.v2_us_equity_session(o.market_date))) for update of e
    loop
      -- A series pause/end-date or official closure is a source cancellation,
      -- not the user's per-event deletion. It can return if the series is
      -- reactivated or that closure is officially corrected. Keep real manual
      -- tombstones, which have no event row and never enter this loop.
      select market_date into day_value from public.v2_us_equity_open_occurrences
        where schedule_id=s.id and event_id=previous.id for update;
      delete from public.economic_events where id=previous.id;
      delete from public.v2_us_equity_open_occurrences where schedule_id=s.id and market_date=day_value;
    end loop;
    return total;
  end if;

  -- Existing ordinary manual-series behavior is unchanged.
  if s.active and first_day<=last_day then
    for day_value in select d::date from generate_series(first_day::timestamp,last_day::timestamp,interval '1 day') d loop
      if extract(dow from day_value)::smallint=any(s.weekdays) then
        foreach clock_time in array s.times loop
          occurrence := day_value::text||'T'||clock_time;
          event_time := (day_value+clock_time::time) at time zone s.timezone;
          if event_time<=now() then continue; end if;
          desired := array_append(desired,occurrence);
          insert into public.economic_events(name,event_at,importance,workspace_id,recurring_schedule_id,occurrence_key,created_by,updated_by)
          values(s.name,event_time,s.importance,s.workspace_id,s.id,occurrence,auth.uid(),auth.uid())
          on conflict(recurring_schedule_id,occurrence_key) do update
          set name=excluded.name,event_at=excluded.event_at,importance=excluded.importance,updated_by=auth.uid()
          returning id into v_event_id;
          insert into public.economic_event_reminders(event_id,offset_minutes,due_at,user_id)
          select v_event_id,m,event_time-make_interval(mins=>m),null::uuid from unnest(s.reminder_offsets) m
          on conflict(event_id,offset_minutes,user_id) do nothing;
          delete from public.economic_event_reminders r
          where r.event_id=v_event_id and r.user_id is null and not(r.offset_minutes=any(s.reminder_offsets));
          total := total+1;
        end loop;
      end if;
    end loop;
  end if;
  delete from public.economic_events e
  where e.recurring_schedule_id=s.id and e.event_at>now()
    and (not s.active
      or (e.event_at at time zone s.timezone)::date<s.starts_on
      or (s.ends_on is not null and (e.event_at at time zone s.timezone)::date>s.ends_on)
      or not(extract(dow from e.event_at at time zone s.timezone)::smallint=any(s.weekdays))
      or not(to_char(e.event_at at time zone s.timezone,'HH24:MI')=any(s.times))
      or ((e.event_at at time zone s.timezone)::date<=last_day and not(e.occurrence_key=any(desired))));
  return total;
end $$;
revoke all on function public.v2_materialize_economic_schedule(uuid,integer) from public,anon;
grant execute on function public.v2_materialize_economic_schedule(uuid,integer) to authenticated;

create or replace function public.v2_create_us_equity_open_schedule(p_workspace_id uuid)
returns public.v2_economic_schedules language plpgsql security definer set search_path=public,pg_temp as $$
declare s public.v2_economic_schedules%rowtype;
begin
  if auth.uid() is null or not public.v2_can_edit_workspace(p_workspace_id) then
    raise exception 'workspace edit permission required' using errcode='42501';
  end if;
  -- Serialize only this new logical series. A retry never changes preferences,
  -- edited titles, active status or individual occurrence tombstones.
  perform pg_advisory_xact_lock(hashtextextended('us-equity-open:'||p_workspace_id::text,0));
  select * into s from public.v2_economic_schedules
    where workspace_id=p_workspace_id and market_calendar='us_equities' for update;
  if found then return s; end if;
  if not exists(select 1 from public.v2_us_equity_calendar_coverage c where c.singleton
    and (now() at time zone 'America/New_York')::date between c.verified_from and c.verified_through) then
    raise exception 'US equity calendar dates have not been verified for today' using errcode='22023';
  end if;
  insert into public.v2_economic_schedules(workspace_id,name,importance,weekdays,times,timezone,
    reminder_offsets,starts_on,ends_on,market_calendar,created_by,updated_by)
  values(p_workspace_id,'פתיחת שוק המניות בארה״ב',2,array[1,2,3,4,5]::smallint[],array['09:30'],
    'America/New_York',array[60],(now() at time zone 'America/New_York')::date,null,'us_equities',auth.uid(),auth.uid())
    returning * into s;
  insert into public.v2_economic_reminder_preferences(schedule_id,user_id,reminder_offsets)
  select s.id,m.user_id,array[60] from public.v2_workspace_members m where m.workspace_id=s.workspace_id
  on conflict(schedule_id,user_id) do nothing;
  return s;
end $$;
revoke all on function public.v2_create_us_equity_open_schedule(uuid) from public,anon;
grant execute on function public.v2_create_us_equity_open_schedule(uuid) to authenticated;

commit;
