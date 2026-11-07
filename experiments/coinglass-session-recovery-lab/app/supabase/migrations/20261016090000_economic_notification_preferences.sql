begin;

-- Stable event identities keep a checkmark across future releases. Star rating is
-- source importance; the new checkmark is a separate shared workspace choice.
create or replace function app_private.economic_event_type_key(p_name text,p_source_url text,p_schedule_id uuid,p_market text)
returns text language plpgsql immutable set search_path=pg_catalog,public,pg_temp as $$
declare n text:=btrim(regexp_replace(lower(coalesce(p_name,'')),'[^a-z0-9]+',' ','g')); slug text;
begin
  if p_market='us_equities' then return 'market:us_equities_open'; end if;
  if n ~ '^(u s |us |united states )' then
    if n like '%core%' and (n like '%cpi%' or n like '%consumer price index%') then return 'us:core_cpi'; end if;
    if n like '%initial jobless claims%' then return 'us:initial_jobless_claims'; end if;
  end if;
  if p_schedule_id is not null then return 'schedule:'||p_schedule_id::text; end if;
  if p_source_url ~ '^https://www\.investing\.com/economic-calendar/[a-zA-Z0-9-]+-[0-9]+$' then
    -- Investing's numeric indicator ID is stable across releases and separates
    -- countries that reuse an identical slug (for example retail-sales).
    slug:=regexp_replace(p_source_url,'^.*/','');
    return 'investing:'||lower(slug);
  end if;
  return 'name:'||md5(lower(btrim(coalesce(p_name,''))));
end $$;
revoke all on function app_private.economic_event_type_key(text,text,uuid,text) from public,anon,authenticated;

alter table public.economic_events add column if not exists event_type_key text;
create or replace function app_private.set_economic_event_type_key()
returns trigger language plpgsql security definer set search_path=pg_catalog,public,app_private,pg_temp as $$
declare market text;
begin
  if new.recurring_schedule_id is not null then
    select market_calendar into market from public.v2_economic_schedules where id=new.recurring_schedule_id;
  end if;
  new.event_type_key:=app_private.economic_event_type_key(new.name,new.source_url,new.recurring_schedule_id,market);
  return new;
end $$;
revoke all on function app_private.set_economic_event_type_key() from public,anon,authenticated;
drop trigger if exists v2_economic_event_type_identity on public.economic_events;
create trigger v2_economic_event_type_identity before insert or update on public.economic_events
for each row execute function app_private.set_economic_event_type_key();
-- Updating only the new column retains names, times and historical reminder rows.
update public.economic_events e set event_type_key=app_private.economic_event_type_key(
  e.name,e.source_url,e.recurring_schedule_id,(select market_calendar from public.v2_economic_schedules where id=e.recurring_schedule_id))
where e.event_type_key is null;
alter table public.economic_events alter column event_type_key set not null;
create index if not exists economic_events_type_future_idx on public.economic_events(workspace_id,event_type_key,event_at);

create table if not exists public.v2_economic_event_types (
  workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
  type_key text not null check(length(type_key) between 1 and 500),
  label text not null check(length(label) between 1 and 300),
  is_important boolean not null default false,
  updated_at timestamptz not null default now(),
  primary key(workspace_id,type_key)
);
create table if not exists public.v2_economic_calendar_notification_settings (
  workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
  user_id uuid not null references auth.users(id) on delete cascade,
  ordinary_offsets integer[] not null default array[60,240,1440],
  important_offsets integer[] not null default array[15,30,60,240,1440],
  updated_at timestamptz not null default now(),
  primary key(workspace_id,user_id),
  check(ordinary_offsets=public.v2_valid_reminder_offsets(ordinary_offsets)),
  check(important_offsets=public.v2_valid_reminder_offsets(important_offsets))
);
create table if not exists public.v2_economic_event_type_reminder_preferences (
  workspace_id uuid not null,
  type_key text not null,
  user_id uuid not null references auth.users(id) on delete cascade,
  reminder_offsets integer[] not null,
  updated_at timestamptz not null default now(),
  primary key(workspace_id,type_key,user_id),
  foreign key(workspace_id,type_key) references public.v2_economic_event_types(workspace_id,type_key) on delete cascade,
  check(reminder_offsets=public.v2_valid_reminder_offsets(reminder_offsets))
);
alter table public.v2_economic_event_types enable row level security;
alter table public.v2_economic_calendar_notification_settings enable row level security;
alter table public.v2_economic_event_type_reminder_preferences enable row level security;
revoke all on public.v2_economic_event_types,public.v2_economic_calendar_notification_settings,
 public.v2_economic_event_type_reminder_preferences from public,anon,authenticated;
grant select on public.v2_economic_event_types,public.v2_economic_calendar_notification_settings,
 public.v2_economic_event_type_reminder_preferences to authenticated;
grant all on public.v2_economic_event_types,public.v2_economic_calendar_notification_settings,
 public.v2_economic_event_type_reminder_preferences to service_role;
drop policy if exists economic_types_read on public.v2_economic_event_types;
create policy economic_types_read on public.v2_economic_event_types for select to authenticated
 using(public.v2_is_workspace_member(workspace_id));
drop policy if exists economic_notification_settings_read on public.v2_economic_calendar_notification_settings;
create policy economic_notification_settings_read on public.v2_economic_calendar_notification_settings for select to authenticated
 using(user_id=auth.uid() and public.v2_is_workspace_member(workspace_id));
drop policy if exists economic_type_reminder_preferences_read on public.v2_economic_event_type_reminder_preferences;
create policy economic_type_reminder_preferences_read on public.v2_economic_event_type_reminder_preferences for select to authenticated
 using(user_id=auth.uid() and public.v2_is_workspace_member(workspace_id));

-- Track defaults separately from explicit per-event choices. Existing customized
-- schedules and reminders always keep precedence over these defaults.
alter table public.v2_economic_reminder_preferences add column if not exists preference_origin text not null default 'manual'
 check(preference_origin in ('manual','automatic'));
update public.v2_economic_reminder_preferences p set preference_origin='automatic'
from public.economic_events e
where p.event_id=e.id and p.updated_at=e.created_at and p.reminder_offsets=array[60,240,1440]
 and exists(select 1 from public.v2_economic_calendar_imports i where i.event_id=e.id);
create or replace function app_private.preserve_economic_preference_origin()
returns trigger language plpgsql set search_path=pg_catalog,public,pg_temp as $$
begin
  -- Direct authenticated preference edits must not masquerade as defaults.
  if current_user='authenticated' then new.preference_origin:='manual'; end if;
  return new;
end $$;
revoke all on function app_private.preserve_economic_preference_origin() from public,anon,authenticated;
drop trigger if exists v2_reminder_preference_origin on public.v2_economic_reminder_preferences;
create trigger v2_reminder_preference_origin before insert or update on public.v2_economic_reminder_preferences
 for each row execute function app_private.preserve_economic_preference_origin();

-- An explicit recurring-series choice also wins when automatic event defaults
-- were created earlier. An explicit one-event choice remains the strongest.
do $personal_precedence$
declare definition text; before_order text:='order by user_id,(event_id is not null) desc';
  after_order text:='order by user_id,(preference_origin=''manual'') desc,(event_id is not null) desc';
begin
  definition:=pg_get_functiondef('public.v2_sync_personal_event_reminders(uuid)'::regprocedure);
  if strpos(definition,after_order)=0 then
    if strpos(definition,before_order)=0 then raise exception 'Unexpected personal reminder precedence'; end if;
    execute replace(definition,before_order,after_order);
  end if;
end $personal_precedence$;

create or replace function app_private.seed_economic_event_types(p_workspace_id uuid)
returns void language sql security definer set search_path=pg_catalog,public,pg_temp as $$
 insert into public.v2_economic_event_types(workspace_id,type_key,label,is_important) values
 (p_workspace_id,'us:core_cpi','U.S. Core CPI',true),
 (p_workspace_id,'us:initial_jobless_claims','U.S. Initial Jobless Claims',true)
 on conflict do nothing;
$$;
revoke all on function app_private.seed_economic_event_types(uuid) from public,anon,authenticated;
select app_private.seed_economic_event_types(id) from public.v2_workspaces;

create or replace function app_private.seed_workspace_economic_event_types()
returns trigger language plpgsql security definer set search_path=pg_catalog,public,app_private,pg_temp as $$
begin perform app_private.seed_economic_event_types(new.id); return new; end $$;
revoke all on function app_private.seed_workspace_economic_event_types() from public,anon,authenticated;
drop trigger if exists v2_workspace_economic_event_types on public.v2_workspaces;
create trigger v2_workspace_economic_event_types after insert on public.v2_workspaces
 for each row execute function app_private.seed_workspace_economic_event_types();

create or replace function app_private.apply_economic_notification_defaults(p_workspace_id uuid,p_type_key text default null,p_user_id uuid default null)
returns void language plpgsql security definer set search_path=pg_catalog,public,app_private,pg_temp as $$
declare e record; member record; offsets integer[]; important boolean;
begin
  -- Imports and settings changes must observe one order even when they overlap.
  perform pg_advisory_xact_lock(hashtextextended('economic-notification-defaults:'||p_workspace_id,0));
  for e in select ev.* from public.economic_events ev where ev.workspace_id=p_workspace_id and ev.event_at>now()
   and (p_type_key is null or ev.event_type_key=p_type_key)
   and (exists(select 1 from public.v2_economic_calendar_imports i where i.event_id=ev.id)
     or exists(select 1 from public.v2_economic_event_types t where t.workspace_id=p_workspace_id
       and t.type_key=ev.event_type_key and t.is_important)
     or exists(select 1 from public.v2_economic_reminder_preferences p where p.event_id=ev.id and p.preference_origin='automatic'))
   order by ev.id
  loop
    select coalesce((select is_important from public.v2_economic_event_types
      where workspace_id=p_workspace_id and type_key=e.event_type_key),false) into important;
    for member in select user_id from public.v2_workspace_members where workspace_id=p_workspace_id
      and (p_user_id is null or user_id=p_user_id) order by user_id
    loop
      if exists(select 1 from public.v2_economic_reminder_preferences p
          where p.user_id=member.user_id and ((p.event_id=e.id and p.preference_origin='manual')
            or p.schedule_id=e.recurring_schedule_id)) then continue; end if;
      select coalesce(
        (select reminder_offsets from public.v2_economic_event_type_reminder_preferences
          where workspace_id=p_workspace_id and type_key=e.event_type_key and user_id=member.user_id and important),
        (select case when important then important_offsets else ordinary_offsets end
          from public.v2_economic_calendar_notification_settings where workspace_id=p_workspace_id and user_id=member.user_id),
        case when important then array[15,30,60,240,1440] else array[60,240,1440] end
      ) into offsets;
      -- The market-opening series keeps its one-hour reminder until explicitly marked important.
      if e.event_type_key='market:us_equities_open' and not important then offsets:=array[60]; end if;
      insert into public.v2_economic_reminder_preferences(event_id,user_id,reminder_offsets,preference_origin)
      values(e.id,member.user_id,offsets,'automatic')
      on conflict(event_id,user_id) do update set reminder_offsets=excluded.reminder_offsets
        where v2_economic_reminder_preferences.preference_origin='automatic'
          and v2_economic_reminder_preferences.reminder_offsets is distinct from excluded.reminder_offsets;
    end loop;
  end loop;
end $$;
revoke all on function app_private.apply_economic_notification_defaults(uuid,text,uuid) from public,anon,authenticated;

create or replace function app_private.economic_import_notification_defaults()
returns trigger language plpgsql security definer set search_path=pg_catalog,public,app_private,pg_temp as $$
begin
  if new.event_id is null then return new; end if;
  update public.v2_economic_reminder_preferences p set preference_origin='automatic'
  from public.economic_events e where e.id=new.event_id and p.event_id=e.id
    and p.updated_at=e.created_at and p.reminder_offsets=array[60,240,1440];
  perform app_private.apply_economic_notification_defaults(new.workspace_id,
    (select event_type_key from public.economic_events where id=new.event_id),null);
  return new;
end $$;
revoke all on function app_private.economic_import_notification_defaults() from public,anon,authenticated;
drop trigger if exists v2_economic_import_notification_defaults on public.v2_economic_calendar_imports;
create trigger v2_economic_import_notification_defaults after insert or update of event_id on public.v2_economic_calendar_imports
 for each row execute function app_private.economic_import_notification_defaults();

create or replace function app_private.economic_important_event_defaults()
returns trigger language plpgsql security definer set search_path=pg_catalog,public,app_private,pg_temp as $$
begin
  if new.workspace_id is not null and exists(select 1 from public.v2_economic_event_types t
    where t.workspace_id=new.workspace_id and t.type_key=new.event_type_key and t.is_important) then
    perform app_private.apply_economic_notification_defaults(new.workspace_id,new.event_type_key,null);
  end if;
  return new;
end $$;
revoke all on function app_private.economic_important_event_defaults() from public,anon,authenticated;
drop trigger if exists v2_economic_important_event_defaults on public.economic_events;
create trigger v2_economic_important_event_defaults after insert on public.economic_events
 for each row execute function app_private.economic_important_event_defaults();

create or replace function public.v2_set_economic_event_type_importance(p_workspace_id uuid,p_type_key text,p_label text,p_important boolean)
returns void language plpgsql security definer set search_path=pg_catalog,public,app_private,pg_temp as $$
begin
  if not public.v2_can_edit_workspace(p_workspace_id) then raise exception 'workspace edit permission required' using errcode='42501'; end if;
  if p_type_key is null or length(p_type_key) not between 1 and 500 or p_label is null
    or length(btrim(p_label)) not between 1 and 300 or p_important is null then
    raise exception 'invalid event type' using errcode='23514';
  end if;
  if not exists(select 1 from public.economic_events where workspace_id=p_workspace_id and event_type_key=p_type_key)
    and not exists(select 1 from public.v2_economic_event_types where workspace_id=p_workspace_id and type_key=p_type_key) then
    raise exception 'unknown event type' using errcode='23514';
  end if;
  perform pg_advisory_xact_lock(hashtextextended('economic-notification-defaults:'||p_workspace_id,0));
  insert into public.v2_economic_event_types(workspace_id,type_key,label,is_important)
    values(p_workspace_id,p_type_key,btrim(p_label),p_important)
  on conflict(workspace_id,type_key) do update set label=excluded.label,is_important=excluded.is_important,updated_at=now();
  perform app_private.apply_economic_notification_defaults(p_workspace_id,p_type_key,null);
end $$;

create or replace function public.v2_save_economic_notification_defaults(p_workspace_id uuid,p_ordinary_offsets integer[],p_important_offsets integer[])
returns void language plpgsql security definer set search_path=pg_catalog,public,app_private,pg_temp as $$
begin
  if auth.uid() is null or not public.v2_is_workspace_member(p_workspace_id) then raise exception 'workspace access required' using errcode='42501'; end if;
  perform pg_advisory_xact_lock(hashtextextended('economic-notification-defaults:'||p_workspace_id,0));
  insert into public.v2_economic_calendar_notification_settings(workspace_id,user_id,ordinary_offsets,important_offsets)
   values(p_workspace_id,auth.uid(),public.v2_valid_reminder_offsets(p_ordinary_offsets),public.v2_valid_reminder_offsets(p_important_offsets))
  on conflict(workspace_id,user_id) do update set ordinary_offsets=excluded.ordinary_offsets,important_offsets=excluded.important_offsets,updated_at=now();
  perform app_private.apply_economic_notification_defaults(p_workspace_id,null,auth.uid());
end $$;

create or replace function public.v2_set_economic_type_reminders(p_workspace_id uuid,p_type_key text,p_offsets integer[] default null)
returns void language plpgsql security definer set search_path=pg_catalog,public,app_private,pg_temp as $$
begin
  if auth.uid() is null or not public.v2_is_workspace_member(p_workspace_id) then raise exception 'workspace access required' using errcode='42501'; end if;
  if not exists(select 1 from public.v2_economic_event_types where workspace_id=p_workspace_id and type_key=p_type_key and is_important) then
    raise exception 'important event type required' using errcode='23514';
  end if;
  perform pg_advisory_xact_lock(hashtextextended('economic-notification-defaults:'||p_workspace_id,0));
  if p_offsets is null then
    delete from public.v2_economic_event_type_reminder_preferences where workspace_id=p_workspace_id and type_key=p_type_key and user_id=auth.uid();
  else
    insert into public.v2_economic_event_type_reminder_preferences(workspace_id,type_key,user_id,reminder_offsets)
      values(p_workspace_id,p_type_key,auth.uid(),public.v2_valid_reminder_offsets(p_offsets))
    on conflict(workspace_id,type_key,user_id) do update set reminder_offsets=excluded.reminder_offsets,updated_at=now();
  end if;
  perform app_private.apply_economic_notification_defaults(p_workspace_id,p_type_key,auth.uid());
end $$;

-- Explicit one-event choices always detach that event from future default changes.
create or replace function public.v2_set_economic_reminders(p_event_id uuid,p_schedule_id uuid,p_offsets integer[])
returns void language plpgsql security definer set search_path=public,pg_temp as $$
declare wid uuid;
begin
  if not public.v2_can_configure_reminders(p_event_id,p_schedule_id) then raise exception 'calendar access required' using errcode='42501'; end if;
  select workspace_id into wid from public.economic_events where id=p_event_id
    union all select workspace_id from public.v2_economic_schedules where id=p_schedule_id;
  -- Serialize a manual choice with import/default generation, so the last
  -- resynchronization observes both the choice and any automatic event row.
  perform pg_advisory_xact_lock(hashtextextended('economic-notification-defaults:'||wid,0));
  if p_event_id is not null then
    insert into public.v2_economic_reminder_preferences(event_id,user_id,reminder_offsets,preference_origin)
    values(p_event_id,auth.uid(),public.v2_valid_reminder_offsets(p_offsets),'manual')
    on conflict(event_id,user_id) do update set reminder_offsets=excluded.reminder_offsets,preference_origin='manual';
  else
    insert into public.v2_economic_reminder_preferences(schedule_id,user_id,reminder_offsets,preference_origin)
    values(p_schedule_id,auth.uid(),public.v2_valid_reminder_offsets(p_offsets),'manual')
    on conflict(schedule_id,user_id) do update set reminder_offsets=excluded.reminder_offsets,preference_origin='manual';
  end if;
end $$;

-- One confirmation acknowledges exactly the displayed set. Authorization is
-- checked for every id first; any failure rolls back the complete transaction.
create or replace function public.v2_acknowledge_economic_reminders(p_reminder_ids uuid[])
returns void language plpgsql security definer set search_path=public,pg_temp as $$
declare target record;
begin
  if auth.uid() is null then raise exception 'reminder access required' using errcode='42501'; end if;
  if p_reminder_ids is null or cardinality(p_reminder_ids) not between 1 and 4096
    or array_position(p_reminder_ids,null) is not null then raise exception 'invalid reminder batch' using errcode='23514'; end if;
  if (select count(*) from public.economic_event_reminders r where r.id=any(p_reminder_ids))
      <> (select count(distinct id) from unnest(p_reminder_ids) id) then
    raise exception 'reminder access required' using errcode='42501';
  end if;
  for target in select id,event_id from public.economic_event_reminders where id=any(p_reminder_ids) order by event_id,id loop
    perform public.v2_acknowledge_economic_reminder(target.id);
  end loop;
end $$;

create or replace function public.v2_snooze_economic_reminder(p_reminder_id uuid,p_minutes integer default 10)
returns timestamptz language plpgsql security definer set search_path=public,pg_temp as $$
declare r public.economic_event_reminders%rowtype; due timestamptz; starts_at timestamptz; reminder_ids uuid[];
begin
  if p_minutes is null or p_minutes not between 1 and 15 then
    raise exception 'snooze must be between 1 and 15 minutes' using errcode='23514';
  end if;
  reminder_ids:=app_private.lock_economic_reminder_due_group(p_reminder_id);
  select * into r from public.economic_event_reminders where id=p_reminder_id;
  select event_at into starts_at from public.economic_events where id=r.event_id;
  due:=now()+make_interval(mins=>p_minutes);
  if starts_at is null or clock_timestamp()+make_interval(mins=>p_minutes)>=starts_at then
    raise exception 'snooze must end before the event starts' using errcode='23514';
  end if;
  if r.due_at>now() then raise exception 'reminder is not due' using errcode='23514'; end if;
  if exists(select 1 from public.economic_reminder_acknowledgements a where a.reminder_id=r.id and a.user_id=auth.uid()) then
    raise exception 'reminder already acknowledged' using errcode='23514';
  end if;
  insert into public.economic_reminder_acknowledgements(reminder_id,user_id)
    select id,auth.uid() from unnest(reminder_ids) id where id<>r.id on conflict(reminder_id,user_id) do nothing;
  delete from public.v2_economic_reminder_snoozes where reminder_id=any(reminder_ids) and reminder_id<>r.id and user_id=auth.uid();
  insert into public.v2_economic_reminder_snoozes(reminder_id,user_id,snoozed_until,original_due_at)
    values(r.id,auth.uid(),due,r.due_at)
  on conflict(reminder_id,user_id) do update set snoozed_until=excluded.snoozed_until,
    original_due_at=excluded.original_due_at,updated_at=now();
  return due;
end $$;

revoke all on function public.v2_set_economic_event_type_importance(uuid,text,text,boolean),
 public.v2_save_economic_notification_defaults(uuid,integer[],integer[]),
 public.v2_set_economic_type_reminders(uuid,text,integer[]),public.v2_acknowledge_economic_reminders(uuid[])
 from public,anon;
grant execute on function public.v2_set_economic_event_type_importance(uuid,text,text,boolean),
 public.v2_save_economic_notification_defaults(uuid,integer[],integer[]),
 public.v2_set_economic_type_reminders(uuid,text,integer[]),public.v2_acknowledge_economic_reminders(uuid[])
 to authenticated;
select app_private.apply_economic_notification_defaults(id,null,null) from public.v2_workspaces;
commit;

