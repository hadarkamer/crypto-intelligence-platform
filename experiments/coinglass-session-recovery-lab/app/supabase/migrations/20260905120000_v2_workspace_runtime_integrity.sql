-- Complete V2 runtime contracts without moving or rewriting legacy records.
-- Existing unscoped calendar rows retain their shared-calendar behavior.

-- Replace the partial index on already-upgraded installations too. PostgreSQL
-- cannot infer a partial unique index from a plain PostgREST on_conflict list.
drop index if exists public.economic_events_recurring_occurrence_idx;
create unique index economic_events_recurring_occurrence_idx
  on public.economic_events(recurring_schedule_id, occurrence_key);

alter table public.v2_economic_schedules
  add column if not exists timezone text not null default 'Asia/Jerusalem';

create or replace function public.v2_validate_economic_schedule()
returns trigger language plpgsql set search_path = public, pg_temp as $$
begin
  if btrim(new.name) = '' or cardinality(new.weekdays) not between 1 and 7
     or exists (select 1 from unnest(new.weekdays) d where d is null or d not between 0 and 6)
     or cardinality(new.times) not between 1 and 96
     or exists (select 1 from unnest(new.times) t where t is null or t !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$')
     or cardinality(new.reminder_offsets) > 24
     or exists (select 1 from unnest(new.reminder_offsets) m where m is null or m not between 1 and 10080)
     or not exists(select 1 from pg_timezone_names where name = new.timezone) then
    raise exception 'invalid recurring calendar configuration' using errcode = '23514';
  end if;
  new.name := btrim(new.name);
  new.weekdays := array(select distinct d from unnest(new.weekdays) d order by d);
  new.times := array(select distinct t from unnest(new.times) t order by t);
  new.reminder_offsets := array(select distinct m from unnest(new.reminder_offsets) m order by m);
  return new;
end $$;
drop trigger if exists v2_economic_schedules_validate on public.v2_economic_schedules;
create trigger v2_economic_schedules_validate before insert or update on public.v2_economic_schedules
for each row execute function public.v2_validate_economic_schedule();

create or replace function public.v2_calendar_can_read(p_workspace_id uuid)
returns boolean language sql stable security definer set search_path = public, pg_temp as $$
  select case when p_workspace_id is null then app_private.is_app_member()
    else public.v2_is_workspace_member(p_workspace_id) end;
$$;
create or replace function public.v2_calendar_can_edit(p_workspace_id uuid)
returns boolean language sql stable security definer set search_path = public, pg_temp as $$
  select case when p_workspace_id is null then app_private.can_edit_shared_calendar()
    else public.v2_can_edit_workspace(p_workspace_id) end;
$$;
revoke all on function public.v2_calendar_can_read(uuid), public.v2_calendar_can_edit(uuid) from public, anon;
grant execute on function public.v2_calendar_can_read(uuid), public.v2_calendar_can_edit(uuid) to authenticated;

-- Replace existing permissive policies: adding another permissive policy alone
-- would leave workspace calendar entries exposed to unrelated legacy members.
drop policy if exists "economic events read" on public.economic_events;
drop policy if exists "economic events insert" on public.economic_events;
drop policy if exists "economic events update" on public.economic_events;
drop policy if exists "economic events delete" on public.economic_events;
create policy "economic events read" on public.economic_events for select to authenticated
using (public.v2_calendar_can_read(workspace_id));
create policy "economic events insert" on public.economic_events for insert to authenticated
with check (public.v2_calendar_can_edit(workspace_id) and created_by = auth.uid() and updated_by = auth.uid());
create policy "economic events update" on public.economic_events for update to authenticated
using (public.v2_calendar_can_edit(workspace_id)) with check (public.v2_calendar_can_edit(workspace_id));
create policy "economic events delete" on public.economic_events for delete to authenticated
using (public.v2_calendar_can_edit(workspace_id));

drop policy if exists "economic reminders read" on public.economic_event_reminders;
drop policy if exists "economic reminders insert" on public.economic_event_reminders;
drop policy if exists "economic reminders update" on public.economic_event_reminders;
drop policy if exists "economic reminders delete" on public.economic_event_reminders;
create policy "economic reminders read" on public.economic_event_reminders for select to authenticated
using (exists(select 1 from public.economic_events e where e.id = event_id and public.v2_calendar_can_read(e.workspace_id)));
create policy "economic reminders insert" on public.economic_event_reminders for insert to authenticated
with check (exists(select 1 from public.economic_events e where e.id = event_id and public.v2_calendar_can_edit(e.workspace_id)));
create policy "economic reminders update" on public.economic_event_reminders for update to authenticated
using (exists(select 1 from public.economic_events e where e.id = event_id and public.v2_calendar_can_edit(e.workspace_id)))
with check (exists(select 1 from public.economic_events e where e.id = event_id and public.v2_calendar_can_edit(e.workspace_id)));
create policy "economic reminders delete" on public.economic_event_reminders for delete to authenticated
using (exists(select 1 from public.economic_events e where e.id = event_id and public.v2_calendar_can_edit(e.workspace_id)));

drop policy if exists "economic ack read own" on public.economic_reminder_acknowledgements;
drop policy if exists "economic ack insert own" on public.economic_reminder_acknowledgements;
drop policy if exists "economic ack delete own" on public.economic_reminder_acknowledgements;
drop policy if exists "economic ack update own" on public.economic_reminder_acknowledgements;
create policy "economic ack read own" on public.economic_reminder_acknowledgements for select to authenticated
using (user_id = auth.uid() and exists(select 1 from public.economic_event_reminders r where r.id = reminder_id));
create policy "economic ack insert own" on public.economic_reminder_acknowledgements for insert to authenticated
with check (user_id = auth.uid() and exists(select 1 from public.economic_event_reminders r where r.id = reminder_id));
create policy "economic ack delete own" on public.economic_reminder_acknowledgements for delete to authenticated
using (user_id = auth.uid() and exists(select 1 from public.economic_event_reminders r where r.id = reminder_id));
create policy "economic ack update own" on public.economic_reminder_acknowledgements for update to authenticated
using (user_id = auth.uid() and exists(select 1 from public.economic_event_reminders r where r.id = reminder_id))
with check (user_id = auth.uid() and exists(select 1 from public.economic_event_reminders r where r.id = reminder_id));

create or replace function public.v2_validate_calendar_event_scope()
returns trigger language plpgsql security definer set search_path = public, pg_temp as $$
begin
  if tg_op = 'UPDATE' then
    if new.workspace_id is distinct from old.workspace_id then
      raise exception 'calendar workspace is immutable' using errcode = '23514';
    end if;
    new.created_by := old.created_by;
  elsif auth.uid() is not null then
    new.created_by := auth.uid(); new.updated_by := auth.uid();
  end if;
  if new.recurring_schedule_id is not null and not exists(
    select 1 from public.v2_economic_schedules s where s.id = new.recurring_schedule_id
      and s.workspace_id = new.workspace_id and new.occurrence_key is not null
  ) then
    raise exception 'calendar occurrence must belong to its schedule workspace' using errcode = '23514';
  end if;
  return new;
end $$;
drop trigger if exists v2_calendar_event_scope_guard on public.economic_events;
create trigger v2_calendar_event_scope_guard before insert or update on public.economic_events
for each row execute function public.v2_validate_calendar_event_scope();

-- A single call changes the schedule occurrences and reminder rows together.
-- Locking the schedule serializes concurrent refreshes from separate tabs.
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
          insert into public.economic_event_reminders(event_id,offset_minutes,due_at)
          select v_event_id, m, event_time - make_interval(mins => m) from unnest(s.reminder_offsets) m
          on conflict(event_id,offset_minutes) do nothing;
          delete from public.economic_event_reminders r
          where r.event_id = v_event_id and not (r.offset_minutes = any(s.reminder_offsets));
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
revoke all on function public.v2_materialize_economic_schedule(uuid,integer) from public, anon;
grant execute on function public.v2_materialize_economic_schedule(uuid,integer) to authenticated;

create or replace function public.v2_sync_economic_schedule()
returns trigger language plpgsql security definer set search_path = public, pg_temp as $$
begin
  if tg_op = 'DELETE' then
    delete from public.economic_events where recurring_schedule_id = old.id and event_at > now();
    return old;
  end if;
  -- Trusted migrations may seed schedules without a user JWT; they explicitly
  -- materialize later as an editor. Client writes are always atomic.
  if auth.uid() is not null then perform public.v2_materialize_economic_schedule(new.id,14); end if;
  return new;
end $$;
drop trigger if exists v2_economic_schedules_sync on public.v2_economic_schedules;
create trigger v2_economic_schedules_sync after insert or update on public.v2_economic_schedules
for each row execute function public.v2_sync_economic_schedule();
drop trigger if exists v2_economic_schedules_delete_occurrences on public.v2_economic_schedules;
create trigger v2_economic_schedules_delete_occurrences before delete on public.v2_economic_schedules
for each row execute function public.v2_sync_economic_schedule();

-- Match every configured template/reference to the owning workspace and kind.
-- Abort a partially upgraded database with inconsistent references rather than
-- silently legitimizing rows that the new runtime cannot safely edit.
do $$ begin
  if exists(select 1 from public.v2_asset_settings a cross join lateral (values
      (a.default_collection_template_id,'collection'),(a.default_scenario_template_id,'scenario'),(a.default_trade_template_id,'trade')) r(template_id,kind)
    join public.v2_templates t on t.id = r.template_id
    where t.workspace_id <> a.workspace_id or t.kind <> r.kind)
  or exists(select 1 from public.v2_scenarios s join public.v2_templates t on t.id = s.template_id
    where t.workspace_id <> s.workspace_id or t.kind <> 'scenario')
  or exists(select 1 from public.v2_scenario_values v join public.v2_scenarios s on s.id = v.scenario_id
    join public.v2_template_fields f on f.id = v.field_id join public.v2_template_sections sec on sec.id = f.section_id
    join public.v2_templates t on t.id = sec.template_id
    where s.template_id is null or s.template_id <> t.id or s.workspace_id <> t.workspace_id or t.kind <> 'scenario')
  or exists(select 1 from public.v2_spot_values v join public.v2_spot_assets a on a.id = v.asset_id
    join public.v2_template_fields f on f.id = v.field_id join public.v2_template_sections sec on sec.id = f.section_id
    join public.v2_templates t on t.id = sec.template_id
    where a.workspace_id <> t.workspace_id or t.kind <> 'spot') then
    raise exception 'existing V2 template references violate workspace/kind integrity' using errcode = '23514';
  end if;
end $$;

create or replace function public.v2_validate_runtime_template_links()
returns trigger language plpgsql security definer set search_path = public, pg_temp as $$
declare pair record; target_workspace uuid; target_template uuid; field_template uuid; field_workspace uuid; field_kind text;
begin
  if tg_table_name = 'v2_asset_settings' then
    for pair in select * from (values
      (new.default_collection_template_id, 'collection'),
      (new.default_scenario_template_id, 'scenario'),
      (new.default_trade_template_id, 'trade')) x(template_id,kind)
    loop
      if pair.template_id is not null and not exists(select 1 from public.v2_templates t
        where t.id = pair.template_id and t.workspace_id = new.workspace_id and t.kind = pair.kind) then
        raise exception 'asset default template has wrong workspace or kind' using errcode = '23514';
      end if;
    end loop;
  elsif tg_table_name = 'v2_scenarios' then
    if new.template_id is not null and not exists(select 1 from public.v2_templates t
      where t.id = new.template_id and t.workspace_id = new.workspace_id and t.kind = 'scenario') then
      raise exception 'scenario template has wrong workspace or kind' using errcode = '23514';
    end if;
    if tg_op = 'UPDATE' and new.template_id is distinct from old.template_id
       and exists(select 1 from public.v2_scenario_values v where v.scenario_id = old.id) then
      raise exception 'cannot change template after scenario answers exist' using errcode = '23514';
    end if;
  else
    select t.id,t.workspace_id,t.kind into field_template,field_workspace,field_kind
    from public.v2_template_fields f join public.v2_template_sections s on s.id = f.section_id
    join public.v2_templates t on t.id = s.template_id where f.id = new.field_id;
    if tg_table_name = 'v2_scenario_values' then
      select workspace_id,template_id into target_workspace,target_template
      from public.v2_scenarios where id = new.scenario_id;
      if target_workspace is null or target_template is null or target_template <> field_template
        or target_workspace <> field_workspace or field_kind <> 'scenario' then
        raise exception 'scenario answer must use its own template field' using errcode = '23514';
      end if;
    elsif tg_table_name = 'v2_spot_values' then
      select workspace_id into target_workspace from public.v2_spot_assets where id = new.asset_id;
      if target_workspace is null or field_workspace is null or target_workspace <> field_workspace or field_kind <> 'spot' then
        raise exception 'spot answer must use a spot template in its workspace' using errcode = '23514';
      end if;
    end if;
  end if;
  return new;
end $$;

do $$ declare t text; begin
  foreach t in array array['v2_asset_settings','v2_scenarios','v2_scenario_values','v2_spot_values'] loop
    execute format('drop trigger if exists %I on public.%I',t || '_runtime_links',t);
    execute format('create trigger %I before insert or update on public.%I for each row execute function public.v2_validate_runtime_template_links()',t || '_runtime_links',t);
  end loop;
  foreach t in array array['v2_asset_settings','v2_economic_schedules'] loop
    execute format('drop trigger if exists %I on public.%I',t || '_workspace_immutable',t);
    execute format('create trigger %I before update of workspace_id on public.%I for each row execute function public.v2_reject_workspace_reassignment()',t || '_workspace_immutable',t);
  end loop;
  foreach t in array array['v2_spot_transactions','v2_spot_orders','v2_spot_targets'] loop
    execute format('drop trigger if exists %I on public.%I',t || '_updated_by',t);
    execute format('create trigger %I before insert or update on public.%I for each row execute function public.v2_force_updated_by()',t || '_updated_by',t);
  end loop;
end $$;

-- Once a field has answers, changing its parent template would corrupt those
-- answers even when both templates happen to be in the same workspace.
create or replace function public.v2_guard_used_template_moves()
returns trigger language plpgsql security definer set search_path = public, pg_temp as $$
declare field_ids uuid[];
begin
  if tg_table_name = 'v2_template_sections' then
    if new.template_id is not distinct from old.template_id then return new; end if;
    select array_agg(id) into field_ids from public.v2_template_fields where section_id = old.id;
  else
    if new.section_id is not distinct from old.section_id then return new; end if;
    if (select template_id from public.v2_template_sections where id = new.section_id)
       = (select template_id from public.v2_template_sections where id = old.section_id) then return new; end if;
    field_ids := array[old.id];
  end if;
  if exists(select 1 from public.v2_collection_values where field_id = any(field_ids))
    or exists(select 1 from public.v2_trade_values where field_id = any(field_ids))
    or exists(select 1 from public.v2_scenario_values where field_id = any(field_ids))
    or exists(select 1 from public.v2_spot_values where field_id = any(field_ids)) then
    raise exception 'answered fields cannot move to a different template' using errcode = '23514';
  end if;
  return new;
end $$;
drop trigger if exists v2_template_sections_used_move_guard on public.v2_template_sections;
create trigger v2_template_sections_used_move_guard before update of template_id on public.v2_template_sections
for each row execute function public.v2_guard_used_template_moves();
drop trigger if exists v2_template_fields_used_move_guard on public.v2_template_fields;
create trigger v2_template_fields_used_move_guard before update of section_id on public.v2_template_fields
for each row execute function public.v2_guard_used_template_moves();

-- Signed private images: the first folder identifies the workspace, followed by
-- module and entity. Invalid UUID/path input returns false instead of a cast error.
insert into storage.buckets(id,name,public,file_size_limit,allowed_mime_types)
values('workspace-images','workspace-images',false,10485760,array['image/jpeg','image/png','image/webp','image/gif','image/avif'])
on conflict(id) do update set public = false,file_size_limit = excluded.file_size_limit,allowed_mime_types = excluded.allowed_mime_types;

create or replace function public.v2_image_workspace(p_name text)
returns uuid language plpgsql immutable set search_path = public, pg_temp as $$
declare parts text[];
begin
  parts := string_to_array(p_name,'/');
  if cardinality(parts) <> 4 or parts[2] not in ('collection','scenario','trade','spot')
    or parts[1] !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or parts[3] !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or parts[4] = '' then return null; end if;
  return parts[1]::uuid;
end $$;
revoke all on function public.v2_image_workspace(text) from public,anon;
grant execute on function public.v2_image_workspace(text) to authenticated;
drop policy if exists "v2 workspace images read" on storage.objects;
create policy "v2 workspace images read" on storage.objects for select to authenticated
using (bucket_id = 'workspace-images' and public.v2_is_workspace_member(public.v2_image_workspace(name)));
drop policy if exists "v2 workspace images insert" on storage.objects;
create policy "v2 workspace images insert" on storage.objects for insert to authenticated
with check (bucket_id = 'workspace-images' and public.v2_can_edit_workspace(public.v2_image_workspace(name)));
drop policy if exists "v2 workspace images update" on storage.objects;
create policy "v2 workspace images update" on storage.objects for update to authenticated
using (bucket_id = 'workspace-images' and public.v2_can_edit_workspace(public.v2_image_workspace(name)))
with check (bucket_id = 'workspace-images' and public.v2_can_edit_workspace(public.v2_image_workspace(name)));
drop policy if exists "v2 workspace images delete" on storage.objects;
create policy "v2 workspace images delete" on storage.objects for delete to authenticated
using (bucket_id = 'workspace-images' and public.v2_can_edit_workspace(public.v2_image_workspace(name)));

-- Explicit privileges work on both Supabase defaults and a clean PostgreSQL
-- installation. RLS remains the authority for individual rows.
do $$ declare t text; begin
  for t in select tablename from pg_tables where schemaname = 'public' and tablename like 'v2\_%' escape '\' loop
    execute format('revoke all on public.%I from anon',t);
    execute format('grant select,insert,update,delete on public.%I to authenticated',t);
  end loop;
end $$;
revoke all on function public.v2_validate_economic_schedule(),public.v2_validate_calendar_event_scope(),
  public.v2_sync_economic_schedule(),public.v2_validate_runtime_template_links(),public.v2_guard_used_template_moves()
from public,anon,authenticated;

-- Future workspaces receive the same four defaults as migrated workspaces.
create or replace function public.v2_seed_default_templates(p_workspace_id uuid)
returns void
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  collection_template uuid;
  trade_template uuid;
  section_news uuid;
  section_data uuid;
  section_scan uuid;
  section_entry uuid;
  section_manage uuid;
  section_journal uuid;
  extra_template uuid;
  extra_section uuid;
  template_kind text;
begin
  if not public.v2_can_edit_workspace(p_workspace_id) then
    raise exception 'not allowed';
  end if;

  -- Serialize initialization from multiple browser tabs.
  perform 1 from public.v2_workspaces where id = p_workspace_id for update;

  select id into collection_template
  from public.v2_templates
  where workspace_id = p_workspace_id and kind = 'collection' and is_default and active and asset_symbol is null
  limit 1;

  if collection_template is null then
    insert into public.v2_templates(workspace_id, kind, name, is_default, created_by)
    values (p_workspace_id, 'collection', 'איסוף מידע – ברירת מחדל', true, auth.uid())
    returning id into collection_template;

    insert into public.v2_template_sections(template_id, title, position)
    values (collection_template, 'חדשות ומאקרו', 0)
    returning id into section_news;
    insert into public.v2_template_sections(template_id, title, position)
    values (collection_template, 'נזילות, טכני ונתונים', 1)
    returning id into section_data;

    insert into public.v2_template_fields(section_id, field_key, title, field_type, position)
    values
      (section_news, 'news', 'חדשות', 'news_source_table', 0),
      (section_news, 'macro', 'מאקרו', 'dynamic_text', 1),
      (section_data, 'liquidity', 'נזילות', 'liquidity_table', 0),
      (section_data, 'technical', 'טכני ונתונים', 'dynamic_text', 1);
  end if;

  select id into trade_template
  from public.v2_templates
  where workspace_id = p_workspace_id and kind = 'trade' and is_default and active and asset_symbol is null
  limit 1;

  if trade_template is null then
    insert into public.v2_templates(workspace_id, kind, name, is_default, created_by)
    values (p_workspace_id, 'trade', 'טרייד – ברירת מחדל', true, auth.uid())
    returning id into trade_template;

    insert into public.v2_template_sections(template_id, title, position)
    values (trade_template, 'סריקת טרייד', 0) returning id into section_scan;
    insert into public.v2_template_sections(template_id, title, position)
    values (trade_template, 'כניסה', 1) returning id into section_entry;
    insert into public.v2_template_sections(template_id, title, position)
    values (trade_template, 'ניהול טרייד', 2) returning id into section_manage;
    insert into public.v2_template_sections(template_id, title, position)
    values (trade_template, 'יומן', 3) returning id into section_journal;

    insert into public.v2_template_fields(section_id, field_key, title, field_type, position, required)
    values
      (section_scan, 'trade_thesis', 'תזה ותרחיש', 'long_text', 0, true),
      (section_scan, 'trade_risks', 'מה יבטל את התרחיש', 'long_text', 1, true),
      (section_entry, 'entry_plan', 'תכנית כניסה', 'long_text', 0, true),
      (section_entry, 'stop_loss', 'סטופ', 'short_text', 1, true),
      (section_manage, 'management_plan', 'תכנית ניהול', 'long_text', 0, true),
      (section_journal, 'conclusions', 'מסקנות', 'long_text', 0, false),
      (section_journal, 'notes', 'הערות', 'long_text', 1, false);
  end if;

  foreach template_kind in array array['scenario','spot'] loop
    select id into extra_template from public.v2_templates
    where workspace_id = p_workspace_id and kind = template_kind and is_default and active and asset_symbol is null limit 1;
    if extra_template is null then
      insert into public.v2_templates(workspace_id,kind,name,is_default,created_by)
      values(p_workspace_id,template_kind,case when template_kind = 'scenario' then 'תרחיש – ברירת מחדל' else 'יומן ספוט – ברירת מחדל' end,true,auth.uid())
      returning id into extra_template;
      insert into public.v2_template_sections(template_id,title,position,settings)
      values(extra_template,case when template_kind = 'scenario' then 'פרטי התרחיש' else 'תיעוד הנכס' end,0,
        case when template_kind = 'scenario' then '{"base_section":true}'::jsonb else '{}'::jsonb end)
      returning id into extra_section;
      if template_kind = 'scenario' then
        insert into public.v2_template_fields(section_id,field_key,title,field_type,position) values
          (extra_section,'scenario_body','התרחיש','rich_text',0),
          (extra_section,'scenario_reasons','סיבות ואינדיקציות','rich_text',1),
          (extra_section,'scenario_invalidation','מה יבטל את התרחיש','rich_text',2);
      else
        insert into public.v2_template_fields(section_id,field_key,title,field_type,position) values
          (extra_section,'spot_plan','תכנית ונקודות חשובות','rich_text',0),
          (extra_section,'spot_notes','הערות','dynamic_text',1);
      end if;
    end if;
  end loop;
  select s.id into section_journal from public.v2_template_sections s
  where s.template_id = trade_template and s.title = 'יומן' order by s.position limit 1;
  if section_journal is not null and not exists(select 1 from public.v2_template_fields where section_id = section_journal and field_key = 'trade_pnl')
    and not exists(select 1 from public.v2_trades where template_id = trade_template) then
    insert into public.v2_template_fields(section_id,field_key,title,field_type,position)
    values(section_journal,'trade_pnl','PnL','pnl',coalesce((select max(position)+1 from public.v2_template_fields where section_id = section_journal),0));
  end if;
  insert into public.v2_asset_settings(workspace_id,symbol,display_name,default_collection_template_id,default_scenario_template_id,default_trade_template_id,created_by,updated_by)
  values(p_workspace_id,'BTC','Bitcoin',collection_template,
    (select id from public.v2_templates where workspace_id = p_workspace_id and kind = 'scenario' and active and is_default and asset_symbol is null limit 1),
    trade_template,auth.uid(),auth.uid())
  on conflict(workspace_id,symbol) do nothing;
end;
$$;

revoke all on function public.v2_seed_default_templates(uuid) from public,anon;
grant execute on function public.v2_seed_default_templates(uuid) to authenticated;

-- Initialize missing defaults once; preserve custom/answered templates.
do $$ declare w record; prior_subject text := current_setting('request.jwt.claim.sub', true); begin
  for w in select id,owner_id from public.v2_workspaces loop
    perform set_config('request.jwt.claim.sub',w.owner_id::text,true);
    perform public.v2_seed_default_templates(w.id);
  end loop;
  perform set_config('request.jwt.claim.sub',coalesce(prior_subject,''),true);
end $$;

-- Updating only the parent template must not invalidate historical answer rows.
create or replace function public.v2_guard_answered_parent_template()
returns trigger language plpgsql security definer set search_path = public, pg_temp as $$
begin
  if new.template_id is not distinct from old.template_id then return new; end if;
  if tg_table_name = 'v2_collection_sessions' then
    if exists(select 1 from public.v2_collection_values where session_id = old.id) then
      raise exception 'cannot change template after collection answers exist' using errcode = '23514';
    end if;
  elsif tg_table_name = 'v2_trades' then
    if exists(select 1 from public.v2_trade_values where trade_id = old.id) then
      raise exception 'cannot change template after trade answers exist' using errcode = '23514';
    end if;
  end if;
  return new;
end $$;
revoke all on function public.v2_guard_answered_parent_template() from public,anon,authenticated;
drop trigger if exists v2_collection_sessions_answered_template_guard on public.v2_collection_sessions;
create trigger v2_collection_sessions_answered_template_guard before update of template_id on public.v2_collection_sessions
for each row execute function public.v2_guard_answered_parent_template();
drop trigger if exists v2_trades_answered_template_guard on public.v2_trades;
create trigger v2_trades_answered_template_guard before update of template_id on public.v2_trades
for each row execute function public.v2_guard_answered_parent_template();
