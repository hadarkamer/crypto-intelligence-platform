-- One shared calendar import for every occurrence, including duplicate source fields.
begin;

alter table public.economic_events add column if not exists description text not null default '';
alter table public.economic_events add column if not exists source_url text;

create table if not exists public.v2_economic_calendar_imports (
  workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
  source_key text not null check (source_key='investing'),
  source_event_id text not null check (source_event_id ~ '^[1-9][0-9]{0,14}$'),
  event_id uuid references public.economic_events(id) on delete set null,
  source_name text not null,
  source_event_at timestamptz not null,
  source_description text not null default '',
  last_seen_at timestamptz not null default now(),
  primary key(workspace_id,source_key,source_event_id)
);
alter table public.v2_economic_calendar_imports enable row level security;
revoke all on public.v2_economic_calendar_imports from public,anon,authenticated;
grant select on public.v2_economic_calendar_imports to authenticated;
grant all on public.v2_economic_calendar_imports to service_role;
drop policy if exists "economic import workspace read" on public.v2_economic_calendar_imports;
create policy "economic import workspace read" on public.v2_economic_calendar_imports
  for select to authenticated using(public.v2_calendar_can_read(workspace_id));

create or replace function app_private.v2_economic_calendar_source(p_field jsonb)
returns boolean language sql immutable set search_path=public,pg_temp as $source$
  with settings as (select coalesce(p_field->'settings','{}') as value),
  links as (select value,case when value ? 'autofill_source_urls' then
      case when jsonb_typeof(value->'autofill_source_urls')='array' then value->'autofill_source_urls' else '[]'::jsonb end
    else case when jsonb_typeof(coalesce(value->'links_json',value->'links',value#>'{legacy_question,links_json}'))='array'
      then coalesce(value->'links_json',value->'links',value#>'{legacy_question,links_json}') else '[]'::jsonb end end as entries from settings),
  urls as (
    select coalesce(entry->>'url',entry#>>'{}') as url from links,jsonb_array_elements(entries) entry
    union all select coalesce(value->>'source_url',value#>>'{legacy_question,source_url}') from links
      where not(value ? 'autofill_source_urls')
  ) select p_field->>'field_type'='economic_calendar' and count(*)>0 and bool_and(url in (
    'https://www.investing.com/economic-calendar','https://www.investing.com/economic-calendar/',
    'https://investing.com/economic-calendar','https://investing.com/economic-calendar/'))
    from urls where nullif(btrim(url),'') is not null;
$source$;
revoke all on function app_private.v2_economic_calendar_source(jsonb) from public,anon,authenticated,service_role;

-- This server-only endpoint uses the existing apply transaction to check the
-- original session, active field, template revision, lease and cancellation.
-- Calendar writes and completion/cursors either commit together or roll back.
create or replace function public.v2_collection_autofill_apply_calendar(
  p_run_id uuid,p_snapshot jsonb,p_cursors jsonb,p_scan_started_at timestamptz
) returns jsonb language plpgsql security definer set search_path=public,pg_temp as $function$
declare
  run public.v2_collection_autofill_runs; field public.v2_template_fields;
  result jsonb; event jsonb; mapped public.v2_economic_calendar_imports;
  current_event public.economic_events; event_id uuid; owner_id uuid; native_present boolean;
  source_id text; event_name text; event_description text; event_time timestamptz;
  start_at timestamptz; end_at timestamptz; captured_at timestamptz;
  expected_start timestamptz; expected_end timestamptz; amount integer:=0;
  final_message text;
  shared_state public.v2_collection_autofill_state;
begin
  select * into run from public.v2_collection_autofill_runs where id=p_run_id;
  if run.id is null then raise exception 'Unknown calendar run' using errcode='23514'; end if;
  -- Match claim/base-apply lock order so replacement of an expired run cannot
  -- deadlock with its delayed calendar apply.
  perform 1 from public.v2_workspaces where id=run.workspace_id for update;
  select * into run from public.v2_collection_autofill_runs where id=p_run_id for update;
  select * into field from public.v2_template_fields where id=run.field_id;
  if field.field_type is distinct from 'economic_calendar' then
    raise exception 'Economic calendar field required' using errcode='23514';
  end if;
  if run.status<>'running' then
    return jsonb_build_object('run_id',run.id,'status',run.status,'session_id',run.session_id,
      'count',run.count,'message',run.message);
  end if;
  if auth.uid() is not null and not public.v2_calendar_can_edit(run.workspace_id) then
    raise exception 'Calendar edit permission required' using errcode='42501';
  end if;
  -- Field source validation is repeated in SQL; a changed configured source
  -- must never accept a delayed scan from another URL.
  if app_private.v2_economic_calendar_source(to_jsonb(field)) is distinct from true then
    raise exception 'Configured economic calendar source required' using errcode='23514';
  end if;
  expected_start:=date_trunc('day',p_scan_started_at at time zone 'Asia/Jerusalem') at time zone 'Asia/Jerusalem';
  expected_end:=(date_trunc('day',p_scan_started_at at time zone 'Asia/Jerusalem')+interval '7 days') at time zone 'Asia/Jerusalem';
  if jsonb_typeof(p_snapshot) is distinct from 'object' or
    jsonb_typeof(p_snapshot->'sourceUrl') is distinct from 'string' or
    p_snapshot->>'sourceUrl' is distinct from 'https://www.investing.com/economic-calendar' or
    jsonb_typeof(p_snapshot->'events') is distinct from 'array' or
    jsonb_array_length(p_snapshot->'events')>300 or
    jsonb_typeof(p_snapshot->'capturedAt') is distinct from 'string' or
    jsonb_typeof(p_snapshot->'rangeStart') is distinct from 'string' or
    jsonb_typeof(p_snapshot->'rangeEnd') is distinct from 'string' or
    jsonb_typeof(p_snapshot->'partial') is distinct from 'boolean' or
    p_scan_started_at is null or abs(extract(epoch from(now()-p_scan_started_at)))>1200 or
    coalesce(p_snapshot->>'capturedAt','') !~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,3})?Z$' or
    coalesce(p_snapshot->>'rangeStart','') !~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,3})?Z$' or
    coalesce(p_snapshot->>'rangeEnd','') !~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,3})?Z$' then
    raise exception 'Invalid economic calendar snapshot' using errcode='23514';
  end if;
  start_at:=(p_snapshot->>'rangeStart')::timestamptz;
  end_at:=(p_snapshot->>'rangeEnd')::timestamptz;
  captured_at:=(p_snapshot->>'capturedAt')::timestamptz;
  if to_char(start_at at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') is distinct from p_snapshot->>'rangeStart' or
    to_char(end_at at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') is distinct from p_snapshot->>'rangeEnd' or
    to_char(captured_at at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') is distinct from p_snapshot->>'capturedAt' then
    raise exception 'Economic calendar timestamps must be canonical UTC instants' using errcode='23514';
  end if;
  if start_at is distinct from expected_start or end_at<=start_at or end_at>expected_end or
    (p_snapshot->>'partial')::boolean is distinct from (end_at<expected_end) or
    abs(extract(epoch from(captured_at-p_scan_started_at)))>600 then
    raise exception 'Economic calendar window or capture time is invalid' using errcode='23514';
  end if;
  if exists(select 1 from jsonb_array_elements(p_snapshot->'events') e
    group by e->>'sourceId' having count(*)>1) then
    raise exception 'Duplicate economic calendar occurrence' using errcode='23514';
  end if;
  -- Empty payload leaves worksheet answers unchanged. The existing endpoint
  -- still verifies all target/lease/revision guards before granting completion.
  result:=public.v2_collection_autofill_apply(p_run_id,'{}'::jsonb,p_cursors,p_scan_started_at);
  if result->>'status' is distinct from 'unchanged' then return result; end if;
  select w.owner_id into owner_id from public.v2_workspaces w where w.id=run.workspace_id;
  for event in select * from jsonb_array_elements(p_snapshot->'events') loop
    source_id:=event->>'sourceId'; event_name:=btrim(event->>'name');
    event_description:=event->>'description';
    if jsonb_typeof(event) is distinct from 'object' or
      jsonb_typeof(event->'sourceId') is distinct from 'string' or
      jsonb_typeof(event->'name') is distinct from 'string' or
      jsonb_typeof(event->'description') is distinct from 'string' or
      jsonb_typeof(event->'eventAt') is distinct from 'string' or
      jsonb_typeof(event->'sourceUrl') is distinct from 'string' or
      jsonb_typeof(event->'importance') is distinct from 'number' or
      coalesce(source_id,'') !~ '^[1-9][0-9]{0,14}$' or
      event->>'importance' is distinct from '3' or
      event_name is null or length(event_name) not between 1 and 300 or
      event_description is null or length(event_description)>4000 or
      coalesce(event->>'eventAt','') !~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,3})?Z$' or
      coalesce(event->>'sourceUrl','') !~ '^https://www\.investing\.com/economic-calendar/[a-zA-Z0-9-]+-[0-9]+$' then
      raise exception 'Invalid economic event identity, importance, time or source' using errcode='23514';
    end if;
    event_time:=(event->>'eventAt')::timestamptz;
    if to_char(event_time at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') is distinct from event->>'eventAt' then
      raise exception 'Economic event timestamp must be a canonical UTC instant' using errcode='23514';
    end if;
    if event_time<start_at or event_time>=end_at then
      raise exception 'Economic event lies outside the verified window' using errcode='23514';
    end if;
    select * into mapped from public.v2_economic_calendar_imports
      where workspace_id=run.workspace_id and source_key='investing' and source_event_id=source_id;
    if not found then
      -- Adopt one exact manual event only when its source/description also
      -- identifies the release. Different countries can share title and time.
      -- Events already mapped to another occurrence are never candidates.
      select * into current_event from public.economic_events e
        where e.workspace_id=run.workspace_id and e.name=event_name and e.event_at=event_time
        and e.importance=3 and (e.description=event_description or e.source_url=event->>'sourceUrl')
        and not exists(select 1 from public.v2_economic_calendar_imports known where known.event_id=e.id)
        and 1=(select count(*) from public.economic_events candidate
          where candidate.workspace_id=run.workspace_id and candidate.name=event_name
          and candidate.event_at=event_time and candidate.importance=3
          and (candidate.description=event_description or candidate.source_url=event->>'sourceUrl')
          and not exists(select 1 from public.v2_economic_calendar_imports known where known.event_id=candidate.id))
        for update;
      if found then event_id:=current_event.id;
      else
        insert into public.economic_events(name,event_at,importance,workspace_id,created_by,updated_by,description,source_url)
          values(event_name,event_time,3,run.workspace_id,owner_id,owner_id,event_description,event->>'sourceUrl')
          returning id into event_id;
        -- Every member starts with the existing 3-star defaults. Their later
        -- personal choices are retained by ON CONFLICT and every later scan.
        insert into public.v2_economic_reminder_preferences(event_id,user_id,reminder_offsets)
          select event_id,m.user_id,array[60,240,1440] from public.v2_workspace_members m
          where m.workspace_id=run.workspace_id on conflict do nothing;
        amount:=amount+1;
      end if;
      insert into public.v2_economic_calendar_imports(workspace_id,source_key,source_event_id,event_id,
        source_name,source_event_at,source_description,last_seen_at)
        values(run.workspace_id,'investing',source_id,event_id,event_name,event_time,event_description,captured_at);
    else
      if captured_at<mapped.last_seen_at then continue; end if;
      -- A null target is a deletion tombstone: rescanning must not resurrect it.
      if mapped.event_id is not null then
        -- Native DELETE locks the event before its FK tombstones the map. Match
        -- that order, then reread the map if a deletion completed while waiting.
        -- The workspace lock already serializes all scanner imports.
        select * into current_event from public.economic_events where id=mapped.event_id for update;
        native_present:=found;
        select * into mapped from public.v2_economic_calendar_imports
          where workspace_id=run.workspace_id and source_key='investing' and source_event_id=source_id for update;
        if not found then
          raise exception 'Economic import mapping changed' using errcode='23514';
        end if;
        if mapped.event_id is not null and (not native_present
          or current_event.id is distinct from mapped.event_id
          or current_event.workspace_id is distinct from run.workspace_id) then
          raise exception 'Economic import workspace changed' using errcode='23514';
        end if;
        if mapped.event_id is not null then
          update public.economic_events e set
          name=case when e.name=mapped.source_name then event_name else e.name end,
          event_at=case when e.event_at=mapped.source_event_at then event_time else e.event_at end,
          description=case when e.description=mapped.source_description then event_description else e.description end,
          source_url=case when e.source_url is not null then event->>'sourceUrl' else e.source_url end,
          updated_by=owner_id
          where e.id=mapped.event_id and (
            (e.name=mapped.source_name and e.name is distinct from event_name) or
            (e.event_at=mapped.source_event_at and e.event_at is distinct from event_time) or
            (e.description=mapped.source_description and e.description is distinct from event_description));
          if found then amount:=amount+1; end if;
        end if;
      end if;
      update public.v2_economic_calendar_imports set source_name=event_name,source_event_at=event_time,
        source_description=event_description,last_seen_at=captured_at
        where workspace_id=run.workspace_id and source_key='investing' and source_event_id=source_id;
    end if;
  end loop;
  -- Both visible calendar copies share one schedule/cursor as well as events.
  -- Skip an already-running alias entirely; its original lease and captured
  -- cursor remain independently guarded until its own apply completes.
  -- An older concurrent alias may complete last. Choose the newest accepted
  -- idle calendar state before propagating, including back to this run's row.
  select state.* into shared_state from public.v2_collection_autofill_state state
    join public.v2_template_sections section on section.template_id=(select template_id from public.v2_template_sections where id=field.section_id)
    join public.v2_templates template on template.id=section.template_id and template.workspace_id=run.workspace_id
    join public.v2_template_fields alias on alias.section_id=section.id
    where state.workspace_id=run.workspace_id and section.active and alias.active
      and alias.settings#>>'{autofill,enabled}'='true' and app_private.v2_economic_calendar_source(to_jsonb(alias))
      and state.sheet_key=public.v2_collection_sheet_key(to_jsonb(section))
      and state.logical_key=public.v2_collection_field_logical_key(to_jsonb(alias))
      and coalesce(state.lease_until,'-infinity')<=now()
    order by state.last_scanned_at desc nulls last,state.next_due_at desc nulls last limit 1;
  insert into public.v2_collection_autofill_state(workspace_id,sheet_key,logical_key,
    next_due_at,last_scanned_at,source_cursors)
    select distinct run.workspace_id,public.v2_collection_sheet_key(to_jsonb(section)),
      public.v2_collection_field_logical_key(to_jsonb(alias)),shared_state.next_due_at,
      shared_state.last_scanned_at,shared_state.source_cursors
    from public.v2_template_fields alias join public.v2_template_sections section on section.id=alias.section_id
    join public.v2_template_sections original on original.id=field.section_id
    join public.v2_templates template on template.id=original.template_id and template.workspace_id=run.workspace_id
    where section.template_id=original.template_id and section.active and alias.active
      and alias.field_type='economic_calendar' and alias.settings#>>'{autofill,enabled}'='true'
      and app_private.v2_economic_calendar_source(to_jsonb(alias))
    on conflict(workspace_id,sheet_key,logical_key) do update set
      next_due_at=excluded.next_due_at,last_scanned_at=excluded.last_scanned_at,
      source_cursors=excluded.source_cursors
    where coalesce(v2_collection_autofill_state.lease_until,'-infinity')<=now()
      and excluded.last_scanned_at>=coalesce(v2_collection_autofill_state.last_scanned_at,'-infinity');
  final_message:=case when (p_snapshot->>'partial')::boolean then 'סריקה חלקית: ' else '' end||
    'נסרקו אירועי 3 כוכבים עד '||to_char(end_at at time zone 'Asia/Jerusalem','DD.MM.YY HH24:MI')
    ||case when (p_snapshot->>'partial')::boolean then '; המקור לא טען את יתר הטווח.' else '.' end||
    ' היומן משותף לשתי התיבות.';
  update public.v2_collection_autofill_runs set
    status=case when amount>0 then 'completed' else 'unchanged' end,count=amount,
    message=final_message where id=run.id;
  return jsonb_build_object('run_id',run.id,'status',case when amount>0 then 'completed' else 'unchanged' end,
    'session_id',run.session_id,'count',amount,'message',final_message);
end;
$function$;
revoke all on function public.v2_collection_autofill_apply_calendar(uuid,jsonb,jsonb,timestamptz) from public,anon,authenticated;
grant execute on function public.v2_collection_autofill_apply_calendar(uuid,jsonb,jsonb,timestamptz) to service_role;

commit;
