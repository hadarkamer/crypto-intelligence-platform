begin;

-- The worker polls frequently; scheduled slots are wall-clock hours in Israel,
-- anchored afresh every day rather than measured from the previous scan.
alter table public.v2_collection_autofill_state add column if not exists calendar_schedule_key text;

create or replace function app_private.collection_autofill_effective_schedule(
 p_workspace uuid,p_sheet text,p_logical text,p_settings jsonb,p_now timestamptz default now()
) returns jsonb language plpgsql stable security definer
set search_path=pg_catalog,public,app_private,pg_temp as $$
declare cfg jsonb:=coalesce(p_settings#>'{autofill,schedule}','{}'); result jsonb; model jsonb; profile record;
 windows jsonb; valid boolean; first_hour integer:=6; hours integer:=24; start_hour integer:=8; end_hour integer:=0;
begin
 if coalesce(p_settings#>>'{autofill,interval_hours}','') ~ '^([1-9]|1[0-9]|2[0-4])$' then
  hours:=(p_settings#>>'{autofill,interval_hours}')::integer;end if;
 if coalesce(cfg->>'first_hour','') ~ '^([0-9]|1[0-9]|2[0-3])$' then first_hour:=(cfg->>'first_hour')::integer;end if;
 if coalesce(cfg#>>'{active_hours,start}','') ~ '^([0-9]|1[0-9]|2[0-3])$' then start_hour:=(cfg#>>'{active_hours,start}')::integer;end if;
 if coalesce(cfg#>>'{active_hours,end}','') ~ '^([0-9]|1[0-9]|2[0-3])$' then end_hour:=(cfg#>>'{active_hours,end}')::integer;end if;
 result:=jsonb_build_object('version',1,'timezone','Asia/Jerusalem','first_hour',first_hour,'interval_hours',hours,
  'active_hours',jsonb_build_object('enabled',coalesce(cfg#>>'{active_hours,enabled}','false')='true','start',start_hour,'end',end_hour),
  'source_fingerprint',app_private.collection_autofill_source_fingerprint(p_settings));
 if cfg->>'adapt_to_publications'='true' then
  select * into profile from public.v2_collection_autofill_publication_profiles
   where workspace_id=p_workspace and sheet_key=p_sheet and logical_key=p_logical
    and source_fingerprint=app_private.collection_autofill_source_fingerprint(p_settings);
  model:=profile.model;
  if profile.model_revision=profile.evidence_revision and profile.updated_at>=p_now-interval '7 days'
   and model->>'version'='publication_schedule.v1' and model->>'eligible'='true'
   and model->>'confidence' in ('provisional','learned')
   and coalesce(model->>'first_hour','') ~ '^([0-9]|1[0-9]|2[0-3])$'
   and coalesce(model->>'interval_hours','') ~ '^([1-9]|1[0-9]|2[0-4])$'
   and jsonb_typeof(model->'weekly_windows')='array' then
   windows:=model->'weekly_windows';
   if jsonb_array_length(windows) between 1 and 336 then
    select coalesce(bool_and(
     jsonb_typeof(w)='object' and coalesce(w->>'day','') ~ '^[0-6]$'
     and coalesce(w->>'start_minute','') ~ '^([0-9]|[1-9][0-9]{1,2}|1[0-3][0-9]{2}|14[0-3][0-9])$'
     and coalesce(w->>'end_minute','') ~ '^([1-9]|[1-9][0-9]{1,2}|1[0-3][0-9]{2}|14[0-3][0-9]|1440)$'
     and case when coalesce(w->>'start_minute','') ~ '^[0-9]{1,4}$' and coalesce(w->>'end_minute','') ~ '^[0-9]{1,4}$'
      then (w->>'start_minute')::integer<(w->>'end_minute')::integer else false end),false)
     into valid from jsonb_array_elements(windows) w;
    if valid then result:=result||jsonb_build_object('first_hour',(model->>'first_hour')::integer,
     'interval_hours',(model->>'interval_hours')::integer,'weekly_windows',windows);end if;
   end if;
  end if;
 end if;
 return result;
end $$;

create or replace function app_private.collection_autofill_active(p_config jsonb,p_at timestamptz)
returns boolean language plpgsql immutable set search_path=pg_catalog,pg_temp as $$
declare wall timestamp:=p_at at time zone 'Asia/Jerusalem'; minute integer; start_min integer; end_min integer;
begin
 minute:=extract(hour from wall)::integer*60+extract(minute from wall)::integer;
 if p_config#>>'{active_hours,enabled}'='true' then
  start_min:=(p_config#>>'{active_hours,start}')::integer*60;end_min:=(p_config#>>'{active_hours,end}')::integer*60;
  if start_min<end_min and not(minute>=start_min and minute<end_min) then return false;end if;
  if start_min>end_min and not(minute>=start_min or minute<end_min) then return false;end if;
  -- Equal endpoints deliberately mean all hours.
 end if;
 if p_config ? 'weekly_windows' then
  return exists(select 1 from jsonb_array_elements(p_config->'weekly_windows') w
   where (w->>'day')::integer=extract(dow from wall)::integer
    and minute>=(w->>'start_minute')::integer and minute<(w->>'end_minute')::integer);
 end if;
 return true;
end $$;

create or replace function app_private.collection_autofill_next_slot(p_config jsonb,p_after timestamptz)
returns timestamptz language sql immutable set search_path=pg_catalog,pg_temp as $$
 select coalesce(min(instant),'infinity'::timestamptz) from (
  select wall,wall at time zone 'Asia/Jerusalem' as instant from (
   select ((p_after at time zone 'Asia/Jerusalem')::date+d)::timestamp
    +((p_config->>'first_hour')::integer+n*(p_config->>'interval_hours')::integer)*interval '1 hour' as wall
   from generate_series(-1,8) d cross join generate_series(0,23/(p_config->>'interval_hours')::integer) n
  ) local_slots
 ) slots where instant>p_after and instant at time zone 'Asia/Jerusalem'=wall
  and app_private.collection_autofill_active(p_config,instant);
 -- A spring gap is skipped; an autumn repeated hour resolves once to standard time.
$$;

create or replace function app_private.collection_autofill_cycle_start(p_config jsonb,p_at timestamptz)
returns timestamptz language sql immutable set search_path=pg_catalog,pg_temp as $$
 select (wall::date-case when extract(hour from wall)<(p_config->>'first_hour')::integer then 1 else 0 end
  +(p_config->>'first_hour')::integer*interval '1 hour') at time zone 'Asia/Jerusalem'
 from (select p_at at time zone 'Asia/Jerusalem' as wall) local_clock;
$$;

create or replace function app_private.collection_autofill_sync_clock(
 p_workspace uuid,p_sheet text,p_logical text,p_settings jsonb,p_now timestamptz default now()
) returns jsonb language plpgsql security definer set search_path=pg_catalog,public,app_private,pg_temp as $$
declare cfg jsonb:=app_private.collection_autofill_effective_schedule(p_workspace,p_sheet,p_logical,p_settings,p_now);
 fingerprint text:=md5(cfg::text);
begin
 insert into public.v2_collection_autofill_state(workspace_id,sheet_key,logical_key,next_due_at,calendar_schedule_key)
  values(p_workspace,p_sheet,p_logical,app_private.collection_autofill_next_slot(cfg,p_now),fingerprint)
 on conflict(workspace_id,sheet_key,logical_key) do update
  set next_due_at=excluded.next_due_at,calendar_schedule_key=excluded.calendar_schedule_key
  where v2_collection_autofill_state.calendar_schedule_key is distinct from excluded.calendar_schedule_key;
 return cfg;
end $$;

create or replace function app_private.collection_autofill_reenabled()
returns trigger language plpgsql security definer set search_path=pg_catalog,public,pg_temp as $$
begin
 if new.settings#>>'{autofill,enabled}'='true' and old.settings#>>'{autofill,enabled}' is distinct from 'true' then
  update public.v2_collection_autofill_state state set calendar_schedule_key=null
   from public.v2_template_sections sec,public.v2_collection_sessions s
   where sec.id=new.section_id and s.template_id=sec.template_id
    and state.workspace_id=s.workspace_id and state.sheet_key=public.v2_collection_sheet_key(to_jsonb(sec))
    and state.logical_key=public.v2_collection_field_logical_key(to_jsonb(new));
 end if;
 return new;
end $$;
drop trigger if exists collection_autofill_reenabled on public.v2_template_fields;
create trigger collection_autofill_reenabled after update of settings on public.v2_template_fields
for each row execute function app_private.collection_autofill_reenabled();

create or replace function public.v2_collection_autofill_due(p_limit integer default 250)
returns table(field_id uuid,session_id uuid) language plpgsql security definer
set search_path=pg_catalog,public,app_private,pg_temp as $$
declare target record; cfg jsonb;
begin
 -- Lazy synchronization also covers new fields/templates and learned-profile
 -- refresh/expiry. An unchanged effective schedule never pushes its clock forward.
 for target in
  select w.id as workspace_id,f.settings,public.v2_collection_sheet_key(to_jsonb(sec)) as sheet,
   public.v2_collection_field_logical_key(to_jsonb(f)) as logical
  from public.v2_workspaces w
  join lateral(select c.* from public.v2_collection_sessions c where c.workspace_id=w.id and c.deleted_at is null
   and c.status<>'archived' order by c.created_at desc,c.id desc limit 1) s on true
  join public.v2_template_sections sec on sec.template_id=s.template_id and sec.active
  join public.v2_template_fields f on f.section_id=sec.id and f.active
  where f.settings#>>'{autofill,enabled}'='true'
 loop
  cfg:=app_private.collection_autofill_sync_clock(target.workspace_id,target.sheet,target.logical,target.settings);
  update public.v2_collection_autofill_state state
   set next_due_at=app_private.collection_autofill_next_slot(cfg,now())
   where state.workspace_id=target.workspace_id and state.sheet_key=target.sheet and state.logical_key=target.logical
    and state.next_due_at<=now() and coalesce(state.lease_until,'-infinity')<=now()
    and (not app_private.collection_autofill_active(cfg,now())
     or state.next_due_at<app_private.collection_autofill_cycle_start(cfg,now()));
 end loop;
 return query select f.id,s.id from public.v2_workspaces w
 join lateral(select c.* from public.v2_collection_sessions c where c.workspace_id=w.id and c.deleted_at is null
  and c.status<>'archived' order by c.created_at desc,c.id desc limit 1) s on true
 join public.v2_template_sections sec on sec.template_id=s.template_id and sec.active
 join public.v2_template_fields f on f.section_id=sec.id and f.active
 join public.v2_collection_autofill_state state on state.workspace_id=w.id
  and state.sheet_key=public.v2_collection_sheet_key(to_jsonb(sec))
  and state.logical_key=public.v2_collection_field_logical_key(to_jsonb(f))
 where f.settings#>>'{autofill,enabled}'='true' and state.next_due_at<=now()
  and coalesce(state.lease_until,'-infinity')<=now()
 order by state.next_due_at,f.id limit least(greatest(p_limit,1),500);
end $$;

CREATE OR REPLACE FUNCTION public.v2_collection_autofill_claim(p_field_id uuid, p_session_id uuid, p_manual boolean DEFAULT false)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
 declare f public.v2_template_fields; sec public.v2_template_sections; s public.v2_collection_sessions;
 state public.v2_collection_autofill_state; rid uuid; logical text; sheet text; hours integer; target_session_id uuid; baseline jsonb; rev bigint;
begin
 select * into f from public.v2_template_fields where id=p_field_id and active;
 select * into sec from public.v2_template_sections where id=f.section_id and active;
 select * into s from public.v2_collection_sessions where id=p_session_id and deleted_at is null and status<>'archived';
 if s.id is null or sec.template_id is distinct from s.template_id or f.settings#>>'{autofill,enabled}' is distinct from 'true' then
 raise exception 'Active automation and a matching collection are required' using errcode='23514';end if;
 logical:=public.v2_collection_field_logical_key(to_jsonb(f));sheet:=public.v2_collection_sheet_key(to_jsonb(sec));
 perform 1 from public.v2_workspaces where id=s.workspace_id for update;
 -- A queued automatic call must re-read the current editor settings and prove
 -- eligibility before rollover can create a worksheet. Manual selection keeps
 -- the released contract and never passes through this automatic-only gate.
 if not p_manual then
 select * into f from public.v2_template_fields where id=p_field_id and active for share;
 select * into sec from public.v2_template_sections where id=f.section_id and active for share;
 select * into s from public.v2_collection_sessions where id=p_session_id and deleted_at is null and status<>'archived' for share;
 if s.id is null or sec.template_id is distinct from s.template_id or f.settings#>>'{autofill,enabled}' is distinct from 'true' then
 raise exception 'Active automation and a matching collection are required' using errcode='23514';end if;
 logical:=public.v2_collection_field_logical_key(to_jsonb(f));sheet:=public.v2_collection_sheet_key(to_jsonb(sec));
 perform app_private.collection_autofill_sync_clock(s.workspace_id,sheet,logical,f.settings);
 select * into state from public.v2_collection_autofill_state where workspace_id=s.workspace_id and sheet_key=sheet and logical_key=logical for update;
 if state.lease_until>now() then return jsonb_build_object('run_id',state.lease_id,'status','running');end if;
 if state.next_due_at>now() or not app_private.collection_autofill_active(app_private.collection_autofill_effective_schedule(s.workspace_id,sheet,logical,f.settings),now())
 or state.next_due_at<app_private.collection_autofill_cycle_start(app_private.collection_autofill_effective_schedule(s.workspace_id,sheet,logical,f.settings),now()) then return null;end if;
 end if;
 -- Scheduled news follows rollover; manual scans keep the selected session.
 if sheet='news_macro' and not p_manual then
 -- Resolve the volatile creator before reading its new row: a single SELECT
 -- cannot see rows inserted by its WHERE expression under that command snapshot.
 target_session_id:=public.v2_collection_autofill_target(s.workspace_id,s.template_id);
 select * into s from public.v2_collection_sessions where id=target_session_id;
 select target.* into f from public.v2_template_sections target_sec join public.v2_template_fields target on target.section_id=target_sec.id
 where target_sec.template_id=s.template_id and target_sec.active and target.active
 and public.v2_collection_sheet_key(to_jsonb(target_sec))=sheet and public.v2_collection_field_logical_key(to_jsonb(target))=logical limit 1;
 select * into sec from public.v2_template_sections where id=f.section_id;
 if f.id is null or f.settings#>>'{autofill,enabled}' is distinct from 'true' then
 raise exception 'The latest news field must have active automation' using errcode='23514';end if;
 end if;
 select editor_revision into rev from public.v2_templates where id=s.template_id;
 perform app_private.collection_autofill_sync_clock(s.workspace_id,sheet,logical,f.settings);
 select * into state from public.v2_collection_autofill_state where workspace_id=s.workspace_id and sheet_key=sheet and logical_key=logical for update;
 if state.lease_until>now() then return jsonb_build_object('run_id',state.lease_id,'status','running');end if;
 if state.lease_id is not null then update public.v2_collection_autofill_runs set status='failed',message='הסריקה הקודמת חרגה מזמן ההמתנה.',finished_at=now() where id=state.lease_id and status='running';end if;
 if not p_manual and (state.next_due_at>now() or not app_private.collection_autofill_active(app_private.collection_autofill_effective_schedule(s.workspace_id,sheet,logical,f.settings),now())
 or state.next_due_at<app_private.collection_autofill_cycle_start(app_private.collection_autofill_effective_schedule(s.workspace_id,sheet,logical,f.settings),now())) then return null;end if;
 if sheet='data' then select l.value into baseline from public.v2_collection_live_values l where l.workspace_id=s.workspace_id and l.logical_key=logical;
 else select v.value into baseline from public.v2_collection_values v where v.session_id=s.id and v.field_id=f.id;end if;
 baseline:=coalesce(baseline,'{}');
 insert into public.v2_collection_autofill_runs(workspace_id,field_id,sheet_key,logical_key,manual,baseline,template_revision,session_id,baseline_version)
 values(s.workspace_id,f.id,sheet,logical,p_manual,baseline,rev,s.id,coalesce((select max(version) from public.v2_collection_field_activity
 where workspace_id=s.workspace_id and sheet_key=sheet and logical_key=logical and (sheet='data' or session_id=s.id)),0)) returning id into rid;
 hours:=case when coalesce(f.settings#>>'{autofill,interval_hours}','')~'^(?:[1-9]|1[0-9]|2[0-4])$'
 then (f.settings#>>'{autofill,interval_hours}')::integer else 24 end;
 update public.v2_collection_autofill_state set lease_id=rid,lease_until=now()+interval '20 minutes',
 next_due_at=case when p_manual then next_due_at else app_private.collection_autofill_next_slot(app_private.collection_autofill_effective_schedule(s.workspace_id,sheet,logical,f.settings),now()) end
 where workspace_id=s.workspace_id and sheet_key=sheet and logical_key=logical;
 return jsonb_build_object('run_id',rid,'status','claimed','field',to_jsonb(f),'section',to_jsonb(sec),'session',to_jsonb(s),
 'baseline',baseline,'source_cursors',state.source_cursors,'last_scanned_at',state.last_scanned_at);
end $function$;


-- Keep the clear capability, stale-editor protection and lease cancellation;
-- only replace its old elapsed-hours postponement with the next calendar slot.
do $patch_clear$
declare fn regprocedure:='public.v2_clear_collection_values(uuid,text,uuid)'::regprocedure; definition text;
 old_clause text:='values(session_row.workspace_id,p_sheet_key,field_row.logical_key,now()+interval_hours*interval ''1 hour'')';
 new_clause text:='values(session_row.workspace_id,p_sheet_key,field_row.logical_key,app_private.collection_autofill_next_slot(app_private.collection_autofill_effective_schedule(session_row.workspace_id,p_sheet_key,field_row.logical_key,field_row.settings),now()))';
begin
 definition:=pg_get_functiondef(fn);
 if position(old_clause in definition)>0 then
  definition:=replace(definition,old_clause,new_clause);
  definition:=replace(definition,'next_due_at=greatest(v2_collection_autofill_state.next_due_at,excluded.next_due_at)',
   'next_due_at=excluded.next_due_at');
  execute definition;
 elsif position(new_clause in definition)=0 then raise exception 'Unexpected collection clear schedule contract';end if;
end $patch_clear$;

revoke all on function app_private.collection_autofill_effective_schedule(uuid,text,text,jsonb,timestamptz),
 app_private.collection_autofill_active(jsonb,timestamptz),app_private.collection_autofill_next_slot(jsonb,timestamptz),
 app_private.collection_autofill_cycle_start(jsonb,timestamptz),app_private.collection_autofill_sync_clock(uuid,text,text,jsonb,timestamptz),
 app_private.collection_autofill_reenabled() from public,anon,authenticated,service_role;
revoke all on function public.v2_collection_autofill_due(integer),public.v2_collection_autofill_claim(uuid,uuid,boolean) from public,anon,authenticated;
grant execute on function public.v2_collection_autofill_due(integer),public.v2_collection_autofill_claim(uuid,uuid,boolean) to service_role;

-- Initialize existing active clocks once. State identity, source cursors, leases,
-- previous answers and manual edits are untouched.
select count(app_private.collection_autofill_sync_clock(w.id,public.v2_collection_sheet_key(to_jsonb(sec)),
 public.v2_collection_field_logical_key(to_jsonb(f)),f.settings))
from public.v2_workspaces w
join lateral(select c.* from public.v2_collection_sessions c where c.workspace_id=w.id and c.deleted_at is null
 and c.status<>'archived' order by c.created_at desc,c.id desc limit 1) s on true
join public.v2_template_sections sec on sec.template_id=s.template_id and sec.active
join public.v2_template_fields f on f.section_id=sec.id and f.active
where f.settings#>>'{autofill,enabled}'='true';
notify pgrst,'reload schema';
commit;
