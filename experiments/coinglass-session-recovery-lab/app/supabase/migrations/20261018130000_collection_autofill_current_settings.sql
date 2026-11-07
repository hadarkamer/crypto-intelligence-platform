begin;

-- Worksheet answers retain their original field/template identity. Automation
-- cadence is operational configuration and follows the current saved default.
-- Only these three scheduling properties are read through; links, instructions,
-- parser contracts and existing answers are never copied or rewritten.
alter table public.v2_collection_autofill_runs add column if not exists automation_config_key text;

create or replace function app_private.collection_autofill_current_config(p_field_id uuid,p_session_id uuid)
returns jsonb language plpgsql stable security definer
set search_path=pg_catalog,public,app_private,pg_temp as $$
declare f public.v2_template_fields; sec public.v2_template_sections; original public.v2_templates;
 s public.v2_collection_sessions; authority public.v2_templates; canonical public.v2_template_fields;
 canonical_sec public.v2_template_sections; matches integer; settings jsonb; scheduling jsonb;
 sheet text; logical text; reason text; fingerprint text;
begin
 select * into f from public.v2_template_fields where id=p_field_id;
 select * into sec from public.v2_template_sections where id=f.section_id;
 select * into original from public.v2_templates where id=sec.template_id and kind='collection';
 select * into s from public.v2_collection_sessions where id=p_session_id;
 if s.id is null or original.id is null or s.template_id is distinct from original.id
  or s.workspace_id is distinct from original.workspace_id or s.deleted_at is not null or s.status='archived'
  or not coalesce(f.active and sec.active,false) then
  return jsonb_build_object('enabled',false,'reason','inactive_target');
 end if;
 sheet:=public.v2_collection_sheet_key(to_jsonb(sec));logical:=public.v2_collection_field_logical_key(to_jsonb(f));
 select * into authority from public.v2_templates t where t.workspace_id=s.workspace_id
  and t.kind='collection' and t.active and t.is_default and t.asset_symbol is not distinct from original.asset_symbol;
 -- Legacy/custom workspaces with no active default retain their explicit
 -- worksheet configuration. An existing default with a missing field cannot.
 if authority.id is null then
  if not original.active then return jsonb_build_object('enabled',false,'reason','inactive_original_template');end if;
  authority:=original;
 end if;
 select count(*) into matches from public.v2_template_fields candidate
  join public.v2_template_sections candidate_sec on candidate_sec.id=candidate.section_id
  where candidate_sec.template_id=authority.id
   and public.v2_collection_sheet_key(to_jsonb(candidate_sec))=sheet
   and public.v2_collection_field_logical_key(to_jsonb(candidate))=logical;
 if matches<>1 then return jsonb_build_object('enabled',false,
  'reason',case when matches=0 then 'missing_current_field' else 'ambiguous_current_field' end,
  'template_id',authority.id);end if;
 select candidate.* into canonical from public.v2_template_fields candidate
  join public.v2_template_sections candidate_sec on candidate_sec.id=candidate.section_id
  where candidate_sec.template_id=authority.id
   and public.v2_collection_sheet_key(to_jsonb(candidate_sec))=sheet
   and public.v2_collection_field_logical_key(to_jsonb(candidate))=logical;
 select * into canonical_sec from public.v2_template_sections where id=canonical.section_id;
 if not canonical.active or not canonical_sec.active then reason:='inactive_current_field';
 elsif exists(select 1 from jsonb_array_elements(coalesce(canonical_sec.settings->'groups',canonical_sec.settings->'legacy_groups','[]')) g
   where g->>'id'=coalesce(canonical.settings->>'group_id',canonical.settings->>'legacy_group_id') and g->>'active'='false')
  or exists(select 1 from jsonb_array_elements(coalesce(sec.settings->'groups',sec.settings->'legacy_groups','[]')) g
   where g->>'id'=coalesce(f.settings->>'group_id',f.settings->>'legacy_group_id') and g->>'active'='false') then reason:='inactive_group';
 elsif f.field_type is distinct from canonical.field_type and not(
   f.field_type in ('news_table','news_source_table','news_group','unbiased_news')
   and canonical.field_type in ('news_table','news_source_table','news_group','unbiased_news')) then reason:='incompatible_type';
 elsif app_private.collection_autofill_source_fingerprint(f.settings)
   is distinct from app_private.collection_autofill_source_fingerprint(canonical.settings) then reason:='source_changed';
 elsif canonical.settings#>>'{autofill,enabled}' is distinct from 'true' then reason:='disabled';
 end if;
 if reason is not null then return jsonb_build_object('enabled',false,'reason',reason,
  'template_id',authority.id,'field_id',canonical.id);end if;
 scheduling:=jsonb_build_object('enabled',true,
  'interval_hours',coalesce(canonical.settings#>'{autofill,interval_hours}','24'::jsonb),
  'schedule',coalesce(canonical.settings#>'{autofill,schedule}','{}'::jsonb));
 settings:=jsonb_set(f.settings,'{autofill}',
  (case when jsonb_typeof(f.settings->'autofill')='object' then f.settings->'autofill' else '{}'::jsonb end)||scheduling,true);
 fingerprint:=md5(jsonb_build_object('template_id',authority.id,'field_id',canonical.id,
  'target_field_id',f.id,'field_type',canonical.field_type,'scheduling',scheduling,
  'source',app_private.collection_autofill_source_fingerprint(canonical.settings))::text);
 return jsonb_build_object('enabled',true,'reason','ready','template_id',authority.id,
  'field_id',canonical.id,'config_key',fingerprint,'settings',settings);
end $$;

create or replace function app_private.collection_autofill_run_config_current(p_run_id uuid)
returns boolean language plpgsql stable security definer
set search_path=pg_catalog,public,app_private,pg_temp as $$
declare run public.v2_collection_autofill_runs; current_config jsonb;
begin
 select * into run from public.v2_collection_autofill_runs where id=p_run_id;
 if run.id is null or run.automation_config_key is null then return false;end if;
 current_config:=app_private.collection_autofill_current_config(run.field_id,run.session_id);
 return current_config->>'enabled'='true' and current_config->>'config_key'=run.automation_config_key;
end $$;

create or replace function public.v2_collection_autofill_due(p_limit integer default 250)
returns table(field_id uuid,session_id uuid) language plpgsql security definer
set search_path=pg_catalog,public,app_private,pg_temp as $$
declare target record; cfg jsonb;
begin
 for target in
  select w.id as workspace_id,runtime.config->'settings' as settings,
   public.v2_collection_sheet_key(to_jsonb(sec)) as sheet,public.v2_collection_field_logical_key(to_jsonb(f)) as logical
  from public.v2_workspaces w
  join lateral(select c.* from public.v2_collection_sessions c where c.workspace_id=w.id and c.deleted_at is null
   and c.status<>'archived' order by c.created_at desc,c.id desc limit 1) s on true
  join public.v2_template_sections sec on sec.template_id=s.template_id and sec.active
  join public.v2_template_fields f on f.section_id=sec.id and f.active
  cross join lateral(select app_private.collection_autofill_current_config(f.id,s.id) config) runtime
  where runtime.config->>'enabled'='true'
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
 cross join lateral(select app_private.collection_autofill_current_config(f.id,s.id) config) runtime
 join public.v2_collection_autofill_state state on state.workspace_id=w.id
  and state.sheet_key=public.v2_collection_sheet_key(to_jsonb(sec))
  and state.logical_key=public.v2_collection_field_logical_key(to_jsonb(f))
 where runtime.config->>'enabled'='true' and state.next_due_at<=now()
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
 state public.v2_collection_autofill_state; rid uuid; logical text; sheet text; hours integer; target_session_id uuid; baseline jsonb; rev bigint; runtime_config jsonb;
begin
 select * into f from public.v2_template_fields where id=p_field_id and active;
 select * into sec from public.v2_template_sections where id=f.section_id and active;
 select * into s from public.v2_collection_sessions where id=p_session_id and deleted_at is null and status<>'archived';
 if s.id is null or sec.template_id is distinct from s.template_id then
 raise exception 'Active automation and a matching collection are required' using errcode='23514';end if;
 logical:=public.v2_collection_field_logical_key(to_jsonb(f));sheet:=public.v2_collection_sheet_key(to_jsonb(sec));
 perform pg_advisory_xact_lock(hashtextextended(s.workspace_id::text||'collection',0));
 perform 1 from public.v2_workspaces where id=s.workspace_id for update;
 select * into f from public.v2_template_fields where id=p_field_id and active for share;
 select * into sec from public.v2_template_sections where id=f.section_id and active for share;
 select * into s from public.v2_collection_sessions where id=p_session_id and deleted_at is null and status<>'archived' for share;
 if s.id is null or sec.template_id is distinct from s.template_id then
 raise exception 'Active automation and a matching collection are required' using errcode='23514';end if;
 runtime_config:=app_private.collection_autofill_current_config(f.id,s.id);
 if runtime_config->>'enabled' is distinct from 'true' then
  if p_manual then raise exception 'Current automation is unavailable: %',runtime_config->>'reason' using errcode='23514';end if;
  return null;
 end if;
 f.settings:=runtime_config->'settings';
 logical:=public.v2_collection_field_logical_key(to_jsonb(f));sheet:=public.v2_collection_sheet_key(to_jsonb(sec));
 -- A queued automatic call must re-read the current editor settings and prove
 -- eligibility before rollover can create a worksheet. Manual selection keeps
 -- the released contract and never passes through this automatic-only gate.
 if not p_manual then
 select * into f from public.v2_template_fields where id=p_field_id and active for share;
 select * into sec from public.v2_template_sections where id=f.section_id and active for share;
 select * into s from public.v2_collection_sessions where id=p_session_id and deleted_at is null and status<>'archived' for share;
 if s.id is null or sec.template_id is distinct from s.template_id then
 raise exception 'Active automation and a matching collection are required' using errcode='23514';end if;
 runtime_config:=app_private.collection_autofill_current_config(f.id,s.id);
 if runtime_config->>'enabled' is distinct from 'true' then
  if p_manual then raise exception 'Current automation is unavailable: %',runtime_config->>'reason' using errcode='23514';end if;
  return null;
 end if;
 f.settings:=runtime_config->'settings';
 logical:=public.v2_collection_field_logical_key(to_jsonb(f));sheet:=public.v2_collection_sheet_key(to_jsonb(sec));
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
 runtime_config:=app_private.collection_autofill_current_config(f.id,s.id);
 if f.id is null or runtime_config->>'enabled' is distinct from 'true' then
 raise exception 'The latest news field must have active automation' using errcode='23514';end if;
 f.settings:=runtime_config->'settings';
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
 insert into public.v2_collection_autofill_runs(workspace_id,field_id,sheet_key,logical_key,manual,baseline,template_revision,session_id,baseline_version,automation_config_key)
 values(s.workspace_id,f.id,sheet,logical,p_manual,baseline,rev,s.id,coalesce((select max(version) from public.v2_collection_field_activity
 where workspace_id=s.workspace_id and sheet_key=sheet and logical_key=logical and (sheet='data' or session_id=s.id)),0),runtime_config->>'config_key') returning id into rid;
 hours:=case when coalesce(f.settings#>>'{autofill,interval_hours}','')~'^(?:[1-9]|1[0-9]|2[0-4])$'
 then (f.settings#>>'{autofill,interval_hours}')::integer else 24 end;
 update public.v2_collection_autofill_state set lease_id=rid,lease_until=now()+interval '20 minutes',
 next_due_at=case when p_manual then next_due_at else app_private.collection_autofill_next_slot(app_private.collection_autofill_effective_schedule(s.workspace_id,sheet,logical,f.settings),now()) end
 where workspace_id=s.workspace_id and sheet_key=sheet and logical_key=logical;
 return jsonb_build_object('run_id',rid,'status','claimed','field',to_jsonb(f),'section',to_jsonb(sec),'session',to_jsonb(s),
 'baseline',baseline,'source_cursors',state.source_cursors,'last_scanned_at',state.last_scanned_at);
end $function$;

-- Patch the reviewed installed apply/managed-snapshot gates, retaining every
-- existing cursor, manual-edit, source-dedup, lease and permission check.
do $runtime_guards$
declare signature text; definition text; original text; needle text; replacement text;
 fn oid; before_contract jsonb; after_contract jsonb;
begin
 foreach signature in array array['public.v2_collection_autofill_apply(uuid,jsonb,jsonb,timestamptz)',
  'public.v2_collection_managed_snapshot_write_guard()',
  'public.v2_collection_autofill_apply_calendar(uuid,jsonb,jsonb,timestamptz)'] loop
  fn:=to_regprocedure(signature);
  select pg_get_functiondef(fn),jsonb_build_object('oid',p.oid,'owner',p.proowner,'acl',p.proacl,
   'security',p.prosecdef,'settings',p.proconfig,'result',p.prorettype,'volatility',p.provolatile)
   into definition,before_contract from pg_proc p where p.oid=fn;
  if definition is null then raise exception 'Required automation RPC is missing: %',signature;end if;
  if position('collection_autofill_current_settings_v1' in definition)>0 then continue;end if;
  original:=definition;
  if signature like '%autofill_apply_calendar(%' then
   needle:='perform 1 from public.v2_workspaces where id=run.workspace_id for update;';
   replacement:='perform pg_advisory_xact_lock(hashtextextended(run.workspace_id::text||''collection'',0));'||E'\n  '||needle;
  elsif signature like '%autofill_apply(%' then
   needle:=$old$or f.settings#>>'{autofill,enabled}' is distinct from 'true' then$old$;
   replacement:=$new$or app_private.collection_autofill_run_config_current(run.id) is distinct from true then$new$;
  else
   needle:='and r.template_revision=revision and automation_enabled';
   replacement:='and r.template_revision=revision and app_private.collection_autofill_run_config_current(r.id)';
  end if;
  if (length(definition)-length(replace(definition,needle,'')))/length(needle)<>1 then
   raise exception 'Unexpected automation validation anchor: %',signature;end if;
  definition:=replace(definition,needle,'/* collection_autofill_current_settings_v1 */ '||replacement);
  if signature like '%autofill_apply(%' then
   needle:='perform 1 from public.v2_workspaces where id=run.workspace_id for update;';
   if (length(definition)-length(replace(definition,needle,'')))/length(needle)<>1 then
    raise exception 'Unexpected apply configuration lock anchor';end if;
   definition:=replace(definition,needle,
    'perform pg_advisory_xact_lock(hashtextextended(run.workspace_id::text||''collection'',0));'||E'\n '||needle);
  end if;
  execute definition;
  select jsonb_build_object('oid',p.oid,'owner',p.proowner,'acl',p.proacl,
   'security',p.prosecdef,'settings',p.proconfig,'result',p.prorettype,'volatility',p.provolatile)
   into after_contract from pg_proc p where p.oid=fn;
  if before_contract is distinct from after_contract or pg_get_functiondef(fn) is distinct from definition then
   raise exception 'Runtime scheduling changed RPC security contract: %',signature;end if;
 end loop;
end $runtime_guards$;

revoke all on function app_private.collection_autofill_current_config(uuid,uuid),
 app_private.collection_autofill_run_config_current(uuid) from public,anon,authenticated,service_role;
revoke all on function public.v2_collection_autofill_due(integer),public.v2_collection_autofill_claim(uuid,uuid,boolean)
 from public,anon,authenticated;
grant execute on function public.v2_collection_autofill_due(integer),public.v2_collection_autofill_claim(uuid,uuid,boolean) to service_role;
notify pgrst,'reload schema';
commit;
