-- Versioned generated snapshots replace only their generated portion. Human
-- notes are merged from the current locked row, including permanent live data.
-- No content, workspace membership, lease or cadence is migrated here.

create or replace function public.v2_collection_managed_snapshot_valid(
 p_value jsonb, p_workspace_id uuid, p_run_id uuid default null
) returns boolean language plpgsql immutable set search_path=public,pg_temp as $function$
declare snapshot jsonb; image_name text; parts text[];
begin
 if jsonb_typeof(p_value) is distinct from 'object' then return false;end if;
 snapshot:=p_value->'_autofill_snapshot';
 if jsonb_typeof(snapshot) is distinct from 'object'
 or snapshot->'version' is distinct from '1'::jsonb
 or snapshot->>'kind' not in ('btc_liquidity_destination','hyperliquid_liquidation_spikes')
 or jsonb_typeof(snapshot->'kind') is distinct from 'string'
 or jsonb_typeof(snapshot->'generated_html') is distinct from 'string'
 or jsonb_typeof(snapshot->'manual_html') is distinct from 'string' then return false;end if;
 if snapshot ? 'image_path' then
  if jsonb_typeof(snapshot->'image_path') is distinct from 'string'
  or left(snapshot->>'image_path',17)<>'workspace-images:' then return false;end if;
  image_name:=substr(snapshot->>'image_path',18);
  parts:=string_to_array(image_name,'/');
  if public.v2_image_workspace(image_name) is distinct from p_workspace_id
  or cardinality(parts)<>4 or parts[2]<>'collection-autofill'
  or parts[4] !~ '^[A-Za-z0-9][A-Za-z0-9._-]*$'
  or (p_run_id is not null and parts[3]<>p_run_id::text) then return false;end if;
 end if;
 return true;
end $function$;

create or replace function public.v2_collection_managed_snapshot_merge(
 p_current jsonb, p_incoming jsonb, p_workspace_id uuid, p_run_id uuid default null
) returns jsonb language plpgsql immutable set search_path=public,pg_temp as $function$
declare current_snapshot jsonb; incoming_snapshot jsonb; next_snapshot jsonb;
 next_value jsonb; editable_value jsonb; manual_html text; combined_html text;
begin
 p_current:=coalesce(p_current,'{}'::jsonb);
 if jsonb_typeof(p_current) is distinct from 'object'
 or jsonb_typeof(p_incoming) is distinct from 'object' then
  raise exception 'Invalid collection snapshot value' using errcode='23514';
 end if;
 current_snapshot:=p_current->'_autofill_snapshot';
 incoming_snapshot:=p_incoming->'_autofill_snapshot';

 if p_run_id is null and not (p_current ? '_autofill_snapshot') then
  -- An ordinary editor may still save text, but only the server starts managed
  -- snapshots. Ignore a forged marker instead of changing ordinary save rules.
  return p_incoming-'_autofill_snapshot';
 end if;
 if p_current ? '_autofill_snapshot' and
 not public.v2_collection_managed_snapshot_valid(p_current,p_workspace_id) then
  raise exception 'Unsupported current collection snapshot; reload or review it' using errcode='23514';
 end if;

 if p_run_id is null then
  -- A stale editor owns only manual_html. Its old generated content, image,
  -- chart, provenance and existing snapshot metadata cannot replace newer data.
  if jsonb_typeof(incoming_snapshot) is distinct from 'object'
  or incoming_snapshot->'version' is distinct from '1'::jsonb
  or incoming_snapshot->'kind' is distinct from current_snapshot->'kind'
  or jsonb_typeof(incoming_snapshot->'manual_html') is distinct from 'string' then
   raise exception 'Invalid manual collection snapshot; reload before saving' using errcode='23514';
  end if;
  -- Ordinary user attachments, evidence and completion flags retain their
  -- usual last-writer behavior. Only generated aliases/provenance are guarded.
  select coalesce(jsonb_object_agg(key,value),'{}'::jsonb) into editable_value
  from jsonb_each(p_incoming) where key not in ('text','html','text_value','image_path','chart')
  and left(key,9)<>'_autofill';
  next_value:=p_current||editable_value;
  next_snapshot:=(incoming_snapshot-array['version','kind','generated_html','manual_html','image_path','chart'])
   ||current_snapshot||jsonb_build_object('manual_html',incoming_snapshot->'manual_html');
 else
  if not public.v2_collection_managed_snapshot_valid(p_incoming,p_workspace_id,p_run_id) then
   raise exception 'Invalid generated collection snapshot or image scope' using errcode='23514';
  end if;
  if p_current ? '_autofill_snapshot' and incoming_snapshot->'kind' is distinct from current_snapshot->'kind' then
   raise exception 'Collection snapshot kind changed' using errcode='23514';
  end if;
  -- Unclassified legacy text remains manual; it is never guessed to be AI.
  if p_current ? '_autofill_snapshot' then manual_html:=current_snapshot->>'manual_html';
  elsif jsonb_typeof(p_current->'text_value')='string' then manual_html:=p_current->>'text_value';
  elsif jsonb_typeof(p_current->'html')='string' then manual_html:=p_current->>'html';
  elsif jsonb_typeof(p_current->'text')='string' then manual_html:=p_current->>'text';
  else manual_html:='';end if;
  next_value:=p_current||p_incoming;
  -- Omitted optional generated assets clear the previous generated assets.
  next_snapshot:=(coalesce(current_snapshot,'{}'::jsonb)-'image_path'-'chart')
   ||incoming_snapshot||jsonb_build_object('manual_html',manual_html);
 end if;
 combined_html:=concat_ws(E'\n',nullif(next_snapshot->>'generated_html',''),nullif(next_snapshot->>'manual_html',''));
 next_value:=next_value||jsonb_build_object('_autofill_snapshot',next_snapshot,'text',combined_html);
 if p_current ? 'html' or p_incoming ? 'html' then next_value:=next_value||jsonb_build_object('html',combined_html);end if;
 if p_current ? 'text_value' or p_incoming ? 'text_value' then next_value:=next_value||jsonb_build_object('text_value',combined_html);end if;
 return next_value;
end $function$;
revoke all on function public.v2_collection_managed_snapshot_valid(jsonb,uuid,uuid),
 public.v2_collection_managed_snapshot_merge(jsonb,jsonb,uuid,uuid) from public,anon,authenticated;
grant execute on function public.v2_collection_managed_snapshot_valid(jsonb,uuid,uuid),
 public.v2_collection_managed_snapshot_merge(jsonb,jsonb,uuid,uuid) to service_role;

-- Patch reviewed anchors in the installed definitions and verify exact RPC
-- identity, owner, grants, security mode and configuration remain unchanged.
do $patch$
declare signature text; function_oid oid; definition text; original_definition text;
 before_contract jsonb; after_contract jsonb; pair jsonb; old_text text; new_text text;
 old_count integer; new_count integer; changes jsonb;
begin
 foreach signature in array array[
  'public.v2_collection_autofill_apply(uuid,jsonb,jsonb,timestamptz)',
  'public.v2_save_collection_value(uuid,uuid,jsonb)'
 ] loop
  function_oid:=to_regprocedure(signature);
  if function_oid is null then raise exception 'Existing snapshot RPC required: %',signature;end if;
  select pg_get_functiondef(p.oid),jsonb_build_object('oid',p.oid,'owner',p.proowner,'acl',p.proacl,
   'signature',p.oid::regprocedure::text,'security_definer',p.prosecdef,'volatility',p.provolatile,
   'settings',p.proconfig,'return_type',p.prorettype) into original_definition,before_contract
  from pg_proc p where p.oid=function_oid;
  definition:=original_definition;
  if signature like '%_autofill_apply(%' then
   changes:=jsonb_build_array(
    jsonb_build_array('replace_value boolean; rev bigint; final_status text;',
     'replace_value boolean; rev bigint; final_status text; managed_snapshot boolean:=false;'),
    jsonb_build_array(' if replace_value and (current_value is distinct from run.baseline or exists',
     $replacement$ -- Managed snapshot v1: only generated content changes; manual content is merged from the locked row.
 managed_snapshot:=coalesce((p_payload->'value') ? '_autofill_snapshot',false);
 if current_value ? '_autofill_snapshot' and p_payload ? 'value' and not managed_snapshot then
 raise exception 'Managed collection snapshot requires a versioned generated payload' using errcode='23514';end if;
 if managed_snapshot then
 if f.field_type in ('news_table','news_source_table','unbiased_news') or not public.v2_collection_managed_snapshot_valid(p_payload->'value',run.workspace_id,run.id) then
 raise exception 'Invalid generated collection snapshot or image scope' using errcode='23514';end if;
 -- Bind the capability to the exact target already selected by scheduled rollover.
 -- This transaction rolls the binding back if a later validation/write fails.
 if sid is distinct from run.session_id then
 update public.v2_collection_autofill_runs set session_id=sid where id=run.id;
 end if;
 end if;
 if replace_value and not managed_snapshot and (current_value is distinct from run.baseline or exists$replacement$),
    jsonb_build_array(' next_value:=current_value||(p_payload->''value'');',
     $replacement$ if managed_snapshot then
 next_value:=public.v2_collection_managed_snapshot_merge(current_value,p_payload->'value',run.workspace_id,run.id);
 else
 next_value:=current_value||(p_payload->'value');$replacement$),
    jsonb_build_array(' amount:=1;'||E'\n'||' else'||E'\n'||' next_value:=current_value||jsonb_build_object(''text'',case when replace_value',
     ' end if;'||E'\n'||' amount:=1;'||E'\n'||' else'||E'\n'||' next_value:=current_value||jsonb_build_object(''text'',case when replace_value')
   );
  else
   changes:=jsonb_build_array(
    jsonb_build_array('declare ws uuid;', 'declare ws uuid; current_value jsonb; selected_sheet text; selected_logical text;'),
    jsonb_build_array(' perform 1 from public.v2_collection_sessions where id=p_session_id for update;',
     $replacement$ perform 1 from public.v2_collection_sessions where id=p_session_id for update;
 -- Managed snapshot v1: permanent data reads its latest workspace logical key.
 select public.v2_collection_sheet_key(to_jsonb(sec)),public.v2_collection_field_logical_key(to_jsonb(f))
 into selected_sheet,selected_logical from public.v2_collection_sessions s
 join public.v2_template_sections sec on sec.template_id=s.template_id
 join public.v2_template_fields f on f.section_id=sec.id and f.id=p_field_id where s.id=p_session_id;
 if selected_sheet='data' then
 select value into current_value from public.v2_collection_live_values where workspace_id=ws and logical_key=selected_logical for update;
 end if;
 if current_value is null then
 select value into current_value from public.v2_collection_values where session_id=p_session_id and field_id=p_field_id for update;
 end if;
 p_value:=public.v2_collection_managed_snapshot_merge(coalesce(current_value,'{}'::jsonb),p_value,ws);$replacement$)
   );
  end if;
  for pair in select value from jsonb_array_elements(changes) loop
   old_text:=pair->>0;new_text:=pair->>1;
   new_count:=(length(definition)-length(replace(definition,new_text,'')))/length(new_text);
   -- Some replacements deliberately contain their old anchor. Remove the exact
   -- new body before counting so repeat applications are genuinely idempotent.
   old_count:=(length(replace(definition,new_text,''))-length(replace(replace(definition,new_text,''),old_text,'')))/length(old_text);
   if new_count=0 and old_count=1 then definition:=replace(definition,old_text,new_text);
   elsif new_count=1 and old_count=0 then null;
   else raise exception 'Unexpected managed snapshot anchor in % (old %, new %)',signature,old_count,new_count;end if;
  end loop;
  if definition is distinct from original_definition then execute definition;end if;
  select jsonb_build_object('oid',p.oid,'owner',p.proowner,'acl',p.proacl,
   'signature',p.oid::regprocedure::text,'security_definer',p.prosecdef,'volatility',p.provolatile,
   'settings',p.proconfig,'return_type',p.prorettype) into after_contract from pg_proc p where p.oid=function_oid;
  if before_contract is distinct from after_contract or pg_get_functiondef(function_oid) is distinct from definition then
   raise exception 'Snapshot migration changed RPC identity, permissions or unreviewed body';end if;
 end loop;
end $patch$;

-- Direct PostgREST writes have the same managed-content boundary as the save
-- RPC. The live table remains read-only to clients and receives this merged row
-- through the existing mirror trigger. No table grants or RLS policies change.
create or replace function public.v2_collection_managed_snapshot_write_guard()
returns trigger language plpgsql security definer set search_path=public,pg_temp as $function$
declare ws uuid; selected_sheet text; selected_logical text; current_value jsonb;
 transaction_run uuid; trusted_run uuid; revision bigint; automation_enabled boolean;
begin
 if tg_op='UPDATE' and old.value ? '_autofill_snapshot'
 and (new.session_id is distinct from old.session_id or new.field_id is distinct from old.field_id) then
  raise exception 'Managed collection snapshot identity cannot change' using errcode='23514';
 end if;
 select s.workspace_id,public.v2_collection_sheet_key(to_jsonb(sec)),
 public.v2_collection_field_logical_key(to_jsonb(f)),t.editor_revision,
 sec.active and f.active and f.settings#>>'{autofill,enabled}'='true'
 into ws,selected_sheet,selected_logical,revision,automation_enabled
 from public.v2_collection_sessions s join public.v2_templates t on t.id=s.template_id
 join public.v2_template_sections sec on sec.template_id=t.id
 join public.v2_template_fields f on f.section_id=sec.id and f.id=new.field_id where s.id=new.session_id;
 if ws is null then return new;end if; -- The existing field guard rejects invalid targets.
 perform 1 from public.v2_workspaces where id=ws for update;
 select token.run_id into transaction_run from public.v2_collection_autofill_write_tokens token
 where token.transaction_id=txid_current();
 if transaction_run is not null then
  select r.id into trusted_run from public.v2_collection_autofill_runs r
  join public.v2_collection_autofill_state state on state.workspace_id=r.workspace_id
   and state.sheet_key=r.sheet_key and state.logical_key=r.logical_key
  where r.id=transaction_run and r.status='running' and r.workspace_id=ws
   and r.session_id=new.session_id and r.field_id=new.field_id
   and r.sheet_key=selected_sheet and r.logical_key=selected_logical
   and r.template_revision=revision and automation_enabled
   and state.lease_id=r.id and state.lease_until>=now();
 end if;
 if trusted_run is null and auth.uid() is not null and not coalesce(public.v2_can_edit_workspace(ws),false) then
  raise exception 'Collection edit permission required' using errcode='42501';
 end if;
 if selected_sheet='data' then
  select value into current_value from public.v2_collection_live_values
  where workspace_id=ws and logical_key=selected_logical for update;
 end if;
 if current_value is null then
  if tg_op='UPDATE' and new.session_id=old.session_id and new.field_id=old.field_id then current_value:=old.value;
  else select value into current_value from public.v2_collection_values
   where session_id=new.session_id and field_id=new.field_id for update;end if;
 end if;
 current_value:=coalesce(current_value,'{}'::jsonb);
 if not (current_value ? '_autofill_snapshot') and not (new.value ? '_autofill_snapshot') then return new;end if;
 if transaction_run is not null and trusted_run is null then
  raise exception 'Managed collection snapshot token does not match target' using errcode='23514';
 end if;
 if trusted_run is null and not (current_value ? '_autofill_snapshot') then
  new.value:=new.value-'_autofill_snapshot'; -- An ordinary editor cannot initiate a snapshot.
 else
  new.value:=public.v2_collection_managed_snapshot_merge(current_value,new.value,ws,trusted_run);
 end if;
 return new;
end $function$;
revoke all on function public.v2_collection_managed_snapshot_write_guard() from public,anon,authenticated;
drop trigger if exists v2_collection_values_snapshot_guard on public.v2_collection_values;
create trigger v2_collection_values_snapshot_guard before insert or update on public.v2_collection_values
 for each row execute function public.v2_collection_managed_snapshot_write_guard();
notify pgrst,'reload schema';
