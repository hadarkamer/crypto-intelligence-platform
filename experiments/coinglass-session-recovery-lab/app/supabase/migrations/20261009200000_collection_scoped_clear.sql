-- Explicit clear actions blank the permanent worksheet, retain a recovery
-- snapshot and invalidate existing scans atomically. Ordinary editing cannot
-- use this capability to replace managed generated content.
create table if not exists public.v2_collection_clear_history (
 id uuid primary key default gen_random_uuid(),
 operation_id uuid not null,
 workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
 session_id uuid not null references public.v2_collection_sessions(id) on delete cascade,
 field_id uuid not null references public.v2_template_fields(id) on delete cascade,
 logical_key text not null,
 previous_value jsonb not null,
 actor_id uuid references auth.users(id) on delete set null,
 created_at timestamptz not null default now()
);
create index if not exists v2_collection_clear_history_workspace_idx
 on public.v2_collection_clear_history(workspace_id,created_at desc);
alter table public.v2_collection_clear_history enable row level security;
revoke all on public.v2_collection_clear_history from public,anon,authenticated;
grant select on public.v2_collection_clear_history to authenticated;
drop policy if exists v2_collection_clear_history_read on public.v2_collection_clear_history;
create policy v2_collection_clear_history_read on public.v2_collection_clear_history
 for select to authenticated using(public.v2_is_workspace_member(workspace_id));

create table if not exists public.v2_collection_clear_tokens (
 transaction_id bigint not null,
 workspace_id uuid not null,
 session_id uuid not null,
 field_id uuid not null,
 actor_id uuid not null,
 expected_value jsonb not null,
 primary key(transaction_id,field_id)
);
alter table public.v2_collection_clear_tokens enable row level security;
revoke all on public.v2_collection_clear_tokens from public,anon,authenticated,service_role;

-- Patch just the reviewed guard anchor, preserving its existing ACL/owner and
-- the exact scan/manual authorization contract outside explicit empty writes.
do $patch$
declare fn oid:=to_regprocedure('public.v2_collection_managed_snapshot_write_guard()');
 definition text; original_definition text; old_text text; new_text text;
 before_contract jsonb; after_contract jsonb;
begin
 if fn is null then raise exception 'Managed snapshot guard required';end if;
 select pg_get_functiondef(p.oid),jsonb_build_object('owner',p.proowner,'acl',p.proacl,
  'security',p.prosecdef,'settings',p.proconfig,'result',p.prorettype)
 into original_definition,before_contract from pg_proc p where p.oid=fn;
 definition:=original_definition;
 old_text:=' perform 1 from public.v2_workspaces where id=ws for update;';
 new_text:=old_text||$clear_guard$
 -- Only this transaction's authenticated, exact-field clear may drop a snapshot.
 if selected_sheet='data' and exists(
  select 1 from public.v2_collection_clear_tokens token
  where token.transaction_id=txid_current() and token.workspace_id=ws
   and token.session_id=new.session_id and token.field_id=new.field_id
   and token.actor_id=auth.uid() and token.expected_value=new.value
 ) then return new;end if;
$clear_guard$;
 if strpos(definition,new_text)=0 then
  if (length(definition)-length(replace(definition,old_text,'')))/length(old_text)<>1 then
   raise exception 'Unexpected clear guard anchor';end if;
  definition:=replace(definition,old_text,new_text);
  execute definition;
 end if;
 select jsonb_build_object('owner',p.proowner,'acl',p.proacl,'security',p.prosecdef,
  'settings',p.proconfig,'result',p.prorettype) into after_contract from pg_proc p where p.oid=fn;
 if before_contract is distinct from after_contract or pg_get_functiondef(fn) is distinct from definition then
  raise exception 'Clear guard altered existing RPC contract';end if;
end $patch$;

-- A pre-clear editor must reload instead of restoring removed notes/images.
-- Both the save RPC (which merges before its write) and direct table writes
-- enforce this generation, including ordinary non-managed answers.
do $generation_patch$
declare signature text; fn oid; definition text; original_definition text;
 old_text text; new_text text; before_contract jsonb; after_contract jsonb;
begin
 foreach signature in array array[
  'public.v2_collection_managed_snapshot_merge(jsonb,jsonb,uuid,uuid)',
  'public.v2_collection_managed_snapshot_write_guard()'
 ] loop
  fn:=to_regprocedure(signature);
  if fn is null then raise exception 'Clear generation prerequisite required';end if;
  select pg_get_functiondef(p.oid),jsonb_build_object('owner',p.proowner,'acl',p.proacl,
   'security',p.prosecdef,'settings',p.proconfig,'result',p.prorettype)
  into original_definition,before_contract from pg_proc p where p.oid=fn;
  definition:=original_definition;
  if signature like '%_merge(%' then
   old_text:=' current_snapshot:=p_current->''_autofill_snapshot'';';
   new_text:=$merge_generation$
 if p_run_id is null and p_current ? '_clear_generation'
 and p_incoming->'_clear_generation' is distinct from p_current->'_clear_generation' then
  raise exception 'התיבה נוקתה; יש לרענן לפני שמירה.' using errcode='23514';end if;
 p_incoming:=p_incoming-'_clear_generation';
 if p_current ? '_clear_generation' then
  p_incoming:=p_incoming||jsonb_build_object('_clear_generation',p_current->'_clear_generation');end if;
$merge_generation$||old_text;
  else
   old_text:=' current_value:=coalesce(current_value,''{}''::jsonb);';
   new_text:=old_text||$guard_generation$
 if trusted_run is null and current_value ? '_clear_generation'
 and new.value->'_clear_generation' is distinct from current_value->'_clear_generation' then
  raise exception 'התיבה נוקתה; יש לרענן לפני שמירה.' using errcode='23514';end if;
 if trusted_run is null and not (current_value ? '_clear_generation') then
  new.value:=new.value-'_clear_generation';end if;
$guard_generation$;
  end if;
  if strpos(definition,new_text)=0 then
   if (length(definition)-length(replace(definition,old_text,'')))/length(old_text)<>1 then
    raise exception 'Unexpected clear generation anchor in %',signature;end if;
   definition:=replace(definition,old_text,new_text);
   execute definition;
  end if;
  select jsonb_build_object('owner',p.proowner,'acl',p.proacl,'security',p.prosecdef,
   'settings',p.proconfig,'result',p.prorettype) into after_contract from pg_proc p where p.oid=fn;
  if before_contract is distinct from after_contract or pg_get_functiondef(fn) is distinct from definition then
   raise exception 'Clear generation altered existing RPC contract';end if;
 end loop;
end $generation_patch$;

create or replace function public.v2_clear_collection_values(
 p_session_id uuid,p_sheet_key text,p_field_id uuid default null
) returns integer language plpgsql security definer set search_path=public,pg_temp as $function$
declare session_row public.v2_collection_sessions; field_row record; current_value jsonb; cleared_value jsonb;
 operation uuid:=gen_random_uuid(); cleared integer:=0; interval_hours integer;
 locked_workspace uuid; locked_template uuid;
begin
 if auth.uid() is null or p_sheet_key is distinct from 'data' then
  raise exception 'Permanent collection edit required' using errcode='42501';end if;
 select * into session_row from public.v2_collection_sessions where id=p_session_id;
 if session_row.id is null or session_row.deleted_at is not null or session_row.status='archived'
 or not coalesce(public.v2_can_edit_workspace(session_row.workspace_id),false)
 or not exists(select 1 from public.v2_templates where id=session_row.template_id and kind='collection') then
  raise exception 'Collection edit permission required' using errcode='42501';end if;
 locked_workspace:=session_row.workspace_id;locked_template:=session_row.template_id;
 perform 1 from public.v2_workspaces where id=locked_workspace for update;
 select * into session_row from public.v2_collection_sessions where id=p_session_id for update;
 if session_row.id is null or session_row.deleted_at is not null or session_row.status='archived'
 or session_row.workspace_id is distinct from locked_workspace or session_row.template_id is distinct from locked_template
 or not exists(select 1 from public.v2_templates where id=session_row.template_id and kind='collection') then
  raise exception 'Active collection required' using errcode='23514';end if;
 if not coalesce(public.v2_can_edit_workspace(locked_workspace),false) then
  raise exception 'Collection edit permission required' using errcode='42501';end if;
 if not exists(select 1 from public.v2_template_sections sec join public.v2_template_fields f on f.section_id=sec.id
  where sec.template_id=session_row.template_id and sec.active and f.active
   and public.v2_collection_sheet_key(to_jsonb(sec))=p_sheet_key
   and f.field_type not in ('info','button','economic_calendar') and (p_field_id is null or f.id=p_field_id)) then
  raise exception 'Active field in the selected worksheet required' using errcode='23514';end if;
 for field_row in
  select f.*,public.v2_collection_field_logical_key(to_jsonb(f)) as logical_key
  from public.v2_template_sections sec join public.v2_template_fields f on f.section_id=sec.id
  where sec.template_id=session_row.template_id and sec.active and f.active
   and public.v2_collection_sheet_key(to_jsonb(sec))=p_sheet_key
   and f.field_type not in ('info','button','economic_calendar') and (p_field_id is null or f.id=p_field_id)
  order by sec.position,f.position,f.id
 loop
  -- A scan claimed before this clear can never put the removed content back.
  update public.v2_collection_autofill_runs set status='failed',finished_at=now(),message='התיבה נוקתה בזמן הסריקה.'
   where workspace_id=session_row.workspace_id and sheet_key=p_sheet_key
    and logical_key=field_row.logical_key and status='running';
  interval_hours:=case when coalesce(field_row.settings#>>'{autofill,interval_hours}','')~'^(?:[1-9]|1[0-9]|2[0-4])$'
   then (field_row.settings#>>'{autofill,interval_hours}')::integer else 24 end;
  insert into public.v2_collection_autofill_state(workspace_id,sheet_key,logical_key,next_due_at)
   values(session_row.workspace_id,p_sheet_key,field_row.logical_key,now()+interval_hours*interval '1 hour')
   on conflict(workspace_id,sheet_key,logical_key) do update set lease_id=null,lease_until=null,
    next_due_at=greatest(v2_collection_autofill_state.next_due_at,excluded.next_due_at);
  current_value:=null;
  select value into current_value from public.v2_collection_live_values
   where workspace_id=session_row.workspace_id and logical_key=field_row.logical_key for update;
  if current_value is null then select value into current_value from public.v2_collection_values
   where session_id=p_session_id and field_id=field_row.id for update;end if;
  -- Keep an empty managed marker so a stale editor cannot resurrect removed
  -- generated text or images through an ordinary manual save after clearing.
  cleared_value:=case when public.v2_collection_managed_snapshot_valid(current_value,session_row.workspace_id) then
   jsonb_build_object('text','','_autofill_snapshot',jsonb_build_object('version',1,
    'kind',current_value#>'{_autofill_snapshot,kind}','generated_html','','manual_html',''))
   else '{}'::jsonb end;
  if coalesce(current_value,'{}'::jsonb)-'_clear_generation'='{}'::jsonb
  or current_value-'_clear_generation'=cleared_value then continue;end if;
  cleared_value:=cleared_value||jsonb_build_object('_clear_generation',operation);
  insert into public.v2_collection_clear_history(operation_id,workspace_id,session_id,field_id,logical_key,previous_value,actor_id)
   values(operation,session_row.workspace_id,p_session_id,field_row.id,field_row.logical_key,current_value,auth.uid());
  insert into public.v2_collection_clear_tokens(transaction_id,workspace_id,session_id,field_id,actor_id,expected_value)
   values(txid_current(),session_row.workspace_id,p_session_id,field_row.id,auth.uid(),cleared_value);
  insert into public.v2_collection_values(session_id,field_id,value,updated_by,updated_at)
   values(p_session_id,field_row.id,cleared_value,auth.uid(),now())
   on conflict(session_id,field_id) do update set value=excluded.value,updated_by=auth.uid(),updated_at=now();
  delete from public.v2_collection_clear_tokens where transaction_id=txid_current() and field_id=field_row.id;
  cleared:=cleared+1;
 end loop;
 return cleared;
end $function$;
revoke all on function public.v2_clear_collection_values(uuid,text,uuid) from public,anon,service_role;
grant execute on function public.v2_clear_collection_values(uuid,text,uuid) to authenticated;
notify pgrst,'reload schema';
