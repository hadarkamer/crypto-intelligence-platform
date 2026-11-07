-- Per-row rich text receipts and concurrent, clear-safe timeframe edits.
begin;

-- Rich text wrappers alone are empty; visible legacy paragraphs and images
-- remain meaningful in the existing completion/history contracts.
create or replace function public.v2_timeframe_value_filled(p_value jsonb)
returns boolean language plpgsql immutable set search_path=public,pg_temp as $filled$
declare item jsonb;content text;structured jsonb:=coalesce(nullif(p_value->'structured','null'::jsonb),p_value->'timeframe_scenarios');
begin
 if p_value->>'completion'='na' then return true;end if;
 if jsonb_typeof(structured->'rows')='array' then
  for item in select value from jsonb_array_elements(structured->'rows') loop
   content:=coalesce(item->>'text','');
   if content ~* '<img[[:space:]]' then return true;end if;
   content:=regexp_replace(content,'<[^>]*>','','g');
   content:=regexp_replace(content,'&(?:nbsp|#0*160|#x0*a0);','','gi');
   content:=regexp_replace(content,U&'[[:space:]\00A0\200B-\200D\FEFF]','','g');
   if content<>'' then return true;end if;
  end loop;
 end if;
 content:=coalesce(p_value->>'text_value',p_value->>'html',p_value->>'text','');
 if content ~* '<img[[:space:]]' then return true;end if;
 content:=regexp_replace(content,'<[^>]*>','','g');
 content:=regexp_replace(content,'&(?:nbsp|#0*160|#x0*a0);','','gi');
 content:=regexp_replace(content,U&'[[:space:]\00A0\200B-\200D\FEFF]','','g');
 return content<>'';
end $filled$;
revoke all on function public.v2_timeframe_value_filled(jsonb) from public,anon;
grant execute on function public.v2_timeframe_value_filled(jsonb) to authenticated,service_role;
do $completion$
declare signature text;fn oid;definition text;needle text:=E'\nbegin\n';replacement text;
 before_contract jsonb;after_contract jsonb;
begin
 foreach signature in array array['public.v2_field_complete(text,jsonb)','public.v2_collection_value_filled(text,jsonb)'] loop
  fn:=to_regprocedure(signature);
  if fn is null then raise exception 'Timeframe completion prerequisite missing';end if;
  select pg_get_functiondef(fn),jsonb_build_object('oid',p.oid,'owner',p.proowner,'acl',p.proacl,
   'security',p.prosecdef,'settings',p.proconfig,'result',p.prorettype,'volatility',p.provolatile)
  into definition,before_contract from pg_proc p where p.oid=fn;
  if position('timeframe_richtext_completion_v1' in definition)>0 then continue;end if;
  replacement:=needle||E' -- timeframe_richtext_completion_v1\n if p_type=''timeframe_scenarios'' then return public.v2_timeframe_value_filled(p_value);end if;\n';
  if (length(definition)-length(replace(definition,needle,'')))/length(needle)<>1 then raise exception 'Unexpected timeframe completion anchor';end if;
  definition:=replace(definition,needle,replacement);execute definition;
  select jsonb_build_object('oid',p.oid,'owner',p.proowner,'acl',p.proacl,'security',p.prosecdef,
   'settings',p.proconfig,'result',p.prorettype,'volatility',p.provolatile) into after_contract from pg_proc p where p.oid=fn;
  if before_contract is distinct from after_contract or pg_get_functiondef(fn) is distinct from definition then raise exception 'Timeframe completion altered security contract';end if;
 end loop;
end $completion$;

create or replace function public.v2_timeframe_stamp_value(
 p_current jsonb,p_incoming jsonb,p_actor uuid,p_origin text,p_stamp timestamptz
) returns jsonb language plpgsql set search_path=public,pg_temp as $stamp$
declare current_struct jsonb; incoming_struct jsonb; rows jsonb:='[]'; item jsonb;
 previous jsonb; item_id text; seen text[]:='{}'; stamp jsonb;
 current_text text; incoming_text text; source_key text;
begin
 p_current:=coalesce(p_current,'{}'::jsonb);p_incoming:=coalesce(p_incoming,'{}'::jsonb);
 current_struct:=coalesce(nullif(p_current->'structured','null'::jsonb),p_current->'timeframe_scenarios','{}'::jsonb);
 source_key:=case when p_incoming#>>'{structured,type}'='timeframe_scenarios' then 'structured'
  when p_incoming#>>'{timeframe_scenarios,type}'='timeframe_scenarios' then 'timeframe_scenarios' else null end;
 stamp:=jsonb_build_object('updated_at',p_stamp,'updated_by',p_actor,'updated_origin',p_origin);
 if source_key is not null then
  incoming_struct:=p_incoming->source_key;
  if jsonb_typeof(incoming_struct->'rows') is distinct from 'array' then
   raise exception 'Timeframe rows must be an array' using errcode='22023';end if;
  for item in select value||jsonb_build_object('id',coalesce(nullif(value->>'id',''),'saved-'||(ordinality-1)))
   from jsonb_array_elements(incoming_struct->'rows') with ordinality loop
   item_id:=item->>'id';
   if item_id=any(seen) then raise exception 'Duplicate timeframe row' using errcode='22023';end if;
   seen:=array_append(seen,item_id);
   select value||jsonb_build_object('id',coalesce(nullif(value->>'id',''),'saved-'||(ordinality-1))) into previous
   from jsonb_array_elements(case when jsonb_typeof(current_struct->'rows')='array' then current_struct->'rows' else '[]' end) with ordinality
   where coalesce(nullif(value->>'id',''),'saved-'||(ordinality-1))=item_id limit 1;
   -- Ignore any incoming claimed authorship or dates. Only an actual content,
   -- timeframe or formatting change receives a new server timestamp.
   item:=item-'updated_at'-'updated_by'-'updated_origin';
   if item is distinct from (previous-'updated_at'-'updated_by'-'updated_origin') then
    item:=item||stamp;
   elsif previous is not null then
    item:=item||(previous-(select coalesce(array_agg(key),'{}'::text[]) from jsonb_object_keys(previous) key where key not in ('updated_at','updated_by','updated_origin')));
   end if;
   rows:=rows||jsonb_build_array(item);
  end loop;
  p_incoming:=jsonb_set(p_incoming,array[source_key],incoming_struct||jsonb_build_object('rows',rows));
 end if;
 current_text:=coalesce(p_current->>'text_value',p_current->>'html',p_current->>'text','');
 incoming_text:=coalesce(p_incoming->>'text_value',p_incoming->>'html',p_incoming->>'text','');
 p_incoming:=p_incoming-'timeframe_notes_update';
 if incoming_text is distinct from current_text then
  p_incoming:=p_incoming||jsonb_build_object('timeframe_notes_update',stamp);
 elsif p_current ? 'timeframe_notes_update' then
  p_incoming:=p_incoming||jsonb_build_object('timeframe_notes_update',p_current->'timeframe_notes_update');
 end if;
 return p_incoming;
end $stamp$;
revoke all on function public.v2_timeframe_stamp_value(jsonb,jsonb,uuid,text,timestamptz) from public,anon,authenticated,service_role;

-- Stamp at the existing latest-value lookup. Its workspace lock, RLS checks,
-- exact automatic-run capability and native clear barrier remain unchanged.
do $guard$
declare fn oid:=to_regprocedure('public.v2_collection_managed_snapshot_write_guard()');
 definition text; needle text; replacement text; before_contract jsonb; after_contract jsonb;
begin
 if fn is null then raise exception 'Collection snapshot guard required';end if;
 select pg_get_functiondef(fn),jsonb_build_object('oid',p.oid,'owner',p.proowner,'acl',p.proacl,
  'security',p.prosecdef,'settings',p.proconfig,'result',p.prorettype,'volatility',p.provolatile)
 into definition,before_contract from pg_proc p where p.oid=fn;
 if position('timeframe_server_receipts_v1' in definition)>0 then return;end if;
 needle:=' if not (current_value ? ''_autofill_snapshot'') and not (new.value ? ''_autofill_snapshot'') then return new;end if;';
 replacement:=$replacement$
 -- timeframe_server_receipts_v1
 if exists(select 1 from public.v2_template_fields where id=new.field_id and field_type='timeframe_scenarios') then
  if transaction_run is not null and trusted_run is null then
   raise exception 'Timeframe automation token does not match target' using errcode='23514';end if;
  new.value:=public.v2_timeframe_stamp_value(current_value,new.value,
   case when trusted_run is not null then null else auth.uid() end,
   case when trusted_run is not null then 'automation' else 'user' end,clock_timestamp());
 end if;
$replacement$||needle;
 if (length(definition)-length(replace(definition,needle,'')))/length(needle)<>1 then
  raise exception 'Unexpected timeframe receipt guard anchor';end if;
 definition:=replace(definition,needle,replacement);execute definition;
 select jsonb_build_object('oid',p.oid,'owner',p.proowner,'acl',p.proacl,'security',p.prosecdef,
  'settings',p.proconfig,'result',p.prorettype,'volatility',p.provolatile) into after_contract from pg_proc p where p.oid=fn;
 if before_contract is distinct from after_contract or pg_get_functiondef(fn) is distinct from definition then
  raise exception 'Timeframe receipts altered existing security contract';end if;
end $guard$;

create or replace function public.v2_save_timeframe_scenario(
 p_session_id uuid,p_field_id uuid,p_operation jsonb,p_clear_generation text default null
) returns jsonb language plpgsql security definer set search_path=public,pg_temp as $edit$
declare session_row public.v2_collection_sessions; field_row public.v2_template_fields;
 workspace uuid; template uuid; sheet text; logical text; current_value jsonb; structured jsonb;
 rows jsonb; ordered jsonb:='[]'; current_row jsonb; changes jsonb; row_id text;
 ids text[]:='{}'; item jsonb; operation text:=p_operation->>'type'; actor uuid:=auth.uid();
 receipt public.v2_collection_values;
begin
 if actor is null then raise exception 'Collection edit permission required' using errcode='42501';end if;
 if jsonb_typeof(p_operation) is distinct from 'object' or operation is null or operation not in ('upsert_row','delete_row','reorder','notes') then
  raise exception 'Valid timeframe operation required' using errcode='22023';end if;
 select * into session_row from public.v2_collection_sessions where id=p_session_id;
 if session_row.id is null or not coalesce(public.v2_can_edit_workspace(session_row.workspace_id),false) then
  raise exception 'Collection edit permission required' using errcode='42501';end if;
 workspace:=session_row.workspace_id;template:=session_row.template_id;
 perform pg_advisory_xact_lock(hashtextextended(workspace::text||'collection',0));
 perform 1 from public.v2_workspaces where id=workspace for update;
 select * into session_row from public.v2_collection_sessions where id=p_session_id for update;
 if session_row.id is null or session_row.workspace_id is distinct from workspace
 or session_row.template_id is distinct from template or session_row.deleted_at is not null or session_row.status='archived'
 or not exists(select 1 from public.v2_templates where id=template and workspace_id=workspace and kind='collection') then
  raise exception 'Active collection required' using errcode='23514';end if;
 if not coalesce(public.v2_can_edit_workspace(workspace),false) then
  raise exception 'Collection edit permission required' using errcode='42501';end if;
 select f.* into field_row from public.v2_template_fields f join public.v2_template_sections sec on sec.id=f.section_id
 where f.id=p_field_id and sec.template_id=template and f.active and sec.active and f.field_type='timeframe_scenarios';
 if field_row.id is null then raise exception 'Active timeframe field in this worksheet required' using errcode='23514';end if;
 select public.v2_collection_sheet_key(to_jsonb(sec)),public.v2_collection_field_logical_key(to_jsonb(field_row))
 into sheet,logical from public.v2_template_sections sec where sec.id=field_row.section_id;
 if sheet='data' then select value into current_value from public.v2_collection_live_values where workspace_id=workspace and logical_key=logical for update;end if;
 if current_value is null then select value into current_value from public.v2_collection_values where session_id=p_session_id and field_id=p_field_id for update;end if;
 current_value:=coalesce(current_value,'{}'::jsonb);
 if current_value->>'_clear_generation' is distinct from p_clear_generation then
  raise exception 'התיבה נוקתה; יש לרענן לפני שמירה.' using errcode='23514';end if;
 structured:=coalesce(nullif(current_value->'structured','null'::jsonb),current_value->'timeframe_scenarios','{}'::jsonb);
 if structured->>'type' is distinct from 'timeframe_scenarios' then
  structured:=jsonb_build_object('type','timeframe_scenarios','rows',jsonb_build_array(
   jsonb_build_object('id','initial-15m','timeframe','15m','text',''),jsonb_build_object('id','initial-1H','timeframe','1H','text',''),
   jsonb_build_object('id','initial-4H','timeframe','4H','text',''),jsonb_build_object('id','initial-1D','timeframe','1D','text',''),jsonb_build_object('id','initial-1W','timeframe','1W','text','')));
 end if;
 if jsonb_typeof(structured->'rows') is distinct from 'array' then raise exception 'Invalid timeframe rows' using errcode='22023';end if;
 select coalesce(jsonb_agg(value||jsonb_build_object('id',coalesce(nullif(value->>'id',''),'saved-'||(ordinality-1))) order by ordinality),'[]') into rows
 from jsonb_array_elements(structured->'rows') with ordinality;
 if operation='notes' then
  if jsonb_typeof(p_operation->'text') is distinct from 'string' or length(p_operation->>'text')>500000 then
   raise exception 'Valid timeframe notes required' using errcode='22023';end if;
  current_value:=current_value||jsonb_build_object('text',p_operation->>'text');
  if current_value ? 'html' then current_value:=current_value||jsonb_build_object('html',p_operation->>'text');end if;
  if current_value ? 'text_value' then current_value:=current_value||jsonb_build_object('text_value',p_operation->>'text');end if;
 elsif operation in ('upsert_row','delete_row') then
  row_id:=p_operation->>'id';
  if row_id is null or length(row_id) not between 1 and 160 then raise exception 'Valid timeframe row required' using errcode='22023';end if;
  select value into current_row from jsonb_array_elements(rows) where value->>'id'=row_id limit 1;
  if operation='delete_row' then
   select coalesce(jsonb_agg(value order by ordinality),'[]') into rows from jsonb_array_elements(rows) with ordinality where value->>'id'<>row_id;
   structured:=structured||jsonb_build_object('_deleted_row_ids',coalesce(structured->'_deleted_row_ids','[]')||jsonb_build_array(row_id));
  else
   changes:=p_operation->'changes';
   if jsonb_typeof(changes) is distinct from 'object' or changes='{}'::jsonb
   or exists(select 1 from jsonb_object_keys(changes) key where key not in ('text','timeframe'))
   or (changes ? 'text' and (jsonb_typeof(changes->'text') is distinct from 'string' or length(changes->>'text')>20000))
   or (changes ? 'timeframe' and (jsonb_typeof(changes->'timeframe') is distinct from 'string' or changes->>'timeframe' not in ('15m','1H','4H','1D','1W','1M'))) then
    raise exception 'Valid timeframe row changes required' using errcode='22023';end if;
   if current_row is null then
    if p_operation->>'create' is distinct from 'true' or not (changes ? 'text' and changes ? 'timeframe')
    or coalesce(structured->'_deleted_row_ids','[]') ? row_id then
     raise exception 'מסגרת הזמן הוסרה; יש לרענן לפני שמירה.' using errcode='23514';end if;
    if jsonb_array_length(rows)>=128 then raise exception 'Too many timeframe rows' using errcode='22023';end if;
    rows:=rows||jsonb_build_array(changes||jsonb_build_object('id',row_id));
   else
    select jsonb_agg(case when value->>'id'=row_id then value||changes else value end order by ordinality) into rows from jsonb_array_elements(rows) with ordinality;
   end if;
  end if;
 else
  if jsonb_typeof(p_operation->'ids') is distinct from 'array' or jsonb_array_length(p_operation->'ids')>1000 then
   raise exception 'Valid timeframe order required' using errcode='22023';end if;
  for item in select value from jsonb_array_elements(p_operation->'ids') loop
   if jsonb_typeof(item) is distinct from 'string' then raise exception 'Invalid timeframe order' using errcode='22023';end if;
   row_id:=item#>>'{}';
   if row_id=any(ids) then raise exception 'Duplicate timeframe order entry' using errcode='22023';end if;
   ids:=array_append(ids,row_id);
   select value into current_row from jsonb_array_elements(rows) where value->>'id'=row_id limit 1;
   if current_row is not null then ordered:=ordered||jsonb_build_array(current_row);end if;
  end loop;
  select ordered||coalesce(jsonb_agg(value order by ordinality),'[]') into rows from jsonb_array_elements(rows) with ordinality where not ((value->>'id')=any(ids));
 end if;
 if operation<>'notes' then current_value:=current_value||jsonb_build_object('structured',structured||jsonb_build_object('rows',rows));end if;
 perform public.v2_save_collection_value(p_session_id,p_field_id,current_value);
 select effective.* into receipt from public.v2_collection_current_values(p_session_id) effective where effective.field_id=p_field_id;
 if receipt.id is null then raise exception 'Saved timeframe receipt missing' using errcode='23514';end if;
 return jsonb_build_object('value',receipt.value,'revision',receipt.updated_at);
end $edit$;
revoke all on function public.v2_save_timeframe_scenario(uuid,uuid,jsonb,text) from public,anon,service_role;
grant execute on function public.v2_save_timeframe_scenario(uuid,uuid,jsonb,text) to authenticated;
notify pgrst,'reload schema';
commit;
