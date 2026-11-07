-- Historical news actions select one saved row. Whole-answer pins remain valid.
begin;
alter table public.v2_collection_previous_answer_pins add column if not exists source_selector jsonb;

create or replace function app_private.collection_history_select_row(p_source jsonb,p_selector jsonb)
returns jsonb language plpgsql immutable set search_path=pg_catalog,public,app_private,pg_temp as $$
declare kind text:=p_selector->>'kind'; origin text:=p_selector->>'origin'; selected jsonb; candidate record;
 rows jsonb; body text; value jsonb; key text; row_id text; name text;
begin
 if jsonb_typeof(p_selector) is distinct from 'object' then raise exception 'בחרו ידיעה בודדת.' using errcode='23514';end if;
 if kind='news' then
  if origin not in ('native','rows','structured') or origin is null or
   (not(p_selector ? 'id') and (origin='native' or coalesce(p_selector->>'index','') !~ '^(0|[1-9][0-9]{0,7})$')) or
   (p_selector ? 'id' and (jsonb_typeof(p_selector->'id')<>'string' or length(p_selector->>'id') not between 1 and 2000)) then
   raise exception 'מזהה הידיעה אינו תקין.' using errcode='23514';end if;
  -- Match the historical renderer: nested rows, retained top-level rows, then
  -- native news; duplicate IDs use the latest timestamp and the last tie.
  rows:=case when jsonb_typeof(p_source#>'{value,structured,rows}')='array' then p_source#>'{value,structured,rows}' else '[]'::jsonb end;
  if p_selector ? 'id' then
   select x.item,x.origin into candidate from (
    select e.value item,'structured'::text origin,0 priority,e.ordinality pos from jsonb_array_elements(rows) with ordinality e
    union all select e.value,'rows',1,e.ordinality from jsonb_array_elements(case when jsonb_typeof(p_source#>'{value,rows}')='array' then p_source#>'{value,rows}' else '[]'::jsonb end) with ordinality e
    union all select e.value,'native',2,e.ordinality from jsonb_array_elements(coalesce(p_source->'news_items','[]')) with ordinality e
   ) x where jsonb_typeof(x.item)='object' and x.item->>'id'=p_selector->>'id'
   order by coalesce(x.item->>'updated_at','') collate "C" desc,x.priority desc,x.pos desc limit 1;
   if candidate.origin is distinct from origin then raise exception 'הידיעה השתנתה. טענו שוב את התשובות הקודמות.' using errcode='40001';end if;
   selected:=candidate.item;row_id:=p_selector->>'id';key:='news:id:'||row_id;
  else
   if origin='rows' then rows:=case when jsonb_typeof(p_source#>'{value,rows}')='array' then p_source#>'{value,rows}' else '[]'::jsonb end;end if;
   selected:=rows->((p_selector->>'index')::integer);
   if jsonb_typeof(selected) is distinct from 'object' or (jsonb_typeof(selected->'id')='string' and nullif(selected->>'id','') is not null) then
    raise exception 'הידיעה אינה קיימת עוד במיקום שנבחר.' using errcode='40001';end if;
   key:='news:index:'||origin||':'||(p_selector->>'index');row_id:='history-'||origin||'-row:'||(p_selector->>'index');
  end if;
  if selected is null then raise exception 'הידיעה אינה קיימת עוד.' using errcode='40001';end if;
  body:=coalesce(selected->>'body',selected->>'text','');
  if btrim(regexp_replace(replace(body,'&nbsp;',' '),'<[^>]*>',' ','g'))='' and body !~* '<img\y' then
   raise exception 'אין תוכן שמור בידיעה.' using errcode='23514';end if;
  selected:=selected||jsonb_build_object('id',row_id,'body',body);
  value:=jsonb_build_object('rows',jsonb_build_array(selected),'structured',jsonb_build_object('type','news_table','rows',jsonb_build_array(selected)));
  name:=coalesce(selected#>>'{source,name}',case when jsonb_typeof(selected->'source')='string' then selected->>'source' end,selected->>'source_name',p_source#>>'{field,title}');
 elsif kind='text' then
  body:=coalesce(p_source#>>'{value,text_value}',p_source#>>'{value,html}',p_source#>>'{value,text}',p_source#>>'{value,choice}','');
  if btrim(regexp_replace(replace(body,'&nbsp;',' '),'<[^>]*>',' ','g'))='' and body !~* '<img\y' then
   raise exception 'הטקסט אינו קיים עוד.' using errcode='40001';end if;
  key:='text';value:=jsonb_build_object('text',body,'images',coalesce(p_source#>'{value,images}','[]'));
 elsif kind='weekly_consensus' then
  if jsonb_typeof(p_source#>'{value,_autofill_consensus}') is distinct from 'object' then
   raise exception 'צילום המקור אינו קיים עוד.' using errcode='40001';end if;
  key:='weekly_consensus';value:=jsonb_build_object('_autofill_consensus',p_source#>'{value,_autofill_consensus}');
  if p_source#>>'{value,_autofill,kind}'='weekly_consensus' then value:=value||jsonb_build_object('_autofill',p_source#>'{value,_autofill}');end if;
 else raise exception 'סוג הידיעה אינו תקין.' using errcode='23514';end if;
 return p_source||jsonb_build_object('snapshot_key',(p_source->>'snapshot_key')||':'||key,
  'history_selector',p_selector,'evidence_parent_key',(p_source->>'session_id')||':'||(p_source->>'snapshot_key'),
  'evidence_news_id',row_id,'evidence_source_name',coalesce(name,p_source#>>'{field,title}'),
  'field',(p_source->'field')||jsonb_build_object('field_type',case when kind='text' then 'long_text' else 'news_table' end),
  'value',value,'news_items','[]'::jsonb,
  'updated_at',coalesce(selected->>'updated_at',selected->>'occurred_at',selected->>'created_at',p_source->>'updated_at'),
  'updated_by',coalesce(selected->'updated_by',p_source->'updated_by'));
end $$;
revoke all on function app_private.collection_history_select_row(jsonb,jsonb) from public,anon,authenticated;

create or replace function app_private.collection_history_row_source(p_scope jsonb,p_source_session_id text,p_source_field_id text,p_selector jsonb,p_pin_id uuid default null)
returns jsonb language plpgsql volatile security invoker set search_path=pg_catalog,public,app_private,pg_temp as $$
declare result jsonb; source jsonb; pin public.v2_collection_previous_answer_pins; selector_key text;
begin
 if p_pin_id is not null then
  select * into pin from public.v2_collection_previous_answer_pins p where p.id=p_pin_id
   and p.workspace_id=(p_scope#>>'{session,workspace_id}')::uuid and p.sheet_key=p_scope->>'sheet_key'
   and p.field_keys && app_private.collection_history_field_keys(p_scope->'field') for share;
  if pin.id is null or (pin.source_selector is not null and pin.source_selector is distinct from p_selector)
   or pin.snapshot->>'session_id' is distinct from p_source_session_id or pin.snapshot->>'field_id' is distinct from p_source_field_id then
   raise exception 'הנעיצה אינה זמינה במקור זה.' using errcode='42501';end if;
  if pin.source_selector is null then
   source:=app_private.collection_history_select_row(pin.snapshot,p_selector);
   selector_key:=case p_selector->>'kind' when 'news' then case when p_selector ? 'id' then 'news:id:'||(p_selector->>'id') else 'news:index:'||(p_selector->>'origin')||':'||(p_selector->>'index') end else p_selector->>'kind' end;
   return jsonb_build_object('snapshot',source,'original_snapshot',pin.snapshot,'source_title',pin.source_title,'source_key',pin.source_key||':'||selector_key,'row_order','[]'::jsonb);
  end if;
  return jsonb_build_object('snapshot',pin.snapshot,'original_snapshot',pin.snapshot,'source_title',pin.source_title,'source_key',pin.source_key,'row_order',pin.row_order);
 end if;
 result:=app_private.collection_history_pin_source(p_scope,p_source_session_id,p_source_field_id);
 source:=app_private.collection_history_select_row(result->'snapshot',p_selector);
 selector_key:=case p_selector->>'kind' when 'news' then case when p_selector ? 'id' then 'news:id:'||(p_selector->>'id') else 'news:index:'||(p_selector->>'origin')||':'||(p_selector->>'index') end else p_selector->>'kind' end;
 return result||jsonb_build_object('original_snapshot',result->'snapshot','snapshot',source,'row_order','[]'::jsonb,'source_key',(result->>'source_key')||':'||selector_key);
end $$;
revoke all on function app_private.collection_history_row_source(jsonb,text,text,jsonb,uuid) from public,anon,authenticated;

create or replace function public.v2_prepare_collection_previous_news_pin(
 p_session_id uuid,p_field_id uuid,p_source_session_id text,p_source_field_id text,p_selector jsonb,p_pin_id uuid default null)
returns jsonb language plpgsql volatile security definer set search_path=pg_catalog,public,app_private,pg_temp as $$
declare scope jsonb; prepared jsonb; pin jsonb;
begin
 scope:=app_private.collection_history_pin_scope(p_session_id,p_field_id,true);
 prepared:=app_private.collection_history_row_source(scope,p_source_session_id,p_source_field_id,p_selector,p_pin_id);
 select to_jsonb(p) into pin from public.v2_collection_previous_answer_pins p
   where p.workspace_id=(scope#>>'{session,workspace_id}')::uuid and p.sheet_key=scope->>'sheet_key' and p.source_key=prepared->>'source_key';
 return prepared||jsonb_build_object('existing_pin',pin);
end $$;
revoke all on function public.v2_prepare_collection_previous_news_pin(uuid,uuid,text,text,jsonb,uuid) from public,anon;
grant execute on function public.v2_prepare_collection_previous_news_pin(uuid,uuid,text,text,jsonb,uuid) to authenticated;


create or replace function public.v2_pin_collection_previous_news(
 p_session_id uuid,p_field_id uuid,p_source_session_id text,p_source_field_id text,
 p_selector jsonb,p_expected_snapshot jsonb,p_expected_row_order jsonb,p_image_copies jsonb default '[]',p_pin_id uuid default null)
returns public.v2_collection_previous_answer_pins language plpgsql volatile security definer
set search_path=pg_catalog,public,app_private,pg_temp as $$
declare scope jsonb; prepared jsonb; source jsonb; pin public.v2_collection_previous_answer_pins; originals text[];
 original text; copy jsonb; target text; original_path text; bucket text; readable boolean; project_id uuid;
begin
 scope:=app_private.collection_history_pin_scope(p_session_id,p_field_id,true);
 prepared:=app_private.collection_history_row_source(scope,p_source_session_id,p_source_field_id,p_selector,p_pin_id);
 select * into pin from public.v2_collection_previous_answer_pins p where p.workspace_id=(scope#>>'{session,workspace_id}')::uuid
   and p.sheet_key=scope->>'sheet_key' and p.source_key=prepared->>'source_key';
 if found then return pin; end if;
 source:=prepared->'snapshot';
 if p_expected_snapshot is distinct from source or p_expected_row_order is distinct from prepared->'row_order' then
   raise exception 'תשובת המקור השתנתה. נסו לנעוץ שוב.' using errcode='40001'; end if;
 if jsonb_typeof(p_image_copies) is distinct from 'array' or jsonb_array_length(p_image_copies)>100 then
   raise exception 'רשימת תמונות התשובה אינה תקינה.' using errcode='23514'; end if;
 select coalesce(array_agg(distinct p order by p),'{}'::text[]) into originals
   from app_private.collection_evidence_image_paths(jsonb_build_object('value',source->'value','news_items',source->'news_items')) p;
 if jsonb_array_length(p_image_copies)<>cardinality(originals) or exists(
   select 1 from jsonb_array_elements(p_image_copies) e where jsonb_typeof(e) is distinct from 'object'
   or jsonb_typeof(e->'original') is distinct from 'string' or jsonb_typeof(e->'copied') is distinct from 'string'
   or not coalesce(e->>'original'=any(originals),false))
   or (select count(distinct e->>'original') from jsonb_array_elements(p_image_copies) e)<>cardinality(originals)
   or (select count(distinct e->>'copied') from jsonb_array_elements(p_image_copies) e)<>cardinality(originals) then
   raise exception 'נדרשת העתקה מלאה של תמונות התשובה.' using errcode='23514'; end if;
 foreach original in array originals loop
   select e into copy from jsonb_array_elements(p_image_copies) e where e->>'original'=original;
   target:=regexp_replace(copy->>'copied','^workspace-images:','');
   if not starts_with(copy->>'copied','workspace-images:'||(scope#>>'{session,workspace_id}')||'/collection/'||p_session_id::text||'/')
     or public.v2_image_workspace(target) is distinct from (scope#>>'{session,workspace_id}')::uuid
     or target ~ '(^|/)(\.|\.\.)(/|$)' or position(chr(92) in target)>0
     or not exists(select 1 from storage.objects o where o.bucket_id='workspace-images' and o.name=target) then
     raise exception 'תמונת הנעיצה אינה שייכת לגליון שנבחר.' using errcode='42501'; end if;
   bucket:=case when starts_with(original,'workspace-images:') then 'workspace-images' else 'project-images' end;
   original_path:=regexp_replace(original,'^workspace-images:',''); readable:=false;
   if bucket='workspace-images' then readable:=coalesce(public.v2_is_workspace_member(public.v2_image_workspace(original_path)),false);
   else
     if split_part(original_path,'/',1) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       and to_regprocedure('app_private.can_access_project(uuid)') is not null then
       project_id:=split_part(original_path,'/',1)::uuid;
       execute 'select app_private.can_access_project($1)' into readable using project_id;
     end if;
     if not coalesce(readable,false) and to_regprocedure('app_private.can_read_copied_legacy_image(text)') is not null then
       execute 'select app_private.can_read_copied_legacy_image($1)' into readable using original_path;
     end if;
   end if;
   if not coalesce(readable,false) or not exists(select 1 from storage.objects o where o.bucket_id=bucket and o.name=original_path) then
     raise exception 'אין הרשאה לתמונת המקור.' using errcode='42501'; end if;
 end loop;
 insert into public.v2_collection_previous_answer_pins(workspace_id,sheet_key,logical_key,field_keys,source_key,source_title,snapshot,row_order,pinned_by,source_selector)
 values((scope#>>'{session,workspace_id}')::uuid,scope->>'sheet_key',public.v2_collection_field_logical_key(scope->'field'),
   app_private.collection_history_field_keys(scope->'field'),prepared->>'source_key',prepared->>'source_title',
   source||jsonb_build_object('value',app_private.replace_collection_evidence_images(source->'value',p_image_copies),
     'news_items',app_private.replace_collection_evidence_images(source->'news_items',p_image_copies)),prepared->'row_order',auth.uid(),p_selector)
 on conflict(workspace_id,sheet_key,source_key) do nothing returning * into pin;
 if pin.id is null then select * into pin from public.v2_collection_previous_answer_pins p
   where p.workspace_id=(scope#>>'{session,workspace_id}')::uuid and p.sheet_key=scope->>'sheet_key' and p.source_key=prepared->>'source_key'; end if;
 return pin;
end $$;
revoke all on function public.v2_pin_collection_previous_news(uuid,uuid,text,text,jsonb,jsonb,jsonb,jsonb,uuid) from public,anon;
grant execute on function public.v2_pin_collection_previous_news(uuid,uuid,text,text,jsonb,jsonb,jsonb,jsonb,uuid) to authenticated;


create or replace function public.v2_prepare_collection_previous_news_evidence(
  p_session_id uuid,p_scenario_id uuid,p_field_id uuid,p_source_session_id text,p_source_field_id text,p_selector jsonb,p_role text,p_pin_id uuid default null)
returns jsonb language plpgsql volatile security definer
set search_path=pg_catalog,public,app_private,pg_temp as $$
declare c public.v2_collection_sessions; s public.v2_scenarios; source jsonb; evidence jsonb; source_key text; scope jsonb; prepared jsonb;
begin
  if p_role is null or p_role not in ('supporting','invalidating','general') then
    raise exception 'סוג סימוכין לא תקין.' using errcode='23514'; end if;
  scope:=app_private.collection_history_pin_scope(p_session_id,p_field_id,true);
  c:=jsonb_populate_record(null::public.v2_collection_sessions,scope->'session');
  select * into s from public.v2_scenarios where id=p_scenario_id and workspace_id=c.workspace_id for update;
  if not found or s.status='archived' or not exists(select 1 from public.v2_collection_scenario_links l
    where l.session_id=c.id and l.scenario_id=s.id and l.workspace_id=c.workspace_id) then
    raise exception 'אין הרשאה לתרחיש בגליון זה.' using errcode='42501'; end if;
  prepared:=app_private.collection_history_row_source(scope,p_source_session_id,p_source_field_id,p_selector,p_pin_id);
  source:=prepared->'snapshot';
  source_key:=(source->>'session_id')||':'||(source->>'snapshot_key');
  evidence:=case p_role when 'supporting' then s.supporting_evidence when 'invalidating' then s.invalidating_evidence else s.general_evidence end;
  return jsonb_build_object('workspace_id',c.workspace_id,'snapshot',source,'original_snapshot',prepared->'original_snapshot',
    'already_added',exists(select 1 from jsonb_array_elements(evidence) e where e->>'source_key'=source_key
      or e->>'source_key'=source->>'evidence_parent_key'),
    'bundle',public.v2_collection_scenario_bundle(c.id,s.id));
end $$;
revoke all on function public.v2_prepare_collection_previous_news_evidence(uuid,uuid,uuid,text,text,jsonb,text,uuid) from public,anon;
grant execute on function public.v2_prepare_collection_previous_news_evidence(uuid,uuid,uuid,text,text,jsonb,text,uuid) to authenticated;


create or replace function public.v2_add_collection_previous_news_evidence(
  p_session_id uuid,p_scenario_id uuid,p_field_id uuid,p_source_session_id text,p_source_field_id text,p_selector jsonb,p_role text,
  p_expected_snapshot jsonb,p_image_copies jsonb default '[]'::jsonb,p_pin_id uuid default null)
returns jsonb language plpgsql volatile security definer
set search_path=pg_catalog,public,app_private,pg_temp as $$
declare c public.v2_collection_sessions; s public.v2_scenarios; source jsonb; evidence jsonb; source_key text;
  originals text[]; original text; copy jsonb; target text; bucket text; original_path text;
  source_readable boolean; project_id uuid; captured jsonb; scope jsonb; prepared jsonb;
begin
  if p_role is null or p_role not in ('supporting','invalidating','general') or
    jsonb_typeof(p_expected_snapshot) is distinct from 'object' or
    jsonb_typeof(p_image_copies) is distinct from 'array' or jsonb_array_length(p_image_copies)>100 then
    raise exception 'סימוכין לתרחיש אינם תקינים.' using errcode='23514'; end if;
  scope:=app_private.collection_history_pin_scope(p_session_id,p_field_id,true);
  c:=jsonb_populate_record(null::public.v2_collection_sessions,scope->'session');
  select * into s from public.v2_scenarios where id=p_scenario_id and workspace_id=c.workspace_id for update;
  if not found or s.status='archived' or not exists(select 1 from public.v2_collection_scenario_links l
    where l.session_id=c.id and l.scenario_id=s.id and l.workspace_id=c.workspace_id) then
    raise exception 'אין הרשאה לתרחיש בגליון זה.' using errcode='42501'; end if;
  prepared:=app_private.collection_history_row_source(scope,p_source_session_id,p_source_field_id,p_selector,p_pin_id);
  source:=prepared->'snapshot';
  source_key:=(source->>'session_id')||':'||(source->>'snapshot_key');
  evidence:=case p_role when 'supporting' then s.supporting_evidence when 'invalidating' then s.invalidating_evidence else s.general_evidence end;
  if exists(select 1 from jsonb_array_elements(evidence) e where e->>'source_key'=source_key
    or e->>'source_key'=source->>'evidence_parent_key') then
    return public.v2_collection_scenario_bundle(c.id,s.id)||jsonb_build_object('evidence_added',false);
  end if;
  if p_expected_snapshot is distinct from source then
    raise exception 'תיבת המקור השתנתה במהלך ההוספה. נסו שוב.' using errcode='40001'; end if;
  select coalesce(array_agg(distinct p order by p),'{}'::text[]) into originals
    from app_private.collection_evidence_image_paths(jsonb_build_object('value',source->'value','news_items',source->'news_items')) p;
  if jsonb_array_length(p_image_copies)<>cardinality(originals) or
    exists(select 1 from jsonb_array_elements(p_image_copies) e where jsonb_typeof(e) is distinct from 'object'
      or jsonb_typeof(e->'original') is distinct from 'string' or jsonb_typeof(e->'copied') is distinct from 'string'
      or not coalesce(e->>'original'=any(originals),false)) or
    (select count(distinct e->>'original') from jsonb_array_elements(p_image_copies) e)<>cardinality(originals) or
    (select count(distinct e->>'copied') from jsonb_array_elements(p_image_copies) e)<>cardinality(originals) then
    raise exception 'נדרשת העתקה מלאה של תמונות המקור.' using errcode='23514'; end if;
  foreach original in array originals loop
    select e into copy from jsonb_array_elements(p_image_copies) e where e->>'original'=original;
    target:=replace(copy->>'copied','workspace-images:','');
    if not starts_with(copy->>'copied','workspace-images:'||c.workspace_id::text||'/scenario/'||s.id::text||'/')
      or public.v2_image_workspace(target) is distinct from c.workspace_id
      or target ~ '(^|/)(\.|\.\.)(/|$)' or position(chr(92) in target)>0
      or not exists(select 1 from storage.objects o where o.bucket_id='workspace-images' and o.name=target) then
      raise exception 'תמונת הסימוכין אינה שייכת לתרחיש שנבחר.' using errcode='42501'; end if;
    bucket:=case when starts_with(original,'workspace-images:') then 'workspace-images' else 'project-images' end;
    original_path:=regexp_replace(original,'^workspace-images:','');
    source_readable:=false;
    if bucket='workspace-images' then
      source_readable:=coalesce(public.v2_is_workspace_member(public.v2_image_workspace(original_path)),false);
    else
      if split_part(original_path,'/',1) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        and to_regprocedure('app_private.can_access_project(uuid)') is not null then
        project_id:=split_part(original_path,'/',1)::uuid;
        execute 'select app_private.can_access_project($1)' into source_readable using project_id;
      end if;
      if not coalesce(source_readable,false) and to_regprocedure('app_private.can_read_copied_legacy_image(text)') is not null then
        execute 'select app_private.can_read_copied_legacy_image($1)' into source_readable using original_path;
      end if;
    end if;
    if not coalesce(source_readable,false) or not exists(select 1 from storage.objects o where o.bucket_id=bucket and o.name=original_path) then
      raise exception 'אין הרשאה לקרוא את תמונת המקור.' using errcode='42501'; end if;
  end loop;
  captured:=jsonb_build_object('id',gen_random_uuid(),'captured_at',clock_timestamp(),'source_key',source_key,
    'original_image_paths',to_jsonb(originals),'snapshot',source||jsonb_build_object(
      'value',app_private.replace_collection_evidence_images(source->'value',p_image_copies),
      'news_items',app_private.replace_collection_evidence_images(source->'news_items',p_image_copies)));
  evidence:=evidence||jsonb_build_array(captured);
  if p_role='supporting' then
    update public.v2_scenarios set supporting_evidence=evidence,updated_by=auth.uid() where id=s.id;
  elsif p_role='invalidating' then
    update public.v2_scenarios set invalidating_evidence=evidence,updated_by=auth.uid() where id=s.id;
  else
    update public.v2_scenarios set general_evidence=evidence,updated_by=auth.uid() where id=s.id;
  end if;
  return public.v2_collection_scenario_bundle(c.id,s.id)||jsonb_build_object('evidence_added',true);
end $$;
revoke all on function public.v2_add_collection_previous_news_evidence(uuid,uuid,uuid,text,text,jsonb,text,jsonb,jsonb,uuid) from public,anon;
grant execute on function public.v2_add_collection_previous_news_evidence(uuid,uuid,uuid,text,text,jsonb,text,jsonb,jsonb,uuid) to authenticated;

notify pgrst,'reload schema';
commit;
