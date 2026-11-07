-- A collection box becomes an independent snapshot attached to its canonical
-- linked scenario. Existing manual role text, values and attachments survive.
begin;

alter table public.v2_scenarios add column if not exists general_evidence jsonb not null default '[]'::jsonb;
do $constraint$ begin
  if not exists(select 1 from pg_constraint where conrelid='public.v2_scenarios'::regclass
    and conname='v2_scenarios_general_evidence_array') then
    alter table public.v2_scenarios add constraint v2_scenarios_general_evidence_array
      check(jsonb_typeof(general_evidence)='array');
  end if;
end $constraint$;

-- Preserve the installed save function and its permission checks. A full editor
-- supplies its baseline revision, preventing stale saves from dropping evidence
-- appended from a collection worksheet after that editor was opened.
do $save_contract$
declare definition text; needle text; replacement text;
begin
  definition:=pg_get_functiondef('public.save_v2_scenario(jsonb,jsonb)'::regprocedure);
  if strpos(definition,'general_evidence')=0 then
    needle:='invalidating_evidence,created_by,updated_by)';
    if strpos(definition,needle)=0 then raise exception 'Unexpected scenario insert columns'; end if;
    definition:=replace(definition,needle,'invalidating_evidence,general_evidence,created_by,updated_by)');
    needle:='coalesce(p_scenario->''invalidating_evidence'',''[]''),auth.uid(),auth.uid())';
    if strpos(definition,needle)=0 then raise exception 'Unexpected scenario insert values'; end if;
    definition:=replace(definition,needle,'coalesce(p_scenario->''invalidating_evidence'',''[]''),coalesce(p_scenario->''general_evidence'',''[]''),auth.uid(),auth.uid())');
    needle:='invalidating_evidence=coalesce(p_scenario->''invalidating_evidence'',invalidating_evidence),updated_by=auth.uid()';
    if strpos(definition,needle)=0 then raise exception 'Unexpected scenario attachment update'; end if;
    definition:=replace(definition,needle,'invalidating_evidence=coalesce(p_scenario->''invalidating_evidence'',invalidating_evidence),general_evidence=coalesce(p_scenario->''general_evidence'',general_evidence),updated_by=auth.uid()');
  end if;
  if strpos(definition,'expected_updated_at')=0 then
    needle:='  if exists_id is null then';
    replacement:=$revision$
  if exists_id is null and nullif(p_scenario->>'expected_updated_at','') is not null then
    raise exception 'התרחיש אינו זמין עוד לעריכה.' using errcode='40001';
  end if;
  if exists_id is not null and p_scenario ? 'expected_updated_at'
    and exists(select 1 from public.v2_scenarios s where s.id=sid and s.workspace_id=wid
      and s.updated_at is distinct from (p_scenario->>'expected_updated_at')::timestamptz) then
    raise exception 'התרחיש עודכן במקום אחר. טענו את העדכון לפני שמירה חוזרת.' using errcode='40001';
  end if;
$revision$||needle;
    if strpos(definition,needle)=0 then raise exception 'Unexpected scenario revision boundary'; end if;
    definition:=replace(definition,needle,replacement);
  end if;
  execute definition;
end $save_contract$;

create or replace function app_private.collection_evidence_image_paths(p_value jsonb,p_key text default '')
returns setof text language plpgsql immutable security invoker
set search_path=pg_catalog,public,app_private,pg_temp as $$
declare entry record; path text; matched text[];
begin
  if jsonb_typeof(p_value)='object' then
    for entry in select key,value from jsonb_each(p_value) loop
      return query select * from app_private.collection_evidence_image_paths(entry.value,entry.key);
    end loop;
  elsif jsonb_typeof(p_value)='array' then
    for entry in select value from jsonb_array_elements(p_value) loop
      return query select * from app_private.collection_evidence_image_paths(entry.value,'');
    end loop;
  elsif jsonb_typeof(p_value)='string' then
    path:=p_value#>>'{}';
    if p_key in ('storage_path','original_storage_path','image_path') and path<>'' then return next path; end if;
    for matched in select regexp_matches(path,$rx$data-path=["']([^"']+)["']$rx$,'g') loop
      return next matched[1];
    end loop;
  end if;
end $$;
revoke all on function app_private.collection_evidence_image_paths(jsonb,text) from public,anon,authenticated;

create or replace function app_private.replace_collection_evidence_images(p_value jsonb,p_copies jsonb,p_key text default '')
returns jsonb language plpgsql immutable security invoker
set search_path=pg_catalog,public,app_private,pg_temp as $$
declare entry record; result jsonb; text_value text; copy jsonb;
begin
  if jsonb_typeof(p_value)='object' then
    result:='{}'::jsonb;
    for entry in select key,value from jsonb_each(p_value) loop
      result:=result||jsonb_build_object(entry.key,app_private.replace_collection_evidence_images(entry.value,p_copies,entry.key));
    end loop;
    return result;
  elsif jsonb_typeof(p_value)='array' then
    result:='[]'::jsonb;
    for entry in select value from jsonb_array_elements(p_value) loop
      result:=result||jsonb_build_array(app_private.replace_collection_evidence_images(entry.value,p_copies,''));
    end loop;
    return result;
  elsif jsonb_typeof(p_value)='string' then
    text_value:=p_value#>>'{}';
    for copy in select value from jsonb_array_elements(p_copies) loop
      if p_key in ('storage_path','original_storage_path','image_path') and text_value=copy->>'original' then
        text_value:=copy->>'copied';
      else
        text_value:=replace(text_value,'data-path="'||(copy->>'original')||'"','data-path="'||(copy->>'copied')||'"');
        text_value:=replace(text_value,'data-path='''||(copy->>'original')||'''','data-path='''||(copy->>'copied')||'''');
      end if;
    end loop;
    return to_jsonb(text_value);
  end if;
  return p_value;
end $$;
revoke all on function app_private.replace_collection_evidence_images(jsonb,jsonb,text) from public,anon,authenticated;

-- Resolve the selected saved source, including permanent data live values.
-- Individual news preserve their own source URL/date/rating, with no siblings.
create or replace function app_private.collection_scenario_evidence_source(p_session_id uuid,p_field_id uuid,p_news_id text default null)
returns jsonb language plpgsql volatile security invoker
set search_path=pg_catalog,public,app_private,pg_temp as $$
declare c public.v2_collection_sessions; field_type text; source jsonb; embedded jsonb;
  item jsonb; raw_rows jsonb; body text; source_name text; value jsonb;
  field_metadata jsonb; section_metadata jsonb; saved public.v2_collection_values;
  native_items jsonb; native_updated_at timestamptz; native_updated_by uuid; sheet_key text; logical_key text;
begin
  select * into c from public.v2_collection_sessions where id=p_session_id;
  if not found or auth.uid() is null or not public.v2_can_edit_workspace(c.workspace_id)
    or c.deleted_at is not null or c.status='archived' then
    raise exception 'אין הרשאה לתיבת המקור בגליון זה.' using errcode='42501'; end if;
  select f.field_type,to_jsonb(f),to_jsonb(sec) into field_type,field_metadata,section_metadata from public.v2_template_fields f
    join public.v2_template_sections sec on sec.id=f.section_id
    where f.id=p_field_id and f.active and sec.active and sec.template_id=c.template_id;
  if not found then raise exception 'תיבת המקור אינה בגליון שנבחר.' using errcode='23514'; end if;
  sheet_key:=public.v2_collection_sheet_key(section_metadata);
  logical_key:=public.v2_collection_field_logical_key(field_metadata);
  if field_type in ('news_table','news_source_table','unbiased_news','unbiased') and sheet_key<>'data' then
    -- Read this exact active source field, never presence-ranked history. Older
    -- historical readers do not recognize structured unbiased_news, and native
    -- news may legitimately have no v2_collection_values parent row at all.
    select * into saved from public.v2_collection_values where session_id=c.id and field_id=p_field_id;
    select coalesce(jsonb_agg(to_jsonb(n)||jsonb_build_object('source',to_jsonb(ns)) order by n.occurred_at,n.id),'[]'::jsonb),
      max(n.updated_at),(array_agg(n.updated_by order by n.updated_at desc,n.id desc))[1]
      into native_items,native_updated_at,native_updated_by
      from public.v2_news_items n left join public.v2_news_sources ns on ns.id=n.source_id
      where n.workspace_id=c.workspace_id and n.collection_session_id=c.id and n.field_id=p_field_id
        and nullif(btrim(n.body),'') is not null;
    source:=jsonb_build_object('sheet_key',sheet_key,'logical_key',logical_key,'field_id',p_field_id,
      'field',field_metadata,'section',section_metadata,'session_id',c.id,'session_created_at',c.created_at,
      'value',coalesce(saved.value,'{}'::jsonb)||jsonb_build_object('_updated_origin',coalesce(saved.updated_origin,'user')),
      'news_items',native_items,'updated_at',coalesce(greatest(saved.updated_at,native_updated_at),c.created_at),
      'updated_by',case when native_updated_at is not null and (saved.updated_at is null or native_updated_at>=saved.updated_at)
        then native_updated_by else saved.updated_by end,
      'snapshot_key',sheet_key||':'||logical_key);
  else
    select to_jsonb(s) into source from public.v2_collection_snapshot(c.workspace_id,'session',p_session_id) s
      where s.field_id=p_field_id and s.session_id=p_session_id;
    if source is null then raise exception 'אין תוכן שמור בתיבת המקור.' using errcode='23514'; end if;
  end if;
  if field_type in ('news_table','news_source_table','unbiased_news','unbiased') then
    if p_news_id is null or length(p_news_id)=0 or length(p_news_id)>200 then
      raise exception 'בחרו ידיעה בודדת להוספה לתרחיש.' using errcode='23514'; end if;
    select n into item from jsonb_array_elements(coalesce(source->'news_items','[]'::jsonb)) n
      where n->>'id'=p_news_id limit 1;
    embedded:=case when jsonb_typeof(source#>'{value,structured}')='object' then source#>'{value,structured}' else '{}'::jsonb end;
    raw_rows:=case when jsonb_typeof(embedded->'rows')='array' then embedded->'rows'
      when jsonb_typeof(source#>'{value,rows}')='array' then source#>'{value,rows}' else '[]'::jsonb end;
    if item is null then
      select n.value||jsonb_build_object('id',coalesce(n.value->>'id','legacy-position:'||(n.ordinality-1)::text)) into item
        from jsonb_array_elements(raw_rows) with ordinality n(value,ordinality)
        where coalesce(n.value->>'id','legacy-position:'||(n.ordinality-1)::text)=p_news_id limit 1;
    end if;
    if item is null then raise exception 'הידיעה אינה קיימת עוד בתיבת המקור.' using errcode='23514'; end if;
    body:=coalesce(item->>'body',item->>'text','');
    if btrim(regexp_replace(replace(body,'&nbsp;',' '),'<[^>]*>',' ','g'))='' and body !~* '<img\y' then
      raise exception 'אין תוכן שמור בידיעה שנבחרה.' using errcode='23514'; end if;
    item:=item||jsonb_build_object('id',p_news_id,'body',body);
    source_name:=coalesce(item#>>'{source,name}',case when jsonb_typeof(item->'source')='string' then item->>'source' end,
      item->>'source_name',source#>>'{field,title}');
    value:=jsonb_build_object('rows',jsonb_build_array(item),'structured',jsonb_build_object('type','news_table','rows',jsonb_build_array(item)));
    if source#>'{value,_updated_origin}' is not null then value:=value||jsonb_build_object('_updated_origin',source#>'{value,_updated_origin}'); end if;
    source:=source||jsonb_build_object('snapshot_key',(source->>'snapshot_key')||':news:'||p_news_id,
      'evidence_parent_key',p_session_id::text||':'||(source->>'snapshot_key'),
      'evidence_news_id',p_news_id,'evidence_source_name',source_name,
      'value',value,'news_items','[]'::jsonb,
      'updated_at',coalesce(item->>'updated_at',item->>'occurred_at',item->>'created_at',source->>'updated_at'),
      'updated_by',coalesce(item->'updated_by',source->'updated_by'));
    if jsonb_typeof(item->'source')='object' then source:=source||jsonb_build_object('evidence_source_status',item->'source'); end if;
  elsif p_news_id is not null then
    raise exception 'תיבת המקור אינה תיבת חדשות.' using errcode='23514';
  elsif not (
    coalesce(public.v2_collection_value_filled(field_type,(source->'value')-'completion'-'_updated_origin'),false)
    -- The historical presence function predates these structured data fields.
    -- Capture their actual saved content without treating empty placeholder
    -- rows or completion metadata as evidence, and without changing history.
    or (field_type='correlation' and exists(select 1 from unnest(array['qqq','spy']) keys(k)
      where jsonb_typeof(source->'value'->k) in ('string','number') and nullif(btrim(source->'value'->>k),'') is not null))
    or (field_type='options_expiry' and exists(
      select 1 from jsonb_array_elements(case when jsonb_typeof(source#>'{value,dates}')='array'
        then source#>'{value,dates}' else '[]'::jsonb end) d
      where nullif(btrim(d->>'date'),'') is not null and exists(
        select 1 from jsonb_array_elements(case when jsonb_typeof(d->'exchanges')='array'
          then d->'exchanges' else '[]'::jsonb end) ex
        where nullif(btrim(ex->>'price'),'') is not null or nullif(btrim(ex->>'amount'),'') is not null)))
  ) then
    raise exception 'אין תוכן שמור בתיבת המקור.' using errcode='23514';
  end if;
  return source;
end $$;
revoke all on function app_private.collection_scenario_evidence_source(uuid,uuid,text) from public,anon,authenticated;

create or replace function public.v2_prepare_collection_scenario_evidence(
  p_session_id uuid,p_scenario_id uuid,p_field_id uuid,p_news_id text,p_role text)
returns jsonb language plpgsql volatile security definer
set search_path=pg_catalog,public,app_private,pg_temp as $$
declare c public.v2_collection_sessions; s public.v2_scenarios; source jsonb; evidence jsonb; source_key text;
begin
  if p_role is null or p_role not in ('supporting','invalidating','general') then
    raise exception 'סוג סימוכין לא תקין.' using errcode='23514'; end if;
  c:=public.v2_lock_collection_scenario_session(p_session_id);
  select * into s from public.v2_scenarios where id=p_scenario_id and workspace_id=c.workspace_id for update;
  if not found or s.status='archived' or not exists(select 1 from public.v2_collection_scenario_links l
    where l.session_id=c.id and l.scenario_id=s.id and l.workspace_id=c.workspace_id) then
    raise exception 'אין הרשאה לתרחיש בגליון זה.' using errcode='42501'; end if;
  source:=app_private.collection_scenario_evidence_source(c.id,p_field_id,p_news_id);
  source_key:=c.id::text||':'||(source->>'snapshot_key');
  evidence:=case p_role when 'supporting' then s.supporting_evidence when 'invalidating' then s.invalidating_evidence else s.general_evidence end;
  return jsonb_build_object('workspace_id',c.workspace_id,'snapshot',source,
    'already_added',exists(select 1 from jsonb_array_elements(evidence) e where e->>'source_key'=source_key
      or (p_news_id is not null and e->>'source_key'=source->>'evidence_parent_key')),
    'bundle',public.v2_collection_scenario_bundle(c.id,s.id));
end $$;
revoke all on function public.v2_prepare_collection_scenario_evidence(uuid,uuid,uuid,text,text) from public,anon;
grant execute on function public.v2_prepare_collection_scenario_evidence(uuid,uuid,uuid,text,text) to authenticated;

create or replace function public.v2_add_collection_scenario_evidence(
  p_session_id uuid,p_scenario_id uuid,p_field_id uuid,p_news_id text,p_role text,
  p_expected_snapshot jsonb,p_image_copies jsonb default '[]'::jsonb)
returns jsonb language plpgsql volatile security definer
set search_path=pg_catalog,public,app_private,pg_temp as $$
declare c public.v2_collection_sessions; s public.v2_scenarios; source jsonb; evidence jsonb; source_key text;
  originals text[]; original text; copy jsonb; target text; bucket text; original_path text;
  source_readable boolean; project_id uuid; captured jsonb;
begin
  if p_role is null or p_role not in ('supporting','invalidating','general') or
    jsonb_typeof(p_expected_snapshot) is distinct from 'object' or
    jsonb_typeof(p_image_copies) is distinct from 'array' or jsonb_array_length(p_image_copies)>100 then
    raise exception 'סימוכין לתרחיש אינם תקינים.' using errcode='23514'; end if;
  c:=public.v2_lock_collection_scenario_session(p_session_id);
  select * into s from public.v2_scenarios where id=p_scenario_id and workspace_id=c.workspace_id for update;
  if not found or s.status='archived' or not exists(select 1 from public.v2_collection_scenario_links l
    where l.session_id=c.id and l.scenario_id=s.id and l.workspace_id=c.workspace_id) then
    raise exception 'אין הרשאה לתרחיש בגליון זה.' using errcode='42501'; end if;
  source:=app_private.collection_scenario_evidence_source(c.id,p_field_id,p_news_id);
  source_key:=c.id::text||':'||(source->>'snapshot_key');
  evidence:=case p_role when 'supporting' then s.supporting_evidence when 'invalidating' then s.invalidating_evidence else s.general_evidence end;
  if exists(select 1 from jsonb_array_elements(evidence) e where e->>'source_key'=source_key
    or (p_news_id is not null and e->>'source_key'=source->>'evidence_parent_key')) then
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
revoke all on function public.v2_add_collection_scenario_evidence(uuid,uuid,uuid,text,text,jsonb,jsonb) from public,anon;
grant execute on function public.v2_add_collection_scenario_evidence(uuid,uuid,uuid,text,text,jsonb,jsonb) to authenticated;

commit;
