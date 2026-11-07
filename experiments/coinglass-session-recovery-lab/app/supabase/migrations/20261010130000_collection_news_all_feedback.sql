-- All collected news can receive explicit feedback and recoverable removal.
-- An absent robot provenance is an unverified origin, never a model prediction.
alter table public.v2_collection_news_recycle_bin drop constraint if exists v2_collection_news_recycle_bin_storage_check;
alter table public.v2_collection_news_recycle_bin add constraint v2_collection_news_recycle_bin_storage_check
  check(storage in ('native','embedded','live'));

create or replace function public.v2_locked_collection_feedback_item(p_workspace_id uuid,p_session_id uuid,p_field_id uuid,p_row_id text)
returns jsonb language plpgsql security definer set search_path=public,pg_temp as $$
declare c public.v2_collection_sessions; p public.v2_collection_news_provenance;
  n public.v2_news_items; v public.v2_collection_values; l public.v2_collection_live_values;
  f public.v2_template_fields; sec public.v2_template_sections; a public.v2_collection_news_assessments;
  snapshot jsonb; contents jsonb; r jsonb; pos integer; path text[]; storage text; trusted boolean;
  source_url text; source_name text; sheet text; logical text; item_key text; matches integer;
  stable_rows jsonb; target_row_id text:=p_row_id;
begin
  c:=public.v2_lock_collection_scenario_session(p_session_id);
  if c.workspace_id is distinct from p_workspace_id then raise exception 'אין הרשאה לגליון.' using errcode='42501';end if;
  select tf.* into f from public.v2_template_fields tf join public.v2_template_sections s on s.id=tf.section_id
    where tf.id=p_field_id and tf.active and s.active and s.template_id=c.template_id
      and tf.field_type in ('news_table','news_source_table','unbiased_news');
  if not found then raise exception 'תיבת חדשות תקינה נדרשת.' using errcode='23514';end if;
  select * into sec from public.v2_template_sections where id=f.section_id;
  sheet:=public.v2_collection_sheet_key(to_jsonb(sec)); logical:=public.v2_collection_field_logical_key(to_jsonb(f));
  select * into p from public.v2_collection_news_provenance where workspace_id=p_workspace_id
    and session_id=p_session_id and field_id=p_field_id and row_id=p_row_id;
  trusted:=found;
  if sheet='data' then
    storage:='live';
    select * into l from public.v2_collection_live_values where workspace_id=p_workspace_id and logical_key=logical for update;
    if l.id is null then raise exception 'הידיעה כבר אינה קיימת.' using errcode='40001';end if;
    contents:=l.value;
    select prov.* into p from public.v2_collection_news_provenance prov
      where prov.workspace_id=p_workspace_id and prov.sheet_key='data' and prov.logical_key=logical and prov.row_id=p_row_id
      order by prov.created_at desc limit 1;
    trusted:=found;
  else
    select * into n from public.v2_news_items where workspace_id=p_workspace_id and collection_session_id=p_session_id
      and field_id=p_field_id and id::text=p_row_id for update;
    if found then
      storage:='native';
      snapshot:=jsonb_build_object('native',to_jsonb(n),'importance',n.importance,'body',n.body);
      source_url:=n.source_url;
      source_name:=coalesce(n.source_name,(select name from public.v2_news_sources where id=n.source_id));
    else
      storage:='embedded';
      select * into v from public.v2_collection_values where session_id=p_session_id and field_id=p_field_id for update;
      if v.id is null then raise exception 'הידיעה כבר אינה קיימת.' using errcode='40001';end if;
      contents:=v.value;
    end if;
  end if;
  if storage<>'native' then
    path:=case when jsonb_typeof(contents#>'{structured,rows}')='array' then array['structured','rows'] else array['rows'] end;
    if jsonb_typeof(contents#>path) is distinct from 'array' then raise exception 'הידיעה כבר אינה קיימת.' using errcode='40001';end if;
    select count(*) into matches from jsonb_array_elements(contents#>path) with ordinality as entries(entry,ordinality)
      where coalesce(entry->>'id',p_field_id::text||':news:'||(ordinality-1)::text)=p_row_id
        or entry->>'_legacy_feedback_id'=p_row_id;
    if matches<>1 then raise exception 'הידיעה חסרה או שקיימת כפילות; רעננו את הגליון.' using errcode='40001';end if;
    select entry,ordinality::integer-1 into r,pos from jsonb_array_elements(contents#>path) with ordinality as entries(entry,ordinality)
      where coalesce(entry->>'id',p_field_id::text||':news:'||(ordinality-1)::text)=p_row_id
        or entry->>'_legacy_feedback_id'=p_row_id;
    -- Persist stable identities for every ID-less sibling before any mutation.
    -- The old index alias supports a retry of an already rendered legacy row;
    -- it never supplies automatic provenance or a model assessment.
    select jsonb_agg(case when entry->>'id' is null then entry||jsonb_build_object('id',gen_random_uuid()::text,
      '_legacy_feedback_id',p_field_id::text||':news:'||(ordinality-1)::text) else entry end order by ordinality)
      into stable_rows from jsonb_array_elements(contents#>path) with ordinality as entries(entry,ordinality);
    if stable_rows is distinct from contents#>path then
      perform public.v2_write_collection_feedback_rows(jsonb_build_object('storage',storage,'workspace_id',p_workspace_id,
        'logical_key',logical,'value_id',coalesce(l.id,v.id),'path',to_jsonb(path)),stable_rows);
      contents:=jsonb_set(contents,path,stable_rows);
      r:=stable_rows->pos;
    end if;
    target_row_id:=r->>'id';
    snapshot:=jsonb_build_object('embedded',r,'value_id',coalesce(l.id,v.id),'path',to_jsonb(path),'position',pos,
      'importance',coalesce(r->>'importance','neutral'),'body',coalesce(r->>'body',r->>'text',''));
    source_url:=r->>'source_url'; source_name:=r->>'source_name';
  end if;
  if trusted and (p.storage=storage or (storage='live' and p.sheet_key='data' and p.logical_key=logical)) then
    select aa.* into a from public.v2_collection_news_assessments aa where aa.workspace_id=p.workspace_id
      and aa.sheet_key=p.sheet_key and aa.logical_key=p.logical_key and aa.item_key=p.item_key;
    source_url:=p.source_url; item_key:=p.item_key;
  else
    -- A copied HTML marker/URL never inherits the genuine article's assessment
    -- or calibration identity. The real row has its own stable feedback key.
    trusted:=false;
    item_key:='unverified:'||case when storage='live' then logical else p_session_id::text||':'||p_field_id::text end||':'||target_row_id;
  end if;
  return snapshot||jsonb_build_object('workspace_id',p_workspace_id,'session_id',p_session_id,'field_id',p_field_id,'row_id',target_row_id,
    'storage',storage,'sheet_key',sheet,'logical_key',logical,'item_key',item_key,'source_url',coalesce(source_url,''),
    'source_name',coalesce(a.source_name,source_name,f.title),'origin',case when trusted then 'automatic' else 'unverified' end,
    'learning_source_url',coalesce(a.learning_source_url,public.v2_news_learning_source(source_url),''),
    'model_evaluation',case when trusted then a.model_evaluation else null end);
end $$;
revoke all on function public.v2_locked_collection_feedback_item(uuid,uuid,uuid,text) from public,anon,authenticated;

-- Permanent data news retains its immutable origin when viewed from another
-- sheet. Matching workspace/logical field/real live row is required throughout.
create or replace function public.v2_read_collection_news_feedback_origins(p_workspace_id uuid,p_session_id uuid,p_field_id uuid)
returns table(row_id text,model_evaluation jsonb) language plpgsql stable security definer set search_path=public,pg_temp as $$
declare sheet text; logical text;
begin
  if auth.uid() is null or not public.v2_is_workspace_member(p_workspace_id) then
    raise exception 'Workspace news access required' using errcode='42501';end if;
  select public.v2_collection_sheet_key(to_jsonb(sec)),public.v2_collection_field_logical_key(to_jsonb(f)) into sheet,logical
    from public.v2_collection_sessions s join public.v2_template_sections sec on sec.template_id=s.template_id
    join public.v2_template_fields f on f.section_id=sec.id where s.id=p_session_id and s.workspace_id=p_workspace_id
      and s.deleted_at is null and f.id=p_field_id and f.field_type in ('news_table','news_source_table','unbiased_news');
  if logical is null then raise exception 'Workspace news access required' using errcode='42501';end if;
  if sheet='data' then
    return query select distinct prov.row_id,a.model_evaluation from public.v2_collection_news_provenance prov
      left join public.v2_collection_news_assessments a on a.workspace_id=prov.workspace_id and a.sheet_key=prov.sheet_key
        and a.logical_key=prov.logical_key and a.item_key=prov.item_key
      where prov.workspace_id=p_workspace_id and prov.sheet_key='data' and prov.logical_key=logical
        and exists(select 1 from public.v2_collection_live_values lv
          cross join lateral jsonb_array_elements(public.v2_collection_embedded_news_rows(lv.value)) r
          where lv.workspace_id=p_workspace_id and lv.logical_key=logical and r->>'id'=prov.row_id)
      order by prov.row_id;
  else
    return query select prov.row_id,a.model_evaluation from public.v2_collection_news_provenance prov
      left join public.v2_collection_news_assessments a on a.workspace_id=prov.workspace_id and a.sheet_key=prov.sheet_key
        and a.logical_key=prov.logical_key and a.item_key=prov.item_key
      where prov.workspace_id=p_workspace_id and prov.session_id=p_session_id and prov.field_id=p_field_id
        and ((prov.storage='native' and exists(select 1 from public.v2_news_items n where n.id::text=prov.row_id
          and n.workspace_id=p_workspace_id and n.collection_session_id=p_session_id and n.field_id=p_field_id))
        or (prov.storage='embedded' and exists(select 1 from public.v2_collection_values v
          cross join lateral jsonb_array_elements(public.v2_collection_embedded_news_rows(v.value)) r
          where v.session_id=p_session_id and v.field_id=p_field_id and r->>'id'=prov.row_id)))
      order by prov.row_id;
  end if;
end $$;
revoke all on function public.v2_read_collection_news_feedback_origins(uuid,uuid,uuid) from public,anon;
grant execute on function public.v2_read_collection_news_feedback_origins(uuid,uuid,uuid) to authenticated;

-- One internal writer handles the two JSON layouts and the enduring data sheet.
-- It receives only snapshots created under the workspace/session/row locks above.
create or replace function public.v2_write_collection_feedback_rows(p_item jsonb,p_rows jsonb)
returns void language plpgsql security definer set search_path=public,pg_temp as $$
declare path text[]; affected integer;
begin
  select array_agg(value) into path from jsonb_array_elements_text(p_item->'path');
  if p_item->>'storage'='live' then
    update public.v2_collection_live_values set value=jsonb_set(case when cardinality(path)=2 and value->'structured' is null
      then value||jsonb_build_object('structured','{}'::jsonb) else value end,path,p_rows),updated_by=auth.uid(),updated_origin='user',updated_at=now()
      where workspace_id=(p_item->>'workspace_id')::uuid and logical_key=p_item->>'logical_key' and id=(p_item->>'value_id')::uuid;
  else
    update public.v2_collection_values set value=jsonb_set(case when cardinality(path)=2 and value->'structured' is null
      then value||jsonb_build_object('structured','{}'::jsonb) else value end,path,p_rows),updated_by=auth.uid()
      where id=(p_item->>'value_id')::uuid;
  end if;
  get diagnostics affected=row_count;
  if affected<>1 then raise exception 'התיבה השתנתה; לא בוצעה כתיבה או שחזור.' using errcode='40001';end if;
end $$;
revoke all on function public.v2_write_collection_feedback_rows(jsonb,jsonb) from public,anon,authenticated;

create or replace function public.v2_rate_collection_news_feedback(p_workspace_id uuid,p_session_id uuid,p_field_id uuid,p_row_id text,p_sentiment text)
returns void language plpgsql security definer set search_path=public,pg_temp as $$
declare item jsonb; score integer; level text; path text[]; contents jsonb; rows_json jsonb;
begin
  item:=public.v2_locked_collection_feedback_item(p_workspace_id,p_session_id,p_field_id,p_row_id);
  score:=case p_sentiment when 'very_bullish' then 3 when 'very_bearish' then 3 when 'bullish' then 2 when 'bearish' then 2
    when 'positive' then 1 when 'negative' then 1 when 'neutral' then 0 end;
  if score is null then raise exception 'דירוג אינו תקין.' using errcode='23514';end if;
  level:=case score when 3 then 'most_important' when 2 then 'very_important' when 1 then 'important' else 'useful' end;
  if item->>'storage'='native' then
    update public.v2_news_items set importance=p_sentiment,rating_pending=false,updated_by=auth.uid() where id=(item#>>'{native,id}')::uuid;
  else
    select array_agg(value) into path from jsonb_array_elements_text(item->'path');
    if item->>'storage'='live' then select value into contents from public.v2_collection_live_values where id=(item->>'value_id')::uuid;
    else select value into contents from public.v2_collection_values where id=(item->>'value_id')::uuid;end if;
    rows_json:=jsonb_set(contents#>path,array[item->>'position'],(item->'embedded')||
      jsonb_build_object('importance',p_sentiment,'rating_pending',false,'updated_by',auth.uid(),'updated_at',now()));
    perform public.v2_write_collection_feedback_rows(item,rows_json);
  end if;
  perform public.v2_store_collection_news_feedback(item,'rate',score,level,p_sentiment,null);
end $$;

create or replace function public.v2_archive_collection_news_feedback(p_workspace_id uuid,p_session_id uuid,p_field_id uuid,p_row_id text,p_reason text,p_detail text default null)
returns jsonb language plpgsql security definer set search_path=public,pg_temp as $$
declare item jsonb; score integer; trash uuid; path text[]; contents jsonb; rows_json jsonb;
begin
  item:=public.v2_locked_collection_feedback_item(p_workspace_id,p_session_id,p_field_id,p_row_id);
  score:=case p_reason when 'borderline' then -1 when 'irrelevant' then -2 when 'completely_irrelevant' then -3 end;
  if score is null or length(p_detail)>1000 then raise exception 'סיבת המחיקה אינה תקינה.' using errcode='23514';end if;
  p_detail:=nullif(btrim(p_detail),'');
  insert into public.v2_collection_news_recycle_bin(workspace_id,session_id,field_id,row_id,sheet_key,logical_key,item_key,
    storage,snapshot,body,source_url,learning_source_url,source_name,importance,model_evaluation,reason,detail,deleted_by)
    values(p_workspace_id,p_session_id,p_field_id,item->>'row_id',item->>'sheet_key',item->>'logical_key',item->>'item_key',
      item->>'storage',item,item->>'body',item->>'source_url',item->>'learning_source_url',item->>'source_name',
      item->>'importance',nullif(item->'model_evaluation','null'::jsonb),p_reason,p_detail,auth.uid()) returning id into trash;
  perform public.v2_store_collection_news_feedback(item,'delete',score,p_reason,null,p_detail);
  if item->>'storage'='native' then
    delete from public.v2_news_items where id=(item#>>'{native,id}')::uuid;
  else
    select array_agg(value) into path from jsonb_array_elements_text(item->'path');
    if item->>'storage'='live' then select value into contents from public.v2_collection_live_values where id=(item->>'value_id')::uuid;
    else select value into contents from public.v2_collection_values where id=(item->>'value_id')::uuid;end if;
    select coalesce(jsonb_agg(entry order by ordinality),'[]'::jsonb) into rows_json
      from jsonb_array_elements(contents#>path) with ordinality as entries(entry,ordinality)
      where ordinality-1<>(item->>'position')::integer;
    perform public.v2_write_collection_feedback_rows(item,rows_json);
  end if;
  return jsonb_build_object('trash_id',trash);
end $$;
revoke all on function public.v2_rate_collection_news_feedback(uuid,uuid,uuid,text,text),
  public.v2_archive_collection_news_feedback(uuid,uuid,uuid,text,text,text) from public,anon;
grant execute on function public.v2_rate_collection_news_feedback(uuid,uuid,uuid,text,text),
  public.v2_archive_collection_news_feedback(uuid,uuid,uuid,text,text,text) to authenticated;

-- Preserve the reviewed native/embedded restore implementation; add the live
-- layout and ID-less legacy row conflict guard with checked exact anchors.
do $restore$
declare definition text; needle text; replacement text;
begin
  definition:=pg_get_functiondef('public.v2_restore_collection_news_feedback(uuid,uuid)'::regprocedure);
  if strpos(definition,'v2_feedback_restore_live')=0 then
    definition:=replace(definition,'n public.v2_news_items; v public.v2_collection_values; path text[];',
      'n public.v2_news_items; v public.v2_collection_values; live public.v2_collection_live_values; path text[];');
    needle:='select * into v from public.v2_collection_values where session_id=c.id and field_id=t.field_id for update;';
    replacement:=$live$
    -- v2_feedback_restore_live
    if t.storage='live' then
      select * into live from public.v2_collection_live_values where workspace_id=p_workspace_id and logical_key=t.logical_key for update;
      v.id:=live.id;v.value:=live.value;
    else
      select * into v from public.v2_collection_values where session_id=c.id and field_id=t.field_id for update;
    end if;
$live$;
    if strpos(definition,needle)=0 then raise exception 'Unexpected news restore lock anchor';end if;
    definition:=replace(definition,needle,replacement);
    needle:='update public.v2_collection_values set value=jsonb_set(v.value,path,restored_rows),updated_by=auth.uid() where id=v.id;';
    if strpos(definition,needle)=0 then raise exception 'Unexpected news restore write anchor';end if;
    definition:=replace(definition,needle,'perform public.v2_write_collection_feedback_rows(item,restored_rows);');
    definition:=replace(definition,$anchor$union all select item->'embedded',least$anchor$,
      'union all select (item->''embedded'')||jsonb_build_object(''id'',t.row_id),least');
  end if;
  if strpos(definition,'v2_feedback_restore_locked_value')=0 then
    needle:='perform public.v2_write_collection_feedback_rows(item,restored_rows);';
    if strpos(definition,needle)=0 then raise exception 'Unexpected news restore helper anchor';end if;
    definition:=replace(definition,needle,'-- v2_feedback_restore_locked_value'||chr(10)||
      'item:=item||jsonb_build_object(''value_id'',v.id);'||chr(10)||needle);
  end if;
  execute definition;
end $restore$;
notify pgrst,'reload schema';
