-- Yoav's collection/news contract. Additive read APIs and a new imported version;
-- no legacy answers, used templates, or workspace sharing are rewritten.
create or replace function public.v2_import_legacy_collection_template(p_workspace_id uuid)
returns uuid language plpgsql security invoker set search_path=public,pg_temp as $$
declare
  version_id uuid; imported_id uuid; section_id uuid; group_row record;
  section_position integer:=0; category_count integer; selected_count integer;
begin
  if auth.uid() is null or not coalesce(public.v2_can_edit_workspace(p_workspace_id),false) then
    raise exception 'workspace edit permission required' using errcode='42501';
  end if;
  perform pg_advisory_xact_lock(hashtextextended('v2-legacy-import:'||p_workspace_id::text,0));
  select active_template_version_id into version_id from public.app_settings order by updated_at desc limit 1;
  if version_id is null then return null; end if;
  select id into imported_id from public.v2_templates
  where workspace_id=p_workspace_id and kind='collection'
    and settings->>'legacy_import_version'='3'
    and settings->>'legacy_template_version_id'=version_id::text
  order by created_at,id limit 1;
  if imported_id is not null then return imported_id; end if;

  select count(*),count(distinct category) into selected_count,category_count from (
    select case when trim(g.title)='חדשות' then 'news' when trim(g.title)='מאקרו' then 'macro'
      when trim(g.title)='נזילות' then 'liquidity'
      when trim(g.title) ~ '^(טכני( ונתונים| / נתונים)?|נתונים וטכני|טכני, נתונים)$' then 'technical' end category
    from public.template_groups g join public.template_stages st on st.id=g.stage_id
    where st.template_version_id=version_id and st.active and g.active
  ) matched where category is not null;
  if selected_count<>4 or category_count<>4 then
    raise exception 'Legacy import requires exactly the four groups חדשות, מאקרו, נזילות, טכני ונתונים; found % groups / % categories',selected_count,category_count using errcode='23514';
  end if;

  -- Full source preservation includes field types whose UI is not implemented
  -- yet. Stage the backend structure without changing live template selection.
  insert into public.v2_templates(workspace_id,kind,name,active,is_default,settings,created_by)
  values(p_workspace_id,'collection','איסוף מידע – מהתבנית הקיימת',false,false,jsonb_build_object(
    'legacy_import',true,'legacy_import_version',3,'legacy_template_version_id',version_id,'imported_at',now(),
    'backend_import_ready',true,'activation_pending_ui',true),auth.uid())
  returning id into imported_id;
  for group_row in
    select g.*,st.id legacy_stage_id,to_jsonb(st) legacy_stage,
      case when trim(g.title) in ('חדשות','מאקרו') then 'news_macro' else 'data' end sheet_key
    from public.template_groups g join public.template_stages st on st.id=g.stage_id
    where st.template_version_id=version_id and st.active and g.active
      and (trim(g.title) in ('חדשות','מאקרו','נזילות') or trim(g.title) ~ '^(טכני( ונתונים| / נתונים)?|נתונים וטכני|טכני, נתונים)$')
    order by case when trim(g.title)='חדשות' then 0 when trim(g.title)='מאקרו' then 1 when trim(g.title)='נזילות' then 2 else 3 end
  loop
    if not exists(select 1 from public.template_questions q where q.group_id=group_row.id and q.active) then
      raise exception 'Legacy import group % contains no active questions',group_row.title using errcode='23514';
    end if;
    insert into public.v2_template_sections(template_id,title,position,settings)
    values(imported_id,group_row.title,section_position,jsonb_build_object(
      'sheet_key',group_row.sheet_key,'legacy_stage_id',group_row.legacy_stage_id,'legacy_group_id',group_row.id,
      'description',group_row.description,'behavior',group_row.behavior,'legacy_stage',group_row.legacy_stage,
      'legacy_group',to_jsonb(group_row)-'legacy_stage'-'sheet_key'-'legacy_stage_id')) returning id into section_id;
    insert into public.v2_template_fields(section_id,field_key,title,field_type,position,required,default_align,pinned_scope,active,settings)
    select section_id,coalesce(nullif(q.question_key,''),nullif(q.field_key,''),'legacy')||'_'||q.id::text,q.title,
      public.v2_legacy_field_type(q.question_type,q.field_key),
      (row_number() over(order by q.position,q.id)-1)::integer,q.required,
      case when q.default_text_align='left' then 'left' else 'right' end,
      case when to_jsonb(q)->>'pinned_scope' in ('all','template') then 'template'
           when to_jsonb(q)->>'pinned_scope' in ('stage','section') then 'section' else 'none' end,q.active,
      jsonb_build_object('legacy_question_id',q.id,'legacy_question_key',q.question_key,
        'question_key',q.question_key,'legacy_field_key',q.field_key,'legacy_question',to_jsonb(q),
        'description',q.description,'help_text',q.help_text,'links',coalesce(q.links_json,'[]'::jsonb),
        'options_json',q.options_json,'visibility_rule_json',q.visibility_rule_json,'links_json',q.links_json,'source_url',q.source_url,
        'original_required',q.required,'legacy_pinned_stage_id',to_jsonb(q)->'pinned_stage_id')
    from public.template_questions q where q.group_id=group_row.id;
    section_position:=section_position+1;
  end loop;
  return imported_id;
end $$;
revoke all on function public.v2_import_legacy_collection_template(uuid) from public,anon;
grant execute on function public.v2_import_legacy_collection_template(uuid) to authenticated;

create or replace function public.v2_collection_field_logical_key(p_field jsonb)
returns text language plpgsql stable security invoker set search_path=public,pg_temp as $$
declare result text; legacy_id text;
begin
  result:=coalesce(nullif(p_field#>>'{settings,question_key}',''),nullif(p_field#>>'{settings,legacy_question_key}',''));
  if result is not null then return result; end if;
  legacy_id:=p_field#>>'{settings,legacy_question_id}';
  -- Read old v2 imports through their original stable question identity without
  -- mutating immutable versions to add a previously omitted metadata key.
  if legacy_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     and to_regclass('public.template_questions') is not null then
    select coalesce(nullif(q.question_key,''),nullif(q.field_key,'')) into result
    from public.template_questions q where q.id=legacy_id::uuid;
  end if;
  return coalesce(result,nullif(p_field->>'field_key',''),p_field->>'id');
end $$;
revoke all on function public.v2_collection_field_logical_key(jsonb) from public,anon;
grant execute on function public.v2_collection_field_logical_key(jsonb) to authenticated;

create or replace function public.v2_collection_value_filled(p_type text,p_value jsonb)
returns boolean language plpgsql immutable set search_path=public,pg_temp as $$
declare row_value jsonb; level_value jsonb; content text;
begin
  if p_value is null or p_value='null'::jsonb then return false; end if;
  -- Presence differs from completion: an explicitly saved unchecked checkbox
  -- must supersede its older checked answer. The shared calendar has no local answer.
  if p_type='checkbox' then return coalesce(jsonb_typeof(p_value->'checked')='boolean',false); end if;
  if p_type='economic_calendar' then return false; end if;
  if p_type='image' then
    return (case when jsonb_typeof(p_value->'images')='array' then jsonb_array_length(p_value->'images')>0 else false end)
      or coalesce(p_value->>'text','') ~* '<img[[:space:]]'
      or coalesce(p_value->>'html','') ~* '<img[[:space:]]';
  end if;
  if p_value->>'completion'='na' then return true; end if;
  if p_type='yes_no_na' then return coalesce(p_value->>'choice','') in ('yes','no','na'); end if;
  if p_type in ('news_table','news_source_table','liquidity_table') then
    for row_value in select value from jsonb_array_elements(case when jsonb_typeof(p_value->'rows')='array' then p_value->'rows' else '[]'::jsonb end) loop
      if p_type in ('news_table','news_source_table') and nullif(btrim(coalesce(row_value->>'text',row_value->>'body','')),'') is not null then return true; end if;
      if p_type='liquidity_table' then
        if nullif(btrim(coalesce(row_value->>'price','')),'') is not null then return true; end if;
        for level_value in select value from jsonb_array_elements(
          (case when jsonb_typeof(row_value->'above')='array' then row_value->'above' else '[]'::jsonb end)||
          (case when jsonb_typeof(row_value->'below')='array' then row_value->'below' else '[]'::jsonb end)) loop
          if nullif(btrim(coalesce(level_value->>'price',level_value->>'price_text','')),'') is not null then return true; end if;
        end loop;
      end if;
    end loop;
    return false;
  end if;
  if p_type='multi_choice' then
    return (jsonb_typeof(p_value->'choices')='array' and p_value->'choices'<>'[]'::jsonb)
      or nullif(btrim(coalesce(p_value->>'text','')),'') is not null;
  end if;
  content:=coalesce(p_value->>'html',p_value->>'text',p_value->>'choice',p_value->>'date','');
  if content ~* '<img[[:space:]]' then return true; end if;
  content:=regexp_replace(content,'<[^>]*>','','g');
  -- Empty editors may persist invisible Unicode or its HTML entity encoding.
  -- Strip both forms before ranking a cell as the latest meaningful answer.
  content:=regexp_replace(content,'&(?:nbsp|#0*160|#x0*a0|ZeroWidthSpace|zwnj|zwj|#0*820[3-5]|#x0*200[b-d]|#0*65279|#x0*feff);','','gi');
  content:=regexp_replace(content,U&'[[:space:]\00A0\200B-\200D\FEFF]','','g');
  return content<>'';
end $$;
revoke all on function public.v2_collection_value_filled(text,jsonb) from public,anon;
grant execute on function public.v2_collection_value_filled(text,jsonb) to authenticated;

create or replace function public.v2_collection_snapshot(
  p_workspace_id uuid,p_mode text default 'latest_filled',p_session_id uuid default null)
returns table(sheet_key text,logical_key text,field_id uuid,field jsonb,section jsonb,
  session_id uuid,session_created_at timestamptz,value jsonb,news_items jsonb,updated_at timestamptz,updated_by uuid,
  snapshot_key text)
language plpgsql stable security invoker set search_path=public,pg_temp as $$
declare target_session uuid;
begin
  if auth.uid() is null or not coalesce(public.v2_is_workspace_member(p_workspace_id),false) then
    raise exception 'workspace membership required' using errcode='42501';
  end if;
  if p_mode not in ('latest_filled','last_session','session') or (p_mode='session' and p_session_id is null) then
    raise exception 'invalid collection snapshot mode' using errcode='22023';
  end if;
  if p_mode='last_session' then
    select s.id into target_session from public.v2_collection_sessions s
    where s.workspace_id=p_workspace_id order by s.created_at desc,s.id desc limit 1;
  elsif p_mode='session' then
    select s.id into target_session from public.v2_collection_sessions s where s.id=p_session_id and s.workspace_id=p_workspace_id;
    if target_session is null then raise exception 'collection not found in workspace' using errcode='42501'; end if;
  end if;
  return query with candidates as (
    select case when sec.settings->>'sheet_key' in ('news_macro','data') then sec.settings->>'sheet_key'
                when sec.title ~ '(חדשות|מאקרו)' then 'news_macro' else 'data' end grp,
      public.v2_collection_field_logical_key(to_jsonb(f)) logical,
      f.id fid,to_jsonb(f) field_json,to_jsonb(sec) section_json,s.id sid,s.created_at session_date,
      coalesce(v.value,'{}'::jsonb) contents,coalesce(n.items,'[]'::jsonb) news,
      greatest(case when public.v2_collection_value_filled(f.field_type,v.value) then v.updated_at end,n.last_update) stamp,
      case when n.last_update is not null and (not coalesce(public.v2_collection_value_filled(f.field_type,v.value),false) or n.last_update>=v.updated_at)
        then n.last_editor else v.updated_by end editor
    from public.v2_collection_sessions s
    join public.v2_template_sections sec on sec.template_id=s.template_id and sec.active
    join public.v2_template_fields f on f.section_id=sec.id and f.active
    left join public.v2_collection_values v on v.session_id=s.id and v.field_id=f.id
    left join lateral (
      select jsonb_agg(to_jsonb(ni)||jsonb_build_object('source',to_jsonb(src)) order by ni.occurred_at,ni.id) items,
        max(ni.updated_at) last_update,(array_agg(ni.updated_by order by ni.updated_at desc,ni.id desc))[1] last_editor
      from public.v2_news_items ni left join public.v2_news_sources src on src.id=ni.source_id
      where ni.collection_session_id=s.id and ni.field_id=f.id and nullif(btrim(ni.body),'') is not null
    ) n on true
    where s.workspace_id=p_workspace_id and (p_mode='latest_filled' or s.id=target_session)
      and (public.v2_collection_value_filled(f.field_type,v.value) or n.last_update is not null)
  ), ranked as (
    select c.*,row_number() over(partition by c.grp,c.logical order by c.stamp desc,c.session_date desc,c.sid desc,c.fid desc) rnk from candidates c
  ) select r.grp,r.logical,r.fid,r.field_json,r.section_json,r.sid,r.session_date,r.contents,r.news,r.stamp,r.editor,
      r.grp||':'||r.logical
    from ranked r where r.rnk=1 order by r.grp,(r.section_json->>'position')::integer,(r.field_json->>'position')::integer,r.fid;
end $$;
revoke all on function public.v2_collection_snapshot(uuid,text,uuid) from public,anon;
grant execute on function public.v2_collection_snapshot(uuid,text,uuid) to authenticated;

-- This feed is a caller-authorized projection, never an ingestion into a shared
-- workspace. IDs outside v2_news_items are namespaced and explicitly read-only.
-- PostgREST can filter worksheet_created_at and importance and page/order by id.
create or replace function public.v2_news_feed(p_workspace_id uuid)
returns table(id text,workspace_id uuid,collection_session_id uuid,field_id uuid,source_id text,
  body text,importance text,occurred_at timestamptz,created_by uuid,updated_by uuid,
  created_at timestamptz,updated_at timestamptz,source jsonb,source_name text,origin text,
  read_only boolean,project_id uuid,trade_id uuid,question_id uuid,worksheet_created_at timestamptz,
  entity_id uuid,raw_content jsonb)
language plpgsql stable security invoker set search_path=public,pg_temp as $$
begin
  if auth.uid() is null or not coalesce(public.v2_is_workspace_member(p_workspace_id),false) then
    raise exception 'workspace membership required' using errcode='42501';
  end if;
  return query
    select n.id::text,n.workspace_id,n.collection_session_id,n.field_id,
      coalesce(n.source_id::text,case when f.id is not null then 'field:'||public.v2_collection_field_logical_key(to_jsonb(f)) end),
      n.body,n.importance,n.occurred_at,n.created_by,n.updated_by,n.created_at,n.updated_at,to_jsonb(src),
      coalesce(src.name,f.title,'ללא מקור'),'collection_news'::text,false,null::uuid,null::uuid,null::uuid,
      coalesce(c.created_at,n.created_at),n.collection_session_id,to_jsonb(n)
    from public.v2_news_items n left join public.v2_news_sources src on src.id=n.source_id
    left join public.v2_template_fields f on f.id=n.field_id
    left join public.v2_collection_sessions c on c.id=n.collection_session_id
    where n.workspace_id=p_workspace_id and nullif(btrim(n.body),'') is not null;
  return query
    with values_to_read as (
      select 'trade_value'::text entity_kind,v.id value_id,t.workspace_id,t.id parent_id,t.id trade_entity,
        t.created_at sheet_date,t.created_by,v.field_id,v.value,v.created_at,v.updated_at,v.updated_by
      from public.v2_trade_values v join public.v2_trades t on t.id=v.trade_id where t.workspace_id=p_workspace_id
      union all
      select 'collection_value',v.id,c.workspace_id,c.id,null::uuid,c.created_at,c.created_by,
        v.field_id,v.value,v.created_at,v.updated_at,v.updated_by
      from public.v2_collection_values v join public.v2_collection_sessions c on c.id=v.session_id
      where c.workspace_id=p_workspace_id
      union all
      select 'scenario_value',v.id,s.workspace_id,s.id,null::uuid,s.created_at,s.created_by,
        v.field_id,v.value,v.created_at,v.updated_at,v.updated_by
      from public.v2_scenario_values v join public.v2_scenarios s on s.id=v.scenario_id where s.workspace_id=p_workspace_id
      union all
      select 'spot_value',v.id,a.workspace_id,a.id,null::uuid,a.created_at,a.created_by,
        v.field_id,v.value,v.created_at,v.updated_at,v.updated_by
      from public.v2_spot_values v join public.v2_spot_assets a on a.id=v.asset_id where a.workspace_id=p_workspace_id
    )
    select v.entity_kind||':'||v.value_id::text||':'||row.ordinality::text,v.workspace_id,
      null::uuid,v.field_id,coalesce(src.id::text,
        case when nullif(btrim(row.entry->>'source'),'') is not null then 'source-name:'||lower(btrim(row.entry->>'source'))
          else 'field:'||public.v2_collection_field_logical_key(to_jsonb(f)) end),
      coalesce(row.entry->>'text',row.entry->>'body'),
      case when row.entry->>'importance' in ('very_bullish','bullish','positive','neutral','negative','bearish','very_bearish')
        then row.entry->>'importance' else 'neutral' end,
      v.updated_at,null::uuid,v.updated_by,v.created_at,v.updated_at,to_jsonb(src),
      coalesce(src.name,nullif(btrim(row.entry->>'source'),''),f.title),v.entity_kind,true,null::uuid,v.trade_entity,null::uuid,v.sheet_date,
      v.parent_id,row.entry
    from values_to_read v join public.v2_template_fields f on f.id=v.field_id
    cross join lateral jsonb_array_elements(case when jsonb_typeof(v.value->'rows')='array' then v.value->'rows' else '[]'::jsonb end)
      with ordinality row(entry,ordinality)
    left join lateral (
      select ns.* from public.v2_news_sources ns where ns.workspace_id=p_workspace_id
        and lower(ns.name)=lower(btrim(coalesce(nullif(btrim(row.entry->>'source'),''),f.title)))
      order by ns.created_at,ns.id limit 1
    ) src on true
    where f.field_type in ('news_table','news_source_table') and nullif(btrim(coalesce(row.entry->>'text',row.entry->>'body','')),'') is not null
      -- A JSON row is a duplicate only when its original ID is explicitly the
      -- same normalized record in this session/field. Equal body text is not proof.
      and not (v.entity_kind='collection_value' and exists(select 1 from public.v2_news_items n
        where n.collection_session_id=v.parent_id and n.field_id=v.field_id and n.id::text=row.entry->>'id'));
  if to_regclass('public.project_news_rows') is not null then
    return query
      select 'legacy:'||n.id::text,p_workspace_id,null::uuid,null::uuid,
        'legacy-question:'||coalesce(nullif(q.question_key,''),q.id::text),
        n.body,n.importance,n.created_at,null::uuid,n.updated_by,n.created_at,n.updated_at,null::jsonb,
        coalesce(q.title,'ללא מקור'),'legacy_project'::text,true,n.project_id,null::uuid,q.id,p.created_at,n.project_id,to_jsonb(n)
      from public.project_news_rows n join public.projects p on p.id=n.project_id
      join public.template_questions q on q.id=n.question_id
      where nullif(btrim(n.body),'') is not null;
  end if;
end $$;
revoke all on function public.v2_news_feed(uuid) from public,anon;
grant execute on function public.v2_news_feed(uuid) to authenticated;
notify pgrst,'reload schema';
