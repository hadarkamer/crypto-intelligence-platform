-- Unified news history preserves text source boxes as well as normalized news rows.
-- SECURITY INVOKER keeps each legacy project subject to its original RLS.
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
        coalesce(src.id::text,'field:'||coalesce(nullif(q.question_key,''),nullif(q.field_key,''),q.id::text)),
        n.body,n.importance,n.created_at,null::uuid,n.updated_by,n.created_at,n.updated_at,to_jsonb(src),
        coalesce(src.name,q.title,'ללא מקור'),'legacy_project'::text,true,n.project_id,null::uuid,q.id,p.created_at,n.project_id,to_jsonb(n)
      from public.project_news_rows n join public.projects p on p.id=n.project_id
      join public.template_questions q on q.id=n.question_id
      left join lateral (select ns.* from public.v2_news_sources ns where ns.workspace_id=p_workspace_id
        and lower(btrim(ns.name))=lower(btrim(q.title)) order by ns.created_at,ns.id limit 1) src on true
      where nullif(btrim(n.body),'') is not null;
  end if;
  -- Source summaries such as Unbias/Joe Consorti/Micha Stocks were saved as
  -- rich text in the original news and macro sections. Include only these
  -- semantic source boxes; other worksheets, info and checkbox fields are not news.
  return query
    select 'collection-text:'||v.id::text,c.workspace_id,null::uuid,f.id,
      coalesce(src.id::text,'field:'||public.v2_collection_field_logical_key(to_jsonb(f))),
      coalesce(nullif(v.value->>'html',''),v.value->>'text',''),
      case when v.value->>'importance' in ('very_bullish','bullish','positive','neutral','negative','bearish','very_bearish')
        then v.value->>'importance' else 'neutral' end,
      v.created_at,null::uuid,v.updated_by,v.created_at,v.updated_at,to_jsonb(src),
      coalesce(src.name,f.title),'collection_value'::text,true,null::uuid,null::uuid,null::uuid,
      c.created_at,c.id,v.value
    from public.v2_collection_values v join public.v2_collection_sessions c on c.id=v.session_id
    join public.v2_template_fields f on f.id=v.field_id
    join public.v2_template_sections s on s.id=f.section_id
    left join lateral (select ns.* from public.v2_news_sources ns where ns.workspace_id=p_workspace_id
      and lower(btrim(ns.name))=lower(btrim(f.title)) order by ns.created_at,ns.id limit 1) src on true
    where c.workspace_id=p_workspace_id and f.field_type in ('long_text','rich_text','short_text')
      and (btrim(s.title) in ('חדשות','מאקרו') or exists(
        select 1 from jsonb_array_elements(case when jsonb_typeof(s.settings->'groups')='array'
          then s.settings->'groups' else '[]'::jsonb end) as g
        where g->>'id'=f.settings->>'group_id' and btrim(g->>'title') in ('חדשות','מאקרו')))
      and public.v2_collection_value_filled(f.field_type,jsonb_build_object('html',coalesce(nullif(v.value->>'html',''),v.value->>'text','')));
  if to_regclass('public.project_answers') is not null then
    return query
      select 'legacy-text:'||a.id::text,p_workspace_id,null::uuid,null::uuid,
        coalesce(src.id::text,'field:'||coalesce(nullif(q.question_key,''),nullif(q.field_key,''),q.id::text)),
        a.text_value,'neutral'::text,a.created_at,null::uuid,a.last_edited_by,
        a.created_at,a.updated_at,to_jsonb(src),coalesce(src.name,q.title),'legacy_project'::text,true,
        p.id,null::uuid,q.id,p.created_at,p.id,to_jsonb(a)
      from public.project_answers a join public.projects p on p.id=a.project_id
      join public.template_questions q on q.id=a.question_id
      join public.template_groups g on g.id=q.group_id join public.template_stages s on s.id=g.stage_id
      left join lateral (select ns.* from public.v2_news_sources ns where ns.workspace_id=p_workspace_id
        and lower(btrim(ns.name))=lower(btrim(q.title)) order by ns.created_at,ns.id limit 1) src on true
      where q.question_type in ('long_text','rich_text','short_text') and q.active and g.active
        and btrim(g.title) in ('חדשות','מאקרו')
        and s.id=(select first_stage.id from public.template_stages first_stage
          where first_stage.template_version_id=p.template_version_id and first_stage.active
          order by first_stage.position,first_stage.id limit 1)
        and a.trade_cycle_id=(select cycle.id from public.project_trade_cycles cycle
          where cycle.project_id=p.id order by cycle.cycle_number,cycle.id limit 1)
        and public.v2_collection_value_filled(q.question_type,jsonb_build_object('html',a.text_value));
  end if;
end $$;
revoke all on function public.v2_news_feed(uuid) from public,anon;
grant execute on function public.v2_news_feed(uuid) to authenticated;
notify pgrst,'reload schema';

-- Copied/custom sheets have independent identities even when they retain the same question history key.
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
    select case when jsonb_typeof(sec.settings->'sheet_key')='string' and nullif(btrim(sec.settings->>'sheet_key'),'') is not null
                  then sec.settings->>'sheet_key'
                when jsonb_typeof(sec.settings->'groups')='array' or jsonb_typeof(sec.settings->'legacy_groups')='array'
                  then 'sheet:'||sec.id::text
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

notify pgrst,'reload schema';
