-- Personal summary visibility/filter preferences do not mutate original worksheets.
create table if not exists public.v2_news_summary_hidden (
  workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
  user_id uuid not null default auth.uid() references auth.users(id) on delete cascade,
  item_id text not null,
  hidden_at timestamptz not null default now(),
  primary key(workspace_id,user_id,item_id)
);
create table if not exists public.v2_news_summary_preferences (
  workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
  user_id uuid not null default auth.uid() references auth.users(id) on delete cascade,
  source_lists jsonb not null default '[]'::jsonb check(jsonb_typeof(source_lists)='array'),
  selected_source_ids jsonb check(selected_source_ids is null or jsonb_typeof(selected_source_ids)='array'),
  primary key(workspace_id,user_id)
);
alter table public.v2_news_summary_hidden enable row level security;
alter table public.v2_news_summary_preferences enable row level security;
grant select,insert,update,delete on public.v2_news_summary_hidden,public.v2_news_summary_preferences to authenticated;
grant all on public.v2_news_summary_hidden,public.v2_news_summary_preferences to service_role;
drop policy if exists news_summary_hidden_own on public.v2_news_summary_hidden;
create policy news_summary_hidden_own on public.v2_news_summary_hidden for all to authenticated
using(user_id=auth.uid() and public.v2_is_workspace_member(workspace_id))
with check(user_id=auth.uid() and public.v2_is_workspace_member(workspace_id));
drop policy if exists news_summary_preferences_own on public.v2_news_summary_preferences;
create policy news_summary_preferences_own on public.v2_news_summary_preferences for all to authenticated
using(user_id=auth.uid() and public.v2_is_workspace_member(workspace_id))
with check(user_id=auth.uid() and public.v2_is_workspace_member(workspace_id));

create or replace function public.v2_hide_news_summary_item(p_workspace_id uuid,p_item_id text)
returns void language plpgsql security invoker set search_path=public,pg_temp as $$
declare item record; item_key text;
begin
  if auth.uid() is null or not coalesce(public.v2_is_workspace_member(p_workspace_id),false) then
    raise exception 'workspace membership required' using errcode='42501';
  end if;
  select * into item from public.v2_news_feed(p_workspace_id) where id=p_item_id;
  if not found then raise exception 'News source not accessible' using errcode='42501'; end if;
  item_key:=p_item_id;
  if item.origin in ('collection_value','trade_value','scenario_value','spot_value')
    and p_item_id like item.origin||':%' and nullif(item.raw_content->>'id','') is not null then
    item_key:=split_part(p_item_id,':',1)||':'||split_part(p_item_id,':',2)||':row:'||(item.raw_content->>'id');
  end if;
  insert into public.v2_news_summary_hidden(workspace_id,user_id,item_id)
    values(p_workspace_id,auth.uid(),item_key) on conflict do nothing;
end $$;
revoke all on function public.v2_hide_news_summary_item(uuid,text) from public,anon;
grant execute on function public.v2_hide_news_summary_item(uuid,text) to authenticated;

create or replace function public.v2_save_news_summary_preferences(p_workspace_id uuid,p_patch jsonb)
returns void language plpgsql security invoker set search_path=public,pg_temp as $$
declare list_entry jsonb;
begin
  if auth.uid() is null or not coalesce(public.v2_is_workspace_member(p_workspace_id),false) then
    raise exception 'workspace membership required' using errcode='42501';
  end if;
  if jsonb_typeof(p_patch) is distinct from 'object'
    or exists(select 1 from jsonb_object_keys(p_patch) k where k not in ('source_lists','selected_source_ids')) then
    raise exception 'Invalid summary preferences' using errcode='22023';
  end if;
  if p_patch ? 'source_lists' then
    if jsonb_typeof(p_patch->'source_lists') is distinct from 'array' then
      raise exception 'Invalid source lists' using errcode='22023';
    end if;
    for list_entry in select value from jsonb_array_elements(p_patch->'source_lists') loop
      if jsonb_typeof(list_entry) is distinct from 'object' or jsonb_typeof(list_entry->'id') is distinct from 'string'
        or nullif(btrim(list_entry->>'id'),'') is null or jsonb_typeof(list_entry->'name') is distinct from 'string'
        or nullif(btrim(list_entry->>'name'),'') is null or jsonb_typeof(list_entry->'sourceIds') is distinct from 'array' then
        raise exception 'Invalid source list' using errcode='22023';
      end if;
      if exists(select 1 from jsonb_array_elements(list_entry->'sourceIds') id where jsonb_typeof(id)<>'string') then
        raise exception 'Invalid source identifiers' using errcode='22023';
      end if;
    end loop;
  end if;
  if p_patch ? 'selected_source_ids' and p_patch->'selected_source_ids'<>'null'::jsonb then
    if jsonb_typeof(p_patch->'selected_source_ids') is distinct from 'array' then
      raise exception 'Invalid selected sources' using errcode='22023';
    end if;
    if exists(select 1 from jsonb_array_elements(p_patch->'selected_source_ids') id where jsonb_typeof(id)<>'string') then
      raise exception 'Invalid selected source identifiers' using errcode='22023';
    end if;
  end if;
  insert into public.v2_news_summary_preferences(workspace_id,user_id,source_lists,selected_source_ids)
  values(p_workspace_id,auth.uid(),coalesce(p_patch->'source_lists','[]'::jsonb),nullif(p_patch->'selected_source_ids','null'::jsonb))
  on conflict(workspace_id,user_id) do update set
    source_lists=case when p_patch ? 'source_lists' then excluded.source_lists else v2_news_summary_preferences.source_lists end,
    selected_source_ids=case when p_patch ? 'selected_source_ids' then excluded.selected_source_ids else v2_news_summary_preferences.selected_source_ids end;
end $$;
revoke all on function public.v2_save_news_summary_preferences(uuid,jsonb) from public,anon;
grant execute on function public.v2_save_news_summary_preferences(uuid,jsonb) to authenticated;

-- Source edits retain their original storage and original RLS. Locks and expected
-- values make summary edits safe against simultaneous worksheet edits. Unchanged
-- fields (and sibling JSON rows) are always preserved from the current database row.
create or replace function public.v2_update_news_summary_item(
  p_workspace_id uuid,p_item_id text,p_patch jsonb,p_expected jsonb)
returns void language plpgsql security invoker set search_path=public,pg_temp as $$
declare item record; record_id uuid; contents jsonb; answer_meta jsonb;
  current_body text; current_importance text; value_table text; parent_table text; parent_column text;
  entry jsonb; target jsonb; rows_json jsonb; ordinal integer; target_index integer; candidates integer;
  target_row_id text; is_text boolean:=false; changed integer;
begin
  if auth.uid() is null or not coalesce(public.v2_can_edit_workspace(p_workspace_id),false) then
    raise exception 'workspace edit permission required' using errcode='42501';
  end if;
  if jsonb_typeof(p_patch) is distinct from 'object' or jsonb_typeof(p_expected) is distinct from 'object'
    or p_patch='{}'::jsonb
    or exists(select 1 from jsonb_object_keys(p_patch) k where k not in ('body','importance'))
    or (p_patch ? 'body' and jsonb_typeof(p_patch->'body') is distinct from 'string')
    or (p_patch ? 'importance' and (jsonb_typeof(p_patch->'importance') is distinct from 'string'
      or p_patch->>'importance' not in ('very_bullish','bullish','positive','neutral','negative','bearish','very_bearish')))
    or (p_patch ? 'body' and not p_expected ? 'body')
    or (p_patch ? 'importance' and not p_expected ? 'importance') then
    raise exception 'Invalid news patch' using errcode='22023';
  end if;
  select * into item from public.v2_news_feed(p_workspace_id) where id=p_item_id;
  if not found then raise exception 'News source not accessible' using errcode='42501'; end if;
  if item.origin='collection_news' then
    record_id:=p_item_id::uuid;
    select n.body,n.importance into current_body,current_importance from public.v2_news_items n
      where n.id=record_id and n.workspace_id=p_workspace_id for update;
    if not found then raise exception 'News edit permission required' using errcode='42501'; end if;
  elsif item.origin='legacy_project' then
    if not coalesce(app_private.can_edit_project(item.project_id),false) then
      raise exception 'Original project edit permission required' using errcode='42501';
    end if;
    record_id:=split_part(p_item_id,':',2)::uuid;
    is_text:=p_item_id like 'legacy-text:%';
    if is_text then
      select a.text_value,a.choice_value_json into current_body,answer_meta from public.project_answers a
        where a.id=record_id and a.project_id=item.project_id for update;
      if not found then raise exception 'Original answer not accessible' using errcode='42501'; end if;
      current_importance:=coalesce(answer_meta->>'news_summary_importance','neutral');
    else
      select n.body,n.importance into current_body,current_importance from public.project_news_rows n
        where n.id=record_id and n.project_id=item.project_id for update;
      if not found then raise exception 'Original news not accessible' using errcode='42501'; end if;
    end if;
  elsif item.origin in ('collection_value','trade_value','scenario_value','spot_value') then
    record_id:=split_part(p_item_id,':',2)::uuid;
    case item.origin
      when 'collection_value' then value_table:='v2_collection_values';parent_table:='v2_collection_sessions';parent_column:='session_id';
      when 'trade_value' then value_table:='v2_trade_values';parent_table:='v2_trades';parent_column:='trade_id';
      when 'scenario_value' then value_table:='v2_scenario_values';parent_table:='v2_scenarios';parent_column:='scenario_id';
      when 'spot_value' then value_table:='v2_spot_values';parent_table:='v2_spot_assets';parent_column:='asset_id';
    end case;
    -- Same parent-before-value lock order as patch_v2_entity_rows.
    execute format('select id from public.%I where id=$1 and workspace_id=$2 for update',parent_table)
      using item.entity_id,p_workspace_id;
    execute format('select value from public.%I where id=$1 and %I=$2 and field_id=$3 for update',value_table,parent_column)
      into contents using record_id,item.entity_id,item.field_id;
    if contents is null then raise exception 'Original value not accessible' using errcode='42501'; end if;
    is_text:=p_item_id like 'collection-text:%';
    if is_text then
      current_body:=coalesce(nullif(contents->>'html',''),contents->>'text','');
      current_importance:=coalesce(contents->>'importance','neutral');
    else
      rows_json:=contents->'rows';
      if jsonb_typeof(rows_json) is distinct from 'array' then
        raise exception 'News rows changed; reload' using errcode='40001';
      end if;
      target_row_id:=p_expected->>'row_id';
      if nullif(target_row_id,'') is not null then
        select count(*),min(n.ordinality::integer-1) into candidates,target_index
          from jsonb_array_elements(rows_json) with ordinality n(entry,ordinality) where n.entry->>'id'=target_row_id;
        if candidates<>1 then raise exception 'News row changed; reload' using errcode='40001'; end if;
      else
        target_index:=split_part(p_item_id,':',3)::integer-1;
      end if;
      target:=rows_json->target_index;
      if target is null then raise exception 'News row changed; reload' using errcode='40001'; end if;
      current_body:=coalesce(target->>'text',target->>'body','');
      current_importance:=coalesce(target->>'importance','neutral');
    end if;
  else
    raise exception 'Unsupported news origin' using errcode='22023';
  end if;
  if (p_patch ? 'body' and current_body is distinct from p_expected->>'body')
    or (p_patch ? 'importance' and current_importance is distinct from p_expected->>'importance') then
    raise exception 'News changed; reload before editing' using errcode='40001';
  end if;
  if item.origin='collection_news' then
    update public.v2_news_items set
      body=case when p_patch ? 'body' then p_patch->>'body' else body end,
      importance=case when p_patch ? 'importance' then p_patch->>'importance' else importance end,
      updated_by=auth.uid() where id=record_id and workspace_id=p_workspace_id;
  elsif item.origin='legacy_project' then
    if is_text then
      -- Non-object answer metadata cannot be overwritten just to store importance.
      if p_patch ? 'importance' and answer_meta is not null and jsonb_typeof(answer_meta)<>'object' then
        raise exception 'Answer metadata must be retained; importance cannot be changed here' using errcode='23514';
      end if;
      update public.project_answers set
        text_value=case when p_patch ? 'body' then p_patch->>'body' else text_value end,
        choice_value_json=case when p_patch ? 'importance' then coalesce(choice_value_json,'{}'::jsonb)
          ||jsonb_build_object('news_summary_importance',p_patch->'importance') else choice_value_json end,
        last_edited_by=auth.uid(),updated_at=now() where id=record_id and project_id=item.project_id;
    else
      update public.project_news_rows set
        body=case when p_patch ? 'body' then p_patch->>'body' else body end,
        importance=case when p_patch ? 'importance' then p_patch->>'importance' else importance end,
        updated_by=auth.uid() where id=record_id and project_id=item.project_id;
    end if;
  else
    if is_text then
      if p_patch ? 'body' then
        if contents ? 'html' then contents:=contents||jsonb_build_object('html',p_patch->'body'); end if;
        if contents ? 'text' or not contents ? 'html' then contents:=contents||jsonb_build_object('text',p_patch->'body'); end if;
      end if;
      if p_patch ? 'importance' then contents:=contents||jsonb_build_object('importance',p_patch->'importance'); end if;
    else
      if p_patch ? 'body' then
        if target ? 'text' then target:=target||jsonb_build_object('text',p_patch->'body'); end if;
        if target ? 'body' or not target ? 'text' then target:=target||jsonb_build_object('body',p_patch->'body'); end if;
      end if;
      if p_patch ? 'importance' then target:=target||jsonb_build_object('importance',p_patch->'importance'); end if;
      rows_json:=jsonb_set(rows_json,array[target_index::text],target);
      contents:=contents||jsonb_build_object('rows',rows_json);
      if jsonb_typeof(contents#>'{structured,rows}')='array' then contents:=jsonb_set(contents,'{structured,rows}',rows_json); end if;
    end if;
    execute format('update public.%I set value=$1,updated_by=auth.uid(),updated_at=now() where id=$2 and %I=$3',value_table,parent_column)
      using contents,record_id,item.entity_id;
  end if;
  get diagnostics changed=row_count;
  if changed<>1 then raise exception 'Original news edit was not permitted' using errcode='42501'; end if;
end $$;
revoke all on function public.v2_update_news_summary_item(uuid,text,jsonb,jsonb) from public,anon;
grant execute on function public.v2_update_news_summary_item(uuid,text,jsonb,jsonb) to authenticated;

-- Preserve all feed branches and original-source RLS; only expose explicitly saved legacy text importance.
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
    where n.workspace_id=p_workspace_id and nullif(btrim(n.body),'') is not null
      and (n.collection_session_id is null or (c.deleted_at is null and c.status<>'archived'));
  return query
    with values_to_read as (
      select 'trade_value'::text entity_kind,v.id value_id,t.workspace_id,t.id parent_id,t.id trade_entity,
        t.created_at sheet_date,t.created_by,v.field_id,v.value,v.created_at,v.updated_at,v.updated_by
      from public.v2_trade_values v join public.v2_trades t on t.id=v.trade_id where t.workspace_id=p_workspace_id
      union all
      select 'collection_value',v.id,c.workspace_id,c.id,null::uuid,c.created_at,c.created_by,
        v.field_id,v.value,v.created_at,v.updated_at,v.updated_by
      from public.v2_collection_values v join public.v2_collection_sessions c on c.id=v.session_id
      where c.workspace_id=p_workspace_id and c.deleted_at is null and c.status<>'archived'
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
        coalesce(src.name,q.title,'ללא מקור'),'legacy_project'::text,true,n.project_id,null::uuid,q.id,p.created_at,n.project_id,to_jsonb(n)||jsonb_build_object('summary_can_edit',coalesce(app_private.can_edit_project(p.id),false))
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
    where c.workspace_id=p_workspace_id and c.deleted_at is null and c.status<>'archived' and f.field_type in ('long_text','rich_text','short_text')
      and (btrim(s.title) in ('חדשות','מאקרו') or exists(
        select 1 from jsonb_array_elements(case when jsonb_typeof(s.settings->'groups')='array'
          then s.settings->'groups' else '[]'::jsonb end) as g
        where g->>'id'=f.settings->>'group_id' and btrim(g->>'title') in ('חדשות','מאקרו')))
      and public.v2_collection_value_filled(f.field_type,jsonb_build_object('html',coalesce(nullif(v.value->>'html',''),v.value->>'text','')));
  if to_regclass('public.project_answers') is not null then
    return query
      select 'legacy-text:'||a.id::text,p_workspace_id,null::uuid,null::uuid,
        coalesce(src.id::text,'field:'||coalesce(nullif(q.question_key,''),nullif(q.field_key,''),q.id::text)),
        a.text_value,case when a.choice_value_json->>'news_summary_importance' in ('very_bullish','bullish','positive','neutral','negative','bearish','very_bearish') then a.choice_value_json->>'news_summary_importance' else 'neutral' end,a.created_at,null::uuid,a.last_edited_by,
        a.created_at,a.updated_at,to_jsonb(src),coalesce(src.name,q.title),'legacy_project'::text,true,
        p.id,null::uuid,q.id,p.created_at,p.id,to_jsonb(a)||jsonb_build_object('summary_can_edit',coalesce(app_private.can_edit_project(p.id),false))
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

