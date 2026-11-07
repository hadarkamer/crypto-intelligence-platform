-- Collection-only history, recycle bin and personal field notifications.
-- No legacy project rows, template definitions, or answers are rewritten.
alter table public.v2_collection_sessions
  add column if not exists deleted_at timestamptz,
  add column if not exists deleted_by uuid references auth.users(id) on delete set null,
  add column if not exists creation_origin text not null default 'user'
    check (creation_origin in ('user','chatgpt'));
create index if not exists v2_collection_sessions_live_idx
  on public.v2_collection_sessions(workspace_id,created_at desc,id) where deleted_at is null;

create table public.v2_collection_user_state (
  session_id uuid not null references public.v2_collection_sessions(id) on delete cascade,
  user_id uuid not null references auth.users(id) on delete cascade,
  first_opened_at timestamptz not null default now(),
  baseline_version bigint not null default 0 check (baseline_version>=0),
  primary key(session_id,user_id)
);
create table public.v2_collection_field_activity (
  version bigint generated always as identity primary key,
  session_id uuid not null references public.v2_collection_sessions(id) on delete cascade,
  field_id uuid not null references public.v2_template_fields(id) on delete cascade,
  actor_id uuid references auth.users(id) on delete set null,
  changed_at timestamptz not null default now()
);
create index v2_collection_field_activity_session_idx
  on public.v2_collection_field_activity(session_id,field_id,version desc);
create table public.v2_collection_field_seen (
  session_id uuid not null references public.v2_collection_sessions(id) on delete cascade,
  field_id uuid not null references public.v2_template_fields(id) on delete cascade,
  user_id uuid not null references auth.users(id) on delete cascade,
  seen_version bigint not null check (seen_version>=0),
  primary key(session_id,field_id,user_id)
);

alter table public.v2_collection_user_state enable row level security;
alter table public.v2_collection_field_activity enable row level security;
alter table public.v2_collection_field_seen enable row level security;
create policy v2_collection_user_state_read on public.v2_collection_user_state for select to authenticated
  using(user_id=auth.uid() and exists(select 1 from public.v2_collection_sessions s
    where s.id=session_id and public.v2_is_workspace_member(s.workspace_id)));
create policy v2_collection_field_seen_read on public.v2_collection_field_seen for select to authenticated
  using(user_id=auth.uid() and exists(select 1 from public.v2_collection_sessions s
    where s.id=session_id and public.v2_is_workspace_member(s.workspace_id)));
create policy v2_collection_field_activity_read on public.v2_collection_field_activity for select to authenticated
  using(exists(select 1 from public.v2_collection_sessions s
    where s.id=session_id and public.v2_is_workspace_member(s.workspace_id)));
revoke all on public.v2_collection_user_state,public.v2_collection_field_activity,public.v2_collection_field_seen from public,anon,authenticated;
grant select on public.v2_collection_user_state,public.v2_collection_field_activity,public.v2_collection_field_seen to authenticated;
revoke all on sequence public.v2_collection_field_activity_version_seq from public,anon,authenticated;

-- Migration baseline: existing creators have already worked on their own sheets.
insert into public.v2_collection_user_state(session_id,user_id,first_opened_at,baseline_version)
  select id,created_by,created_at,0 from public.v2_collection_sessions on conflict do nothing;

create or replace function public.v2_collection_identity_state()
returns trigger language plpgsql security definer set search_path=public,pg_temp as $$
begin
  if tg_when='BEFORE' then
    if tg_op='UPDATE' then new.creation_origin:=old.creation_origin;
    elsif auth.uid() is not null then new.creation_origin:='user'; end if;
    if new.deleted_at is null then new.deleted_by:=null;
    elsif tg_op='UPDATE' and old.deleted_at is not null then new.deleted_by:=old.deleted_by;
    elsif auth.uid() is not null then new.deleted_by:=auth.uid(); end if;
    return new;
  end if;
  insert into public.v2_collection_user_state(session_id,user_id,first_opened_at,baseline_version)
    values(new.id,new.created_by,new.created_at,0) on conflict do nothing;
  return new;
end $$;
revoke all on function public.v2_collection_identity_state() from public,anon,authenticated;
create trigger v2_collection_identity_before before insert or update on public.v2_collection_sessions
  for each row execute function public.v2_collection_identity_state();
create trigger v2_collection_creator_opened after insert on public.v2_collection_sessions
  for each row execute function public.v2_collection_identity_state();

-- Short labels are returned only through workspace-authorized report functions.
create or replace function public.v2_collection_actor_label(p_actor uuid)
returns text language sql stable security definer set search_path=public,pg_temp as $$
  select coalesce(nullif(split_part((select email from auth.users where id=p_actor),'@',1),''),'משתמש');
$$;
revoke all on function public.v2_collection_actor_label(uuid) from public,anon,authenticated;

create or replace function public.v2_record_collection_field_activity()
returns trigger language plpgsql security definer set search_path=public,pg_temp as $$
declare sid uuid; fid uuid; actor uuid; event_version bigint;
begin
  if tg_table_name='v2_collection_values' then
    if tg_op='UPDATE' and new.value is not distinct from old.value then return null; end if;
    if tg_op='INSERT' and coalesce(new.value,'{}'::jsonb) in ('{}'::jsonb,'null'::jsonb) then return null; end if;
    if tg_op='DELETE' and coalesce(old.value,'{}'::jsonb) in ('{}'::jsonb,'null'::jsonb) then return null; end if;
    if tg_op='DELETE' then sid:=old.session_id;fid:=old.field_id;actor:=coalesce(auth.uid(),old.updated_by);
    else sid:=new.session_id;fid:=new.field_id;actor:=coalesce(auth.uid(),new.updated_by); end if;
  else
    if tg_op='INSERT' and nullif(btrim(new.body),'') is null then return null; end if;
    if tg_op='DELETE' and nullif(btrim(old.body),'') is null then return null; end if;
    if tg_op='UPDATE' and row(new.body,new.importance,new.source_id,new.field_id,new.collection_session_id)
       is not distinct from row(old.body,old.importance,old.source_id,old.field_id,old.collection_session_id) then return null; end if;
    if tg_op='DELETE' then sid:=old.collection_session_id;fid:=old.field_id;actor:=coalesce(auth.uid(),old.updated_by,old.created_by);
    else sid:=new.collection_session_id;fid:=new.field_id;actor:=coalesce(auth.uid(),new.updated_by,new.created_by); end if;
  end if;
  if sid is null or fid is null then return null; end if;
  -- Serialize activity and first-open watermarks with writes to this sheet.
  -- Cascading deletion has no remaining parent and must not recreate activity.
  perform 1 from public.v2_collection_sessions where id=sid for update;
  if not found then return null; end if;
  insert into public.v2_collection_field_activity(session_id,field_id,actor_id)
    values(sid,fid,actor) returning version into event_version;
  update public.v2_collection_sessions set last_edited_by=actor,updated_at=now() where id=sid;
  if actor is not null then
    insert into public.v2_collection_field_seen(session_id,field_id,user_id,seen_version)
      values(sid,fid,actor,event_version) on conflict(session_id,field_id,user_id)
      do update set seen_version=greatest(v2_collection_field_seen.seen_version,excluded.seen_version);
  end if;
  return null;
end $$;
revoke all on function public.v2_record_collection_field_activity() from public,anon,authenticated;
create trigger v2_collection_values_activity after insert or update or delete on public.v2_collection_values
  for each row execute function public.v2_record_collection_field_activity();
create trigger v2_collection_news_activity after insert or update or delete on public.v2_news_items
  for each row execute function public.v2_record_collection_field_activity();

create or replace function public.v2_guard_deleted_collection_write()
returns trigger language plpgsql security definer set search_path=public,pg_temp as $$
declare sid uuid; removed timestamptz;
begin
  if tg_table_name='v2_collection_values' then sid:=new.session_id;
  else sid:=new.collection_session_id; end if;
  if sid is null then return new; end if;
  select deleted_at into removed from public.v2_collection_sessions where id=sid for update;
  if removed is not null then raise exception 'Restore this collection before editing it' using errcode='23514'; end if;
  return new;
end $$;
revoke all on function public.v2_guard_deleted_collection_write() from public,anon,authenticated;
create trigger v2_collection_values_live_guard before insert or update on public.v2_collection_values
  for each row execute function public.v2_guard_deleted_collection_write();
create trigger v2_collection_news_live_guard before insert or update on public.v2_news_items
  for each row execute function public.v2_guard_deleted_collection_write();

-- Stale editors cannot delete content out of a recycled sheet. The authorized
-- SECURITY DEFINER purge function performs the cascade in one transaction.
create policy v2_collection_values_live_delete on public.v2_collection_values as restrictive
  for delete to authenticated using(exists(select 1 from public.v2_collection_sessions s
    where s.id=session_id and s.deleted_at is null));
create policy v2_collection_news_live_delete on public.v2_news_items as restrictive
  for delete to authenticated using(collection_session_id is null or exists(
    select 1 from public.v2_collection_sessions s where s.id=collection_session_id and s.deleted_at is null));

create or replace function public.v2_open_collection(p_session_id uuid)
returns void language plpgsql security definer set search_path=public,pg_temp as $$
declare current_version bigint;
begin
  if auth.uid() is null then raise exception 'Authentication required' using errcode='42501'; end if;
  perform 1 from public.v2_collection_sessions s where s.id=p_session_id and s.deleted_at is null
    and public.v2_is_workspace_member(s.workspace_id) for update;
  if not found then raise exception 'Collection access required' using errcode='42501'; end if;
  select coalesce(max(version),0) into current_version from public.v2_collection_field_activity where session_id=p_session_id;
  insert into public.v2_collection_user_state(session_id,user_id,baseline_version)
    values(p_session_id,auth.uid(),current_version) on conflict do nothing;
end $$;

create or replace function public.v2_ack_collection_field(p_session_id uuid,p_field_id uuid,p_observed_version bigint)
returns void language plpgsql security definer set search_path=public,pg_temp as $$
begin
  if auth.uid() is null then raise exception 'Authentication required' using errcode='42501'; end if;
  perform 1 from public.v2_collection_sessions s where s.id=p_session_id and s.deleted_at is null
    and public.v2_is_workspace_member(s.workspace_id) for update;
  if not found then raise exception 'Collection access required' using errcode='42501'; end if;
  if not exists(select 1 from public.v2_collection_user_state where session_id=p_session_id and user_id=auth.uid()) then
    raise exception 'Open this collection before acknowledging a field' using errcode='23514'; end if;
  if not exists(select 1 from public.v2_collection_field_activity
    where session_id=p_session_id and field_id=p_field_id and version=p_observed_version) then
    raise exception 'Unknown observed field version' using errcode='22023'; end if;
  insert into public.v2_collection_field_seen(session_id,field_id,user_id,seen_version)
    values(p_session_id,p_field_id,auth.uid(),p_observed_version)
    on conflict(session_id,field_id,user_id) do update
      set seen_version=greatest(v2_collection_field_seen.seen_version,excluded.seen_version);
end $$;

create or replace function public.v2_set_collection_trash(p_session_id uuid,p_deleted boolean)
returns void language plpgsql security definer set search_path=public,pg_temp as $$
begin
  if auth.uid() is null or p_deleted is null then raise exception 'Authentication and state required' using errcode='42501'; end if;
  perform 1 from public.v2_collection_sessions s where s.id=p_session_id and public.v2_can_edit_workspace(s.workspace_id) for update;
  if not found then raise exception 'Collection edit permission required' using errcode='42501'; end if;
  update public.v2_collection_sessions set deleted_at=case when p_deleted then coalesce(deleted_at,now()) else null end,
    deleted_by=case when p_deleted then coalesce(deleted_by,auth.uid()) else null end
    where id=p_session_id and (deleted_at is not null) is distinct from p_deleted;
end $$;

create or replace function public.v2_purge_collection(p_session_id uuid)
returns void language plpgsql security definer set search_path=public,pg_temp as $$
begin
  if auth.uid() is null then raise exception 'Authentication required' using errcode='42501'; end if;
  perform 1 from public.v2_collection_sessions s where s.id=p_session_id
    and public.v2_can_edit_workspace(s.workspace_id) and s.deleted_at is not null for update;
  if not found then raise exception 'Only an accessible trashed collection can be permanently deleted' using errcode='42501'; end if;
  -- The historical FK is SET NULL. Explicit cleanup prevents orphan news from
  -- resurfacing in summaries after the originating sheet is permanently removed.
  delete from public.v2_news_items where collection_session_id=p_session_id;
  delete from public.v2_collection_sessions where id=p_session_id;
end $$;

-- Clients use the guarded purge RPC. Direct deletion cannot skip the recycle bin.
revoke delete on public.v2_collection_sessions from authenticated;

create or replace function public.v2_collection_activity(p_workspace_id uuid)
returns table(session_id uuid,is_new boolean,unread jsonb)
language plpgsql stable security definer set search_path=public,pg_temp as $$
begin
  if auth.uid() is null or not coalesce(public.v2_is_workspace_member(p_workspace_id),false) then
    raise exception 'Workspace membership required' using errcode='42501'; end if;
  return query
    select s.id,st.user_id is null and s.created_by<>auth.uid(),
      case when st.user_id is null then '[]'::jsonb else coalesce(a.items,'[]'::jsonb) end
    from public.v2_collection_sessions s
    left join public.v2_collection_user_state st on st.session_id=s.id and st.user_id=auth.uid()
    left join lateral (
      select jsonb_agg(jsonb_build_object('field_id',e.field_id,'title',f.title,'actor_id',e.actor_id,
        'actor_label',public.v2_collection_actor_label(e.actor_id),'updated_at',e.changed_at,'version',e.version::text)
        order by e.version desc) items
      from (
        select distinct on(e.field_id) e.* from public.v2_collection_field_activity e
        left join public.v2_collection_field_seen seen on seen.session_id=e.session_id
          and seen.field_id=e.field_id and seen.user_id=auth.uid()
        where e.session_id=s.id and e.actor_id is distinct from auth.uid()
          and e.version>greatest(coalesce(st.baseline_version,0),coalesce(seen.seen_version,0))
        order by e.field_id,e.version desc
      ) e join public.v2_template_fields f on f.id=e.field_id
    ) a on st.user_id is not null
    where s.workspace_id=p_workspace_id and s.deleted_at is null and s.status<>'archived';
end $$;

create or replace function public.v2_collection_history(p_workspace_id uuid,p_trash boolean default false)
returns table(id uuid,workspace_id uuid,template_id uuid,title text,status text,created_by uuid,last_edited_by uuid,
  created_at timestamptz,updated_at timestamptz,creator_label text,creation_origin text,deleted_at timestamptz,
  done integer,total integer,percent integer,is_new boolean,unread jsonb)
language plpgsql stable security definer set search_path=public,pg_temp as $$
begin
  if auth.uid() is null or not coalesce(public.v2_is_workspace_member(p_workspace_id),false) then
    raise exception 'Workspace membership required' using errcode='42501'; end if;
  return query
  with fields as (
    select s.id sid,f.id fid,f.field_type,f.position,f.section_id,
      coalesce(f.settings->>'group_id',f.settings->>'legacy_group_id',sec.settings#>>'{groups,0,id}',
        sec.settings#>>'{legacy_groups,0,id}',sec.id::text) group_key,
      coalesce(f.settings->'visibility_rule_json',f.settings#>'{legacy_question,visibility_rule_json}') visibility,
      coalesce(v.value,'{}'::jsonb)||jsonb_strip_nulls(jsonb_build_object(
        'text',coalesce(v.value->'text',v.value->'html',v.value->'text_value',v.value->'number',v.value->'number_value'),
        'checked',coalesce(v.value->'checked',v.value->'boolean_value'),
        'choice',coalesce(v.value->'choice',v.value->'text_value'),
        'date',coalesce(v.value->'date',v.value->'datetime',v.value->'date_value'),
        'choices',coalesce(v.value->'choices',v.value->'choice_value_json'))) value,
      exists(select 1 from public.v2_news_items n where n.collection_session_id=s.id and n.field_id=f.id
        and nullif(btrim(n.body),'') is not null) has_news
    from public.v2_collection_sessions s
    join public.v2_template_sections sec on sec.template_id=s.template_id and sec.active
    join public.v2_template_fields f on f.section_id=sec.id and f.active
    left join public.v2_collection_values v on v.session_id=s.id and v.field_id=f.id
    where s.workspace_id=p_workspace_id and (s.deleted_at is not null)=p_trash
      and (p_trash or s.status<>'archived')
      and not exists(select 1 from jsonb_array_elements(case
        when jsonb_typeof(sec.settings->'groups')='array' then sec.settings->'groups'
        when jsonb_typeof(sec.settings->'legacy_groups')='array' then sec.settings->'legacy_groups' else '[]'::jsonb end) g
        where g->>'id'=coalesce(f.settings->>'group_id',f.settings->>'legacy_group_id',
          sec.settings#>>'{groups,0,id}',sec.settings#>>'{legacy_groups,0,id}') and g->>'active'='false')
  ), ordered_fields as (
    select f.*,lag(case when f.field_type='checkbox' then f.value->>'checked'='true'
      else coalesce(f.value->>'choice',f.value->>'text')='yes' end)
      over(partition by f.sid,f.section_id,f.group_key order by f.position,f.fid) prev_yes from fields f
  ), progress as (
    select f.sid,count(*)::integer total,
      count(*) filter(where f.value->>'not_relevant'='true' or f.value->>'completion'='na' or f.has_news
        or public.v2_field_complete(f.field_type,f.value||case when f.value ? 'html' and not (f.value ? 'text')
          then jsonb_build_object('text',f.value->'html') else '{}'::jsonb end))::integer done
    from ordered_fields f where f.field_type not in ('info','button')
      and (coalesce(f.visibility->>'show_when','')<>'prev_yes' or coalesce(f.prev_yes,false)) group by f.sid
  )
  select s.id,s.workspace_id,s.template_id,s.title,s.status,s.created_by,s.last_edited_by,s.created_at,s.updated_at,
    case when s.creation_origin='chatgpt' then 'chatGPT' else public.v2_collection_actor_label(s.created_by) end,
    s.creation_origin,s.deleted_at,coalesce(p.done,0),coalesce(p.total,0),
    case when coalesce(p.total,0)=0 then 0 else round(100.0*p.done/p.total)::integer end,
    coalesce(a.is_new,false),coalesce(a.unread,'[]'::jsonb)
  from public.v2_collection_sessions s left join progress p on p.sid=s.id
  left join public.v2_collection_activity(p_workspace_id) a on a.session_id=s.id
  where s.workspace_id=p_workspace_id and (s.deleted_at is not null)=p_trash and (p_trash or s.status<>'archived');
end $$;

revoke all on function public.v2_open_collection(uuid),public.v2_ack_collection_field(uuid,uuid,bigint),
  public.v2_set_collection_trash(uuid,boolean),public.v2_purge_collection(uuid),
  public.v2_collection_activity(uuid),public.v2_collection_history(uuid,boolean) from public,anon;
grant execute on function public.v2_open_collection(uuid),public.v2_ack_collection_field(uuid,uuid,bigint),
  public.v2_set_collection_trash(uuid,boolean),public.v2_purge_collection(uuid),
  public.v2_collection_activity(uuid),public.v2_collection_history(uuid,boolean) to authenticated;

-- Filter before ranking snapshots so a trashed latest sheet falls back to the
-- previous live answer. Legacy branches of the news feed are unchanged.
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
    where s.workspace_id=p_workspace_id and s.deleted_at is null and s.status<>'archived' order by s.created_at desc,s.id desc limit 1;
  elsif p_mode='session' then
    select s.id into target_session from public.v2_collection_sessions s where s.id=p_session_id and s.workspace_id=p_workspace_id and s.deleted_at is null and s.status<>'archived';
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
    where s.workspace_id=p_workspace_id and s.deleted_at is null and s.status<>'archived' and (p_mode='latest_filled' or s.id=target_session)
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
