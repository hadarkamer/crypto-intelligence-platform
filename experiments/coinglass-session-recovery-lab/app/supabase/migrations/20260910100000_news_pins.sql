-- Shared collection pins are references with a recovery snapshot, not copied answers.
-- No existing worksheet, legacy project, source or news row is rewritten.
create table if not exists public.v2_news_pins (
  workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
  item_key text not null,
  origin_item_id text not null,
  origin_session_id uuid not null references public.v2_collection_sessions(id) on delete cascade,
  field_id uuid not null references public.v2_template_fields(id) on delete cascade,
  logical_key text not null,
  snapshot jsonb not null check (jsonb_typeof(snapshot)='object'),
  pinned_at timestamptz not null default clock_timestamp(),
  pinned_by uuid references auth.users(id) on delete set null,
  primary key(workspace_id,item_key)
);
alter table public.v2_news_pins enable row level security;
revoke all on public.v2_news_pins from public,anon,authenticated;
grant select on public.v2_news_pins to authenticated;
grant all on public.v2_news_pins to service_role;
create policy news_pins_member_read on public.v2_news_pins for select to authenticated
  using(public.v2_is_workspace_member(workspace_id));

-- The content hash is only a fallback for old embedded rows without their own ID.
-- It follows a reordered row and never silently selects its new ordinal neighbor.
create or replace function public.v2_news_pin_item_key(p_id text,p_origin text,p_raw jsonb)
returns text language sql immutable security invoker set search_path=public,pg_temp as $$
  select case when p_origin='collection_value' and p_id like 'collection_value:%' then
    split_part(p_id,':',1)||':'||split_part(p_id,':',2)||
      case when nullif(p_raw->>'id','') is not null then ':row:'||(p_raw->>'id')
        else ':content:'||md5(p_raw::text) end
    else p_id end
$$;
revoke all on function public.v2_news_pin_item_key(text,text,jsonb) from public,anon;
grant execute on function public.v2_news_pin_item_key(text,text,jsonb) to authenticated;

-- Writes go through this bounded RPC so callers cannot forge a pin's originating
-- workspace, source sheet, future-only start date, snapshot or editor identity.
create or replace function public.v2_set_news_pin(p_workspace_id uuid,p_item_id text,p_pinned boolean)
returns void language plpgsql security definer set search_path=public,pg_temp as $$
declare item record; canonical_key text; origin_session uuid; logical text; pinned_count integer;
begin
  if auth.uid() is null or not coalesce(public.v2_can_edit_workspace(p_workspace_id),false) then
    raise exception 'workspace edit permission required' using errcode='42501';
  end if;
  if p_pinned is null or nullif(btrim(p_item_id),'') is null then
    raise exception 'Invalid news pin' using errcode='22023';
  end if;
  if not p_pinned then
    -- A canonical key still permits unpinning if the originating row was removed.
    delete from public.v2_news_pins p where p.workspace_id=p_workspace_id and p.item_key=p_item_id;
    get diagnostics pinned_count = row_count;
    if pinned_count>0 then return; end if;
  end if;
  select n.* into item from public.v2_news_feed(p_workspace_id) n
    where n.workspace_id=p_workspace_id and n.origin in ('collection_news','collection_value')
      and (n.id=p_item_id or public.v2_news_pin_item_key(n.id,n.origin,n.raw_content)=p_item_id)
    order by n.id limit 1;
  if not found then
    if not p_pinned then return; end if;
    raise exception 'Collection news not accessible' using errcode='42501';
  end if;
  origin_session:=case when item.origin='collection_news' then item.collection_session_id else item.entity_id end;
  if not exists(select 1 from public.v2_collection_sessions s
    where s.id=origin_session and s.workspace_id=p_workspace_id and s.deleted_at is null) then
    raise exception 'Original collection not accessible' using errcode='42501';
  end if;
  select public.v2_collection_field_logical_key(to_jsonb(f)) into logical
    from public.v2_template_fields f join public.v2_template_sections sec on sec.id=f.section_id
    join public.v2_templates t on t.id=sec.template_id
    where f.id=item.field_id and t.workspace_id=p_workspace_id;
  if logical is null then raise exception 'Collection field not accessible' using errcode='42501'; end if;
  canonical_key:=public.v2_news_pin_item_key(item.id,item.origin,item.raw_content);
  if p_pinned then
    insert into public.v2_news_pins(workspace_id,item_key,origin_item_id,origin_session_id,field_id,
      logical_key,snapshot,pinned_by)
      values(p_workspace_id,canonical_key,item.id,origin_session,item.field_id,logical,to_jsonb(item),auth.uid())
      on conflict(workspace_id,item_key) do nothing;
  else
    delete from public.v2_news_pins p where p.workspace_id=p_workspace_id and p.item_key=canonical_key;
  end if;
end $$;
revoke all on function public.v2_set_news_pin(uuid,text,boolean) from public,anon;
grant execute on function public.v2_set_news_pin(uuid,text,boolean) to authenticated;

create or replace function public.v2_read_news_pins(p_workspace_id uuid,p_session_id uuid default null)
returns table(item_key text,item_id text,origin_session_id uuid,field_id uuid,logical_key text,
  source_name text,body text,importance text,pinned_at timestamptz,pinned_by uuid,
  updated_at timestamptz,updated_by uuid,value_id uuid,row_id text)
language plpgsql stable security invoker set search_path=public,pg_temp as $$
declare target_created timestamptz;
begin
  if auth.uid() is null or not coalesce(public.v2_is_workspace_member(p_workspace_id),false) then
    raise exception 'workspace membership required' using errcode='42501';
  end if;
  if p_session_id is not null then
    select s.created_at into target_created from public.v2_collection_sessions s
      where s.id=p_session_id and s.workspace_id=p_workspace_id and s.deleted_at is null;
    if target_created is null then raise exception 'Collection not accessible' using errcode='42501'; end if;
  end if;
  return query with live as materialized (
    select n.*,public.v2_news_pin_item_key(n.id,n.origin,n.raw_content) pin_key
    from public.v2_news_feed(p_workspace_id) n where n.origin in ('collection_news','collection_value')
  )
  select p.item_key,coalesce(n.id,p.item_key),p.origin_session_id,p.field_id,p.logical_key,
    coalesce(n.source_name,p.snapshot->>'source_name','ללא מקור'),
    coalesce(n.body,p.snapshot->>'body',''),coalesce(n.importance,p.snapshot->>'importance','neutral'),
    p.pinned_at,p.pinned_by,
    coalesce(n.updated_at,(p.snapshot->>'updated_at')::timestamptz),
    coalesce(n.updated_by,(p.snapshot->>'updated_by')::uuid),
    case when p.item_key like 'collection_value:%' then split_part(p.item_key,':',2)::uuid end,
    coalesce(n.raw_content->>'id',p.snapshot#>>'{raw_content,id}')
  from public.v2_news_pins p
  join public.v2_collection_sessions original on original.id=p.origin_session_id and original.deleted_at is null
  left join lateral(select l.* from live l where l.pin_key=p.item_key order by l.id limit 1) n on true
  where p.workspace_id=p_workspace_id
    and (p_session_id is null or (p.origin_session_id<>p_session_id and target_created>=p.pinned_at))
  order by p.item_key;
end $$;
revoke all on function public.v2_read_news_pins(uuid,uuid) from public,anon;
grant execute on function public.v2_read_news_pins(uuid,uuid) to authenticated;

-- Status-only sources intentionally live apart from news sources. They cannot
-- become collection source options or be resolved by a matching collection name.
create table if not exists public.v2_news_status_sources (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
  name text not null check(nullif(btrim(name),'') is not null),
  current_scenario text,
  current_sentiment text not null default 'neutral' check(current_sentiment in
    ('very_bullish','bullish','positive','neutral','negative','bearish','very_bearish')),
  status_defined boolean not null default true check(status_defined),
  status_only boolean not null default true check(status_only),
  updated_by uuid references auth.users(id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
create unique index if not exists v2_news_status_sources_workspace_name_idx
  on public.v2_news_status_sources(workspace_id,lower(btrim(name)));
alter table public.v2_news_status_sources enable row level security;
revoke all on public.v2_news_status_sources from public,anon;
grant select,insert,update,delete on public.v2_news_status_sources to authenticated;
grant all on public.v2_news_status_sources to service_role;
create policy news_status_sources_read on public.v2_news_status_sources for select to authenticated
  using(public.v2_is_workspace_member(workspace_id));
create policy news_status_sources_insert on public.v2_news_status_sources for insert to authenticated
  with check(public.v2_can_edit_workspace(workspace_id));
create policy news_status_sources_update on public.v2_news_status_sources for update to authenticated
  using(public.v2_can_edit_workspace(workspace_id)) with check(public.v2_can_edit_workspace(workspace_id));
create policy news_status_sources_delete on public.v2_news_status_sources for delete to authenticated
  using(public.v2_can_edit_workspace(workspace_id));
create trigger v2_news_status_sources_force_editor before insert or update on public.v2_news_status_sources
  for each row execute function public.v2_force_updated_by();
create trigger v2_news_status_sources_updated_at before update on public.v2_news_status_sources
  for each row execute function public.v2_set_updated_at();
notify pgrst,'reload schema';
