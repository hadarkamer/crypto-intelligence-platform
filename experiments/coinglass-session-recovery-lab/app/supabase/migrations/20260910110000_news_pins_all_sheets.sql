-- Active pins are shared references in every accessible collection sheet except
-- their originating sheet, regardless of when the destination sheet was created.
-- This replaces only read eligibility; stored pins, snapshots and answers stay intact.
create or replace function public.v2_read_news_pins(p_workspace_id uuid,p_session_id uuid default null)
returns table(item_key text,item_id text,origin_session_id uuid,field_id uuid,logical_key text,
  source_name text,body text,importance text,pinned_at timestamptz,pinned_by uuid,
  updated_at timestamptz,updated_by uuid,value_id uuid,row_id text)
language plpgsql stable security invoker set search_path=public,pg_temp as $$
begin
  if auth.uid() is null or not coalesce(public.v2_is_workspace_member(p_workspace_id),false) then
    raise exception 'workspace membership required' using errcode='42501';
  end if;
  if p_session_id is not null and not exists(
    select 1 from public.v2_collection_sessions s
    where s.id=p_session_id and s.workspace_id=p_workspace_id and s.deleted_at is null
  ) then
    raise exception 'Collection not accessible' using errcode='42501';
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
    and (p_session_id is null or p.origin_session_id<>p_session_id)
  order by p.item_key;
end $$;
revoke all on function public.v2_read_news_pins(uuid,uuid) from public,anon;
grant execute on function public.v2_read_news_pins(uuid,uuid) to authenticated;
notify pgrst,'reload schema';
