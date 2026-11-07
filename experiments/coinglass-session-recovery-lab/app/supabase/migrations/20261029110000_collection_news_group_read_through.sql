begin;

-- Answered worksheets retain their original template, field type and values.
-- Read current group behavior only through an unambiguous stable source in
-- the same workspace, sheet and asset's active default collection template.
create or replace function public.v2_read_collection_news_groups(p_workspace_id uuid,p_session_id uuid)
returns table(field_id uuid,logical_key text)
language sql stable security invoker set search_path=pg_catalog,public,pg_temp as $$
  select f.id,public.v2_collection_field_logical_key(to_jsonb(f))
  from public.v2_collection_sessions c
  join public.v2_templates original on original.id=c.template_id
    and original.workspace_id=c.workspace_id and original.kind='collection'
  join public.v2_template_sections sec on sec.template_id=original.id and sec.active
  join public.v2_template_fields f on f.section_id=sec.id and f.active
  where auth.uid() is not null and public.v2_is_workspace_member(p_workspace_id)
    and c.id=p_session_id and c.workspace_id=p_workspace_id and c.deleted_at is null
    and not exists(select 1 from jsonb_array_elements(coalesce(sec.settings->'groups',sec.settings->'legacy_groups','[]')) g
      where g->>'id'=coalesce(f.settings->>'group_id',f.settings->>'legacy_group_id') and g->>'active'='false')
    and (f.field_type='news_group' or (f.field_type in ('news_table','news_source_table') and exists(
      select 1 from public.v2_templates authority
      join public.v2_template_sections current_sec on current_sec.template_id=authority.id
      join public.v2_template_fields current_field on current_field.section_id=current_sec.id
      where authority.workspace_id=c.workspace_id and authority.kind='collection' and authority.active and authority.is_default
        and authority.asset_symbol is not distinct from original.asset_symbol
        and public.v2_collection_sheet_key(to_jsonb(current_sec))=public.v2_collection_sheet_key(to_jsonb(sec))
        and public.v2_collection_field_logical_key(to_jsonb(current_field))=public.v2_collection_field_logical_key(to_jsonb(f))
      having count(*)=1 and bool_and(current_sec.active and current_field.active and current_field.field_type='news_group'
        and not exists(select 1 from jsonb_array_elements(coalesce(current_sec.settings->'groups',current_sec.settings->'legacy_groups','[]')) g
          where g->>'id'=coalesce(current_field.settings->>'group_id',current_field.settings->>'legacy_group_id') and g->>'active'='false'))
    )))
  order by f.id;
$$;
revoke all on function public.v2_read_collection_news_groups(uuid,uuid) from public,anon;
grant execute on function public.v2_read_collection_news_groups(uuid,uuid) to authenticated;

create or replace function public.v2_read_collection_news_read_markers(p_workspace_id uuid,p_session_id uuid,p_field_id uuid)
returns table(user_id uuid,read_until timestamptz,updated_at timestamptz,is_current_user boolean)
language sql stable security invoker set search_path=public,pg_temp as $$
  select marker.user_id,marker.read_until,marker.updated_at,marker.user_id=auth.uid()
  from public.v2_read_collection_news_groups(p_workspace_id,p_session_id) source
  join public.v2_collection_news_read_markers marker on marker.workspace_id=p_workspace_id
    and marker.logical_key=source.logical_key
  where source.field_id=p_field_id
  order by marker.updated_at desc,marker.user_id;
$$;
revoke all on function public.v2_read_collection_news_read_markers(uuid,uuid,uuid) from public,anon;
grant execute on function public.v2_read_collection_news_read_markers(uuid,uuid,uuid) to authenticated;

create or replace function public.v2_set_collection_news_read_marker(p_workspace_id uuid,p_session_id uuid,p_field_id uuid,p_read_until timestamptz)
returns void language plpgsql security definer set search_path=public,pg_temp as $$
declare c public.v2_collection_sessions; source_key text;
begin
  if auth.uid() is null or not public.v2_is_workspace_member(p_workspace_id) then
    raise exception 'אין הרשאה לסימון קריאה בגליון זה.' using errcode='42501'; end if;
  -- Personal progress is available to viewers, while writes always use auth.uid().
  perform id from public.v2_workspaces where id=p_workspace_id for share;
  perform user_id from public.v2_workspace_members where workspace_id=p_workspace_id and user_id=auth.uid() for share;
  if not found then raise exception 'אין הרשאה לסימון קריאה בגליון זה.' using errcode='42501'; end if;
  select * into c from public.v2_collection_sessions where id=p_session_id and workspace_id=p_workspace_id and deleted_at is null for share;
  if not found then raise exception 'הגליון אינו זמין לקריאה.' using errcode='42501'; end if;
  select source.logical_key into source_key
    from public.v2_read_collection_news_groups(p_workspace_id,p_session_id) source
    join public.v2_template_fields f on f.id=source.field_id
    join public.v2_template_sections sec on sec.id=f.section_id
    where source.field_id=p_field_id for share of f,sec;
  if source_key is null then raise exception 'קבוצת החדשות אינה שייכת לגליון זה.' using errcode='23514'; end if;
  if p_read_until is null or not isfinite(p_read_until) or p_read_until>clock_timestamp()
    or p_read_until<clock_timestamp()-interval '8 days' then
    raise exception 'בחרו זמן קריאה מהיום או משבעת הימים האחרונים.' using errcode='23514'; end if;
  -- Keep the existing shared-source key so all previous personal markers survive.
  insert into public.v2_collection_news_read_markers(workspace_id,logical_key,user_id,read_until,updated_at)
    values(c.workspace_id,source_key,auth.uid(),p_read_until,clock_timestamp())
    on conflict(workspace_id,logical_key,user_id) do update set read_until=excluded.read_until,updated_at=excluded.updated_at;
end $$;
revoke all on function public.v2_set_collection_news_read_marker(uuid,uuid,uuid,timestamptz) from public,anon;
grant execute on function public.v2_set_collection_news_read_marker(uuid,uuid,uuid,timestamptz) to authenticated;

notify pgrst,'reload schema';
commit;
