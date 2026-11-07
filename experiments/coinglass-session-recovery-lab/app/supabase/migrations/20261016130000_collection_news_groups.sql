-- News groups retain the same stored rows, stable field keys and history as
-- news+source. Only the rendering type and shared reading metadata are new.
begin;
do $types$
declare definition text; expression text;
begin
  select pg_get_constraintdef(oid) into definition from pg_constraint
    where conrelid='public.v2_template_fields'::regclass and conname='v2_template_fields_field_type_check';
  if definition is null then raise exception 'Missing template field type constraint'; end if;
  if position('news_group' in definition)=0 then
    expression:=substring(definition from '^CHECK [(](.*)[)]$');
    if expression is null then raise exception 'Unexpected template field type constraint'; end if;
    alter table public.v2_template_fields drop constraint v2_template_fields_field_type_check;
    execute format('alter table public.v2_template_fields add constraint v2_template_fields_field_type_check check ((%s) or field_type=%L)',expression,'news_group');
  end if;
end $types$;

-- Keep current authorization, row locks, conflict handling, receipt and clear
-- safeguards. Extend only these existing IN-list type branches; CREATE OR
-- REPLACE preserves each routine's grants, owner and security settings.
do $routines$
declare routine regprocedure; definition text;
begin
  for routine in select p.oid::regprocedure from pg_proc p join pg_namespace n on n.oid=p.pronamespace
    where (n.nspname='public' and p.proname=any(array[
      'v2_legacy_field_type','v2_validate_news_item_links','patch_v2_entity_rows',
      'v2_field_complete','v2_collection_value_filled','v2_news_feed',
      'v2_collection_autofill_apply','v2_collection_managed_snapshot_merge',
      'v2_guard_collection_news_order','v2_set_collection_news_order',
      'v2_record_collection_field_activity','v2_read_collection_news_unread',
      'v2_capture_collection_news_provenance','v2_locked_collection_feedback_item',
      'v2_read_collection_news_feedback_origins'
    ])) or (n.nspname='app_private' and p.proname='collection_scenario_evidence_source')
  loop
    definition:=pg_get_functiondef(routine);
    if position('''news_source_table''' in definition)>0 and position('''news_group''' in definition)=0 then
      -- All targeted occurrences are type membership lists, never values,
      -- inserted data, equality predicates or source identity strings.
      definition:=replace(definition,'''news_source_table''','''news_source_table'',''news_group''');
      execute definition;
    end if;
  end loop;
end $routines$;

create table if not exists public.v2_collection_news_read_markers (
  workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
  logical_key text not null check(length(logical_key) between 1 and 512),
  user_id uuid not null references auth.users(id) on delete cascade,
  read_until timestamptz not null,
  updated_at timestamptz not null default now(),
  primary key(workspace_id,logical_key,user_id)
);
alter table public.v2_collection_news_read_markers enable row level security;
revoke all on public.v2_collection_news_read_markers from public,anon,authenticated;
grant select on public.v2_collection_news_read_markers to authenticated;
grant all on public.v2_collection_news_read_markers to service_role;
drop policy if exists v2_collection_news_read_markers_read on public.v2_collection_news_read_markers;
create policy v2_collection_news_read_markers_read on public.v2_collection_news_read_markers for select to authenticated
  using(public.v2_is_workspace_member(workspace_id)
    and exists(select 1 from public.v2_workspace_members member where member.workspace_id=v2_collection_news_read_markers.workspace_id
      and member.user_id=v2_collection_news_read_markers.user_id));

create or replace function public.v2_read_collection_news_read_markers(p_workspace_id uuid,p_session_id uuid,p_field_id uuid)
returns table(user_id uuid,read_until timestamptz,updated_at timestamptz,is_current_user boolean)
language sql stable security invoker set search_path=public,pg_temp as $$
  select marker.user_id,marker.read_until,marker.updated_at,marker.user_id=auth.uid()
  from public.v2_collection_sessions c join public.v2_template_sections sec on sec.template_id=c.template_id
  join public.v2_template_fields f on f.section_id=sec.id
  join public.v2_collection_news_read_markers marker on marker.workspace_id=c.workspace_id
    and marker.logical_key=public.v2_collection_field_logical_key(to_jsonb(f))
  where auth.uid() is not null and c.id=p_session_id and c.workspace_id=p_workspace_id and c.deleted_at is null
    and f.id=p_field_id and f.field_type='news_group' and f.active and sec.active
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
  -- Reading progress is personal metadata: viewers can mark their own progress
  -- without gaining news-edit or restore capability. Keep membership/deletion
  -- checks stable while this one-account write is made.
  perform id from public.v2_workspaces where id=p_workspace_id for share;
  perform user_id from public.v2_workspace_members where workspace_id=p_workspace_id and user_id=auth.uid() for share;
  if not found then raise exception 'אין הרשאה לסימון קריאה בגליון זה.' using errcode='42501'; end if;
  select * into c from public.v2_collection_sessions where id=p_session_id and workspace_id=p_workspace_id and deleted_at is null for share;
  if not found then raise exception 'הגליון אינו זמין לקריאה.' using errcode='42501'; end if;
  select public.v2_collection_field_logical_key(to_jsonb(f)) into source_key
    from public.v2_template_fields f join public.v2_template_sections sec on sec.id=f.section_id
    where f.id=p_field_id and f.active and sec.active and sec.template_id=c.template_id and f.field_type='news_group' for share of f,sec;
  if source_key is null then raise exception 'קבוצת החדשות אינה שייכת לגליון זה.' using errcode='23514'; end if;
  if p_read_until is null or not isfinite(p_read_until) or p_read_until>clock_timestamp()
    or p_read_until<clock_timestamp()-interval '8 days' then
    raise exception 'בחרו זמן קריאה מהיום או משבעת הימים האחרונים.' using errcode='23514'; end if;
  insert into public.v2_collection_news_read_markers(workspace_id,logical_key,user_id,read_until,updated_at)
    values(c.workspace_id,source_key,auth.uid(),p_read_until,clock_timestamp())
    on conflict(workspace_id,logical_key,user_id) do update set read_until=excluded.read_until,updated_at=excluded.updated_at;
end $$;
revoke all on function public.v2_set_collection_news_read_marker(uuid,uuid,uuid,timestamptz) from public,anon;
grant execute on function public.v2_set_collection_news_read_marker(uuid,uuid,uuid,timestamptz) to authenticated;
notify pgrst,'reload schema';
commit;
