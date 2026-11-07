-- Add personal presentation and row receipts to the existing notification stream.
-- News bodies, previous answers, pins and shared ordering are untouched.
alter table public.user_preferences add column if not exists collection_news_sort text not null default 'newest';
do $$ begin
  if not exists(select 1 from pg_constraint where conrelid='public.user_preferences'::regclass
    and conname='user_preferences_collection_news_sort_check') then
    alter table public.user_preferences add constraint user_preferences_collection_news_sort_check
      check(collection_news_sort in ('newest','oldest','manual'));
  end if;
end $$;

alter table public.v2_collection_field_activity add column if not exists news_row_ids text[];
create table if not exists public.v2_collection_news_seen (
  session_id uuid not null references public.v2_collection_sessions(id) on delete cascade,
  field_id uuid not null references public.v2_template_fields(id) on delete cascade,
  row_id text not null check(length(row_id) between 1 and 512),
  user_id uuid not null references auth.users(id) on delete cascade,
  seen_version bigint not null check(seen_version>=0),
  primary key(session_id,field_id,row_id,user_id)
);
alter table public.v2_collection_news_seen enable row level security;
drop policy if exists v2_collection_news_seen_read on public.v2_collection_news_seen;
create policy v2_collection_news_seen_read on public.v2_collection_news_seen for select to authenticated
  using(user_id=auth.uid() and exists(select 1 from public.v2_collection_sessions s
    where s.id=session_id and public.v2_is_workspace_member(s.workspace_id)));
revoke all on public.v2_collection_news_seen from public,anon,authenticated;
grant select on public.v2_collection_news_seen to authenticated;

create or replace function public.v2_collection_embedded_news_rows(p_value jsonb)
returns jsonb language sql immutable set search_path=public,pg_temp as $$
  select case when jsonb_typeof(p_value#>'{structured,rows}')='array' then p_value#>'{structured,rows}'
    when jsonb_typeof(p_value->'rows')='array' then p_value->'rows' else '[]'::jsonb end;
$$;
revoke all on function public.v2_collection_embedded_news_rows(jsonb) from public,anon,authenticated;

create or replace function public.v2_changed_collection_news_rows(p_before jsonb,p_after jsonb)
returns text[] language sql immutable set search_path=public,pg_temp as $$
  with before_rows as (select r->>'id' id,r from jsonb_array_elements(public.v2_collection_embedded_news_rows(p_before)) r),
  after_rows as (select r->>'id' id,r from jsonb_array_elements(public.v2_collection_embedded_news_rows(p_after)) r)
  select coalesce(array_agg(distinct coalesce(a.id,b.id) order by coalesce(a.id,b.id)),array[]::text[])
  from before_rows b full join after_rows a using(id)
  where nullif(coalesce(a.id,b.id),'') is not null and length(coalesce(a.id,b.id))<=512
    and row(a.r->'body',a.r->'text',a.r->'importance',a.r->'source_id',a.r->'source_name',a.r->'source_url',a.r->'category')
      is distinct from row(b.r->'body',b.r->'text',b.r->'importance',b.r->'source_id',b.r->'source_name',b.r->'source_url',b.r->'category');
$$;
revoke all on function public.v2_changed_collection_news_rows(jsonb,jsonb) from public,anon,authenticated;

-- Extend the current trigger in place, preserving autofill attribution, locking,
-- no-op protection and all existing field notifications. Reruns are no-ops.
do $$ declare definition text; needle text; replacement text; begin
  definition:=pg_get_functiondef('public.v2_record_collection_field_activity()'::regprocedure);
  if strpos(definition,'v2_changed_collection_news_rows')=0 then
    needle:='update public.v2_collection_sessions set last_edited_by=actor,updated_at=now() where id=sid;';
    replacement:=$replace$
  if tg_table_name='v2_news_items' then
    update public.v2_collection_field_activity set news_row_ids=array[case when tg_op='DELETE' then old.id::text else new.id::text end]
      where version=event_version;
  elsif exists(select 1 from public.v2_template_fields f where f.id=fid
    and f.field_type in ('news_table','news_source_table','unbiased_news')) then
    update public.v2_collection_field_activity set news_row_ids=nullif(public.v2_changed_collection_news_rows(
      case when tg_op='INSERT' then null else old.value end,
      case when tg_op='DELETE' then null else new.value end),array[]::text[]) where version=event_version;
  end if;
  update public.v2_collection_sessions set last_edited_by=actor,updated_at=now() where id=sid;
    $replace$;
    if strpos(definition,needle)=0 then raise exception 'Unexpected collection activity trigger contract';end if;
    definition:=replace(definition,needle,replacement);
    execute definition;
  end if;
end $$;

-- Retained activity has no native/embedded provenance. Even an exact timestamp
-- match can come from a metadata-only native update in the same transaction.
-- Leave those events unmapped, preserving their field notification without
-- fabricating per-post badges or rewriting historical notification records.

create or replace function public.v2_read_collection_news_unread(p_session_id uuid,p_field_id uuid,p_observed_version bigint)
returns jsonb language plpgsql stable security definer set search_path=public,pg_temp as $$
declare result jsonb;
begin
  if auth.uid() is null or p_observed_version is null or p_observed_version<0 then
    raise exception 'Authenticated observed version required' using errcode='42501';end if;
  if not exists(select 1 from public.v2_collection_sessions s join public.v2_template_sections sec on sec.template_id=s.template_id
    join public.v2_template_fields f on f.section_id=sec.id and f.id=p_field_id
    where s.id=p_session_id and s.deleted_at is null and public.v2_is_workspace_member(s.workspace_id)
      and f.field_type in ('news_table','news_source_table','unbiased_news')) then
    raise exception 'Collection news access required' using errcode='42501';end if;
  with threshold as (
    select greatest(coalesce(st.baseline_version,0),coalesce(fs.seen_version,0)) version
    from (select 1) seed left join public.v2_collection_user_state st on st.session_id=p_session_id and st.user_id=auth.uid()
    left join public.v2_collection_field_seen fs on fs.session_id=p_session_id and fs.field_id=p_field_id and fs.user_id=auth.uid()
  ), current_rows as (
    select n.id::text id from public.v2_news_items n where n.collection_session_id=p_session_id and n.field_id=p_field_id
      and nullif(btrim(n.body),'') is not null
    union select r->>'id' from public.v2_collection_values v cross join lateral jsonb_array_elements(public.v2_collection_embedded_news_rows(v.value)) r
      where v.session_id=p_session_id and v.field_id=p_field_id and nullif(r->>'id','') is not null
        and coalesce(nullif(btrim(r->>'body'),''),nullif(btrim(r->>'text'),'')) is not null
  ), pending_events as (
    select e.* from public.v2_collection_field_activity e cross join threshold t
    where e.session_id=p_session_id and e.field_id=p_field_id and e.version>t.version
      and e.version<=p_observed_version and e.actor_id is distinct from auth.uid()
  ), latest as (
    select distinct on(ids.id) ids.id,e.version,e.changed_at from pending_events e
      cross join lateral unnest(e.news_row_ids) ids(id) join current_rows c on c.id=ids.id
    order by ids.id,e.version desc
  ), unread as (
    select l.* from latest l left join public.v2_collection_news_seen seen on seen.session_id=p_session_id
      and seen.field_id=p_field_id and seen.row_id=l.id and seen.user_id=auth.uid()
      where l.version>coalesce(seen.seen_version,0)
  ) select jsonb_build_object('rows',coalesce((select jsonb_agg(jsonb_build_object('row_id',id,'version',version::text,
      'updated_at',changed_at) order by id) from unread),'[]'::jsonb),
    'unmapped_version',(select max(version)::text from pending_events where news_row_ids is null)) into result;
  return result;
end $$;
revoke all on function public.v2_read_collection_news_unread(uuid,uuid,bigint) from public,anon;
grant execute on function public.v2_read_collection_news_unread(uuid,uuid,bigint) to authenticated;

create or replace function public.v2_ack_collection_news_row(p_session_id uuid,p_field_id uuid,p_row_id text,p_observed_version bigint)
returns void language plpgsql security definer set search_path=public,pg_temp as $$
begin
  -- Reuse the same workspace, source-field and observed-version read guard.
  perform public.v2_read_collection_news_unread(p_session_id,p_field_id,p_observed_version);
  if not exists(select 1 from public.v2_collection_user_state where session_id=p_session_id and user_id=auth.uid()) then
    raise exception 'Open this collection before acknowledging a row' using errcode='23514';end if;
  if not exists(select 1 from public.v2_collection_field_activity e where e.session_id=p_session_id and e.field_id=p_field_id
    and e.version=p_observed_version and p_row_id=any(e.news_row_ids)) then
    raise exception 'Unknown observed news row version' using errcode='22023';end if;
  insert into public.v2_collection_news_seen(session_id,field_id,row_id,user_id,seen_version)
    values(p_session_id,p_field_id,p_row_id,auth.uid(),p_observed_version)
    on conflict(session_id,field_id,row_id,user_id) do update
      set seen_version=greatest(v2_collection_news_seen.seen_version,excluded.seen_version);
end $$;
revoke all on function public.v2_ack_collection_news_row(uuid,uuid,text,bigint) from public,anon;
grant execute on function public.v2_ack_collection_news_row(uuid,uuid,text,bigint) to authenticated;
