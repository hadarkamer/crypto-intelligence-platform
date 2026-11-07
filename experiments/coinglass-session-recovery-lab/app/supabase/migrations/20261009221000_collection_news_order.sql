-- A saved display order refers to stable news row IDs. It does not rewrite,
-- copy or delete native news, embedded answers, captions or image references.
create table if not exists public.v2_collection_news_orders (
  workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
  session_id uuid not null references public.v2_collection_sessions(id) on delete cascade,
  field_id uuid not null references public.v2_template_fields(id) on delete cascade,
  row_ids text[] not null default array[]::text[],
  revision integer not null default 1 check (revision >= 1),
  updated_by uuid references auth.users(id) on delete set null,
  updated_at timestamptz not null default now(),
  primary key (workspace_id,session_id,field_id),
  check (cardinality(row_ids)<=2000 and coalesce(array_ndims(row_ids),1)=1
    and array_position(row_ids,null) is null)
);
alter table public.v2_collection_news_orders enable row level security;
revoke all on public.v2_collection_news_orders from public,anon,authenticated;
grant select on public.v2_collection_news_orders to authenticated;
grant all on public.v2_collection_news_orders to service_role;
drop policy if exists v2_collection_news_orders_read on public.v2_collection_news_orders;
create policy v2_collection_news_orders_read on public.v2_collection_news_orders for select to authenticated
  using (public.v2_is_workspace_member(workspace_id)
    and exists(select 1 from public.v2_collection_sessions c where c.id=session_id
      and c.workspace_id=v2_collection_news_orders.workspace_id and c.deleted_at is null));

create or replace function public.v2_guard_collection_news_order()
returns trigger language plpgsql security definer set search_path=public,pg_temp as $$
begin
  if tg_op='UPDATE' and (new.workspace_id is distinct from old.workspace_id
    or new.session_id is distinct from old.session_id or new.field_id is distinct from old.field_id) then
    raise exception 'Collection news order identity is immutable' using errcode='23514'; end if;
  if not exists(select 1 from public.v2_collection_sessions c
    join public.v2_template_sections sec on sec.template_id=c.template_id
    join public.v2_template_fields f on f.section_id=sec.id
    where c.id=new.session_id and c.workspace_id=new.workspace_id and f.id=new.field_id
      and f.field_type in ('news_table','news_source_table','unbiased_news')) then
    raise exception 'News order field must belong to the exact collection template' using errcode='23514'; end if;
  if exists(select 1 from unnest(new.row_ids) id where length(id) not between 1 and 256 or id~'[[:cntrl:]]')
    or cardinality(new.row_ids)<>(select count(distinct id) from unnest(new.row_ids) id) then
    raise exception 'Invalid news row order IDs' using errcode='23514'; end if;
  return new;
end $$;
revoke all on function public.v2_guard_collection_news_order() from public,anon,authenticated;
drop trigger if exists v2_collection_news_orders_scope_guard on public.v2_collection_news_orders;
create trigger v2_collection_news_orders_scope_guard before insert or update
  on public.v2_collection_news_orders for each row execute function public.v2_guard_collection_news_order();

create or replace function public.v2_set_collection_news_order(p_workspace_id uuid,p_session_id uuid,
  p_field_id uuid,p_row_ids text[],p_expected_revision integer)
returns jsonb language plpgsql security definer set search_path=public,pg_temp as $$
declare c public.v2_collection_sessions; saved public.v2_collection_news_orders; current_revision integer:=0;
begin
  -- The earlier worksheet-scenario migration supplies the shared private lock
  -- routine. Permission, deletion, archive and immutable template checks hold
  -- under the same workspace/member/session locks as other worksheet actions.
  c:=public.v2_lock_collection_scenario_session(p_session_id);
  if c.workspace_id is distinct from p_workspace_id then
    raise exception 'אין הרשאה לשינוי סדר בגליון זה.' using errcode='42501'; end if;
  if p_expected_revision is null or p_expected_revision<0 or p_row_ids is null
    or cardinality(p_row_ids)>2000 or coalesce(array_ndims(p_row_ids),1)<>1
    or array_position(p_row_ids,null) is not null
    or exists(select 1 from unnest(p_row_ids) id where length(id) not between 1 and 256 or id~'[[:cntrl:]]')
    or cardinality(p_row_ids)<>(select count(distinct id) from unnest(p_row_ids) id) then
    raise exception 'סדר התשובות אינו תקין.' using errcode='23514'; end if;
  if not exists(select 1 from public.v2_template_fields f join public.v2_template_sections sec on sec.id=f.section_id
    where f.id=p_field_id and f.active and sec.active and sec.template_id=c.template_id
      and f.field_type in ('news_table','news_source_table','unbiased_news')) then
    raise exception 'תיבת החדשות אינה שייכת לגליון זה.' using errcode='23514'; end if;
  select * into saved from public.v2_collection_news_orders
    where workspace_id=c.workspace_id and session_id=c.id and field_id=p_field_id for update;
  if found then current_revision:=saved.revision; end if;
  if current_revision<>p_expected_revision then
    raise exception 'סדר התשובות עודכן במקום אחר. טענו את העדכון לפני שינוי נוסף.' using errcode='40001'; end if;
  if (current_revision=0 and cardinality(p_row_ids)=0) or
    (current_revision>0 and saved.row_ids=p_row_ids) then
    return jsonb_build_object('row_ids',p_row_ids,'revision',current_revision); end if;
  insert into public.v2_collection_news_orders(workspace_id,session_id,field_id,row_ids,revision,updated_by,updated_at)
    values(c.workspace_id,c.id,p_field_id,p_row_ids,current_revision+1,auth.uid(),now())
    on conflict(workspace_id,session_id,field_id) do update set row_ids=excluded.row_ids,
      revision=excluded.revision,updated_by=auth.uid(),updated_at=now() returning * into saved;
  return jsonb_build_object('row_ids',saved.row_ids,'revision',saved.revision);
end $$;
revoke all on function public.v2_set_collection_news_order(uuid,uuid,uuid,text[],integer) from public,anon;
grant execute on function public.v2_set_collection_news_order(uuid,uuid,uuid,text[],integer) to authenticated;

do $$ begin
  if exists(select 1 from pg_publication where pubname='supabase_realtime')
    and not exists(select 1 from pg_publication_tables where pubname='supabase_realtime'
      and schemaname='public' and tablename='v2_collection_news_orders') then
    alter publication supabase_realtime add table public.v2_collection_news_orders;
  end if;
end $$;
notify pgrst, 'reload schema';
