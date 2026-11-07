-- Additive field linkage and an atomic replacement for the interrupted client-side import.
alter table public.v2_template_fields drop constraint if exists v2_template_fields_field_type_check;
alter table public.v2_template_fields add constraint v2_template_fields_field_type_check check (field_type in (
  'dynamic_text','rich_text','short_text','long_text','checkbox','yes_no_na','liquidity_table',
  'news_table','news_source_table','economic_calendar','number','pnl','image'
));
alter table public.v2_news_items
  add column if not exists field_id uuid references public.v2_template_fields(id) on delete restrict;
create index if not exists v2_news_items_session_field_idx
  on public.v2_news_items(collection_session_id, field_id, occurred_at desc);
alter table public.v2_news_sources add column if not exists status_defined boolean not null default false;
update public.v2_news_sources set status_defined = true
where not status_defined and (current_sentiment <> 'neutral' or nullif(trim(current_scenario), '') is not null);

-- Existing news can be linked without guessing only when its template has one news field.
with unambiguous as (
  select s.id session_id, min(f.id::text)::uuid field_id
  from public.v2_collection_sessions s
  join public.v2_template_sections sec on sec.template_id = s.template_id
  join public.v2_template_fields f on f.section_id = sec.id
  where f.field_type in ('news_table', 'news_source_table')
  group by s.id having count(*) = 1
)
update public.v2_news_items n set field_id = u.field_id
from unambiguous u where n.collection_session_id = u.session_id and n.field_id is null;

create or replace function public.v2_validate_news_item_links()
returns trigger language plpgsql security definer set search_path = public, pg_temp as $$
begin
  if new.collection_session_id is not null and not exists (
    select 1 from public.v2_collection_sessions s
    where s.id = new.collection_session_id and s.workspace_id = new.workspace_id
  ) then raise exception 'news collection must belong to its workspace' using errcode = '23514'; end if;
  if new.source_id is not null and not exists (
    select 1 from public.v2_news_sources src where src.id = new.source_id and src.workspace_id = new.workspace_id
  ) then raise exception 'news source must belong to its workspace' using errcode = '23514'; end if;
  if new.field_id is not null and not exists (
    select 1 from public.v2_template_fields f
    join public.v2_template_sections sec on sec.id = f.section_id
    join public.v2_collection_sessions s on s.template_id = sec.template_id
    where f.id = new.field_id and s.id = new.collection_session_id and s.workspace_id = new.workspace_id
      and f.field_type in ('news_table', 'news_source_table')
  ) then raise exception 'news field must belong to its collection template' using errcode = '23514'; end if;
  return new;
end; $$;
revoke all on function public.v2_validate_news_item_links() from public, anon, authenticated;

-- News edits participate in collection recency; the caller's identity is supplied by auth.
create or replace function public.v2_touch_news_collection()
returns trigger language plpgsql security definer set search_path = public, pg_temp as $$
declare target uuid;
begin
  if TG_OP = 'DELETE' then target := old.collection_session_id; else target := new.collection_session_id; end if;
  if target is not null then
    update public.v2_collection_sessions set updated_at = now(), last_edited_by = coalesce(auth.uid(), last_edited_by) where id = target;
  end if;
  return null;
end; $$;
revoke all on function public.v2_touch_news_collection() from public, anon, authenticated;
drop trigger if exists v2_news_items_touch_collection on public.v2_news_items;
create trigger v2_news_items_touch_collection after insert or update or delete on public.v2_news_items
for each row execute function public.v2_touch_news_collection();

create or replace function public.v2_import_legacy_collection_template(p_workspace_id uuid)
returns uuid language plpgsql security invoker set search_path = public, pg_temp as $$
declare
  version_id uuid;
  imported_id uuid;
  section_id uuid;
  group_row record;
  section_position integer := 0;
  category_count integer;
  selected_count integer;
  default_id uuid;
  default_settings jsonb;
begin
  if auth.uid() is null or not public.v2_can_edit_workspace(p_workspace_id) then
    raise exception 'workspace edit permission required' using errcode = '42501';
  end if;
  perform pg_advisory_xact_lock(hashtextextended('v2-legacy-import:' || p_workspace_id::text, 0));
  select id into imported_id from public.v2_templates
  where workspace_id = p_workspace_id and kind = 'collection'
    and settings->>'legacy_import_version' = '2'
  order by created_at limit 1;
  if imported_id is not null then return imported_id; end if;

  select active_template_version_id into version_id from public.app_settings order by updated_at desc limit 1;
  if version_id is null then return null; end if;

  -- Match the four source groups across every active stage, not the first two stage names.
  select count(*), count(distinct category) into selected_count, category_count from (
    select case
      when trim(g.title) ~ '^חדשות$' then 'news'
      when trim(g.title) ~ '^מאקרו$' then 'macro'
      when trim(g.title) ~ '^נזילות$' then 'liquidity'
      when trim(g.title) ~ '^(טכני( ונתונים| / נתונים)?|נתונים וטכני|טכני, נתונים)$' then 'technical'
    end category
    from public.template_groups g join public.template_stages st on st.id = g.stage_id
    where st.template_version_id = version_id and st.active and g.active
  ) matched where category is not null;
  if selected_count <> 4 or category_count <> 4 then
    raise exception 'Legacy import requires exactly the four groups חדשות, מאקרו, נזילות, טכני ונתונים; found % groups / % categories', selected_count, category_count
      using errcode = '23514';
  end if;

  select id, settings into default_id, default_settings from public.v2_templates
  where workspace_id = p_workspace_id and kind = 'collection' and active and is_default and asset_symbol is null limit 1;
  insert into public.v2_templates(workspace_id, kind, name, settings, created_by)
  values (p_workspace_id, 'collection', 'איסוף מידע – מהתבנית הקיימת',
    jsonb_build_object('legacy_import', true, 'legacy_import_version', 2, 'legacy_template_version_id', version_id, 'imported_at', now()), auth.uid())
  returning id into imported_id;

  for group_row in
    select g.*, st.id legacy_stage_id,
      case when trim(g.title) in ('חדשות', 'מאקרו') then 'news_macro' else 'data' end sheet_key
    from public.template_groups g join public.template_stages st on st.id = g.stage_id
    where st.template_version_id = version_id and st.active and g.active
      and (trim(g.title) in ('חדשות','מאקרו','נזילות') or trim(g.title) ~ '^(טכני( ונתונים| / נתונים)?|נתונים וטכני|טכני, נתונים)$')
    order by case when trim(g.title) = 'חדשות' then 0 when trim(g.title) = 'מאקרו' then 1 when trim(g.title) = 'נזילות' then 2 else 3 end
  loop
    if not exists(select 1 from public.template_questions q where q.group_id = group_row.id and q.active) then
      raise exception 'Legacy import group % contains no active questions', group_row.title using errcode = '23514';
    end if;
    insert into public.v2_template_sections(template_id,title,position,settings)
    values (imported_id, group_row.title, section_position,
      jsonb_build_object('sheet_key',group_row.sheet_key,'legacy_stage_id',group_row.legacy_stage_id,'legacy_group_id',group_row.id,'description',group_row.description,'behavior',group_row.behavior))
    returning id into section_id;
    insert into public.v2_template_fields(section_id,field_key,title,field_type,position,required,default_align,settings)
    select section_id, coalesce(nullif(q.field_key,''),'legacy') || '_' || q.id::text, q.title,
      case when q.question_type in ('short_text','long_text','checkbox','yes_no_na','number','image','liquidity_table','news_table','news_source_table','economic_calendar','rich_text','pnl')
        then q.question_type else 'dynamic_text' end,
      (row_number() over(order by q.position,q.id)-1)::integer, q.required,
      case when q.default_text_align = 'left' then 'left' else 'right' end,
      jsonb_build_object('legacy_question_id',q.id,'description',q.description,'help_text',q.help_text,'links',coalesce(q.links_json,'[]'::jsonb))
    from public.template_questions q where q.group_id = group_row.id and q.active;
    section_position := section_position + 1;
  end loop;

  -- Preserve a user-selected custom default; replace only the generated or old import default.
  if default_id is null or coalesce(default_settings->>'legacy_import','false') = 'true'
    or exists(select 1 from public.v2_templates where id = default_id and name = 'איסוף מידע – ברירת מחדל' and settings = '{}'::jsonb)
  then
    update public.v2_templates set is_default = false where id = default_id;
    update public.v2_templates set is_default = true where id = imported_id;
  end if;
  return imported_id;
end; $$;
revoke all on function public.v2_import_legacy_collection_template(uuid) from public, anon;
grant execute on function public.v2_import_legacy_collection_template(uuid) to authenticated;
