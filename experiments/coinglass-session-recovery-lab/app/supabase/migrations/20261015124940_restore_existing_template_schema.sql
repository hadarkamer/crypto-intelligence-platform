-- Reproduce legacy schema already present in the live database. No rows are
-- rewritten or removed. Existing columns, defaults and indexes remain intact.
begin;
alter table public.templates
  add column if not exists kind text not null default 'legacy_project',
  add column if not exists slug text,
  add column if not exists settings_json jsonb not null default '{}';
alter table public.template_questions
  add column if not exists pinned_scope text not null default 'none',
  add column if not exists pinned_stage_id uuid;
do $$ begin
  if not exists(select 1 from pg_constraint where conrelid='public.templates'::regclass and conname='templates_kind_check') then
    alter table public.templates add constraint templates_kind_check
      check(kind in ('legacy_project','information_collection','trade','scenario','spot_journal'));
  end if;
  if not exists(select 1 from pg_constraint where conrelid='public.template_questions'::regclass and conname='template_questions_pinned_scope_check') then
    alter table public.template_questions add constraint template_questions_pinned_scope_check
      check(pinned_scope in ('none','home_stage','all_stages'));
  end if;
  if not exists(select 1 from pg_constraint where conrelid='public.template_questions'::regclass and conname='template_questions_pinned_stage_id_fkey') then
    alter table public.template_questions add constraint template_questions_pinned_stage_id_fkey
      foreign key(pinned_stage_id) references public.template_stages(id) on delete set null;
  end if;
end $$;
create unique index if not exists ux_templates_slug on public.templates(slug) where slug is not null;
create index if not exists idx_templates_kind on public.templates(kind,updated_at desc);
create index if not exists idx_projects_module_type on public.projects(module_type,created_at desc);
commit;
