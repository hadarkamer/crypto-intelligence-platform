
-- roles
create type public.app_role as enum ('admin','user');

create table public.user_roles (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null,
  role public.app_role not null,
  created_at timestamptz not null default now(),
  unique (user_id, role)
);
grant select on public.user_roles to authenticated;
grant all on public.user_roles to service_role;
alter table public.user_roles enable row level security;
create policy "users read own roles" on public.user_roles for select to authenticated using (user_id = auth.uid());

create or replace function public.has_role(_user_id uuid, _role public.app_role)
returns boolean language sql stable security definer set search_path = public as $$
  select exists (select 1 from public.user_roles where user_id = _user_id and role = _role)
$$;

-- ownership
alter table public.projects add column if not exists user_id uuid default auth.uid();

create or replace function public.owns_project(_project_id uuid)
returns boolean language sql stable security definer set search_path = public as $$
  select exists (select 1 from public.projects p where p.id = _project_id and p.user_id = auth.uid())
$$;

-- drop permissive policies
drop policy if exists open_access on public.projects;
drop policy if exists open_access on public.project_answers;
drop policy if exists open_access on public.project_images;
drop policy if exists open_access on public.trades;
drop policy if exists open_access on public.templates;
drop policy if exists open_access on public.template_versions;
drop policy if exists open_access on public.template_stages;
drop policy if exists open_access on public.template_groups;
drop policy if exists open_access on public.template_questions;
drop policy if exists open_access on public.app_settings;

revoke all on public.projects, public.project_answers, public.project_images, public.trades,
  public.templates, public.template_versions, public.template_stages, public.template_groups,
  public.template_questions, public.app_settings from anon;

create policy "own projects" on public.projects for all to authenticated
  using (user_id = auth.uid()) with check (user_id = auth.uid());

create policy "own answers" on public.project_answers for all to authenticated
  using (public.owns_project(project_id)) with check (public.owns_project(project_id));

create policy "own images" on public.project_images for all to authenticated
  using (public.owns_project(project_id)) with check (public.owns_project(project_id));

create policy "own trades" on public.trades for all to authenticated
  using (public.owns_project(project_id)) with check (public.owns_project(project_id));

-- template content: read for signed-in users, write for admins
create policy "read templates" on public.templates for select to authenticated using (true);
create policy "admin write templates" on public.templates for all to authenticated
  using (public.has_role(auth.uid(),'admin')) with check (public.has_role(auth.uid(),'admin'));

create policy "read template_versions" on public.template_versions for select to authenticated using (true);
create policy "admin write template_versions" on public.template_versions for all to authenticated
  using (public.has_role(auth.uid(),'admin')) with check (public.has_role(auth.uid(),'admin'));

create policy "read template_stages" on public.template_stages for select to authenticated using (true);
create policy "admin write template_stages" on public.template_stages for all to authenticated
  using (public.has_role(auth.uid(),'admin')) with check (public.has_role(auth.uid(),'admin'));

create policy "read template_groups" on public.template_groups for select to authenticated using (true);
create policy "admin write template_groups" on public.template_groups for all to authenticated
  using (public.has_role(auth.uid(),'admin')) with check (public.has_role(auth.uid(),'admin'));

create policy "read template_questions" on public.template_questions for select to authenticated using (true);
create policy "admin write template_questions" on public.template_questions for all to authenticated
  using (public.has_role(auth.uid(),'admin')) with check (public.has_role(auth.uid(),'admin'));

create policy "read app_settings" on public.app_settings for select to authenticated using (true);
create policy "admin write app_settings" on public.app_settings for all to authenticated
  using (public.has_role(auth.uid(),'admin')) with check (public.has_role(auth.uid(),'admin'));

-- storage: owner-scoped access to project-images
drop policy if exists project_images_all on storage.objects;
create policy "own project images read" on storage.objects for select to authenticated
  using (bucket_id = 'project-images' and public.owns_project(((storage.foldername(name))[1])::uuid));
create policy "own project images insert" on storage.objects for insert to authenticated
  with check (bucket_id = 'project-images' and public.owns_project(((storage.foldername(name))[1])::uuid));
create policy "own project images update" on storage.objects for update to authenticated
  using (bucket_id = 'project-images' and public.owns_project(((storage.foldername(name))[1])::uuid))
  with check (bucket_id = 'project-images' and public.owns_project(((storage.foldername(name))[1])::uuid));
create policy "own project images delete" on storage.objects for delete to authenticated
  using (bucket_id = 'project-images' and public.owns_project(((storage.foldername(name))[1])::uuid));

-- first signed-up user becomes admin so the template editor keeps working
create or replace function public.grant_first_admin()
returns trigger language plpgsql security definer set search_path = public as $$
begin
  insert into public.user_roles (user_id, role)
  values (new.id, case when exists (select 1 from public.user_roles where role = 'admin') then 'user' else 'admin' end)
  on conflict do nothing;
  return new;
end; $$;
