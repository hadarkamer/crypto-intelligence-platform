-- An explicit workspace choice must not be overridden by ownership of another
-- workspace. Preferences never grant membership or move existing records.
create table if not exists public.v2_workspace_preferences (
  user_id uuid primary key,
  workspace_id uuid not null,
  updated_at timestamptz not null default now(),
  constraint v2_workspace_preferences_member_fkey
    foreign key (workspace_id, user_id)
    references public.v2_workspace_members(workspace_id, user_id) on delete cascade
);
alter table public.v2_workspace_preferences enable row level security;
revoke all on public.v2_workspace_preferences from anon, authenticated;
grant select on public.v2_workspace_preferences to authenticated;
grant all on public.v2_workspace_preferences to service_role;
drop policy if exists v2_workspace_preferences_select on public.v2_workspace_preferences;
create policy v2_workspace_preferences_select on public.v2_workspace_preferences
  for select to authenticated
  using (user_id = auth.uid() and public.v2_is_workspace_member(workspace_id));

create or replace function public.v2_ensure_my_workspace()
returns uuid
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  uid uuid := auth.uid();
  wid uuid;
begin
  if uid is null then
    raise exception 'authentication required';
  end if;

  select p.workspace_id into wid
  from public.v2_workspace_preferences p
  join public.v2_workspace_members m
    on m.workspace_id = p.workspace_id and m.user_id = p.user_id
  where p.user_id = uid;

  if wid is null then
    select m.workspace_id into wid
    from public.v2_workspace_members m
    join public.v2_workspaces w on w.id = m.workspace_id
    where m.user_id = uid
    order by (w.owner_id = uid) desc, w.created_at asc, w.id
    limit 1;
  end if;

  if wid is null then
    insert into public.v2_workspaces(owner_id, name)
    values (uid, 'סביבת העבודה למסחר')
    on conflict (owner_id) do update
    set owner_id = excluded.owner_id
    returning id into wid;
  end if;

  insert into public.v2_workspace_members(workspace_id, user_id, role)
  select w.id, w.owner_id, 'owner'
  from public.v2_workspaces w
  where w.id = wid and w.owner_id = uid
  on conflict (workspace_id, user_id) do update set role = 'owner';

  if public.v2_can_edit_workspace(wid) then
    perform public.v2_seed_default_templates(wid);
  end if;

  return wid;
end;
$$;
