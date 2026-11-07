-- Enforce canonical Owner / Editor / Viewer behavior for Modular Workspace V1.
-- This migration touches only v2_* objects; legacy projects and memberships are unchanged.

-- Repair any inconsistent rows before adding the invariant.
update public.v2_workspace_members m
set role = case
  when m.user_id = w.owner_id then 'owner'
  when m.role = 'owner' then 'editor'
  else m.role
end
from public.v2_workspaces w
where w.id = m.workspace_id
  and (
    (m.user_id = w.owner_id and m.role <> 'owner')
    or (m.user_id <> w.owner_id and m.role = 'owner')
  );

insert into public.v2_workspace_members(workspace_id, user_id, role)
select w.id, w.owner_id, 'owner'
from public.v2_workspaces w
on conflict (workspace_id, user_id) do update
set role = 'owner';

-- There may be exactly one role='owner' row, and it must represent owner_id.
create unique index if not exists v2_workspace_members_one_owner_idx
  on public.v2_workspace_members(workspace_id)
  where role = 'owner';

create or replace function public.v2_enforce_workspace_member_role()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  canonical_owner uuid;
begin
  if tg_op = 'UPDATE' and (
    new.workspace_id is distinct from old.workspace_id
    or new.user_id is distinct from old.user_id
  ) then
    raise exception 'workspace membership identity is immutable; delete and recreate the membership instead'
      using errcode = '23514';
  end if;

  select w.owner_id
  into canonical_owner
  from public.v2_workspaces w
  where w.id = new.workspace_id;

  if canonical_owner is null then
    raise exception 'workspace does not exist'
      using errcode = '23503';
  end if;

  if new.user_id = canonical_owner then
    -- The canonical owner can never be demoted through the membership table.
    new.role := 'owner';
  elsif new.role = 'owner' then
    -- Additional owners are not supported. Ownership is defined only by
    -- v2_workspaces.owner_id, not by a client-provided membership role.
    raise exception 'only the canonical workspace owner may have role owner'
      using errcode = '23514';
  end if;

  return new;
end;
$$;

revoke all on function public.v2_enforce_workspace_member_role() from public;
revoke all on function public.v2_enforce_workspace_member_role() from anon;
revoke all on function public.v2_enforce_workspace_member_role() from authenticated;

drop trigger if exists v2_workspace_members_role_guard
  on public.v2_workspace_members;
create trigger v2_workspace_members_role_guard
before insert or update of workspace_id, user_id, role
on public.v2_workspace_members
for each row execute function public.v2_enforce_workspace_member_role();

-- Keep the canonical owner's membership synchronized for newly created
-- workspaces and for any future trusted ownership transfer.
create or replace function public.v2_sync_workspace_owner_membership()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
begin
  if tg_op = 'UPDATE' and new.owner_id is distinct from old.owner_id then
    update public.v2_workspace_members
    set role = 'editor'
    where workspace_id = new.id
      and user_id = old.owner_id
      and role = 'owner';
  end if;

  insert into public.v2_workspace_members(workspace_id, user_id, role)
  values (new.id, new.owner_id, 'owner')
  on conflict (workspace_id, user_id) do update
  set role = 'owner';

  return new;
end;
$$;

revoke all on function public.v2_sync_workspace_owner_membership() from public;
revoke all on function public.v2_sync_workspace_owner_membership() from anon;
revoke all on function public.v2_sync_workspace_owner_membership() from authenticated;

drop trigger if exists v2_workspaces_sync_owner_membership
  on public.v2_workspaces;
create trigger v2_workspaces_sync_owner_membership
after insert or update of owner_id
on public.v2_workspaces
for each row execute function public.v2_sync_workspace_owner_membership();

-- A viewer may enter and read a shared workspace without being forced through
-- template seeding, which is intentionally restricted to owner/editor roles.
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

  select m.workspace_id
  into wid
  from public.v2_workspace_members m
  join public.v2_workspaces w on w.id = m.workspace_id
  where m.user_id = uid
  order by (w.owner_id = uid) desc, w.created_at asc
  limit 1;

  if wid is null then
    insert into public.v2_workspaces(owner_id, name)
    values (uid, 'סביבת העבודה למסחר')
    on conflict (owner_id) do update
    set owner_id = excluded.owner_id
    returning id into wid;
  end if;

  -- Repair/guarantee the canonical owner's membership when the caller owns wid.
  insert into public.v2_workspace_members(workspace_id, user_id, role)
  select w.id, w.owner_id, 'owner'
  from public.v2_workspaces w
  where w.id = wid
    and w.owner_id = uid
  on conflict (workspace_id, user_id) do update
  set role = 'owner';

  -- Owners and editors may initialize missing defaults. Viewers only read.
  if public.v2_can_edit_workspace(wid) then
    perform public.v2_seed_default_templates(wid);
  end if;

  return wid;
end;
$$;

revoke all on function public.v2_ensure_my_workspace() from public;
revoke all on function public.v2_ensure_my_workspace() from anon;
grant execute on function public.v2_ensure_my_workspace() to authenticated;
