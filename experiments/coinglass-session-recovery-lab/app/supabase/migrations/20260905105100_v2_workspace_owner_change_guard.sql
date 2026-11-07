-- Ownership changes are not exposed to authenticated application clients.
-- A future trusted maintenance process may transfer ownership without a user JWT;
-- the synchronization trigger from the preceding migration will then repair roles.

create or replace function public.v2_guard_workspace_owner_change()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
begin
  if new.owner_id is distinct from old.owner_id
     and auth.uid() is not null then
    raise exception 'workspace ownership cannot be changed from the application'
      using errcode = '42501';
  end if;

  return new;
end;
$$;

revoke all on function public.v2_guard_workspace_owner_change() from public;
revoke all on function public.v2_guard_workspace_owner_change() from anon;
revoke all on function public.v2_guard_workspace_owner_change() from authenticated;

drop trigger if exists v2_workspaces_owner_change_guard
  on public.v2_workspaces;
create trigger v2_workspaces_owner_change_guard
before update of owner_id
on public.v2_workspaces
for each row execute function public.v2_guard_workspace_owner_change();
