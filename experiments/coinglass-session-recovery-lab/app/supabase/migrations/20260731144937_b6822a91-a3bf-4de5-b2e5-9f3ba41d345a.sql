
drop function if exists public.grant_first_admin();

revoke all on function public.has_role(uuid, public.app_role) from public, anon, authenticated;
revoke all on function public.owns_project(uuid) from public, anon, authenticated;
revoke all on function public.set_updated_at() from public, anon, authenticated;

create or replace function public.claim_admin()
returns boolean language plpgsql security definer set search_path = public as $$
declare uid uuid := auth.uid();
begin
  if uid is null then return false; end if;
  if exists (select 1 from public.user_roles where role = 'admin') then
    return exists (select 1 from public.user_roles where user_id = uid and role = 'admin');
  end if;
  insert into public.user_roles (user_id, role) values (uid, 'admin') on conflict do nothing;
  return true;
end; $$;

revoke all on function public.claim_admin() from public, anon;
grant execute on function public.claim_admin() to authenticated;
