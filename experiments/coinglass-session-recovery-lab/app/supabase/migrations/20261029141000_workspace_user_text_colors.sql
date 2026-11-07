-- Share only a member's public writing color inside the caller's own workspace.
-- All other user_preferences columns remain protected by the existing own-user RLS.
begin;

create or replace function public.v2_workspace_user_text_colors(p_workspace_id uuid)
returns table(user_id uuid, default_text_class text)
language sql
stable
security definer
set search_path = public, pg_temp
as $$
  select m.user_id,
    case when p.default_text_class = any(array[
      'rt-color-default','rt-color-red','rt-color-orange','rt-color-gold',
      'rt-color-green','rt-color-blue','rt-color-purple','rt-color-gray'
    ]::text[]) then p.default_text_class else 'rt-color-default' end
  from public.v2_workspace_members m
  left join public.user_preferences p on p.user_id = m.user_id
  where m.workspace_id = p_workspace_id
    and public.v2_is_workspace_member(p_workspace_id);
$$;

revoke all on function public.v2_workspace_user_text_colors(uuid) from public, anon;
grant execute on function public.v2_workspace_user_text_colors(uuid) to authenticated;

commit;
