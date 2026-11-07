-- Collection worksheets reference the canonical scenario, never a copy. One
-- ordered link list belongs to the session and is shared by all of its sheets.
create table if not exists public.v2_collection_scenario_links (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
  session_id uuid not null references public.v2_collection_sessions(id) on delete cascade,
  scenario_id uuid not null references public.v2_scenarios(id) on delete cascade,
  position integer not null check (position >= 0),
  creation_request_id uuid,
  created_by uuid references auth.users(id) on delete set null,
  created_at timestamptz not null default now(),
  unique (session_id, scenario_id),
  unique (session_id, position),
  unique (session_id, creation_request_id)
);
create index if not exists v2_collection_scenario_links_workspace_idx
  on public.v2_collection_scenario_links(workspace_id, session_id, position);
alter table public.v2_collection_scenario_links enable row level security;
revoke all on public.v2_collection_scenario_links from public, anon, authenticated;
grant select on public.v2_collection_scenario_links to authenticated;
grant all on public.v2_collection_scenario_links to service_role;
drop policy if exists v2_collection_scenario_links_read on public.v2_collection_scenario_links;
create policy v2_collection_scenario_links_read on public.v2_collection_scenario_links
  for select to authenticated using (
    public.v2_is_workspace_member(workspace_id)
    and exists (select 1 from public.v2_collection_sessions c where c.id=session_id
      and c.workspace_id=v2_collection_scenario_links.workspace_id and c.deleted_at is null)
    and exists (select 1 from public.v2_scenarios s where s.id=scenario_id
      and s.workspace_id=v2_collection_scenario_links.workspace_id)
  );

create or replace function public.v2_guard_collection_scenario_link()
returns trigger language plpgsql security definer set search_path=public,pg_temp as $$
begin
  if tg_op='UPDATE' and (new.workspace_id is distinct from old.workspace_id
    or new.session_id is distinct from old.session_id or new.scenario_id is distinct from old.scenario_id
    or new.creation_request_id is distinct from old.creation_request_id
    or (new.created_by is distinct from old.created_by and not (new.created_by is null
      and not exists(select 1 from auth.users where id=old.created_by)))
    or new.created_at is distinct from old.created_at) then
    raise exception 'Collection scenario link identity is immutable' using errcode='23514';
  end if;
  if not exists(select 1 from public.v2_collection_sessions c
    join public.v2_templates t on t.id=c.template_id
    where c.id=new.session_id and c.workspace_id=new.workspace_id
      and t.workspace_id=new.workspace_id and t.kind='collection')
    or not exists(select 1 from public.v2_scenarios s where s.id=new.scenario_id
      and s.workspace_id=new.workspace_id) then
    raise exception 'Collection scenario references must share a workspace' using errcode='23514';
  end if;
  return new;
end $$;
revoke all on function public.v2_guard_collection_scenario_link() from public,anon,authenticated;
drop trigger if exists v2_collection_scenario_links_scope_guard on public.v2_collection_scenario_links;
create trigger v2_collection_scenario_links_scope_guard before insert or update
  on public.v2_collection_scenario_links for each row execute function public.v2_guard_collection_scenario_link();

-- Mutation RPCs alone may create links. Lock workspace, membership and then the
-- session so two creations, attachment retries and permission changes serialize.
create or replace function public.v2_lock_collection_scenario_session(p_session_id uuid)
returns public.v2_collection_sessions language plpgsql security definer set search_path=public,pg_temp as $$
declare c public.v2_collection_sessions; wid uuid;
begin
  if auth.uid() is null then raise exception 'נדרשת התחברות.' using errcode='42501'; end if;
  select workspace_id into wid from public.v2_collection_sessions where id=p_session_id;
  if not found or not public.v2_can_edit_workspace(wid) then
    raise exception 'נדרשת הרשאת עריכה לגליון.' using errcode='42501'; end if;
  perform id from public.v2_workspaces where id=wid for update;
  perform user_id from public.v2_workspace_members where workspace_id=wid and user_id=auth.uid()
    and role in ('owner','editor') for share;
  if not found then raise exception 'נדרשת הרשאת עריכה לגליון.' using errcode='42501'; end if;
  select * into c from public.v2_collection_sessions where id=p_session_id and workspace_id=wid for update;
  if not found or c.deleted_at is not null or c.status='archived'
    or not exists(select 1 from public.v2_templates t where t.id=c.template_id
      and t.workspace_id=wid and t.kind='collection') then
    raise exception 'הגליון אינו זמין לעריכה.' using errcode='23514'; end if;
  return c;
end $$;
revoke all on function public.v2_lock_collection_scenario_session(uuid) from public,anon,authenticated;

create or replace function public.v2_collection_scenario_role_fields(p_template_id uuid)
returns jsonb language sql stable security invoker set search_path=public,pg_temp as $$
  select coalesce(jsonb_object_agg(role,id),'{}'::jsonb)||jsonb_build_object(
    'body', (array_agg(id) filter(where role='body'))[1],
    'reasons', (array_agg(id) filter(where role='reasons'))[1],
    'invalidation', (array_agg(id) filter(where role='invalidation'))[1])
  from (
    select distinct on(role) role,id from (
      select f.id, coalesce(f.settings->>'scenario_role', case f.field_key
        when 'scenario_body' then 'body' when 'scenario_reasons' then 'reasons'
        when 'scenario_invalidation' then 'invalidation' end) role,
        sec.position section_position,f.position field_position
      from public.v2_template_fields f join public.v2_template_sections sec on sec.id=f.section_id
      where sec.template_id=p_template_id and sec.active and f.active
    ) roles where role in ('body','reasons','invalidation')
    order by role,section_position,field_position,id
  ) selected;
$$;
revoke all on function public.v2_collection_scenario_role_fields(uuid) from public,anon;
grant execute on function public.v2_collection_scenario_role_fields(uuid) to authenticated;

create or replace function public.v2_collection_scenario_bundle(p_session_id uuid,p_scenario_id uuid)
returns jsonb language plpgsql volatile security invoker set search_path=public,pg_temp as $$
declare linked public.v2_collection_scenario_links; scenario public.v2_scenarios; vals jsonb;
begin
  select * into linked from public.v2_collection_scenario_links
    where session_id=p_session_id and scenario_id=p_scenario_id;
  if not found or auth.uid() is null or not public.v2_is_workspace_member(linked.workspace_id)
    or not exists(select 1 from public.v2_collection_sessions c where c.id=p_session_id
      and c.workspace_id=linked.workspace_id and c.deleted_at is null) then
    raise exception 'אין הרשאה לתרחיש בגליון זה.' using errcode='42501'; end if;
  select * into scenario from public.v2_scenarios where id=p_scenario_id and workspace_id=linked.workspace_id;
  if not found then raise exception 'אין הרשאה לתרחיש בגליון זה.' using errcode='42501'; end if;
  select coalesce(jsonb_agg(to_jsonb(v) order by field_id),'[]'::jsonb) into vals
    from public.v2_scenario_values v where v.scenario_id=scenario.id;
  return jsonb_build_object('link',to_jsonb(linked)-'creation_request_id','scenario',to_jsonb(scenario),
    'values',vals,'role_fields',public.v2_collection_scenario_role_fields(scenario.template_id));
end $$;
revoke all on function public.v2_collection_scenario_bundle(uuid,uuid) from public,anon;
grant execute on function public.v2_collection_scenario_bundle(uuid,uuid) to authenticated;

create or replace function public.v2_collection_scenarios(p_session_id uuid)
returns jsonb language plpgsql stable security invoker set search_path=public,pg_temp as $$
declare wid uuid; result jsonb;
begin
  select workspace_id into wid from public.v2_collection_sessions where id=p_session_id and deleted_at is null;
  if not found or auth.uid() is null or not public.v2_is_workspace_member(wid) then
    raise exception 'אין הרשאה לגליון זה.' using errcode='42501'; end if;
  select coalesce(jsonb_agg(public.v2_collection_scenario_bundle(p_session_id,scenario_id)
    order by position,id),'[]'::jsonb) into result from public.v2_collection_scenario_links
    where session_id=p_session_id and workspace_id=wid;
  return result;
end $$;
revoke all on function public.v2_collection_scenarios(uuid) from public,anon;
grant execute on function public.v2_collection_scenarios(uuid) to authenticated;

create or replace function public.v2_collection_scenario_choices(p_session_id uuid,p_limit integer default 50,
  p_before_created_at timestamptz default null,p_before_id uuid default null)
returns jsonb language plpgsql stable security invoker set search_path=public,pg_temp as $$
declare wid uuid; entries jsonb; tail jsonb; more boolean;
begin
  select workspace_id into wid from public.v2_collection_sessions where id=p_session_id and deleted_at is null;
  if not found or auth.uid() is null or not public.v2_is_workspace_member(wid) then
    raise exception 'אין הרשאה לגליון זה.' using errcode='42501'; end if;
  if p_limit is null or p_limit not between 1 and 100 or
    (p_before_created_at is null)<>(p_before_id is null) then
    raise exception 'בחירת עמוד תרחישים אינה תקינה.' using errcode='23514'; end if;
  select coalesce(jsonb_agg(to_jsonb(s) order by created_at desc,id desc),'[]'::jsonb) into entries
    from (select * from public.v2_scenarios where workspace_id=wid and status<>'archived'
      and (p_before_created_at is null or (created_at,id)<(p_before_created_at,p_before_id))
      order by created_at desc,id desc limit p_limit+1) s;
  more:=jsonb_array_length(entries)>p_limit;
  if more then entries:=entries-p_limit; end if;
  tail:=entries->(jsonb_array_length(entries)-1);
  return jsonb_build_object('scenarios',entries,'nextCursor',case when more then
    jsonb_build_object('createdAt',tail->>'created_at','id',tail->>'id') else null end);
end $$;
revoke all on function public.v2_collection_scenario_choices(uuid,integer,timestamptz,uuid) from public,anon;
grant execute on function public.v2_collection_scenario_choices(uuid,integer,timestamptz,uuid) to authenticated;

create or replace function public.v2_create_collection_scenario(p_session_id uuid,p_request_id uuid)
returns jsonb language plpgsql security definer set search_path=public,pg_temp as $$
declare c public.v2_collection_sessions; prior public.v2_collection_scenario_links;
  tid uuid; sid uuid; preferred uuid; author text;
begin
  c:=public.v2_lock_collection_scenario_session(p_session_id);
  if p_request_id is null then raise exception 'נדרש מזהה יצירה.' using errcode='23514'; end if;
  select * into prior from public.v2_collection_scenario_links
    where session_id=c.id and creation_request_id=p_request_id;
  if found then
    if prior.created_by is distinct from auth.uid() then
      raise exception 'מזהה היצירה אינו שייך למשתמש.' using errcode='42501'; end if;
    return public.v2_collection_scenario_bundle(c.id,prior.scenario_id);
  end if;
  select default_scenario_template_id into preferred from public.v2_asset_settings
    where workspace_id=c.workspace_id and symbol='BTC' and active;
  select id into tid from public.v2_templates where workspace_id=c.workspace_id and kind='scenario' and active
    order by (id=preferred) desc nulls last,(asset_symbol='BTC') desc nulls last,
      is_default desc,created_at,id limit 1;
  if not found then raise exception 'נדרשת תבנית תרחיש פעילה.' using errcode='23514'; end if;
  -- Author attribution comes from the authenticated account, never client input.
  select coalesce(nullif(trim(display_name),''),split_part(coalesce(email,''),'@',1))
    into author from public.profiles where id=auth.uid();
  sid:=gen_random_uuid();
  perform public.save_v2_scenario(jsonb_build_object('id',sid,'workspace_id',c.workspace_id,
    'title','תרחיש מגליון '||to_char(c.created_at at time zone 'Asia/Jerusalem','DD.MM.YYYY'),
    'asset_symbol','BTC','template_id',tid,'author_name',nullif(author,''),
    'scenario_html','','reasons_html','','invalidation_html',''), '[]'::jsonb);
  insert into public.v2_collection_scenario_links(workspace_id,session_id,scenario_id,position,
    creation_request_id,created_by) select c.workspace_id,c.id,sid,coalesce(max(position),-1)+1,
      p_request_id,auth.uid() from public.v2_collection_scenario_links where session_id=c.id;
  return public.v2_collection_scenario_bundle(c.id,sid);
end $$;
revoke all on function public.v2_create_collection_scenario(uuid,uuid) from public,anon;
grant execute on function public.v2_create_collection_scenario(uuid,uuid) to authenticated;

create or replace function public.v2_attach_collection_scenario(p_session_id uuid,p_scenario_id uuid)
returns jsonb language plpgsql security definer set search_path=public,pg_temp as $$
declare c public.v2_collection_sessions; sid uuid;
begin
  c:=public.v2_lock_collection_scenario_session(p_session_id);
  select id into sid from public.v2_scenarios where id=p_scenario_id and workspace_id=c.workspace_id
    and status<>'archived' for update;
  if not found then raise exception 'התרחיש אינו זמין בגליון זה.' using errcode='42501'; end if;
  if not exists(select 1 from public.v2_collection_scenario_links where session_id=c.id and scenario_id=sid) then
    insert into public.v2_collection_scenario_links(workspace_id,session_id,scenario_id,position,created_by)
      select c.workspace_id,c.id,sid,coalesce(max(position),-1)+1,auth.uid()
        from public.v2_collection_scenario_links where session_id=c.id;
  end if;
  return public.v2_collection_scenario_bundle(c.id,sid);
end $$;
revoke all on function public.v2_attach_collection_scenario(uuid,uuid) from public,anon;
grant execute on function public.v2_attach_collection_scenario(uuid,uuid) to authenticated;

create or replace function public.v2_save_collection_scenario_role(p_session_id uuid,p_scenario_id uuid,
  p_role text,p_value jsonb,p_expected_html text,p_expected_value jsonb)
returns jsonb language plpgsql security definer set search_path=public,pg_temp as $$
declare c public.v2_collection_sessions; s public.v2_scenarios; v_field_id uuid;
  prior_value jsonb; prior_html text; next_value jsonb; body text;
begin
  c:=public.v2_lock_collection_scenario_session(p_session_id);
  select * into s from public.v2_scenarios where id=p_scenario_id and workspace_id=c.workspace_id for update;
  if not found or s.status='archived' or not exists(select 1 from public.v2_collection_scenario_links
    where session_id=c.id and scenario_id=s.id and workspace_id=c.workspace_id) then
    raise exception 'התרחיש אינו זמין לעריכה בגליון זה.' using errcode='42501'; end if;
  if p_role is null or p_role not in ('body','reasons','invalidation')
    or jsonb_typeof(p_value) is distinct from 'object' or jsonb_typeof(p_value->'text') is distinct from 'string'
    or p_expected_html is null or (p_expected_value is not null and jsonb_typeof(p_expected_value)<>'object') then
    raise exception 'תוכן התרחיש אינו תקין.' using errcode='23514'; end if;
  prior_html:=coalesce(case p_role when 'body' then s.scenario_html when 'reasons' then s.reasons_html
    else s.invalidation_html end,'');
  v_field_id:=nullif(public.v2_collection_scenario_role_fields(s.template_id)->>p_role,'')::uuid;
  if v_field_id is not null then
    select value into prior_value from public.v2_scenario_values v
      where v.scenario_id=s.id and v.field_id=v_field_id for update;
  end if;
  if prior_html is distinct from p_expected_html or prior_value is distinct from p_expected_value then
    raise exception 'התרחיש עודכן במקום אחר. טענו את העדכון לפני שמירה חוזרת.' using errcode='40001'; end if;
  body:=p_value->>'text'; next_value:=p_value;
  if prior_value ? 'text_value' then next_value:=next_value||jsonb_build_object('text_value',body); end if;
  if prior_value ? 'html' then next_value:=next_value||jsonb_build_object('html',body); end if;
  if v_field_id is not null then
    insert into public.v2_scenario_values(scenario_id,field_id,value,updated_by) values(s.id,v_field_id,next_value,auth.uid())
      on conflict(scenario_id,field_id) do update set value=excluded.value,updated_by=auth.uid();
  end if;
  update public.v2_scenarios set
    scenario_html=case when p_role='body' then body else scenario_html end,
    reasons_html=case when p_role='reasons' then body else reasons_html end,
    invalidation_html=case when p_role='invalidation' then body else invalidation_html end,
    updated_by=auth.uid(),updated_at=now() where id=s.id and workspace_id=c.workspace_id;
  return public.v2_collection_scenario_bundle(c.id,s.id);
end $$;
revoke all on function public.v2_save_collection_scenario_role(uuid,uuid,text,jsonb,text,jsonb) from public,anon;
grant execute on function public.v2_save_collection_scenario_role(uuid,uuid,text,jsonb,text,jsonb) to authenticated;

-- Realtime is optional in isolated SQL fixtures. Member-scoped RLS still applies
-- to published changes; clients also refetch after writes and on a short interval.
do $$
declare tbl text;
begin
  if exists(select 1 from pg_publication where pubname='supabase_realtime') then
    foreach tbl in array array['v2_collection_scenario_links','v2_scenarios','v2_scenario_values'] loop
      if not exists(select 1 from pg_publication_tables where pubname='supabase_realtime'
        and schemaname='public' and tablename=tbl) then
        execute format('alter publication supabase_realtime add table public.%I',tbl);
      end if;
    end loop;
  end if;
end $$;
notify pgrst, 'reload schema';
