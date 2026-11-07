-- One shared currency registry, independently from asset-specific trade settings.
-- Removing an option only hides it from future choices; saved sheet answers stay intact.
create table if not exists public.v2_currency_options (
  workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
  symbol text not null check (symbol ~ '^[A-Z0-9][A-Z0-9._-]{0,19}$'),
  position integer not null check(position >= 0),
  active boolean not null default true,
  primary key(workspace_id,symbol)
);
alter table public.v2_currency_options enable row level security;
revoke all on public.v2_currency_options from public,anon,authenticated;
grant select on public.v2_currency_options to authenticated;
grant all on public.v2_currency_options to service_role;
create policy currency_options_member_read on public.v2_currency_options for select to authenticated
  using(public.v2_is_workspace_member(workspace_id));

insert into public.v2_currency_options(workspace_id,symbol,position)
select w.id,c.symbol,(c.ord-1)::integer from public.v2_workspaces w
cross join unnest(array['BTC','ETH','XRP','HYPE','BNB','DOG','SOL','ZCASH']) with ordinality c(symbol,ord)
on conflict (workspace_id,symbol) do nothing;
-- Preserve previously configured custom symbols in the new shared selection list.
with existing as (
  select distinct workspace_id,upper(btrim(symbol)) symbol from public.v2_asset_settings where active
  union select distinct workspace_id,upper(btrim(asset_symbol)) from public.v2_scenarios
  union select distinct workspace_id,upper(btrim(asset_symbol)) from public.v2_trades
), missing as (
  select e.* from existing e where e.symbol ~ '^[A-Z0-9][A-Z0-9._-]{0,19}$'
  and not exists(select 1 from public.v2_currency_options c where c.workspace_id=e.workspace_id and c.symbol=e.symbol)
)
insert into public.v2_currency_options(workspace_id,symbol,position)
select m.workspace_id,m.symbol,7+row_number() over(partition by m.workspace_id order by m.symbol)::integer from missing m
on conflict (workspace_id,symbol) do nothing;

create or replace function public.v2_seed_currency_options()
returns trigger language plpgsql security definer set search_path=public,pg_temp as $$
begin
  insert into public.v2_currency_options(workspace_id,symbol,position)
  select new.id,c.symbol,(c.ord-1)::integer
  from unnest(array['BTC','ETH','XRP','HYPE','BNB','DOG','SOL','ZCASH']) with ordinality c(symbol,ord)
  on conflict (workspace_id,symbol) do nothing;
  return new;
end $$;
revoke all on function public.v2_seed_currency_options() from public,anon,authenticated;
create trigger v2_workspace_currency_options after insert on public.v2_workspaces
for each row execute function public.v2_seed_currency_options();

create or replace function public.v2_list_currency_options(p_workspace_id uuid)
returns table(symbol text,"position" integer) language plpgsql stable security invoker set search_path=public,pg_temp as $$
begin
  if auth.uid() is null or not coalesce(public.v2_is_workspace_member(p_workspace_id),false) then
    raise exception 'Workspace membership required' using errcode='42501';
  end if;
  return query select c.symbol,c.position from public.v2_currency_options c
    where c.workspace_id=p_workspace_id and c.active order by c.position,c.symbol;
end $$;
revoke all on function public.v2_list_currency_options(uuid) from public,anon;
grant execute on function public.v2_list_currency_options(uuid) to authenticated;

create or replace function public.v2_add_currency_option(p_workspace_id uuid,p_symbol text)
returns text language plpgsql security definer set search_path=public,pg_temp as $$
declare clean text:=upper(btrim(p_symbol));next_position integer;
begin
  if auth.uid() is null or not coalesce(public.v2_can_edit_workspace(p_workspace_id),false) then
    raise exception 'Workspace edit permission required' using errcode='42501';
  end if;
  if clean is null or clean !~ '^[A-Z0-9][A-Z0-9._-]{0,19}$' then raise exception 'Invalid currency symbol' using errcode='22023';end if;
  -- Serialize list changes per workspace so simultaneous additions remain ordered and unique.
  perform pg_advisory_xact_lock(hashtextextended('currency-options:'||p_workspace_id::text,0));
  select coalesce(max(c.position)+1,0) into next_position from public.v2_currency_options c where c.workspace_id=p_workspace_id;
  insert into public.v2_currency_options(workspace_id,symbol,position)
  values(p_workspace_id,clean,next_position)
  on conflict (workspace_id,symbol) do update set active=true;
  return clean;
end $$;
revoke all on function public.v2_add_currency_option(uuid,text) from public,anon;
grant execute on function public.v2_add_currency_option(uuid,text) to authenticated;

create or replace function public.v2_remove_currency_option(p_workspace_id uuid,p_symbol text)
returns void language plpgsql security definer set search_path=public,pg_temp as $$
declare clean text:=upper(btrim(p_symbol));
begin
  if auth.uid() is null or not coalesce(public.v2_can_edit_workspace(p_workspace_id),false) then
    raise exception 'Workspace edit permission required' using errcode='42501';
  end if;
  if clean is null or clean !~ '^[A-Z0-9][A-Z0-9._-]{0,19}$' then raise exception 'Invalid currency symbol' using errcode='22023';end if;
  perform pg_advisory_xact_lock(hashtextextended('currency-options:'||p_workspace_id::text,0));
  if exists(select 1 from public.v2_currency_options c where c.workspace_id=p_workspace_id and c.symbol=clean and c.active)
    and (select count(*) from public.v2_currency_options c where c.workspace_id=p_workspace_id and c.active)<=1 then
    raise exception 'At least one currency must remain' using errcode='23514';
  end if;
  update public.v2_currency_options c set active=false where c.workspace_id=p_workspace_id and c.symbol=clean;
end $$;
revoke all on function public.v2_remove_currency_option(uuid,text) from public,anon;
grant execute on function public.v2_remove_currency_option(uuid,text) to authenticated;

alter table public.v2_template_fields drop constraint if exists v2_template_fields_field_type_check;
alter table public.v2_template_fields add constraint v2_template_fields_field_type_check check(field_type in (
 'dynamic_text','rich_text','short_text','long_text','checkbox','yes_no_na','liquidity_table','news_table','news_source_table',
 'economic_calendar','number','pnl','journal','image','info','single_choice','multi_choice','datetime','link','stage_conclusion','button',
 'currency_select','reasons_with_evidence'));

create or replace function public.v2_legacy_field_type(p_question_type text,p_field_key text)
returns text language plpgsql immutable set search_path=public,pg_temp as $$
declare kind text:=lower(btrim(coalesce(p_question_type,'')));
begin
  if kind='button' then return 'button'; end if;
  if lower(btrim(coalesce(p_field_key,'')))='trade_pnl' then return 'pnl'; end if;
  if kind='news' then kind:='news_table'; end if;
  if kind not in ('dynamic_text','rich_text','short_text','long_text','checkbox','yes_no_na',
    'liquidity_table','news_table','news_source_table','economic_calendar','number','pnl','journal','image',
    'info','single_choice','multi_choice','datetime','link','stage_conclusion','currency_select','reasons_with_evidence') then
    raise exception 'Unsupported legacy field type: %',p_question_type using errcode='23514';
  end if;
  return kind;
end $$;
revoke all on function public.v2_legacy_field_type(text,text) from public,anon;
grant execute on function public.v2_legacy_field_type(text,text) to authenticated;

-- Keep all template-copy safety checks while accepting the two new field types.
create or replace function public.import_template_tree_atomic(_target_version_id uuid,_target_stage_id uuid default null,
 _target_group_id uuid default null,_stages jsonb default '[]'::jsonb)
returns uuid language plpgsql security definer set search_path=public,pg_temp as $$
declare st jsonb; gr jsonb; qu jsonb; sid uuid; gid uuid; parent_stage uuid; version_status text; spos integer; gpos integer; qpos integer; qtype text;
begin
 if auth.uid() is null or not coalesce(app_private.has_role(auth.uid(),'admin'),false) then raise exception 'Template administrator required' using errcode='42501'; end if;
 select status into version_status from public.template_versions where id=_target_version_id for update;
 if not found or version_status<>'draft' or exists(select 1 from public.app_settings where active_template_version_id=_target_version_id)
  or exists(select 1 from public.projects where template_version_id=_target_version_id) then
  raise exception 'Copy into an unused draft template version' using errcode='23514'; end if;
 if _target_stage_id is not null and not exists(select 1 from public.template_stages where id=_target_stage_id and template_version_id=_target_version_id) then
  raise exception 'Target sheet does not belong to this draft' using errcode='23514'; end if;
 if _target_group_id is not null then
  select g.stage_id into parent_stage from public.template_groups g join public.template_stages s on s.id=g.stage_id
   where g.id=_target_group_id and s.template_version_id=_target_version_id;
  if parent_stage is null or (_target_stage_id is not null and parent_stage<>_target_stage_id) then
   raise exception 'Target section does not belong to this draft sheet' using errcode='23514'; end if;
 end if;
 if jsonb_typeof(_stages) is distinct from 'array' or jsonb_array_length(_stages)>100 then raise exception 'Invalid sheets to copy' using errcode='22023'; end if;
 select coalesce(max(position)+1,0) into spos from public.template_stages where template_version_id=_target_version_id;
 for st in select value from jsonb_array_elements(_stages) loop
  if jsonb_typeof(st)<>'object' or jsonb_typeof(st->'groups') is distinct from 'array' then raise exception 'Invalid sheet tree' using errcode='22023'; end if;
  sid:=coalesce(_target_stage_id,parent_stage);
  if sid is null then
   if nullif(btrim(st->>'title'),'') is null then raise exception 'Sheet title required' using errcode='23514'; end if;
   insert into public.template_stages(template_version_id,title,description,position,active)
   values(_target_version_id,st->>'title',st->>'description',spos,coalesce((st->>'active')::boolean,true)) returning id into sid;spos:=spos+1;
  end if;
  select coalesce(max(position)+1,0) into gpos from public.template_groups where stage_id=sid;
  for gr in select value from jsonb_array_elements(st->'groups') loop
   if jsonb_typeof(gr)<>'object' or jsonb_typeof(gr->'questions') is distinct from 'array' then raise exception 'Invalid section tree' using errcode='22023'; end if;
   gid:=_target_group_id;
   if gid is null then
    if nullif(btrim(gr->>'title'),'') is null then raise exception 'Section title required' using errcode='23514'; end if;
    insert into public.template_groups(stage_id,title,description,position,collapsible,active,behavior)
    values(sid,gr->>'title',gr->>'description',gpos,coalesce((gr->>'collapsible')::boolean,true),coalesce((gr->>'active')::boolean,true),gr->>'behavior') returning id into gid;gpos:=gpos+1;
   end if;
   select coalesce(max(position)+1,0) into qpos from public.template_questions where group_id=gid;
   for qu in select value from jsonb_array_elements(gr->'questions') loop
    qtype:=qu->>'question_type';
    if qtype is null or qtype not in ('info','checkbox','yes_no_na','single_choice','multi_choice','short_text','long_text','number','datetime','image','link','stage_conclusion','liquidity_table','news_table','economic_calendar','button','currency_select','reasons_with_evidence')
      or nullif(btrim(qu->>'title'),'') is null then raise exception 'Unsupported legacy question type or missing title: %',qtype using errcode='23514'; end if;
    if qtype='button' and coalesce(qu#>>'{options_json,action}','') not in ('activate','close','limit') then
     raise exception 'Button action must be activate, close or limit' using errcode='23514'; end if;
    insert into public.template_questions(group_id,title,description,question_type,position,required,active,options_json,visibility_rule_json,help_text,source_url,links_json,default_text_align,field_key,question_key)
    values(gid,qu->>'title',qu->>'description',qtype,qpos,coalesce((qu->>'required')::boolean,false),coalesce((qu->>'active')::boolean,true),
     nullif(qu->'options_json','null'::jsonb),nullif(qu->'visibility_rule_json','null'::jsonb),qu->>'help_text',qu->>'source_url',coalesce(nullif(qu->'links_json','null'::jsonb),'[]'::jsonb),
     coalesce(qu->>'default_text_align','right'),qu->>'field_key',coalesce(nullif(qu->>'question_key',''),gen_random_uuid()::text));qpos:=qpos+1;
   end loop;
  end loop;
 end loop;
 return _target_version_id;
end $$;
revoke all on function public.import_template_tree_atomic(uuid,uuid,uuid,jsonb) from public,anon;
grant execute on function public.import_template_tree_atomic(uuid,uuid,uuid,jsonb) to authenticated;



notify pgrst,'reload schema';
