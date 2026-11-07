-- Atomic scenario families and independently addressed trade cycles. Legacy rows/tables stay intact.
alter table public.v2_scenarios add column if not exists alternatives jsonb not null default '[]'::jsonb;
do $$ begin
  if not exists(select 1 from pg_constraint where conrelid='public.v2_scenarios'::regclass and conname='v2_scenarios_alternatives_array') then
    alter table public.v2_scenarios add constraint v2_scenarios_alternatives_array check(jsonb_typeof(alternatives)='array');
  end if;
end $$;
-- Preserve alternatives authored through the original V2 table; never remove its records.
update public.v2_scenarios s set alternatives = a.items from (
  select scenario_id,jsonb_agg(jsonb_build_object('id',id,'scenario_html',coalesce(content_html,''),'reasons_html','','invalidation_html','') order by position) items
  from public.v2_scenario_alternatives group by scenario_id
) a where s.id=a.scenario_id and s.alternatives='[]'::jsonb;

alter table public.v2_trades add column if not exists root_trade_id uuid references public.v2_trades(id) on delete restrict;
alter table public.v2_trades add column if not exists cycle_number integer not null default 1;
create unique index if not exists v2_trade_family_cycle_idx on public.v2_trades((coalesce(root_trade_id,id)),cycle_number);

create or replace function public.v2_field_complete(p_type text,p_value jsonb)
returns boolean language plpgsql immutable set search_path=public,pg_temp as $$
declare v jsonb:=coalesce(p_value,'{}'); txt text;
begin
  if v->>'completion'='na' then return true; end if;
  if p_type='economic_calendar' then return coalesce(v->>'reviewed','false')='true'; end if;
  if p_type='checkbox' then return coalesce(v->>'checked','false')='true'; end if;
  if p_type='yes_no_na' then return coalesce(v->>'choice','') in ('yes','no','na'); end if;
  if p_type in ('number','pnl') then
    txt:=coalesce(v->>'text','');
    if txt !~ '^\s*[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)([eE][+-]?[0-9]+)?\s*$' then return false; end if;
    begin return abs(txt::numeric)<=1.7976931348623157e308::numeric;
    exception when numeric_value_out_of_range or invalid_text_representation then return false; end;
  end if;
  if p_type in ('news_table','news_source_table','liquidity_table') then
    if jsonb_typeof(v->'rows') is distinct from 'array' then return false; end if;
    return exists(select 1 from jsonb_array_elements(v->'rows') r where case when p_type='liquidity_table' then
      length(trim(coalesce(r->>'price','')))>0 and r->>'amount' in ('many','normal','few')
      else length(trim(coalesce(r->>'text','')))>0 and r->>'importance' is not null end);
  end if;
  txt:=coalesce(v->>'text','');
  if txt ~* '<img\y[^>]*(data-path|src)=' then return true; end if;
  txt:=regexp_replace(regexp_replace(regexp_replace(txt,'<[^>]*>',' ','g'),'&(nbsp|#160|#x0*a0);',' ','gi'),'[\s​‌‍﻿]','','g');
  return length(txt)>0;
end $$;

create or replace function public.save_v2_scenario(p_scenario jsonb,p_values jsonb default '[]')
returns public.v2_scenarios language plpgsql security invoker set search_path=public,pg_temp as $$
declare sid uuid:=coalesce(nullif(p_scenario->>'id','')::uuid,gen_random_uuid()); wid uuid:=(p_scenario->>'workspace_id')::uuid; result public.v2_scenarios; entry jsonb; exists_id uuid;
begin
  if auth.uid() is null or not public.v2_can_edit_workspace(wid) then raise exception 'Workspace edit permission required' using errcode='42501'; end if;
  if nullif(trim(p_scenario->>'title'),'') is null or coalesce(p_scenario->>'asset_symbol','') !~ '^[A-Z0-9][A-Z0-9._-]{0,19}$' then raise exception 'Scenario title and valid asset are required' using errcode='23514'; end if;
  if jsonb_typeof(coalesce(p_scenario->'alternatives','[]'))<>'array' or jsonb_typeof(p_values)<>'array' then raise exception 'Invalid scenario values' using errcode='23514'; end if;
  select id into exists_id from public.v2_scenarios where id=sid and workspace_id=wid for update;
  if exists_id is null then
    insert into public.v2_scenarios(id,workspace_id,asset_symbol,title,template_id,author_name,scenario_html,reasons_html,invalidation_html,alternatives,created_by,updated_by)
    values(sid,wid,p_scenario->>'asset_symbol',trim(p_scenario->>'title'),nullif(p_scenario->>'template_id','')::uuid,p_scenario->>'author_name',p_scenario->>'scenario_html',p_scenario->>'reasons_html',p_scenario->>'invalidation_html',coalesce(p_scenario->'alternatives','[]'),auth.uid(),auth.uid()) returning * into result;
  else
    update public.v2_scenarios set title=trim(p_scenario->>'title'),asset_symbol=p_scenario->>'asset_symbol',template_id=nullif(p_scenario->>'template_id','')::uuid,author_name=p_scenario->>'author_name',scenario_html=p_scenario->>'scenario_html',reasons_html=p_scenario->>'reasons_html',invalidation_html=p_scenario->>'invalidation_html',alternatives=coalesce(p_scenario->'alternatives','[]'),updated_by=auth.uid()
    where id=sid and workspace_id=wid returning * into result;
  end if;
  for entry in select value from jsonb_array_elements(p_values) loop
    insert into public.v2_scenario_values(scenario_id,field_id,value,updated_by) values(sid,(entry->>'field_id')::uuid,coalesce(entry->'value','{}'),auth.uid())
    on conflict(scenario_id,field_id) do update set value=excluded.value,updated_by=auth.uid();
  end loop;
  return result;
end $$;

create or replace function public.reorder_v2_scenarios(p_workspace_id uuid,p_ids uuid[])
returns void language plpgsql security invoker set search_path=public,pg_temp as $$
begin
  if auth.uid() is null or not public.v2_can_edit_workspace(p_workspace_id) then raise exception 'Workspace edit permission required' using errcode='42501'; end if;
  if cardinality(p_ids)<>(select count(distinct id) from unnest(p_ids) id) or cardinality(p_ids)<>(select count(*) from public.v2_scenarios where workspace_id=p_workspace_id and id=any(p_ids)) then raise exception 'Invalid reorder list' using errcode='23514'; end if;
  perform id from public.v2_scenarios where workspace_id=p_workspace_id and id=any(p_ids) order by id for update;
  update public.v2_scenarios s set position=r.position::integer,updated_by=auth.uid()
    from unnest(p_ids) with ordinality r(id,position) where s.id=r.id and s.workspace_id=p_workspace_id;
end $$;

create or replace function public.v2_guard_trade_workflow()
returns trigger language plpgsql security definer set search_path=public,pg_temp as $$
declare root_record public.v2_trades;
begin
  if tg_op='INSERT' then
    if new.status<>'evaluating' or new.pnl is not null then raise exception 'A trade starts in evaluation without PnL' using errcode='23514'; end if;
    if new.template_id is null or not exists(select 1 from public.v2_templates where id=new.template_id and workspace_id=new.workspace_id and kind='trade' and active) then raise exception 'An active trade template is required' using errcode='23514'; end if;
    if new.cycle_number<1 then raise exception 'Invalid cycle number' using errcode='23514'; end if;
    if new.root_trade_id is null and new.cycle_number<>1 then raise exception 'Root trade must be cycle one' using errcode='23514'; end if;
    if new.root_trade_id is not null then
      select * into root_record from public.v2_trades where id=new.root_trade_id for update;
      if not found or root_record.root_trade_id is not null or root_record.workspace_id<>new.workspace_id or root_record.asset_symbol<>new.asset_symbol or new.cycle_number<>(select coalesce(max(cycle_number),0)+1 from public.v2_trades where id=new.root_trade_id or root_trade_id=new.root_trade_id) or exists(select 1 from public.v2_trades where (id=new.root_trade_id or root_trade_id=new.root_trade_id) and status<>'closed') then raise exception 'Invalid trade cycle or previous cycle remains open' using errcode='23514'; end if;
    end if;
  else
    if new.workspace_id<>old.workspace_id or new.asset_symbol<>old.asset_symbol or new.template_id is distinct from old.template_id or new.root_trade_id is distinct from old.root_trade_id or new.cycle_number<>old.cycle_number then raise exception 'Trade identity and template are immutable' using errcode='23514'; end if;
    if new.pnl is distinct from old.pnl and new.status<>'closed' then raise exception 'PnL is locked until trade closes' using errcode='23514'; end if;
    if new.status is distinct from old.status then
      if old.status='evaluating' and new.status='active' then
        if new.template_id is null or not exists(select 1 from public.v2_template_fields f join public.v2_template_sections s on s.id=f.section_id where s.template_id=new.template_id and s.active and f.active) or exists(
          select 1 from public.v2_template_fields f join public.v2_template_sections s on s.id=f.section_id left join public.v2_trade_values v on v.field_id=f.id and v.trade_id=new.id
          where s.template_id=new.template_id and s.active and f.active and f.required and f.field_type<>'pnl' and not public.v2_field_complete(f.field_type,v.value)
        ) then raise exception 'Complete required fields across every trade sheet before activation' using errcode='23514'; end if;
        new.activated_at:=now();
      elsif old.status='active' and new.status='closed' then new.closed_at:=now();
      else raise exception 'Only evaluating to active and active to closed transitions are allowed' using errcode='23514'; end if;
    end if;
  end if;
  if new.scenario_id is not null and (tg_op='INSERT' or new.scenario_id is distinct from old.scenario_id) and not exists(select 1 from public.v2_scenarios where id=new.scenario_id and workspace_id=new.workspace_id and asset_symbol=new.asset_symbol and status='active') then raise exception 'Select an active scenario of the same asset' using errcode='23514'; end if;
  return new;
end $$;
drop trigger if exists v2_trades_workflow_guard on public.v2_trades;
create trigger v2_trades_workflow_guard before insert or update on public.v2_trades for each row execute function public.v2_guard_trade_workflow();

create or replace function public.v2_guard_trade_pnl_value()
returns trigger language plpgsql security definer set search_path=public,pg_temp as $$
declare old_tid uuid; new_tid uuid; old_kind text; new_kind text; status_value text;
begin
  if tg_op<>'INSERT' then
    old_tid:=old.trade_id;
    select field_type into old_kind from public.v2_template_fields where id=old.field_id;
  end if;
  if tg_op<>'DELETE' then
    new_tid:=new.trade_id;
    select field_type into new_kind from public.v2_template_fields where id=new.field_id;
  end if;
  -- RLS permits moving a value between editable parents. Lock both consistently
  -- before changing the value so either parent's cached PnL stays atomic.
  perform id from public.v2_trades where id=any(array[old_tid,new_tid]) order by id for update;
  if old_kind='pnl' then
    select status into status_value from public.v2_trades where id=old_tid;
    -- The parent may already be absent during an authorized cascading delete.
    if status_value is not null and status_value<>'closed' then
      raise exception 'PnL is locked until trade closes' using errcode='23514';
    end if;
  end if;
  if new_kind='pnl' then
    select status into status_value from public.v2_trades where id=new_tid;
    if status_value is distinct from 'closed' then raise exception 'PnL is locked until trade closes' using errcode='23514'; end if;
    if nullif(trim(new.value->>'text'),'') is not null and not public.v2_field_complete('pnl',new.value-'completion') then raise exception 'PnL must be a finite number' using errcode='23514'; end if;
  end if;
  if tg_op='DELETE' then return old; end if;
  return new;
end $$;
drop trigger if exists v2_trade_values_pnl_guard on public.v2_trade_values;
create trigger v2_trade_values_pnl_guard before insert or update or delete on public.v2_trade_values for each row execute function public.v2_guard_trade_pnl_value();

-- Keep the journal total consistent for every authorized table write, including
-- direct REST writes and deletes, rather than only writes through the save RPC.
create or replace function public.v2_sync_trade_pnl_value()
returns trigger language plpgsql security definer set search_path=public,pg_temp as $$
declare old_kind text; new_kind text; replacement_pnl numeric;
begin
  if tg_op<>'INSERT' then
    select field_type into old_kind from public.v2_template_fields where id=old.field_id;
  end if;
  if tg_op<>'DELETE' then
    select field_type into new_kind from public.v2_template_fields where id=new.field_id;
  end if;
  if old_kind='pnl' and (tg_op='DELETE' or new_kind is distinct from 'pnl' or new.trade_id is distinct from old.trade_id) then
    -- Clearing/moving the latest field restores another remaining PnL value, if
    -- one exists. An intentionally empty latest value remains NULL.
    select nullif(trim(v.value->>'text'),'')::numeric into replacement_pnl
    from public.v2_trade_values v join public.v2_template_fields f on f.id=v.field_id
    where v.trade_id=old.trade_id and f.field_type='pnl'
    order by v.updated_at desc,v.created_at desc,v.id desc limit 1;
    update public.v2_trades set pnl=replacement_pnl,updated_by=coalesce(auth.uid(),updated_by),updated_at=now()
    where id=old.trade_id and status='closed';
  end if;
  if new_kind='pnl' then
    update public.v2_trades set pnl=nullif(trim(new.value->>'text'),'')::numeric,
      updated_by=coalesce(auth.uid(),updated_by),updated_at=now()
    where id=new.trade_id and status='closed';
  end if;
  if tg_op='DELETE' then return old; end if;
  return new;
end $$;
revoke all on function public.v2_sync_trade_pnl_value() from public,anon,authenticated;
drop trigger if exists v2_trade_values_sync_pnl on public.v2_trade_values;
create trigger v2_trade_values_sync_pnl after insert or update or delete on public.v2_trade_values
for each row execute function public.v2_sync_trade_pnl_value();

create or replace function public.save_v2_trade_value(p_trade_id uuid,p_field_id uuid,p_value jsonb)
returns void language plpgsql security invoker set search_path=public,pg_temp as $$
declare tr public.v2_trades; kind text;
begin
  select * into tr from public.v2_trades where id=p_trade_id for update;
  if not found or auth.uid() is null or not public.v2_can_edit_workspace(tr.workspace_id) then raise exception 'Trade edit permission required' using errcode='42501'; end if;
  select f.field_type into kind from public.v2_template_fields f join public.v2_template_sections s on s.id=f.section_id where f.id=p_field_id and s.template_id=tr.template_id;
  if not found then raise exception 'Field does not belong to trade template' using errcode='23514'; end if;
  insert into public.v2_trade_values(trade_id,field_id,value,updated_by) values(p_trade_id,p_field_id,p_value,auth.uid()) on conflict(trade_id,field_id) do update set value=excluded.value,updated_by=auth.uid();
  -- PnL writes are synchronized by the same trigger used by direct table writes.
  if kind<>'pnl' then
    update public.v2_trades set updated_by=auth.uid(),updated_at=now() where id=p_trade_id;
  end if;
end $$;

create or replace function public.new_v2_trade_cycle(p_trade_id uuid)
returns public.v2_trades language plpgsql security invoker set search_path=public,pg_temp as $$
declare tr public.v2_trades; root_id uuid; latest public.v2_trades; result public.v2_trades;
begin
  select * into tr from public.v2_trades where id=p_trade_id;
  if not found or auth.uid() is null or not public.v2_can_edit_workspace(tr.workspace_id) then raise exception 'Trade edit permission required' using errcode='42501'; end if;
  root_id:=coalesce(tr.root_trade_id,tr.id);
  perform id from public.v2_trades where id=root_id for update;
  select * into latest from public.v2_trades where id=root_id or root_trade_id=root_id order by cycle_number desc limit 1;
  if latest.id<>p_trade_id or latest.status<>'closed' then raise exception 'Only the latest closed cycle can start another cycle' using errcode='23514'; end if;
  insert into public.v2_trades(workspace_id,template_id,scenario_id,asset_symbol,title,root_trade_id,cycle_number,created_by,updated_by)
  values(tr.workspace_id,tr.template_id,tr.scenario_id,tr.asset_symbol,tr.title,root_id,latest.cycle_number+1,auth.uid(),auth.uid()) returning * into result;
  return result;
end $$;

revoke all on function public.v2_guard_trade_workflow() from public,anon,authenticated;
revoke all on function public.v2_guard_trade_pnl_value() from public,anon,authenticated;
revoke all on function public.v2_field_complete(text,jsonb) from public,anon;
revoke all on function public.save_v2_scenario(jsonb,jsonb) from public,anon;
revoke all on function public.reorder_v2_scenarios(uuid,uuid[]) from public,anon;
revoke all on function public.save_v2_trade_value(uuid,uuid,jsonb) from public,anon;
revoke all on function public.new_v2_trade_cycle(uuid) from public,anon;
grant execute on function public.v2_field_complete(text,jsonb) to authenticated;
grant execute on function public.save_v2_scenario(jsonb,jsonb) to authenticated;
grant execute on function public.reorder_v2_scenarios(uuid,uuid[]) to authenticated;
grant execute on function public.save_v2_trade_value(uuid,uuid,jsonb) to authenticated;
grant execute on function public.new_v2_trade_cycle(uuid) to authenticated;
