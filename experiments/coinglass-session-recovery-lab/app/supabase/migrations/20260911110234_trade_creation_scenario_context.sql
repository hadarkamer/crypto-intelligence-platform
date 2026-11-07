-- New trade creation is atomic with its scenario. Existing trades and answers
-- are not backfilled or rewritten by this migration.
alter table public.v2_trades
  add column if not exists source_kind text not null default 'trade',
  add column if not exists bot_alert_html text not null default '',
  add column if not exists allow_cross_asset_scenario boolean not null default false;
alter table public.v2_trades add constraint v2_trades_source_kind_check
  check (source_kind in ('trade','bot_alert'));
alter table public.v2_trades add constraint v2_trades_bot_content_check
  check (source_kind='bot_alert' or bot_alert_html='');

-- Uses the same active scenario template preferences as the scenario editor.
-- SECURITY INVOKER deliberately retains all workspace and template RLS checks.
create or replace function public.v2_create_empty_trade_scenario(p_workspace_id uuid,p_asset_symbol text,p_title text)
returns uuid language plpgsql security invoker set search_path=public,pg_temp as $$
declare tid uuid; result public.v2_scenarios;
begin
  if auth.uid() is null or not coalesce(public.v2_can_edit_workspace(p_workspace_id),false) then
    raise exception 'Workspace edit permission required' using errcode='42501';
  end if;
  select t.id into tid from public.v2_templates t
  left join public.v2_asset_settings a on a.workspace_id=t.workspace_id and a.symbol=p_asset_symbol and a.active
  where t.workspace_id=p_workspace_id and t.kind='scenario' and t.active
  order by (t.id=a.default_scenario_template_id) desc nulls last,
    (t.asset_symbol=p_asset_symbol) desc nulls last,t.is_default desc,t.created_at desc,t.id limit 1;
  if tid is null then
    perform public.v2_seed_default_templates(p_workspace_id);
    select id into tid from public.v2_templates where workspace_id=p_workspace_id and kind='scenario' and active
    order by is_default desc,created_at desc,id limit 1;
  end if;
  if tid is null then raise exception 'An active scenario template is required' using errcode='23514'; end if;
  result:=public.save_v2_scenario(jsonb_build_object(
    'id',gen_random_uuid(),'workspace_id',p_workspace_id,'template_id',tid,
    'asset_symbol',p_asset_symbol,'title',coalesce(nullif(btrim(p_title),''),p_asset_symbol)||' · תרחיש',
    'scenario_html','','reasons_html','','invalidation_html',''),'[]');
  return result.id;
end $$;
revoke all on function public.v2_create_empty_trade_scenario(uuid,text,text) from public,anon;
grant execute on function public.v2_create_empty_trade_scenario(uuid,text,text) to authenticated;

create or replace function public.v2_prepare_trade_scenario()
returns trigger language plpgsql security invoker set search_path=public,pg_temp as $$
begin
  if tg_op='INSERT' then
    if new.scenario_id is null then
      new.scenario_id:=public.v2_create_empty_trade_scenario(new.workspace_id,new.asset_symbol,new.title);
    end if;
  else
    if new.source_kind is distinct from old.source_kind then
      raise exception 'Trade source classification is immutable' using errcode='23514';
    end if;
    if old.scenario_id is not null and new.scenario_id is null then
      raise exception 'A trade must retain its linked scenario' using errcode='23514';
    end if;
  end if;
  return new;
end $$;
revoke all on function public.v2_prepare_trade_scenario() from public,anon,authenticated;
drop trigger if exists v2_trades_ensure_scenario on public.v2_trades;
create trigger v2_trades_ensure_scenario before insert or update on public.v2_trades
  for each row execute function public.v2_prepare_trade_scenario();

-- Cross-asset linking is an explicit stored choice, never a workspace or
-- active-status bypass. Locking the scenario keeps concurrent symbol edits safe.
create or replace function public.v2_lock_trade_scenario_asset()
returns trigger language plpgsql security invoker set search_path=public,pg_temp as $$
declare scenario_asset text; scenario_status text;
begin
  if new.scenario_id is null then return new; end if;
  if tg_op='UPDATE' and new.scenario_id is not distinct from old.scenario_id
    and new.allow_cross_asset_scenario is not distinct from old.allow_cross_asset_scenario then return new; end if;
  select asset_symbol,status into scenario_asset,scenario_status from public.v2_scenarios
    where id=new.scenario_id and workspace_id=new.workspace_id for update;
  if not found or scenario_status<>'active'
    or (scenario_asset is distinct from new.asset_symbol and not new.allow_cross_asset_scenario) then
    raise exception 'Select an active scenario in the same workspace; cross-asset linking requires consent' using errcode='23514';
  end if;
  return new;
end $$;
revoke all on function public.v2_lock_trade_scenario_asset() from public,anon,authenticated;
drop trigger if exists v2_trades_scenario_asset_lock on public.v2_trades;
create trigger v2_trades_scenario_asset_lock before insert or update of scenario_id,allow_cross_asset_scenario on public.v2_trades
  for each row execute function public.v2_lock_trade_scenario_asset();

-- Retain the full deployed lifecycle/PnL/template guards and only extend the
-- final scenario-link clause. Fail loudly if the expected contract has changed.
do $$ declare definition text; old_clause text; new_clause text;
begin
  definition:=pg_get_functiondef('public.v2_guard_trade_workflow()'::regprocedure);
  old_clause:='if new.scenario_id is not null and (tg_op=''INSERT'' or new.scenario_id is distinct from old.scenario_id) and not exists(select 1 from public.v2_scenarios where id=new.scenario_id and workspace_id=new.workspace_id and asset_symbol=new.asset_symbol and status=''active'') then raise exception ''Select an active scenario of the same asset'' using errcode=''23514''; end if;';
  new_clause:='if new.scenario_id is not null and (tg_op=''INSERT'' or new.scenario_id is distinct from old.scenario_id or new.allow_cross_asset_scenario is distinct from old.allow_cross_asset_scenario) and not exists(select 1 from public.v2_scenarios where id=new.scenario_id and workspace_id=new.workspace_id and (asset_symbol=new.asset_symbol or new.allow_cross_asset_scenario) and status=''active'') then raise exception ''Select an active scenario; cross-asset linking requires consent'' using errcode=''23514''; end if;';
  if strpos(definition,old_clause)=0 then raise exception 'Unexpected trade workflow scenario guard'; end if;
  execute replace(definition,old_clause,new_clause);
end $$;

create or replace function public.v2_create_trade(p_trade jsonb,p_scenario jsonb default null,p_scenario_values jsonb default '[]')
returns public.v2_trades language plpgsql security invoker set search_path=public,pg_temp as $$
declare
  wid uuid:=(p_trade->>'workspace_id')::uuid;
  tid uuid:=nullif(p_trade->>'template_id','')::uuid;
  trade_id uuid:=coalesce(nullif(p_trade->>'id','')::uuid,gen_random_uuid());
  sid uuid:=nullif(p_trade->>'scenario_id','')::uuid;
  asset_code text:=upper(btrim(coalesce(p_trade->>'asset_symbol','BTC')));
  trade_source text:=coalesce(p_trade->>'source_kind','trade');
  cross_asset boolean:=coalesce((p_trade->>'allow_cross_asset_scenario')::boolean,false);
  saved_scenario public.v2_scenarios; result public.v2_trades;
begin
  if auth.uid() is null or not coalesce(public.v2_can_edit_workspace(wid),false) then
    raise exception 'Workspace edit permission required' using errcode='42501';
  end if;
  if jsonb_typeof(p_trade)<>'object' or asset_code !~ '^[A-Z0-9][A-Z0-9._-]{0,19}$'
    or trade_source not in ('trade','bot_alert') then raise exception 'Invalid trade' using errcode='23514'; end if;
  -- One stable client UUID serializes double clicks, concurrent retries and
  -- lost network responses without orphan scenarios or duplicate trade rows.
  perform pg_advisory_xact_lock(hashtextextended('v2-create-trade:'||trade_id::text,0));
  select * into result from public.v2_trades where id=trade_id;
  if found then
    if result.workspace_id<>wid or result.created_by<>auth.uid() or result.asset_symbol<>asset_code
      or result.source_kind<>trade_source then raise exception 'Trade request ID is already in use' using errcode='23514'; end if;
    return result;
  end if;
  if tid is null then
    select t.id into tid from public.v2_templates t
    left join public.v2_asset_settings a on a.workspace_id=t.workspace_id and a.symbol=asset_code and a.active
    where t.workspace_id=wid and t.kind='trade' and t.active
    order by (t.id=a.default_trade_template_id) desc nulls last,
      (t.asset_symbol=asset_code) desc nulls last,t.is_default desc,t.created_at desc,t.id limit 1;
  end if;
  if tid is null or not exists(select 1 from public.v2_templates where id=tid and workspace_id=wid and kind='trade' and active) then
    raise exception 'An active trade template is required' using errcode='23514';
  end if;
  if p_scenario is not null and p_scenario<>'null'::jsonb then
    if jsonb_typeof(p_scenario)<>'object' or jsonb_typeof(p_scenario_values)<>'array'
      or (p_scenario->>'workspace_id')::uuid is distinct from wid
      or (sid is not null and sid is distinct from nullif(p_scenario->>'id','')::uuid) then
      raise exception 'Scenario draft must belong to this trade workspace' using errcode='23514';
    end if;
    saved_scenario:=public.save_v2_scenario(p_scenario,p_scenario_values);
    sid:=saved_scenario.id;
  elsif jsonb_array_length(coalesce(p_scenario_values,'[]'))>0 then
    raise exception 'Scenario values require a scenario draft' using errcode='23514';
  end if;
  insert into public.v2_trades(id,workspace_id,template_id,scenario_id,asset_symbol,title,
    source_kind,bot_alert_html,allow_cross_asset_scenario,created_by,updated_by)
  values(trade_id,wid,tid,sid,asset_code,coalesce(nullif(btrim(p_trade->>'title'),''),asset_code||' · '||to_char(now(),'DD.MM.YYYY')),
    trade_source,coalesce(p_trade->>'bot_alert_html',''),cross_asset,auth.uid(),auth.uid()) returning * into result;
  return result;
end $$;
revoke all on function public.v2_create_trade(jsonb,jsonb,jsonb) from public,anon;
grant execute on function public.v2_create_trade(jsonb,jsonb,jsonb) to authenticated;

-- Reentry preserves the source classification and intentional cross-asset link.
-- For a historical trade without a scenario the INSERT trigger creates one.
create or replace function public.new_v2_trade_cycle(p_trade_id uuid)
returns public.v2_trades language plpgsql security invoker set search_path=public,pg_temp as $$
#variable_conflict use_variable
declare tr public.v2_trades; root_id uuid; latest public.v2_trades; result public.v2_trades;
begin
  select * into tr from public.v2_trades where id=p_trade_id;
  if not found or auth.uid() is null or not coalesce(public.v2_can_edit_workspace(tr.workspace_id),false) then raise exception 'Trade edit permission required' using errcode='42501'; end if;
  root_id:=coalesce(tr.root_trade_id,tr.id);
  perform id from public.v2_trades where id=root_id for update;
  select * into latest from public.v2_trades where id=root_id or root_trade_id=root_id order by cycle_number desc limit 1;
  if latest.id<>p_trade_id or latest.status<>'closed' then raise exception 'Only the latest closed cycle can start another cycle' using errcode='23514'; end if;
  insert into public.v2_trades(workspace_id,template_id,scenario_id,asset_symbol,title,root_trade_id,cycle_number,
    source_kind,bot_alert_html,allow_cross_asset_scenario,created_by,updated_by)
  values(tr.workspace_id,tr.template_id,tr.scenario_id,tr.asset_symbol,tr.title,root_id,latest.cycle_number+1,
    tr.source_kind,tr.bot_alert_html,tr.allow_cross_asset_scenario,auth.uid(),auth.uid()) returning * into result;
  return result;
end $$;
revoke all on function public.new_v2_trade_cycle(uuid) from public,anon;
grant execute on function public.new_v2_trade_cycle(uuid) to authenticated;
notify pgrst,'reload schema';
