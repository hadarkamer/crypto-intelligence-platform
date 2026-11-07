-- Preserve the asset meaning of existing scenarios and spot records.
-- These guards do not rewrite historical data or restrict content/status edits.

create or replace function public.v2_guard_linked_scenario_asset()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
begin
  if new.asset_symbol is distinct from old.asset_symbol
    and exists(select 1 from public.v2_trades where scenario_id = old.id) then
    raise exception 'A scenario linked to a trade cannot change asset; create a new scenario'
      using errcode = '23514';
  end if;
  return new;
end;
$$;

drop trigger if exists v2_scenarios_linked_asset_guard on public.v2_scenarios;
create trigger v2_scenarios_linked_asset_guard
before update of asset_symbol on public.v2_scenarios
for each row execute function public.v2_guard_linked_scenario_asset();

create or replace function public.v2_lock_trade_scenario_asset()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  scenario_asset text;
begin
  if new.scenario_id is null then return new; end if;
  if tg_op = 'UPDATE' and new.scenario_id is not distinct from old.scenario_id then
    return new;
  end if;
  -- A symbol edit already locks the parent row. Take the same lock before
  -- linking a trade, then validate the version visible after acquiring it.
  -- This covers both orderings of a concurrent link and scenario symbol edit.
  select asset_symbol into scenario_asset
  from public.v2_scenarios
  where id = new.scenario_id and workspace_id = new.workspace_id
  for update;
  if not found or scenario_asset is distinct from new.asset_symbol then
    raise exception 'Trade scenario must belong to the same workspace and asset'
      using errcode = '23514';
  end if;
  return new;
end;
$$;

drop trigger if exists v2_trades_scenario_asset_lock on public.v2_trades;
create trigger v2_trades_scenario_asset_lock
before insert or update of scenario_id on public.v2_trades
for each row execute function public.v2_lock_trade_scenario_asset();

create or replace function public.v2_guard_recorded_spot_asset()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
begin
  if new.symbol is distinct from old.symbol and (
    exists(select 1 from public.v2_spot_transactions where asset_id = old.id)
    or exists(select 1 from public.v2_spot_orders where asset_id = old.id)
    or exists(select 1 from public.v2_spot_targets where asset_id = old.id)
  ) then
    raise exception 'A spot asset with transactions, orders or price targets cannot change symbol; create a new holding'
      using errcode = '23514';
  end if;
  return new;
end;
$$;

drop trigger if exists v2_spot_assets_recorded_symbol_guard on public.v2_spot_assets;
create trigger v2_spot_assets_recorded_symbol_guard
before update of symbol on public.v2_spot_assets
for each row execute function public.v2_guard_recorded_spot_asset();

create or replace function public.v2_lock_spot_reference_asset()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
begin
  -- Transactions already lock this row in v2_lock_spot_ledger. Orders and
  -- targets need the same protection when they first attach to a holding.
  perform id from public.v2_spot_assets
  where id = new.asset_id and workspace_id = new.workspace_id
  for update;
  if not found then
    raise exception 'Spot record must belong to an asset in the same workspace'
      using errcode = '23514';
  end if;
  return new;
end;
$$;

drop trigger if exists v2_spot_orders_asset_identity_lock on public.v2_spot_orders;
create trigger v2_spot_orders_asset_identity_lock
before insert or update of asset_id, workspace_id on public.v2_spot_orders
for each row execute function public.v2_lock_spot_reference_asset();

drop trigger if exists v2_spot_targets_asset_identity_lock on public.v2_spot_targets;
create trigger v2_spot_targets_asset_identity_lock
before insert or update of asset_id, workspace_id on public.v2_spot_targets
for each row execute function public.v2_lock_spot_reference_asset();

revoke all on function public.v2_guard_linked_scenario_asset(),
  public.v2_lock_trade_scenario_asset(), public.v2_guard_recorded_spot_asset(),
  public.v2_lock_spot_reference_asset() from public, anon, authenticated;

notify pgrst, 'reload schema';
