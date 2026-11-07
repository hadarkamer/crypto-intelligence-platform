-- Prevent cross-workspace references in Modular Workspace V1.
-- This migration touches only v2_* objects and does not modify legacy project data.

-- ---------------------------------------------------------------------------
-- Preflight: fail loudly if any existing V2 row already crosses workspace lines.
-- ---------------------------------------------------------------------------
do $$
begin
  if exists (
    select 1
    from public.v2_collection_sessions s
    join public.v2_templates t on t.id = s.template_id
    where s.template_id is not null
      and (t.workspace_id <> s.workspace_id or t.kind <> 'collection')
  ) then
    raise exception 'cross-workspace or wrong-kind template found in v2_collection_sessions'
      using errcode = '23514';
  end if;

  if exists (
    select 1
    from public.v2_collection_values v
    join public.v2_collection_sessions s on s.id = v.session_id
    join public.v2_template_fields f on f.id = v.field_id
    join public.v2_template_sections sec on sec.id = f.section_id
    join public.v2_templates t on t.id = sec.template_id
    where s.template_id is null
       or t.id <> s.template_id
       or t.workspace_id <> s.workspace_id
  ) then
    raise exception 'collection value is linked to a field outside its session template/workspace'
      using errcode = '23514';
  end if;

  if exists (
    select 1
    from public.v2_news_items n
    join public.v2_collection_sessions s on s.id = n.collection_session_id
    where n.collection_session_id is not null
      and s.workspace_id <> n.workspace_id
  ) then
    raise exception 'cross-workspace collection session found in v2_news_items'
      using errcode = '23514';
  end if;

  if exists (
    select 1
    from public.v2_news_items n
    join public.v2_news_sources src on src.id = n.source_id
    where n.source_id is not null
      and src.workspace_id <> n.workspace_id
  ) then
    raise exception 'cross-workspace source found in v2_news_items'
      using errcode = '23514';
  end if;

  if exists (
    select 1
    from public.v2_trades tr
    join public.v2_templates t on t.id = tr.template_id
    where tr.template_id is not null
      and (t.workspace_id <> tr.workspace_id or t.kind <> 'trade')
  ) then
    raise exception 'cross-workspace or wrong-kind template found in v2_trades'
      using errcode = '23514';
  end if;

  if exists (
    select 1
    from public.v2_trades tr
    join public.v2_scenarios s on s.id = tr.scenario_id
    where tr.scenario_id is not null
      and s.workspace_id <> tr.workspace_id
  ) then
    raise exception 'cross-workspace scenario found in v2_trades'
      using errcode = '23514';
  end if;

  if exists (
    select 1
    from public.v2_trade_values v
    join public.v2_trades tr on tr.id = v.trade_id
    join public.v2_template_fields f on f.id = v.field_id
    join public.v2_template_sections sec on sec.id = f.section_id
    join public.v2_templates t on t.id = sec.template_id
    where tr.template_id is null
       or t.id <> tr.template_id
       or t.workspace_id <> tr.workspace_id
  ) then
    raise exception 'trade value is linked to a field outside its trade template/workspace'
      using errcode = '23514';
  end if;

  if exists (
    select 1
    from public.v2_spot_transactions child
    join public.v2_spot_assets asset on asset.id = child.asset_id
    where asset.workspace_id <> child.workspace_id
  ) then
    raise exception 'cross-workspace asset found in v2_spot_transactions'
      using errcode = '23514';
  end if;

  if exists (
    select 1
    from public.v2_spot_orders child
    join public.v2_spot_assets asset on asset.id = child.asset_id
    where asset.workspace_id <> child.workspace_id
  ) then
    raise exception 'cross-workspace asset found in v2_spot_orders'
      using errcode = '23514';
  end if;

  if exists (
    select 1
    from public.v2_spot_targets child
    join public.v2_spot_assets asset on asset.id = child.asset_id
    where asset.workspace_id <> child.workspace_id
  ) then
    raise exception 'cross-workspace asset found in v2_spot_targets'
      using errcode = '23514';
  end if;
end;
$$;

-- ---------------------------------------------------------------------------
-- Workspace ids are immutable after creation. Moving an entity between two
-- workspaces would otherwise invalidate child references and historical data.
-- ---------------------------------------------------------------------------
create or replace function public.v2_reject_workspace_reassignment()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
begin
  if new.workspace_id is distinct from old.workspace_id then
    raise exception 'workspace_id is immutable for %', tg_table_name
      using errcode = '23514';
  end if;

  return new;
end;
$$;

revoke all on function public.v2_reject_workspace_reassignment() from public;
revoke all on function public.v2_reject_workspace_reassignment() from anon;
revoke all on function public.v2_reject_workspace_reassignment() from authenticated;

do $$
declare
  table_name text;
begin
  foreach table_name in array array[
    'v2_templates',
    'v2_collection_sessions',
    'v2_news_sources',
    'v2_news_items',
    'v2_scenarios',
    'v2_trades',
    'v2_spot_assets',
    'v2_spot_transactions',
    'v2_spot_orders',
    'v2_spot_targets'
  ] loop
    execute format(
      'drop trigger if exists %I on public.%I',
      table_name || '_workspace_immutable',
      table_name
    );
    execute format(
      'create trigger %I before update of workspace_id on public.%I for each row execute function public.v2_reject_workspace_reassignment()',
      table_name || '_workspace_immutable',
      table_name
    );
  end loop;
end;
$$;

-- ---------------------------------------------------------------------------
-- Direct workspace-aware reference validators.
-- ---------------------------------------------------------------------------
create or replace function public.v2_validate_collection_session_template()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
begin
  if new.template_id is not null and not exists (
    select 1
    from public.v2_templates t
    where t.id = new.template_id
      and t.workspace_id = new.workspace_id
      and t.kind = 'collection'
  ) then
    raise exception 'collection session template must belong to the same workspace and be kind collection'
      using errcode = '23514';
  end if;

  return new;
end;
$$;

create or replace function public.v2_validate_news_item_links()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
begin
  if new.collection_session_id is not null and not exists (
    select 1
    from public.v2_collection_sessions s
    where s.id = new.collection_session_id
      and s.workspace_id = new.workspace_id
  ) then
    raise exception 'news item collection session must belong to the same workspace'
      using errcode = '23514';
  end if;

  if new.source_id is not null and not exists (
    select 1
    from public.v2_news_sources src
    where src.id = new.source_id
      and src.workspace_id = new.workspace_id
  ) then
    raise exception 'news item source must belong to the same workspace'
      using errcode = '23514';
  end if;

  return new;
end;
$$;

create or replace function public.v2_validate_trade_links()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
begin
  if new.template_id is not null and not exists (
    select 1
    from public.v2_templates t
    where t.id = new.template_id
      and t.workspace_id = new.workspace_id
      and t.kind = 'trade'
  ) then
    raise exception 'trade template must belong to the same workspace and be kind trade'
      using errcode = '23514';
  end if;

  if new.scenario_id is not null and not exists (
    select 1
    from public.v2_scenarios s
    where s.id = new.scenario_id
      and s.workspace_id = new.workspace_id
  ) then
    raise exception 'trade scenario must belong to the same workspace'
      using errcode = '23514';
  end if;

  return new;
end;
$$;

create or replace function public.v2_validate_spot_asset_link()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
begin
  if not exists (
    select 1
    from public.v2_spot_assets asset
    where asset.id = new.asset_id
      and asset.workspace_id = new.workspace_id
  ) then
    raise exception '% asset must belong to the same workspace', tg_table_name
      using errcode = '23514';
  end if;

  return new;
end;
$$;

revoke all on function public.v2_validate_collection_session_template() from public;
revoke all on function public.v2_validate_collection_session_template() from anon;
revoke all on function public.v2_validate_collection_session_template() from authenticated;
revoke all on function public.v2_validate_news_item_links() from public;
revoke all on function public.v2_validate_news_item_links() from anon;
revoke all on function public.v2_validate_news_item_links() from authenticated;
revoke all on function public.v2_validate_trade_links() from public;
revoke all on function public.v2_validate_trade_links() from anon;
revoke all on function public.v2_validate_trade_links() from authenticated;
revoke all on function public.v2_validate_spot_asset_link() from public;
revoke all on function public.v2_validate_spot_asset_link() from anon;
revoke all on function public.v2_validate_spot_asset_link() from authenticated;

drop trigger if exists v2_collection_sessions_template_workspace_guard
  on public.v2_collection_sessions;
create trigger v2_collection_sessions_template_workspace_guard
before insert or update on public.v2_collection_sessions
for each row execute function public.v2_validate_collection_session_template();

drop trigger if exists v2_news_items_workspace_links_guard
  on public.v2_news_items;
create trigger v2_news_items_workspace_links_guard
before insert or update on public.v2_news_items
for each row execute function public.v2_validate_news_item_links();

drop trigger if exists v2_trades_workspace_links_guard
  on public.v2_trades;
create trigger v2_trades_workspace_links_guard
before insert or update on public.v2_trades
for each row execute function public.v2_validate_trade_links();

do $$
declare
  table_name text;
begin
  foreach table_name in array array[
    'v2_spot_transactions',
    'v2_spot_orders',
    'v2_spot_targets'
  ] loop
    execute format(
      'drop trigger if exists %I on public.%I',
      table_name || '_asset_workspace_guard',
      table_name
    );
    execute format(
      'create trigger %I before insert or update on public.%I for each row execute function public.v2_validate_spot_asset_link()',
      table_name || '_asset_workspace_guard',
      table_name
    );
  end loop;
end;
$$;

-- ---------------------------------------------------------------------------
-- Value rows must use fields from the exact template attached to their parent.
-- This is stricter than checking only workspace_id and prevents silent template
-- mixing within the same workspace as well.
-- ---------------------------------------------------------------------------
create or replace function public.v2_validate_collection_value_field()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  session_workspace uuid;
  session_template uuid;
  field_workspace uuid;
  field_template uuid;
begin
  select s.workspace_id, s.template_id
  into session_workspace, session_template
  from public.v2_collection_sessions s
  where s.id = new.session_id;

  if not found then
    raise exception 'collection session does not exist'
      using errcode = '23503';
  end if;

  select t.workspace_id, t.id
  into field_workspace, field_template
  from public.v2_template_fields f
  join public.v2_template_sections sec on sec.id = f.section_id
  join public.v2_templates t on t.id = sec.template_id
  where f.id = new.field_id;

  if not found then
    raise exception 'collection field does not exist'
      using errcode = '23503';
  end if;

  if session_template is null
     or field_template <> session_template
     or field_workspace <> session_workspace then
    raise exception 'collection value field must belong to the session template and workspace'
      using errcode = '23514';
  end if;

  return new;
end;
$$;

create or replace function public.v2_validate_trade_value_field()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  trade_workspace uuid;
  trade_template uuid;
  field_workspace uuid;
  field_template uuid;
begin
  select tr.workspace_id, tr.template_id
  into trade_workspace, trade_template
  from public.v2_trades tr
  where tr.id = new.trade_id;

  if not found then
    raise exception 'trade does not exist'
      using errcode = '23503';
  end if;

  select t.workspace_id, t.id
  into field_workspace, field_template
  from public.v2_template_fields f
  join public.v2_template_sections sec on sec.id = f.section_id
  join public.v2_templates t on t.id = sec.template_id
  where f.id = new.field_id;

  if not found then
    raise exception 'trade field does not exist'
      using errcode = '23503';
  end if;

  if trade_template is null
     or field_template <> trade_template
     or field_workspace <> trade_workspace then
    raise exception 'trade value field must belong to the trade template and workspace'
      using errcode = '23514';
  end if;

  return new;
end;
$$;

revoke all on function public.v2_validate_collection_value_field() from public;
revoke all on function public.v2_validate_collection_value_field() from anon;
revoke all on function public.v2_validate_collection_value_field() from authenticated;
revoke all on function public.v2_validate_trade_value_field() from public;
revoke all on function public.v2_validate_trade_value_field() from anon;
revoke all on function public.v2_validate_trade_value_field() from authenticated;

drop trigger if exists v2_collection_values_field_workspace_guard
  on public.v2_collection_values;
create trigger v2_collection_values_field_workspace_guard
before insert or update on public.v2_collection_values
for each row execute function public.v2_validate_collection_value_field();

drop trigger if exists v2_trade_values_field_workspace_guard
  on public.v2_trade_values;
create trigger v2_trade_values_field_workspace_guard
before insert or update on public.v2_trade_values
for each row execute function public.v2_validate_trade_value_field();

-- ---------------------------------------------------------------------------
-- Parent-only child objects do not carry workspace_id. They may be reorganized
-- inside one workspace, but cannot be moved into a parent from another workspace.
-- ---------------------------------------------------------------------------
create or replace function public.v2_guard_template_section_workspace_move()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  old_workspace uuid;
  new_workspace uuid;
begin
  if new.template_id is not distinct from old.template_id then
    return new;
  end if;

  select workspace_id into old_workspace
  from public.v2_templates
  where id = old.template_id;

  select workspace_id into new_workspace
  from public.v2_templates
  where id = new.template_id;

  if old_workspace is distinct from new_workspace then
    raise exception 'template section cannot be moved between workspaces'
      using errcode = '23514';
  end if;

  return new;
end;
$$;

create or replace function public.v2_guard_template_field_workspace_move()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  old_workspace uuid;
  new_workspace uuid;
begin
  if new.section_id is not distinct from old.section_id then
    return new;
  end if;

  select t.workspace_id
  into old_workspace
  from public.v2_template_sections sec
  join public.v2_templates t on t.id = sec.template_id
  where sec.id = old.section_id;

  select t.workspace_id
  into new_workspace
  from public.v2_template_sections sec
  join public.v2_templates t on t.id = sec.template_id
  where sec.id = new.section_id;

  if old_workspace is distinct from new_workspace then
    raise exception 'template field cannot be moved between workspaces'
      using errcode = '23514';
  end if;

  return new;
end;
$$;

create or replace function public.v2_guard_scenario_alternative_workspace_move()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  old_workspace uuid;
  new_workspace uuid;
begin
  if new.scenario_id is not distinct from old.scenario_id then
    return new;
  end if;

  select workspace_id into old_workspace
  from public.v2_scenarios
  where id = old.scenario_id;

  select workspace_id into new_workspace
  from public.v2_scenarios
  where id = new.scenario_id;

  if old_workspace is distinct from new_workspace then
    raise exception 'scenario alternative cannot be moved between workspaces'
      using errcode = '23514';
  end if;

  return new;
end;
$$;

revoke all on function public.v2_guard_template_section_workspace_move() from public;
revoke all on function public.v2_guard_template_section_workspace_move() from anon;
revoke all on function public.v2_guard_template_section_workspace_move() from authenticated;
revoke all on function public.v2_guard_template_field_workspace_move() from public;
revoke all on function public.v2_guard_template_field_workspace_move() from anon;
revoke all on function public.v2_guard_template_field_workspace_move() from authenticated;
revoke all on function public.v2_guard_scenario_alternative_workspace_move() from public;
revoke all on function public.v2_guard_scenario_alternative_workspace_move() from anon;
revoke all on function public.v2_guard_scenario_alternative_workspace_move() from authenticated;

drop trigger if exists v2_template_sections_workspace_move_guard
  on public.v2_template_sections;
create trigger v2_template_sections_workspace_move_guard
before update of template_id on public.v2_template_sections
for each row execute function public.v2_guard_template_section_workspace_move();

drop trigger if exists v2_template_fields_workspace_move_guard
  on public.v2_template_fields;
create trigger v2_template_fields_workspace_move_guard
before update of section_id on public.v2_template_fields
for each row execute function public.v2_guard_template_field_workspace_move();

drop trigger if exists v2_scenario_alternatives_workspace_move_guard
  on public.v2_scenario_alternatives;
create trigger v2_scenario_alternatives_workspace_move_guard
before update of scenario_id on public.v2_scenario_alternatives
for each row execute function public.v2_guard_scenario_alternative_workspace_move();
