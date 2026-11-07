-- Modular Workspace V1 foundation.
-- The legacy project model remains untouched; all new modules live in v2_* tables.

create extension if not exists pgcrypto;

create or replace function public.v2_set_updated_at()
returns trigger
language plpgsql
set search_path = public
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

create table if not exists public.v2_workspaces (
  id uuid primary key default gen_random_uuid(),
  owner_id uuid not null references auth.users(id) on delete restrict,
  name text not null default 'סביבת העבודה למסחר',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (owner_id)
);

create table if not exists public.v2_workspace_members (
  workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
  user_id uuid not null references auth.users(id) on delete cascade,
  role text not null default 'editor' check (role in ('owner','editor','viewer')),
  created_at timestamptz not null default now(),
  primary key (workspace_id, user_id)
);

create index if not exists v2_workspace_members_user_idx
  on public.v2_workspace_members(user_id, workspace_id);

create or replace function public.v2_is_workspace_member(p_workspace_id uuid)
returns boolean
language sql
stable
security definer
set search_path = public, pg_temp
as $$
  select auth.uid() is not null
    and exists (
      select 1
      from public.v2_workspace_members m
      where m.workspace_id = p_workspace_id
        and m.user_id = auth.uid()
    );
$$;

create or replace function public.v2_can_edit_workspace(p_workspace_id uuid)
returns boolean
language sql
stable
security definer
set search_path = public, pg_temp
as $$
  select auth.uid() is not null
    and exists (
      select 1
      from public.v2_workspace_members m
      where m.workspace_id = p_workspace_id
        and m.user_id = auth.uid()
        and m.role in ('owner','editor')
    );
$$;

create or replace function public.v2_is_workspace_owner(p_workspace_id uuid)
returns boolean
language sql
stable
security definer
set search_path = public, pg_temp
as $$
  select auth.uid() is not null
    and exists (
      select 1
      from public.v2_workspaces w
      where w.id = p_workspace_id
        and w.owner_id = auth.uid()
    );
$$;

-- SECURITY DEFINER helpers are callable only by signed-in application users.
-- The caller can provide only a workspace id; the checked user is always auth.uid().
revoke all on function public.v2_is_workspace_member(uuid) from public;
revoke all on function public.v2_is_workspace_member(uuid) from anon;
revoke all on function public.v2_can_edit_workspace(uuid) from public;
revoke all on function public.v2_can_edit_workspace(uuid) from anon;
revoke all on function public.v2_is_workspace_owner(uuid) from public;
revoke all on function public.v2_is_workspace_owner(uuid) from anon;

grant execute on function public.v2_is_workspace_member(uuid) to authenticated;
grant execute on function public.v2_can_edit_workspace(uuid) to authenticated;
grant execute on function public.v2_is_workspace_owner(uuid) to authenticated;

create table if not exists public.v2_templates (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
  kind text not null check (kind in ('collection','scenario','trade','spot')),
  name text not null,
  active boolean not null default true,
  is_default boolean not null default false,
  asset_symbol text,
  settings jsonb not null default '{}'::jsonb,
  created_by uuid references auth.users(id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists v2_templates_one_default_per_kind_asset_idx
  on public.v2_templates(workspace_id, kind, coalesce(asset_symbol, ''))
  where is_default and active;
create index if not exists v2_templates_workspace_kind_idx
  on public.v2_templates(workspace_id, kind, active);

create table if not exists public.v2_template_sections (
  id uuid primary key default gen_random_uuid(),
  template_id uuid not null references public.v2_templates(id) on delete cascade,
  title text not null,
  position integer not null default 0,
  active boolean not null default true,
  settings jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (template_id, position)
);

create table if not exists public.v2_template_fields (
  id uuid primary key default gen_random_uuid(),
  section_id uuid not null references public.v2_template_sections(id) on delete cascade,
  field_key text not null,
  title text not null,
  field_type text not null check (field_type in (
    'dynamic_text','short_text','long_text','checkbox','yes_no_na',
    'liquidity_table','news_source_table','economic_calendar','number','image'
  )),
  position integer not null default 0,
  required boolean not null default false,
  default_align text not null default 'right' check (default_align in ('right','left')),
  pinned_scope text not null default 'none' check (pinned_scope in ('none','section','template')),
  settings jsonb not null default '{}'::jsonb,
  active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (section_id, position),
  unique (section_id, field_key)
);

create table if not exists public.v2_collection_sessions (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
  template_id uuid references public.v2_templates(id) on delete set null,
  title text not null,
  status text not null default 'draft' check (status in ('draft','completed','archived')),
  created_by uuid not null references auth.users(id) on delete restrict,
  last_edited_by uuid references auth.users(id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
create index if not exists v2_collection_sessions_workspace_updated_idx
  on public.v2_collection_sessions(workspace_id, updated_at desc);

create table if not exists public.v2_collection_values (
  id uuid primary key default gen_random_uuid(),
  session_id uuid not null references public.v2_collection_sessions(id) on delete cascade,
  field_id uuid not null references public.v2_template_fields(id) on delete restrict,
  value jsonb not null default '{}'::jsonb,
  updated_by uuid references auth.users(id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (session_id, field_id)
);
create index if not exists v2_collection_values_field_updated_idx
  on public.v2_collection_values(field_id, updated_at desc);

create table if not exists public.v2_news_sources (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
  name text not null,
  current_sentiment text not null default 'neutral' check (current_sentiment in (
    'very_bullish','bullish','positive','neutral','negative','bearish','very_bearish'
  )),
  current_scenario text,
  updated_by uuid references auth.users(id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
create unique index if not exists v2_news_sources_workspace_name_idx
  on public.v2_news_sources(workspace_id, lower(name));

create table if not exists public.v2_news_items (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
  collection_session_id uuid references public.v2_collection_sessions(id) on delete set null,
  source_id uuid references public.v2_news_sources(id) on delete set null,
  body text not null,
  importance text not null default 'neutral' check (importance in (
    'very_bullish','bullish','positive','neutral','negative','bearish','very_bearish'
  )),
  occurred_at timestamptz not null default now(),
  created_by uuid not null references auth.users(id) on delete restrict,
  updated_by uuid references auth.users(id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
create index if not exists v2_news_items_workspace_occurred_idx
  on public.v2_news_items(workspace_id, occurred_at desc);
create index if not exists v2_news_items_source_occurred_idx
  on public.v2_news_items(source_id, occurred_at desc);

create table if not exists public.v2_scenarios (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
  asset_symbol text not null default 'BTC',
  title text not null,
  scenario_html text,
  reasons_html text,
  invalidation_html text,
  status text not null default 'active' check (status in ('active','not_relevant','archived')),
  pinned boolean not null default false,
  position integer not null default 0,
  created_by uuid not null references auth.users(id) on delete restrict,
  updated_by uuid references auth.users(id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
create index if not exists v2_scenarios_workspace_asset_status_idx
  on public.v2_scenarios(workspace_id, asset_symbol, status, pinned desc, position);

create table if not exists public.v2_scenario_alternatives (
  id uuid primary key default gen_random_uuid(),
  scenario_id uuid not null references public.v2_scenarios(id) on delete cascade,
  label text not null,
  content_html text,
  position integer not null default 0,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (scenario_id, position)
);

create table if not exists public.v2_trades (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
  template_id uuid references public.v2_templates(id) on delete set null,
  scenario_id uuid references public.v2_scenarios(id) on delete set null,
  asset_symbol text not null default 'BTC',
  title text not null,
  status text not null default 'evaluating' check (status in ('evaluating','active','closed')),
  activated_at timestamptz,
  closed_at timestamptz,
  pnl numeric,
  created_by uuid not null references auth.users(id) on delete restrict,
  updated_by uuid references auth.users(id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
create index if not exists v2_trades_workspace_status_updated_idx
  on public.v2_trades(workspace_id, status, updated_at desc);

create table if not exists public.v2_trade_values (
  id uuid primary key default gen_random_uuid(),
  trade_id uuid not null references public.v2_trades(id) on delete cascade,
  field_id uuid not null references public.v2_template_fields(id) on delete restrict,
  value jsonb not null default '{}'::jsonb,
  updated_by uuid references auth.users(id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (trade_id, field_id)
);

create table if not exists public.v2_spot_assets (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
  symbol text not null,
  category text not null check (category in ('btc','alt','stable')),
  quantity numeric not null default 0,
  location_type text check (location_type in ('exchange','wallet','other')),
  location_name text,
  wallet_address text,
  device_name text,
  security_notes text,
  created_by uuid not null references auth.users(id) on delete restrict,
  updated_by uuid references auth.users(id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists v2_spot_assets_workspace_symbol_location_idx
  on public.v2_spot_assets(workspace_id, symbol, coalesce(location_name, ''));

create table if not exists public.v2_spot_transactions (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
  asset_id uuid not null references public.v2_spot_assets(id) on delete cascade,
  lot_key text,
  side text not null check (side in ('buy','sell')),
  occurred_at timestamptz not null default now(),
  quantity numeric not null check (quantity > 0),
  price numeric not null check (price >= 0),
  fee numeric not null default 0 check (fee >= 0),
  venue text,
  note text,
  created_by uuid not null references auth.users(id) on delete restrict,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
create index if not exists v2_spot_transactions_asset_time_idx
  on public.v2_spot_transactions(asset_id, occurred_at desc);

create table if not exists public.v2_spot_orders (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
  asset_id uuid not null references public.v2_spot_assets(id) on delete cascade,
  venue text,
  order_type text,
  side text not null check (side in ('buy','sell')),
  price numeric not null check (price >= 0),
  quantity numeric not null check (quantity > 0),
  status text not null default 'pending' check (status in ('pending','filled','cancelled')),
  created_by uuid not null references auth.users(id) on delete restrict,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.v2_spot_targets (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
  asset_id uuid not null references public.v2_spot_assets(id) on delete cascade,
  direction text not null default 'above' check (direction in ('above','below')),
  target_price numeric not null check (target_price >= 0),
  active boolean not null default true,
  acknowledged_by jsonb not null default '[]'::jsonb,
  created_by uuid not null references auth.users(id) on delete restrict,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

-- Server-side actor attribution.
-- Authenticated clients cannot forge created_by, updated_by, or last_edited_by.
-- Trusted migration/service contexts without an auth user may still provide an
-- explicit value; existing created_by values are immutable on update.
create or replace function public.v2_force_created_by()
returns trigger
language plpgsql
set search_path = public, pg_temp
as $$
declare
  actor_id uuid := auth.uid();
begin
  if tg_op = 'INSERT' then
    if actor_id is not null then
      new.created_by := actor_id;
    end if;
  else
    new.created_by := old.created_by;
  end if;
  return new;
end;
$$;

create or replace function public.v2_force_updated_by()
returns trigger
language plpgsql
set search_path = public, pg_temp
as $$
declare
  actor_id uuid := auth.uid();
begin
  if actor_id is not null then
    new.updated_by := actor_id;
  end if;
  return new;
end;
$$;

create or replace function public.v2_force_last_edited_by()
returns trigger
language plpgsql
set search_path = public, pg_temp
as $$
declare
  actor_id uuid := auth.uid();
begin
  if actor_id is not null then
    new.last_edited_by := actor_id;
  end if;
  return new;
end;
$$;

do $$
declare
  t text;
begin
  foreach t in array array[
    'v2_templates',
    'v2_collection_sessions',
    'v2_news_items',
    'v2_scenarios',
    'v2_trades',
    'v2_spot_assets',
    'v2_spot_transactions',
    'v2_spot_orders',
    'v2_spot_targets'
  ] loop
    execute format('drop trigger if exists %I on public.%I', t || '_created_by', t);
    execute format(
      'create trigger %I before insert or update on public.%I for each row execute function public.v2_force_created_by()',
      t || '_created_by',
      t
    );
  end loop;
end $$;

do $$
declare
  t text;
begin
  foreach t in array array[
    'v2_collection_values',
    'v2_news_sources',
    'v2_news_items',
    'v2_scenarios',
    'v2_trades',
    'v2_trade_values',
    'v2_spot_assets'
  ] loop
    execute format('drop trigger if exists %I on public.%I', t || '_updated_by', t);
    execute format(
      'create trigger %I before insert or update on public.%I for each row execute function public.v2_force_updated_by()',
      t || '_updated_by',
      t
    );
  end loop;
end $$;

drop trigger if exists v2_collection_sessions_last_edited_by on public.v2_collection_sessions;
create trigger v2_collection_sessions_last_edited_by
before insert or update on public.v2_collection_sessions
for each row execute function public.v2_force_last_edited_by();

-- Timestamp triggers.
do $$
declare
  t text;
begin
  foreach t in array array[
    'v2_workspaces','v2_templates','v2_template_sections','v2_template_fields',
    'v2_collection_sessions','v2_collection_values','v2_news_sources','v2_news_items',
    'v2_scenarios','v2_scenario_alternatives','v2_trades','v2_trade_values',
    'v2_spot_assets','v2_spot_transactions','v2_spot_orders','v2_spot_targets'
  ] loop
    execute format('drop trigger if exists %I on public.%I', t || '_updated_at', t);
    execute format(
      'create trigger %I before update on public.%I for each row execute function public.v2_set_updated_at()',
      t || '_updated_at', t
    );
  end loop;
end $$;

-- RLS: direct workspace tables.
do $$
declare
  t text;
begin
  foreach t in array array[
    'v2_templates','v2_collection_sessions','v2_news_sources','v2_news_items',
    'v2_scenarios','v2_trades','v2_spot_assets','v2_spot_transactions',
    'v2_spot_orders','v2_spot_targets'
  ] loop
    execute format('alter table public.%I enable row level security', t);
    execute format('drop policy if exists %I on public.%I', t || '_select', t);
    execute format('drop policy if exists %I on public.%I', t || '_insert', t);
    execute format('drop policy if exists %I on public.%I', t || '_update', t);
    execute format('drop policy if exists %I on public.%I', t || '_delete', t);
    execute format(
      'create policy %I on public.%I for select to authenticated using (public.v2_is_workspace_member(workspace_id))',
      t || '_select', t
    );
    execute format(
      'create policy %I on public.%I for insert to authenticated with check (public.v2_can_edit_workspace(workspace_id))',
      t || '_insert', t
    );
    execute format(
      'create policy %I on public.%I for update to authenticated using (public.v2_can_edit_workspace(workspace_id)) with check (public.v2_can_edit_workspace(workspace_id))',
      t || '_update', t
    );
    execute format(
      'create policy %I on public.%I for delete to authenticated using (public.v2_can_edit_workspace(workspace_id))',
      t || '_delete', t
    );
  end loop;
end $$;

alter table public.v2_workspaces enable row level security;
drop policy if exists v2_workspaces_select on public.v2_workspaces;
drop policy if exists v2_workspaces_insert on public.v2_workspaces;
drop policy if exists v2_workspaces_update on public.v2_workspaces;
create policy v2_workspaces_select on public.v2_workspaces
  for select to authenticated using (public.v2_is_workspace_member(id));
create policy v2_workspaces_insert on public.v2_workspaces
  for insert to authenticated with check (owner_id = auth.uid());
create policy v2_workspaces_update on public.v2_workspaces
  for update to authenticated using (public.v2_is_workspace_owner(id))
  with check (public.v2_is_workspace_owner(id));

alter table public.v2_workspace_members enable row level security;
drop policy if exists v2_workspace_members_select on public.v2_workspace_members;
drop policy if exists v2_workspace_members_insert on public.v2_workspace_members;
drop policy if exists v2_workspace_members_update on public.v2_workspace_members;
drop policy if exists v2_workspace_members_delete on public.v2_workspace_members;
create policy v2_workspace_members_select on public.v2_workspace_members
  for select to authenticated using (public.v2_is_workspace_member(workspace_id));
create policy v2_workspace_members_insert on public.v2_workspace_members
  for insert to authenticated with check (public.v2_is_workspace_owner(workspace_id));
create policy v2_workspace_members_update on public.v2_workspace_members
  for update to authenticated using (public.v2_is_workspace_owner(workspace_id))
  with check (public.v2_is_workspace_owner(workspace_id));
create policy v2_workspace_members_delete on public.v2_workspace_members
  for delete to authenticated using (
    public.v2_is_workspace_owner(workspace_id)
    and user_id <> auth.uid()
  );

-- RLS for template children.
alter table public.v2_template_sections enable row level security;
drop policy if exists v2_template_sections_select on public.v2_template_sections;
drop policy if exists v2_template_sections_write on public.v2_template_sections;
create policy v2_template_sections_select on public.v2_template_sections
  for select to authenticated using (exists (
    select 1 from public.v2_templates t
    where t.id = template_id and public.v2_is_workspace_member(t.workspace_id)
  ));
create policy v2_template_sections_write on public.v2_template_sections
  for all to authenticated using (exists (
    select 1 from public.v2_templates t
    where t.id = template_id and public.v2_can_edit_workspace(t.workspace_id)
  )) with check (exists (
    select 1 from public.v2_templates t
    where t.id = template_id and public.v2_can_edit_workspace(t.workspace_id)
  ));

alter table public.v2_template_fields enable row level security;
drop policy if exists v2_template_fields_select on public.v2_template_fields;
drop policy if exists v2_template_fields_write on public.v2_template_fields;
create policy v2_template_fields_select on public.v2_template_fields
  for select to authenticated using (exists (
    select 1 from public.v2_template_sections s
    join public.v2_templates t on t.id = s.template_id
    where s.id = section_id and public.v2_is_workspace_member(t.workspace_id)
  ));
create policy v2_template_fields_write on public.v2_template_fields
  for all to authenticated using (exists (
    select 1 from public.v2_template_sections s
    join public.v2_templates t on t.id = s.template_id
    where s.id = section_id and public.v2_can_edit_workspace(t.workspace_id)
  )) with check (exists (
    select 1 from public.v2_template_sections s
    join public.v2_templates t on t.id = s.template_id
    where s.id = section_id and public.v2_can_edit_workspace(t.workspace_id)
  ));

-- RLS for answer/child tables.
alter table public.v2_collection_values enable row level security;
drop policy if exists v2_collection_values_select on public.v2_collection_values;
drop policy if exists v2_collection_values_write on public.v2_collection_values;
create policy v2_collection_values_select on public.v2_collection_values
  for select to authenticated using (exists (
    select 1 from public.v2_collection_sessions s
    where s.id = session_id and public.v2_is_workspace_member(s.workspace_id)
  ));
create policy v2_collection_values_write on public.v2_collection_values
  for all to authenticated using (exists (
    select 1 from public.v2_collection_sessions s
    where s.id = session_id and public.v2_can_edit_workspace(s.workspace_id)
  )) with check (exists (
    select 1 from public.v2_collection_sessions s
    where s.id = session_id and public.v2_can_edit_workspace(s.workspace_id)
  ));

alter table public.v2_scenario_alternatives enable row level security;
drop policy if exists v2_scenario_alternatives_select on public.v2_scenario_alternatives;
drop policy if exists v2_scenario_alternatives_write on public.v2_scenario_alternatives;
create policy v2_scenario_alternatives_select on public.v2_scenario_alternatives
  for select to authenticated using (exists (
    select 1 from public.v2_scenarios s
    where s.id = scenario_id and public.v2_is_workspace_member(s.workspace_id)
  ));
create policy v2_scenario_alternatives_write on public.v2_scenario_alternatives
  for all to authenticated using (exists (
    select 1 from public.v2_scenarios s
    where s.id = scenario_id and public.v2_can_edit_workspace(s.workspace_id)
  )) with check (exists (
    select 1 from public.v2_scenarios s
    where s.id = scenario_id and public.v2_can_edit_workspace(s.workspace_id)
  ));

alter table public.v2_trade_values enable row level security;
drop policy if exists v2_trade_values_select on public.v2_trade_values;
drop policy if exists v2_trade_values_write on public.v2_trade_values;
create policy v2_trade_values_select on public.v2_trade_values
  for select to authenticated using (exists (
    select 1 from public.v2_trades t
    where t.id = trade_id and public.v2_is_workspace_member(t.workspace_id)
  ));
create policy v2_trade_values_write on public.v2_trade_values
  for all to authenticated using (exists (
    select 1 from public.v2_trades t
    where t.id = trade_id and public.v2_can_edit_workspace(t.workspace_id)
  )) with check (exists (
    select 1 from public.v2_trades t
    where t.id = trade_id and public.v2_can_edit_workspace(t.workspace_id)
  ));

-- Create a workspace automatically for existing legacy project owners and inherit project members.
insert into public.v2_workspaces(owner_id, name)
select distinct p.user_id, 'סביבת העבודה למסחר'
from public.projects p
where p.user_id is not null
on conflict (owner_id) do nothing;

insert into public.v2_workspace_members(workspace_id, user_id, role)
select w.id, w.owner_id, 'owner'
from public.v2_workspaces w
on conflict (workspace_id, user_id) do update set role = 'owner';

insert into public.v2_workspace_members(workspace_id, user_id, role)
select
  w.id,
  pm.user_id,
  case when bool_or(pm.role <> 'viewer') then 'editor' else 'viewer' end
from public.project_members pm
join public.projects p on p.id = pm.project_id
join public.v2_workspaces w on w.owner_id = p.user_id
where pm.user_id <> w.owner_id
group by w.id, pm.user_id
on conflict (workspace_id, user_id) do update
set role = case
  when public.v2_workspace_members.role = 'owner' then 'owner'
  when excluded.role = 'editor' then 'editor'
  else public.v2_workspace_members.role
end;

create or replace function public.v2_seed_default_templates(p_workspace_id uuid)
returns void
language plpgsql
security definer
set search_path = public
as $$
declare
  collection_template uuid;
  trade_template uuid;
  section_news uuid;
  section_data uuid;
  section_scan uuid;
  section_entry uuid;
  section_manage uuid;
  section_journal uuid;
begin
  if not public.v2_can_edit_workspace(p_workspace_id) then
    raise exception 'not allowed';
  end if;

  select id into collection_template
  from public.v2_templates
  where workspace_id = p_workspace_id and kind = 'collection' and is_default and active
  limit 1;

  if collection_template is null then
    insert into public.v2_templates(workspace_id, kind, name, is_default, created_by)
    values (p_workspace_id, 'collection', 'איסוף מידע – ברירת מחדל', true, auth.uid())
    returning id into collection_template;

    insert into public.v2_template_sections(template_id, title, position)
    values (collection_template, 'חדשות ומאקרו', 0)
    returning id into section_news;
    insert into public.v2_template_sections(template_id, title, position)
    values (collection_template, 'נזילות, טכני ונתונים', 1)
    returning id into section_data;

    insert into public.v2_template_fields(section_id, field_key, title, field_type, position)
    values
      (section_news, 'news', 'חדשות', 'news_source_table', 0),
      (section_news, 'macro', 'מאקרו', 'dynamic_text', 1),
      (section_data, 'liquidity', 'נזילות', 'liquidity_table', 0),
      (section_data, 'technical', 'טכני ונתונים', 'dynamic_text', 1);
  end if;

  select id into trade_template
  from public.v2_templates
  where workspace_id = p_workspace_id and kind = 'trade' and is_default and active
  limit 1;

  if trade_template is null then
    insert into public.v2_templates(workspace_id, kind, name, is_default, created_by)
    values (p_workspace_id, 'trade', 'טרייד – ברירת מחדל', true, auth.uid())
    returning id into trade_template;

    insert into public.v2_template_sections(template_id, title, position)
    values (trade_template, 'סריקת טרייד', 0) returning id into section_scan;
    insert into public.v2_template_sections(template_id, title, position)
    values (trade_template, 'כניסה', 1) returning id into section_entry;
    insert into public.v2_template_sections(template_id, title, position)
    values (trade_template, 'ניהול טרייד', 2) returning id into section_manage;
    insert into public.v2_template_sections(template_id, title, position)
    values (trade_template, 'יומן', 3) returning id into section_journal;

    insert into public.v2_template_fields(section_id, field_key, title, field_type, position, required)
    values
      (section_scan, 'trade_thesis', 'תזה ותרחיש', 'long_text', 0, true),
      (section_scan, 'trade_risks', 'מה יבטל את התרחיש', 'long_text', 1, true),
      (section_entry, 'entry_plan', 'תכנית כניסה', 'long_text', 0, true),
      (section_entry, 'stop_loss', 'סטופ', 'short_text', 1, true),
      (section_manage, 'management_plan', 'תכנית ניהול', 'long_text', 0, true),
      (section_journal, 'conclusions', 'מסקנות', 'long_text', 0, false),
      (section_journal, 'notes', 'הערות', 'long_text', 1, false);
  end if;
end;
$$;

grant execute on function public.v2_seed_default_templates(uuid) to authenticated;

create or replace function public.v2_ensure_my_workspace()
returns uuid
language plpgsql
security definer
set search_path = public
as $$
declare
  uid uuid := auth.uid();
  wid uuid;
begin
  if uid is null then
    raise exception 'authentication required';
  end if;

  select m.workspace_id into wid
  from public.v2_workspace_members m
  join public.v2_workspaces w on w.id = m.workspace_id
  where m.user_id = uid
  order by (w.owner_id = uid) desc, w.created_at asc
  limit 1;

  if wid is null then
    insert into public.v2_workspaces(owner_id, name)
    values (uid, 'סביבת העבודה למסחר')
    on conflict (owner_id) do update set owner_id = excluded.owner_id
    returning id into wid;

    insert into public.v2_workspace_members(workspace_id, user_id, role)
    values (wid, uid, 'owner')
    on conflict (workspace_id, user_id) do update set role = 'owner';
  end if;

  perform public.v2_seed_default_templates(wid);
  return wid;
end;
$$;

grant execute on function public.v2_ensure_my_workspace() to authenticated;

-- Seed defaults for existing owners in a controlled security-definer context.
-- Any seed failure must abort the migration; a partially initialized workspace
-- must never be reported as a successful migration.
do $$
declare
  rec record;
  prior_subject text := current_setting('request.jwt.claim.sub', true);
begin
  for rec in select id, owner_id from public.v2_workspaces loop
    perform set_config('request.jwt.claim.sub', rec.owner_id::text, true);
    perform public.v2_seed_default_templates(rec.id);

    if not exists (
      select 1
      from public.v2_templates t
      where t.workspace_id = rec.id
        and t.kind = 'collection'
        and t.is_default
        and t.active
    ) then
      raise exception 'default collection template was not created for workspace %', rec.id
        using errcode = '23514';
    end if;

    if not exists (
      select 1
      from public.v2_templates t
      where t.workspace_id = rec.id
        and t.kind = 'trade'
        and t.is_default
        and t.active
    ) then
      raise exception 'default trade template was not created for workspace %', rec.id
        using errcode = '23514';
    end if;
  end loop;
  perform set_config('request.jwt.claim.sub', coalesce(prior_subject, ''), true);
end $$;
