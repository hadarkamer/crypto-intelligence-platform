-- Completion layer for Yoav's V1 workflow requirements.
-- Adds only new V2 objects and nullable metadata columns; legacy rows remain untouched.

create table if not exists public.v2_asset_settings (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
  symbol text not null,
  display_name text,
  active boolean not null default true,
  default_collection_template_id uuid references public.v2_templates(id) on delete set null,
  default_scenario_template_id uuid references public.v2_templates(id) on delete set null,
  default_trade_template_id uuid references public.v2_templates(id) on delete set null,
  watch_links jsonb not null default '[
    {"label":"Cryptocurrency Alerting","href":"https://cryptocurrencyalerting.com/"},
    {"label":"TradingView","href":"https://www.tradingview.com/"},
    {"label":"Binance","href":"https://www.binance.com/"},
    {"label":"Bybit","href":"https://www.bybit.com/"}
  ]'::jsonb,
  settings jsonb not null default '{}'::jsonb,
  created_by uuid references auth.users(id) on delete set null,
  updated_by uuid references auth.users(id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (workspace_id, symbol)
);

insert into public.v2_asset_settings(workspace_id, symbol, display_name, created_by, updated_by)
select id, 'BTC', 'Bitcoin', owner_id, owner_id
from public.v2_workspaces
on conflict (workspace_id, symbol) do nothing;

create table if not exists public.v2_economic_schedules (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
  name text not null,
  importance smallint not null default 1 check (importance between 1 and 3),
  weekdays smallint[] not null default array[1,2,3,4,5]::smallint[],
  times text[] not null default array['10:00']::text[],
  reminder_offsets integer[] not null default array[60]::integer[],
  starts_on date not null default current_date,
  ends_on date,
  active boolean not null default true,
  created_by uuid references auth.users(id) on delete set null,
  updated_by uuid references auth.users(id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  check (ends_on is null or ends_on >= starts_on)
);
create index if not exists v2_economic_schedules_workspace_active_idx
  on public.v2_economic_schedules(workspace_id, active, starts_on);

alter table public.economic_events
  add column if not exists workspace_id uuid references public.v2_workspaces(id) on delete set null,
  add column if not exists recurring_schedule_id uuid references public.v2_economic_schedules(id) on delete set null,
  add column if not exists occurrence_key text;

create unique index if not exists economic_events_recurring_occurrence_idx
  on public.economic_events(recurring_schedule_id, occurrence_key);

-- A scenario can use its own template and expose extra template-driven fields.
alter table public.v2_scenarios
  add column if not exists template_id uuid references public.v2_templates(id) on delete set null,
  add column if not exists author_name text;

create table if not exists public.v2_scenario_values (
  id uuid primary key default gen_random_uuid(),
  scenario_id uuid not null references public.v2_scenarios(id) on delete cascade,
  field_id uuid not null references public.v2_template_fields(id) on delete restrict,
  value jsonb not null default '{}'::jsonb,
  updated_by uuid references auth.users(id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (scenario_id, field_id)
);

-- Spot templates can hold additional notes/checklist fields per asset.
create table if not exists public.v2_spot_values (
  id uuid primary key default gen_random_uuid(),
  asset_id uuid not null references public.v2_spot_assets(id) on delete cascade,
  field_id uuid not null references public.v2_template_fields(id) on delete restrict,
  value jsonb not null default '{}'::jsonb,
  updated_by uuid references auth.users(id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (asset_id, field_id)
);

alter table public.v2_spot_transactions add column if not exists updated_by uuid references auth.users(id) on delete set null;
alter table public.v2_spot_orders add column if not exists updated_by uuid references auth.users(id) on delete set null;
alter table public.v2_spot_targets add column if not exists updated_by uuid references auth.users(id) on delete set null;

-- Extend the field-type constraint with configurable PnL and rich text.
alter table public.v2_template_fields drop constraint if exists v2_template_fields_field_type_check;
alter table public.v2_template_fields
  add constraint v2_template_fields_field_type_check check (field_type in (
    'dynamic_text','rich_text','short_text','long_text','checkbox','yes_no_na',
    'liquidity_table','news_table','news_source_table','economic_calendar','number','pnl','image'
  ));

-- Default scenario and spot templates, plus a configurable PnL field in trade templates.
do $$
declare
  workspace record;
  scenario_template uuid;
  spot_template uuid;
  scenario_section uuid;
  spot_section uuid;
  journal_section uuid;
begin
  for workspace in select id, owner_id from public.v2_workspaces loop
    select id into scenario_template
    from public.v2_templates
    where workspace_id = workspace.id and kind = 'scenario' and is_default and active
    limit 1;
    if scenario_template is null then
      insert into public.v2_templates(workspace_id, kind, name, is_default, active, created_by)
      values (workspace.id, 'scenario', 'תרחיש – ברירת מחדל', true, true, workspace.owner_id)
      returning id into scenario_template;
      insert into public.v2_template_sections(template_id, title, position, settings)
      values (scenario_template, 'פרטי התרחיש', 0, '{"base_section":true}'::jsonb)
      returning id into scenario_section;
      insert into public.v2_template_fields(section_id, field_key, title, field_type, position)
      values
        (scenario_section, 'scenario_body', 'התרחיש', 'rich_text', 0),
        (scenario_section, 'scenario_reasons', 'סיבות ואינדיקציות', 'rich_text', 1),
        (scenario_section, 'scenario_invalidation', 'מה יבטל את התרחיש', 'rich_text', 2);
    end if;

    select id into spot_template
    from public.v2_templates
    where workspace_id = workspace.id and kind = 'spot' and is_default and active
    limit 1;
    if spot_template is null then
      insert into public.v2_templates(workspace_id, kind, name, is_default, active, created_by)
      values (workspace.id, 'spot', 'יומן ספוט – ברירת מחדל', true, true, workspace.owner_id)
      returning id into spot_template;
      insert into public.v2_template_sections(template_id, title, position)
      values (spot_template, 'תיעוד הנכס', 0)
      returning id into spot_section;
      insert into public.v2_template_fields(section_id, field_key, title, field_type, position)
      values
        (spot_section, 'spot_plan', 'תכנית ונקודות חשובות', 'rich_text', 0),
        (spot_section, 'spot_notes', 'הערות', 'dynamic_text', 1);
    end if;

    update public.v2_asset_settings
    set default_scenario_template_id = coalesce(default_scenario_template_id, scenario_template),
        default_trade_template_id = coalesce(
          default_trade_template_id,
          (select id from public.v2_templates where workspace_id = workspace.id and kind = 'trade' and is_default and active limit 1)
        ),
        updated_at = now()
    where workspace_id = workspace.id and symbol = 'BTC';

    select s.id into journal_section
    from public.v2_template_sections s
    join public.v2_templates t on t.id = s.template_id
    where t.workspace_id = workspace.id and t.kind = 'trade' and t.is_default and s.title = 'יומן'
    order by s.position
    limit 1;
    if journal_section is not null and not exists (
      select 1 from public.v2_template_fields
      where section_id = journal_section and field_key = 'trade_pnl'
    ) and not exists (
      select 1 from public.v2_trades tr
      join public.v2_template_sections sec on sec.template_id = tr.template_id
      where sec.id = journal_section
    ) then
      insert into public.v2_template_fields(section_id, field_key, title, field_type, position, required)
      values (
        journal_section,
        'trade_pnl',
        'PnL',
        'pnl',
        coalesce((select max(position) + 1 from public.v2_template_fields where section_id = journal_section), 0),
        false
      );
    end if;
  end loop;
end $$;

-- Identity and timestamp triggers for new/extended tables.
drop trigger if exists v2_asset_settings_created_by on public.v2_asset_settings;
create trigger v2_asset_settings_created_by before insert or update on public.v2_asset_settings
for each row execute function public.v2_force_created_by();
drop trigger if exists v2_asset_settings_updated_by on public.v2_asset_settings;
create trigger v2_asset_settings_updated_by before insert or update on public.v2_asset_settings
for each row execute function public.v2_force_updated_by();

drop trigger if exists v2_economic_schedules_created_by on public.v2_economic_schedules;
create trigger v2_economic_schedules_created_by before insert or update on public.v2_economic_schedules
for each row execute function public.v2_force_created_by();
drop trigger if exists v2_economic_schedules_updated_by on public.v2_economic_schedules;
create trigger v2_economic_schedules_updated_by before insert or update on public.v2_economic_schedules
for each row execute function public.v2_force_updated_by();

drop trigger if exists v2_scenario_values_updated_by on public.v2_scenario_values;
create trigger v2_scenario_values_updated_by before insert or update on public.v2_scenario_values
for each row execute function public.v2_force_updated_by();
drop trigger if exists v2_spot_values_updated_by on public.v2_spot_values;
create trigger v2_spot_values_updated_by before insert or update on public.v2_spot_values
for each row execute function public.v2_force_updated_by();

do $$
declare
  relation_name text;
begin
  foreach relation_name in array array[
    'v2_asset_settings','v2_economic_schedules','v2_scenario_values','v2_spot_values'
  ] loop
    execute format('drop trigger if exists %I_updated_at on public.%I', relation_name, relation_name);
    execute format(
      'create trigger %I_updated_at before update on public.%I for each row execute function public.v2_set_updated_at()',
      relation_name,
      relation_name
    );
  end loop;
end $$;

-- RLS for new workspace-scoped tables.
alter table public.v2_asset_settings enable row level security;
alter table public.v2_economic_schedules enable row level security;
alter table public.v2_scenario_values enable row level security;
alter table public.v2_spot_values enable row level security;

drop policy if exists v2_asset_settings_select on public.v2_asset_settings;
create policy v2_asset_settings_select on public.v2_asset_settings for select to authenticated
using (public.v2_is_workspace_member(workspace_id));
drop policy if exists v2_asset_settings_write on public.v2_asset_settings;
create policy v2_asset_settings_write on public.v2_asset_settings for all to authenticated
using (public.v2_can_edit_workspace(workspace_id))
with check (public.v2_can_edit_workspace(workspace_id));

drop policy if exists v2_economic_schedules_select on public.v2_economic_schedules;
create policy v2_economic_schedules_select on public.v2_economic_schedules for select to authenticated
using (public.v2_is_workspace_member(workspace_id));
drop policy if exists v2_economic_schedules_write on public.v2_economic_schedules;
create policy v2_economic_schedules_write on public.v2_economic_schedules for all to authenticated
using (public.v2_can_edit_workspace(workspace_id))
with check (public.v2_can_edit_workspace(workspace_id));

drop policy if exists v2_scenario_values_select on public.v2_scenario_values;
create policy v2_scenario_values_select on public.v2_scenario_values for select to authenticated
using (exists (
  select 1 from public.v2_scenarios s
  where s.id = scenario_id and public.v2_is_workspace_member(s.workspace_id)
));
drop policy if exists v2_scenario_values_write on public.v2_scenario_values;
create policy v2_scenario_values_write on public.v2_scenario_values for all to authenticated
using (exists (
  select 1 from public.v2_scenarios s
  where s.id = scenario_id and public.v2_can_edit_workspace(s.workspace_id)
)) with check (exists (
  select 1 from public.v2_scenarios s
  where s.id = scenario_id and public.v2_can_edit_workspace(s.workspace_id)
));

drop policy if exists v2_spot_values_select on public.v2_spot_values;
create policy v2_spot_values_select on public.v2_spot_values for select to authenticated
using (exists (
  select 1 from public.v2_spot_assets a
  where a.id = asset_id and public.v2_is_workspace_member(a.workspace_id)
));
drop policy if exists v2_spot_values_write on public.v2_spot_values;
create policy v2_spot_values_write on public.v2_spot_values for all to authenticated
using (exists (
  select 1 from public.v2_spot_assets a
  where a.id = asset_id and public.v2_can_edit_workspace(a.workspace_id)
)) with check (exists (
  select 1 from public.v2_spot_assets a
  where a.id = asset_id and public.v2_can_edit_workspace(a.workspace_id)
));

revoke all on public.v2_asset_settings, public.v2_economic_schedules,
  public.v2_scenario_values, public.v2_spot_values from anon;
grant select, insert, update, delete on public.v2_asset_settings,
  public.v2_economic_schedules, public.v2_scenario_values, public.v2_spot_values to authenticated;
