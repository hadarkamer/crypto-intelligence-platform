-- Schema-only fixture captured through read-only pg_catalog metadata on 2026-09-05.
-- The live Sep03 migration history records versions but no statement bodies.
-- Reconstructs their relevant additions over the real July/August schema.
-- No production rows, secrets, or migration side effects are copied.
-- Also includes the live module/source constraints on legacy projects.

create table if not exists public."scenario_assets" (
  "id" uuid default gen_random_uuid() not null,
  "symbol" text not null,
  "display_name" text not null,
  "active" boolean default true not null,
  "position" integer default 0 not null,
  "template_config" jsonb default '{"authorLabel": "מאת", "reasonsLabel": "סיבות לתרחיש", "scenarioLabel": "תרחיש", "invalidationLabel": "מה יבטל את התרחיש"}'::jsonb not null,
  "created_by" uuid default auth.uid() not null,
  "created_at" timestamp with time zone default now() not null,
  "updated_at" timestamp with time zone default now() not null
);

create table if not exists public."scenario_variants" (
  "id" uuid default gen_random_uuid() not null,
  "scenario_id" uuid not null,
  "label" text not null,
  "scenario_html" text default ''::text not null,
  "reasons" text default ''::text not null,
  "invalidation" text default ''::text not null,
  "position" integer default 0 not null,
  "created_at" timestamp with time zone default now() not null,
  "updated_at" timestamp with time zone default now() not null
);

create table if not exists public."scenario_watch_links" (
  "id" uuid default gen_random_uuid() not null,
  "asset_id" uuid not null,
  "category" text not null,
  "label" text not null,
  "url" text not null,
  "position" integer default 0 not null,
  "active" boolean default true not null,
  "created_at" timestamp with time zone default now() not null,
  "updated_at" timestamp with time zone default now() not null
);

create table if not exists public."scenarios" (
  "id" uuid default gen_random_uuid() not null,
  "asset_id" uuid not null,
  "author_text" text default ''::text not null,
  "status" text default 'active'::text not null,
  "pinned" boolean default false not null,
  "position" integer default 0 not null,
  "created_by" uuid default auth.uid() not null,
  "updated_by" uuid default auth.uid() not null,
  "created_at" timestamp with time zone default now() not null,
  "updated_at" timestamp with time zone default now() not null
);

create table if not exists public."spot_accounts" (
  "id" uuid default gen_random_uuid() not null,
  "label" text not null,
  "account_type" text not null,
  "address" text,
  "device" text,
  "notes" text,
  "active" boolean default true not null,
  "created_by" uuid default auth.uid() not null,
  "created_at" timestamp with time zone default now() not null,
  "updated_at" timestamp with time zone default now() not null
);

create table if not exists public."spot_alert_acknowledgements" (
  "alert_id" uuid not null,
  "user_id" uuid default auth.uid() not null,
  "triggered_price" numeric,
  "acknowledged_at" timestamp with time zone default now() not null
);

create table if not exists public."spot_assets" (
  "id" uuid default gen_random_uuid() not null,
  "symbol" text not null,
  "display_name" text not null,
  "category" text not null,
  "active" boolean default true not null,
  "position" integer default 0 not null,
  "created_at" timestamp with time zone default now() not null,
  "updated_at" timestamp with time zone default now() not null
);

create table if not exists public."spot_positions" (
  "id" uuid default gen_random_uuid() not null,
  "asset_id" uuid not null,
  "account_id" uuid,
  "label" text default ''::text not null,
  "status" text default 'open'::text not null,
  "target_price" numeric,
  "exchange_orders" text,
  "notes" text,
  "created_by" uuid default auth.uid() not null,
  "updated_by" uuid default auth.uid() not null,
  "created_at" timestamp with time zone default now() not null,
  "updated_at" timestamp with time zone default now() not null
);

create table if not exists public."spot_price_alerts" (
  "id" uuid default gen_random_uuid() not null,
  "position_id" uuid not null,
  "target_price" numeric not null,
  "direction" text default 'above'::text not null,
  "active" boolean default true not null,
  "created_by" uuid default auth.uid() not null,
  "created_at" timestamp with time zone default now() not null,
  "updated_at" timestamp with time zone default now() not null
);

create table if not exists public."spot_transactions" (
  "id" uuid default gen_random_uuid() not null,
  "position_id" uuid not null,
  "transaction_type" text not null,
  "quantity" numeric not null,
  "price" numeric,
  "fee" numeric default 0 not null,
  "occurred_at" timestamp with time zone default now() not null,
  "notes" text,
  "created_by" uuid default auth.uid() not null,
  "created_at" timestamp with time zone default now() not null,
  "updated_at" timestamp with time zone default now() not null
);

create table if not exists public."economic_recurring_schedules" (
  "id" uuid default gen_random_uuid() not null,
  "name" text not null,
  "importance" smallint default 1 not null,
  "timezone" text default 'Asia/Jerusalem'::text not null,
  "days_of_week" smallint[] default ARRAY[(1)::smallint, (2)::smallint, (3)::smallint, (4)::smallint, (5)::smallint] not null,
  "start_time" time without time zone not null,
  "end_time" time without time zone,
  "reminder_offsets" integer[] default ARRAY[60] not null,
  "active" boolean default true not null,
  "created_by" uuid default auth.uid() not null,
  "updated_by" uuid default auth.uid() not null,
  "created_at" timestamp with time zone default now() not null,
  "updated_at" timestamp with time zone default now() not null
);

create table if not exists public."economic_recurring_acknowledgements" (
  "schedule_id" uuid not null,
  "occurrence_start" timestamp with time zone not null,
  "offset_minutes" integer not null,
  "user_id" uuid default auth.uid() not null,
  "acknowledged_at" timestamp with time zone default now() not null
);

create table if not exists public."module_template_defaults" (
  "module_type" text not null,
  "symbol" text default '*'::text not null,
  "template_id" uuid not null,
  "created_at" timestamp with time zone default now() not null,
  "updated_at" timestamp with time zone default now() not null
);

create table if not exists public."news_summary_rows" (
  "id" uuid,
  "project_id" uuid,
  "question_id" uuid,
  "body" text,
  "importance" text,
  "position" integer,
  "created_at" timestamp with time zone,
  "updated_at" timestamp with time zone,
  "updated_by" uuid,
  "project_name" text,
  "project_created_at" timestamp with time zone,
  "module_type" text,
  "source_name" text,
  "group_name" text,
  "stage_name" text,
  "editor_name" text,
  "importance_rank" integer
);
do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.economic_recurring_acknowledgements'::regclass and conname='economic_recurring_acknowledgements_pkey') then alter table public."economic_recurring_acknowledgements" add constraint "economic_recurring_acknowledgements_pkey" PRIMARY KEY (schedule_id, occurrence_start, offset_minutes, user_id); end if; end $$;
do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.economic_recurring_schedules'::regclass and conname='economic_recurring_schedules_days_of_week_check') then alter table public."economic_recurring_schedules" add constraint "economic_recurring_schedules_days_of_week_check" CHECK ((days_of_week <@ ARRAY[(0)::smallint, (1)::smallint, (2)::smallint, (3)::smallint, (4)::smallint, (5)::smallint, (6)::smallint])); end if; end $$;
do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.economic_recurring_schedules'::regclass and conname='economic_recurring_schedules_importance_check') then alter table public."economic_recurring_schedules" add constraint "economic_recurring_schedules_importance_check" CHECK (((importance >= 1) AND (importance <= 3))); end if; end $$;
do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.economic_recurring_schedules'::regclass and conname='economic_recurring_schedules_name_check') then alter table public."economic_recurring_schedules" add constraint "economic_recurring_schedules_name_check" CHECK ((char_length(btrim(name)) > 0)); end if; end $$;
do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.economic_recurring_schedules'::regclass and conname='economic_recurring_schedules_pkey') then alter table public."economic_recurring_schedules" add constraint "economic_recurring_schedules_pkey" PRIMARY KEY (id); end if; end $$;
do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.economic_recurring_schedules'::regclass and conname='economic_recurring_schedules_reminder_offsets_check') then alter table public."economic_recurring_schedules" add constraint "economic_recurring_schedules_reminder_offsets_check" CHECK ((0 < ALL (reminder_offsets))); end if; end $$;
do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.module_template_defaults'::regclass and conname='module_template_defaults_module_type_check') then alter table public."module_template_defaults" add constraint "module_template_defaults_module_type_check" CHECK ((module_type = ANY (ARRAY['information_collection'::text, 'trade'::text]))); end if; end $$;
do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.module_template_defaults'::regclass and conname='module_template_defaults_pkey') then alter table public."module_template_defaults" add constraint "module_template_defaults_pkey" PRIMARY KEY (module_type, symbol); end if; end $$;
do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.scenario_assets'::regclass and conname='scenario_assets_pkey') then alter table public."scenario_assets" add constraint "scenario_assets_pkey" PRIMARY KEY (id); end if; end $$;
do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.scenario_assets'::regclass and conname='scenario_assets_symbol_key') then alter table public."scenario_assets" add constraint "scenario_assets_symbol_key" UNIQUE (symbol); end if; end $$;
do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.scenario_variants'::regclass and conname='scenario_variants_pkey') then alter table public."scenario_variants" add constraint "scenario_variants_pkey" PRIMARY KEY (id); end if; end $$;
do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.scenario_watch_links'::regclass and conname='scenario_watch_links_pkey') then alter table public."scenario_watch_links" add constraint "scenario_watch_links_pkey" PRIMARY KEY (id); end if; end $$;
do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.scenarios'::regclass and conname='scenarios_pkey') then alter table public."scenarios" add constraint "scenarios_pkey" PRIMARY KEY (id); end if; end $$;
do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.scenarios'::regclass and conname='scenarios_status_check') then alter table public."scenarios" add constraint "scenarios_status_check" CHECK ((status = ANY (ARRAY['active'::text, 'trash'::text]))); end if; end $$;
do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.spot_accounts'::regclass and conname='spot_accounts_account_type_check') then alter table public."spot_accounts" add constraint "spot_accounts_account_type_check" CHECK ((account_type = ANY (ARRAY['wallet'::text, 'exchange'::text, 'other'::text]))); end if; end $$;
do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.spot_accounts'::regclass and conname='spot_accounts_pkey') then alter table public."spot_accounts" add constraint "spot_accounts_pkey" PRIMARY KEY (id); end if; end $$;
do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.spot_alert_acknowledgements'::regclass and conname='spot_alert_acknowledgements_pkey') then alter table public."spot_alert_acknowledgements" add constraint "spot_alert_acknowledgements_pkey" PRIMARY KEY (alert_id, user_id); end if; end $$;
do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.spot_assets'::regclass and conname='spot_assets_category_check') then alter table public."spot_assets" add constraint "spot_assets_category_check" CHECK ((category = ANY (ARRAY['bitcoin'::text, 'alt'::text, 'stable'::text]))); end if; end $$;
do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.spot_assets'::regclass and conname='spot_assets_pkey') then alter table public."spot_assets" add constraint "spot_assets_pkey" PRIMARY KEY (id); end if; end $$;
do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.spot_assets'::regclass and conname='spot_assets_symbol_key') then alter table public."spot_assets" add constraint "spot_assets_symbol_key" UNIQUE (symbol); end if; end $$;
do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.spot_positions'::regclass and conname='spot_positions_pkey') then alter table public."spot_positions" add constraint "spot_positions_pkey" PRIMARY KEY (id); end if; end $$;
do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.spot_positions'::regclass and conname='spot_positions_status_check') then alter table public."spot_positions" add constraint "spot_positions_status_check" CHECK ((status = ANY (ARRAY['open'::text, 'closed'::text, 'archived'::text]))); end if; end $$;
do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.spot_price_alerts'::regclass and conname='spot_price_alerts_direction_check') then alter table public."spot_price_alerts" add constraint "spot_price_alerts_direction_check" CHECK ((direction = ANY (ARRAY['above'::text, 'below'::text]))); end if; end $$;
do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.spot_price_alerts'::regclass and conname='spot_price_alerts_pkey') then alter table public."spot_price_alerts" add constraint "spot_price_alerts_pkey" PRIMARY KEY (id); end if; end $$;
do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.spot_price_alerts'::regclass and conname='spot_price_alerts_target_price_check') then alter table public."spot_price_alerts" add constraint "spot_price_alerts_target_price_check" CHECK ((target_price > (0)::numeric)); end if; end $$;
do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.spot_transactions'::regclass and conname='spot_transactions_fee_check') then alter table public."spot_transactions" add constraint "spot_transactions_fee_check" CHECK ((fee >= (0)::numeric)); end if; end $$;
do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.spot_transactions'::regclass and conname='spot_transactions_pkey') then alter table public."spot_transactions" add constraint "spot_transactions_pkey" PRIMARY KEY (id); end if; end $$;
do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.spot_transactions'::regclass and conname='spot_transactions_price_check') then alter table public."spot_transactions" add constraint "spot_transactions_price_check" CHECK (((price IS NULL) OR (price >= (0)::numeric))); end if; end $$;
do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.spot_transactions'::regclass and conname='spot_transactions_quantity_check') then alter table public."spot_transactions" add constraint "spot_transactions_quantity_check" CHECK ((quantity > (0)::numeric)); end if; end $$;
do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.spot_transactions'::regclass and conname='spot_transactions_transaction_type_check') then alter table public."spot_transactions" add constraint "spot_transactions_transaction_type_check" CHECK ((transaction_type = ANY (ARRAY['buy'::text, 'sell'::text, 'transfer_in'::text, 'transfer_out'::text]))); end if; end $$;
do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.economic_recurring_acknowledgements'::regclass and conname='economic_recurring_acknowledgements_schedule_id_fkey') then alter table public."economic_recurring_acknowledgements" add constraint "economic_recurring_acknowledgements_schedule_id_fkey" FOREIGN KEY (schedule_id) REFERENCES economic_recurring_schedules(id) ON DELETE CASCADE; end if; end $$;
do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.module_template_defaults'::regclass and conname='module_template_defaults_template_id_fkey') then alter table public."module_template_defaults" add constraint "module_template_defaults_template_id_fkey" FOREIGN KEY (template_id) REFERENCES templates(id) ON DELETE CASCADE; end if; end $$;
do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.scenario_variants'::regclass and conname='scenario_variants_scenario_id_fkey') then alter table public."scenario_variants" add constraint "scenario_variants_scenario_id_fkey" FOREIGN KEY (scenario_id) REFERENCES scenarios(id) ON DELETE CASCADE; end if; end $$;
do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.scenario_watch_links'::regclass and conname='scenario_watch_links_asset_id_fkey') then alter table public."scenario_watch_links" add constraint "scenario_watch_links_asset_id_fkey" FOREIGN KEY (asset_id) REFERENCES scenario_assets(id) ON DELETE CASCADE; end if; end $$;
do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.scenarios'::regclass and conname='scenarios_asset_id_fkey') then alter table public."scenarios" add constraint "scenarios_asset_id_fkey" FOREIGN KEY (asset_id) REFERENCES scenario_assets(id) ON DELETE CASCADE; end if; end $$;
do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.spot_alert_acknowledgements'::regclass and conname='spot_alert_acknowledgements_alert_id_fkey') then alter table public."spot_alert_acknowledgements" add constraint "spot_alert_acknowledgements_alert_id_fkey" FOREIGN KEY (alert_id) REFERENCES spot_price_alerts(id) ON DELETE CASCADE; end if; end $$;
do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.spot_positions'::regclass and conname='spot_positions_account_id_fkey') then alter table public."spot_positions" add constraint "spot_positions_account_id_fkey" FOREIGN KEY (account_id) REFERENCES spot_accounts(id) ON DELETE SET NULL; end if; end $$;
do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.spot_positions'::regclass and conname='spot_positions_asset_id_fkey') then alter table public."spot_positions" add constraint "spot_positions_asset_id_fkey" FOREIGN KEY (asset_id) REFERENCES spot_assets(id) ON DELETE RESTRICT; end if; end $$;
do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.spot_price_alerts'::regclass and conname='spot_price_alerts_position_id_fkey') then alter table public."spot_price_alerts" add constraint "spot_price_alerts_position_id_fkey" FOREIGN KEY (position_id) REFERENCES spot_positions(id) ON DELETE CASCADE; end if; end $$;
do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.spot_transactions'::regclass and conname='spot_transactions_position_id_fkey') then alter table public."spot_transactions" add constraint "spot_transactions_position_id_fkey" FOREIGN KEY (position_id) REFERENCES spot_positions(id) ON DELETE CASCADE; end if; end $$;
alter table public.projects add column if not exists "module_type" text default 'legacy_project'::text not null;
alter table public.projects add column if not exists "source_scenario_id" uuid;

do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.projects'::regclass and conname='projects_module_type_check') then alter table public.projects add constraint "projects_module_type_check" CHECK ((module_type = ANY (ARRAY['legacy_project'::text, 'information_collection'::text, 'trade'::text]))); end if; end $$;
do $$ begin if not exists(select 1 from pg_constraint where conrelid='public.projects'::regclass and conname='projects_source_scenario_id_fkey') then alter table public.projects add constraint "projects_source_scenario_id_fkey" FOREIGN KEY (source_scenario_id) REFERENCES scenarios(id) ON DELETE SET NULL; end if; end $$;

CREATE OR REPLACE FUNCTION app_private.can_edit_shared_calendar()
 RETURNS boolean
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'app_private'
AS $function$
  SELECT auth.uid() IS NOT NULL AND (
    EXISTS (SELECT 1 FROM public.projects p WHERE p.user_id = auth.uid())
    OR EXISTS (
      SELECT 1 FROM public.project_members pm
      WHERE pm.user_id = auth.uid() AND pm.role IN ('owner','editor')
    )
  );
$function$
;

CREATE OR REPLACE FUNCTION app_private.can_edit_shared_workspace()
 RETURNS boolean
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'app_private'
AS $function$
  SELECT auth.uid() IS NOT NULL AND (
    EXISTS (SELECT 1 FROM public.projects p WHERE p.user_id = auth.uid())
    OR EXISTS (
      SELECT 1 FROM public.project_members pm
      WHERE pm.user_id = auth.uid() AND pm.role IN ('owner','editor')
    )
    OR EXISTS (
      SELECT 1 FROM public.user_roles ur
      WHERE ur.user_id = auth.uid() AND ur.role = 'admin'
    )
  );
$function$
;

CREATE OR REPLACE FUNCTION app_private.is_app_member()
 RETURNS boolean
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'app_private'
AS $function$
  SELECT auth.uid() IS NOT NULL AND (
    EXISTS (SELECT 1 FROM public.projects p WHERE p.user_id = auth.uid())
    OR EXISTS (SELECT 1 FROM public.project_members pm WHERE pm.user_id = auth.uid())
    OR EXISTS (
      SELECT 1 FROM public.user_roles ur
      WHERE ur.user_id = auth.uid() AND ur.role = 'admin'
    )
  );
$function$
;

CREATE OR REPLACE FUNCTION public.tg_workspace_touch()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
BEGIN
  NEW.updated_at := now();
  IF to_jsonb(NEW) ? 'updated_by' THEN
    NEW.updated_by := COALESCE(auth.uid(), NEW.updated_by);
  END IF;
  RETURN NEW;
END;
$function$
;
alter table public."scenario_assets" enable row level security;
grant select,insert,update,delete on public."scenario_assets" to authenticated;
alter table public."scenario_variants" enable row level security;
grant select,insert,update,delete on public."scenario_variants" to authenticated;
alter table public."scenario_watch_links" enable row level security;
grant select,insert,update,delete on public."scenario_watch_links" to authenticated;
alter table public."scenarios" enable row level security;
grant select,insert,update,delete on public."scenarios" to authenticated;
alter table public."spot_accounts" enable row level security;
grant select,insert,update,delete on public."spot_accounts" to authenticated;
alter table public."spot_alert_acknowledgements" enable row level security;
grant select,insert,update,delete on public."spot_alert_acknowledgements" to authenticated;
alter table public."spot_assets" enable row level security;
grant select,insert,update,delete on public."spot_assets" to authenticated;
alter table public."spot_positions" enable row level security;
grant select,insert,update,delete on public."spot_positions" to authenticated;
alter table public."spot_price_alerts" enable row level security;
grant select,insert,update,delete on public."spot_price_alerts" to authenticated;
alter table public."spot_transactions" enable row level security;
grant select,insert,update,delete on public."spot_transactions" to authenticated;
alter table public."economic_recurring_schedules" enable row level security;
grant select,insert,update,delete on public."economic_recurring_schedules" to authenticated;
alter table public."economic_recurring_acknowledgements" enable row level security;
grant select,insert,update,delete on public."economic_recurring_acknowledgements" to authenticated;
alter table public."module_template_defaults" enable row level security;
grant select,insert,update,delete on public."module_template_defaults" to authenticated;
alter table public."news_summary_rows" enable row level security;
grant select,insert,update,delete on public."news_summary_rows" to authenticated;
drop policy if exists "recurring_ack_delete_own" on public."economic_recurring_acknowledgements";
create policy "recurring_ack_delete_own" on public."economic_recurring_acknowledgements" as permissive for delete to "authenticated" using(((user_id = auth.uid()) AND app_private.is_app_member()));
drop policy if exists "recurring_ack_insert_own" on public."economic_recurring_acknowledgements";
create policy "recurring_ack_insert_own" on public."economic_recurring_acknowledgements" as permissive for insert to "authenticated" with check(((user_id = auth.uid()) AND app_private.is_app_member()));
drop policy if exists "recurring_ack_read_own" on public."economic_recurring_acknowledgements";
create policy "recurring_ack_read_own" on public."economic_recurring_acknowledgements" as permissive for select to "authenticated" using(((user_id = auth.uid()) AND app_private.is_app_member()));
drop policy if exists "workspace_delete" on public."economic_recurring_schedules";
create policy "workspace_delete" on public."economic_recurring_schedules" as permissive for delete to "authenticated" using(app_private.can_edit_shared_workspace());
drop policy if exists "workspace_insert" on public."economic_recurring_schedules";
create policy "workspace_insert" on public."economic_recurring_schedules" as permissive for insert to "authenticated" with check(app_private.can_edit_shared_workspace());
drop policy if exists "workspace_read" on public."economic_recurring_schedules";
create policy "workspace_read" on public."economic_recurring_schedules" as permissive for select to "authenticated" using(app_private.is_app_member());
drop policy if exists "workspace_update" on public."economic_recurring_schedules";
create policy "workspace_update" on public."economic_recurring_schedules" as permissive for update to "authenticated" using(app_private.can_edit_shared_workspace()) with check(app_private.can_edit_shared_workspace());
drop policy if exists "workspace_delete" on public."module_template_defaults";
create policy "workspace_delete" on public."module_template_defaults" as permissive for delete to "authenticated" using(app_private.can_edit_shared_workspace());
drop policy if exists "workspace_insert" on public."module_template_defaults";
create policy "workspace_insert" on public."module_template_defaults" as permissive for insert to "authenticated" with check(app_private.can_edit_shared_workspace());
drop policy if exists "workspace_read" on public."module_template_defaults";
create policy "workspace_read" on public."module_template_defaults" as permissive for select to "authenticated" using(app_private.is_app_member());
drop policy if exists "workspace_update" on public."module_template_defaults";
create policy "workspace_update" on public."module_template_defaults" as permissive for update to "authenticated" using(app_private.can_edit_shared_workspace()) with check(app_private.can_edit_shared_workspace());
drop policy if exists "workspace_delete" on public."scenario_assets";
create policy "workspace_delete" on public."scenario_assets" as permissive for delete to "authenticated" using(app_private.can_edit_shared_workspace());
drop policy if exists "workspace_insert" on public."scenario_assets";
create policy "workspace_insert" on public."scenario_assets" as permissive for insert to "authenticated" with check(app_private.can_edit_shared_workspace());
drop policy if exists "workspace_read" on public."scenario_assets";
create policy "workspace_read" on public."scenario_assets" as permissive for select to "authenticated" using(app_private.is_app_member());
drop policy if exists "workspace_update" on public."scenario_assets";
create policy "workspace_update" on public."scenario_assets" as permissive for update to "authenticated" using(app_private.can_edit_shared_workspace()) with check(app_private.can_edit_shared_workspace());
drop policy if exists "workspace_delete" on public."scenario_variants";
create policy "workspace_delete" on public."scenario_variants" as permissive for delete to "authenticated" using(app_private.can_edit_shared_workspace());
drop policy if exists "workspace_insert" on public."scenario_variants";
create policy "workspace_insert" on public."scenario_variants" as permissive for insert to "authenticated" with check(app_private.can_edit_shared_workspace());
drop policy if exists "workspace_read" on public."scenario_variants";
create policy "workspace_read" on public."scenario_variants" as permissive for select to "authenticated" using(app_private.is_app_member());
drop policy if exists "workspace_update" on public."scenario_variants";
create policy "workspace_update" on public."scenario_variants" as permissive for update to "authenticated" using(app_private.can_edit_shared_workspace()) with check(app_private.can_edit_shared_workspace());
drop policy if exists "workspace_delete" on public."scenario_watch_links";
create policy "workspace_delete" on public."scenario_watch_links" as permissive for delete to "authenticated" using(app_private.can_edit_shared_workspace());
drop policy if exists "workspace_insert" on public."scenario_watch_links";
create policy "workspace_insert" on public."scenario_watch_links" as permissive for insert to "authenticated" with check(app_private.can_edit_shared_workspace());
drop policy if exists "workspace_read" on public."scenario_watch_links";
create policy "workspace_read" on public."scenario_watch_links" as permissive for select to "authenticated" using(app_private.is_app_member());
drop policy if exists "workspace_update" on public."scenario_watch_links";
create policy "workspace_update" on public."scenario_watch_links" as permissive for update to "authenticated" using(app_private.can_edit_shared_workspace()) with check(app_private.can_edit_shared_workspace());
drop policy if exists "workspace_delete" on public."scenarios";
create policy "workspace_delete" on public."scenarios" as permissive for delete to "authenticated" using(app_private.can_edit_shared_workspace());
drop policy if exists "workspace_insert" on public."scenarios";
create policy "workspace_insert" on public."scenarios" as permissive for insert to "authenticated" with check(app_private.can_edit_shared_workspace());
drop policy if exists "workspace_read" on public."scenarios";
create policy "workspace_read" on public."scenarios" as permissive for select to "authenticated" using(app_private.is_app_member());
drop policy if exists "workspace_update" on public."scenarios";
create policy "workspace_update" on public."scenarios" as permissive for update to "authenticated" using(app_private.can_edit_shared_workspace()) with check(app_private.can_edit_shared_workspace());
drop policy if exists "workspace_delete" on public."spot_accounts";
create policy "workspace_delete" on public."spot_accounts" as permissive for delete to "authenticated" using(app_private.can_edit_shared_workspace());
drop policy if exists "workspace_insert" on public."spot_accounts";
create policy "workspace_insert" on public."spot_accounts" as permissive for insert to "authenticated" with check(app_private.can_edit_shared_workspace());
drop policy if exists "workspace_read" on public."spot_accounts";
create policy "workspace_read" on public."spot_accounts" as permissive for select to "authenticated" using(app_private.is_app_member());
drop policy if exists "workspace_update" on public."spot_accounts";
create policy "workspace_update" on public."spot_accounts" as permissive for update to "authenticated" using(app_private.can_edit_shared_workspace()) with check(app_private.can_edit_shared_workspace());
drop policy if exists "spot_ack_delete_own" on public."spot_alert_acknowledgements";
create policy "spot_ack_delete_own" on public."spot_alert_acknowledgements" as permissive for delete to "authenticated" using(((user_id = auth.uid()) AND app_private.is_app_member()));
drop policy if exists "spot_ack_insert_own" on public."spot_alert_acknowledgements";
create policy "spot_ack_insert_own" on public."spot_alert_acknowledgements" as permissive for insert to "authenticated" with check(((user_id = auth.uid()) AND app_private.is_app_member()));
drop policy if exists "spot_ack_read_own" on public."spot_alert_acknowledgements";
create policy "spot_ack_read_own" on public."spot_alert_acknowledgements" as permissive for select to "authenticated" using(((user_id = auth.uid()) AND app_private.is_app_member()));
drop policy if exists "workspace_delete" on public."spot_assets";
create policy "workspace_delete" on public."spot_assets" as permissive for delete to "authenticated" using(app_private.can_edit_shared_workspace());
drop policy if exists "workspace_insert" on public."spot_assets";
create policy "workspace_insert" on public."spot_assets" as permissive for insert to "authenticated" with check(app_private.can_edit_shared_workspace());
drop policy if exists "workspace_read" on public."spot_assets";
create policy "workspace_read" on public."spot_assets" as permissive for select to "authenticated" using(app_private.is_app_member());
drop policy if exists "workspace_update" on public."spot_assets";
create policy "workspace_update" on public."spot_assets" as permissive for update to "authenticated" using(app_private.can_edit_shared_workspace()) with check(app_private.can_edit_shared_workspace());
drop policy if exists "workspace_delete" on public."spot_positions";
create policy "workspace_delete" on public."spot_positions" as permissive for delete to "authenticated" using(app_private.can_edit_shared_workspace());
drop policy if exists "workspace_insert" on public."spot_positions";
create policy "workspace_insert" on public."spot_positions" as permissive for insert to "authenticated" with check(app_private.can_edit_shared_workspace());
drop policy if exists "workspace_read" on public."spot_positions";
create policy "workspace_read" on public."spot_positions" as permissive for select to "authenticated" using(app_private.is_app_member());
drop policy if exists "workspace_update" on public."spot_positions";
create policy "workspace_update" on public."spot_positions" as permissive for update to "authenticated" using(app_private.can_edit_shared_workspace()) with check(app_private.can_edit_shared_workspace());
drop policy if exists "workspace_delete" on public."spot_price_alerts";
create policy "workspace_delete" on public."spot_price_alerts" as permissive for delete to "authenticated" using(app_private.can_edit_shared_workspace());
drop policy if exists "workspace_insert" on public."spot_price_alerts";
create policy "workspace_insert" on public."spot_price_alerts" as permissive for insert to "authenticated" with check(app_private.can_edit_shared_workspace());
drop policy if exists "workspace_read" on public."spot_price_alerts";
create policy "workspace_read" on public."spot_price_alerts" as permissive for select to "authenticated" using(app_private.is_app_member());
drop policy if exists "workspace_update" on public."spot_price_alerts";
create policy "workspace_update" on public."spot_price_alerts" as permissive for update to "authenticated" using(app_private.can_edit_shared_workspace()) with check(app_private.can_edit_shared_workspace());
drop policy if exists "workspace_delete" on public."spot_transactions";
create policy "workspace_delete" on public."spot_transactions" as permissive for delete to "authenticated" using(app_private.can_edit_shared_workspace());
drop policy if exists "workspace_insert" on public."spot_transactions";
create policy "workspace_insert" on public."spot_transactions" as permissive for insert to "authenticated" with check(app_private.can_edit_shared_workspace());
drop policy if exists "workspace_read" on public."spot_transactions";
create policy "workspace_read" on public."spot_transactions" as permissive for select to "authenticated" using(app_private.is_app_member());
drop policy if exists "workspace_update" on public."spot_transactions";
create policy "workspace_update" on public."spot_transactions" as permissive for update to "authenticated" using(app_private.can_edit_shared_workspace()) with check(app_private.can_edit_shared_workspace());
drop trigger if exists "workspace_touch" on public."economic_recurring_schedules";
CREATE TRIGGER workspace_touch BEFORE UPDATE ON public.economic_recurring_schedules FOR EACH ROW EXECUTE FUNCTION tg_workspace_touch();
drop trigger if exists "workspace_touch" on public."module_template_defaults";
CREATE TRIGGER workspace_touch BEFORE UPDATE ON public.module_template_defaults FOR EACH ROW EXECUTE FUNCTION tg_workspace_touch();
drop trigger if exists "workspace_touch" on public."scenario_assets";
CREATE TRIGGER workspace_touch BEFORE UPDATE ON public.scenario_assets FOR EACH ROW EXECUTE FUNCTION tg_workspace_touch();
drop trigger if exists "workspace_touch" on public."scenario_variants";
CREATE TRIGGER workspace_touch BEFORE UPDATE ON public.scenario_variants FOR EACH ROW EXECUTE FUNCTION tg_workspace_touch();
drop trigger if exists "workspace_touch" on public."scenario_watch_links";
CREATE TRIGGER workspace_touch BEFORE UPDATE ON public.scenario_watch_links FOR EACH ROW EXECUTE FUNCTION tg_workspace_touch();
drop trigger if exists "workspace_touch" on public."scenarios";
CREATE TRIGGER workspace_touch BEFORE UPDATE ON public.scenarios FOR EACH ROW EXECUTE FUNCTION tg_workspace_touch();
drop trigger if exists "workspace_touch" on public."spot_accounts";
CREATE TRIGGER workspace_touch BEFORE UPDATE ON public.spot_accounts FOR EACH ROW EXECUTE FUNCTION tg_workspace_touch();
drop trigger if exists "workspace_touch" on public."spot_assets";
CREATE TRIGGER workspace_touch BEFORE UPDATE ON public.spot_assets FOR EACH ROW EXECUTE FUNCTION tg_workspace_touch();
drop trigger if exists "workspace_touch" on public."spot_positions";
CREATE TRIGGER workspace_touch BEFORE UPDATE ON public.spot_positions FOR EACH ROW EXECUTE FUNCTION tg_workspace_touch();
drop trigger if exists "workspace_touch" on public."spot_price_alerts";
CREATE TRIGGER workspace_touch BEFORE UPDATE ON public.spot_price_alerts FOR EACH ROW EXECUTE FUNCTION tg_workspace_touch();
drop trigger if exists "workspace_touch" on public."spot_transactions";
CREATE TRIGGER workspace_touch BEFORE UPDATE ON public.spot_transactions FOR EACH ROW EXECUTE FUNCTION tg_workspace_touch();
CREATE INDEX IF NOT EXISTS idx_economic_recurring_ack_user ON public.economic_recurring_acknowledgements USING btree (user_id, acknowledged_at DESC);
CREATE INDEX IF NOT EXISTS idx_economic_recurring_active ON public.economic_recurring_schedules USING btree (active, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_scenario_variants_scenario ON public.scenario_variants USING btree (scenario_id, "position");
CREATE INDEX IF NOT EXISTS idx_scenario_watch_links_asset ON public.scenario_watch_links USING btree (asset_id, "position");
CREATE INDEX IF NOT EXISTS idx_scenarios_asset_status ON public.scenarios USING btree (asset_id, status, pinned DESC, "position", created_at DESC);
CREATE INDEX IF NOT EXISTS idx_spot_positions_asset_status ON public.spot_positions USING btree (asset_id, status, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_spot_alerts_active ON public.spot_price_alerts USING btree (active, position_id);
CREATE INDEX IF NOT EXISTS idx_spot_transactions_position_time ON public.spot_transactions USING btree (position_id, occurred_at);
