create table if not exists public.v2_liquidity_ai_runs (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
  session_id uuid not null references public.v2_collection_sessions(id) on delete cascade,
  field_id uuid not null,
  timeframe text not null,
  source_url text not null,
  provider text not null,
  model text not null,
  captured_at timestamptz not null,
  observed_price numeric not null,
  screenshot text,
  analysis jsonb not null default '{}'::jsonb,
  previous_value jsonb not null default '{}'::jsonb,
  usage jsonb,
  created_by uuid not null default auth.uid(),
  created_at timestamptz not null default now()
);

create index if not exists v2_liquidity_ai_runs_field_idx
  on public.v2_liquidity_ai_runs (session_id, field_id, created_at desc);

grant select, insert on public.v2_liquidity_ai_runs to authenticated;
grant all on public.v2_liquidity_ai_runs to service_role;

alter table public.v2_liquidity_ai_runs enable row level security;

drop policy if exists "members read liquidity ai runs" on public.v2_liquidity_ai_runs;
create policy "members read liquidity ai runs" on public.v2_liquidity_ai_runs
  for select to authenticated
  using (public.v2_is_workspace_member(workspace_id));

drop policy if exists "editors insert liquidity ai runs" on public.v2_liquidity_ai_runs;
create policy "editors insert liquidity ai runs" on public.v2_liquidity_ai_runs
  for insert to authenticated
  with check (public.v2_can_edit_workspace(workspace_id) and created_by = auth.uid());