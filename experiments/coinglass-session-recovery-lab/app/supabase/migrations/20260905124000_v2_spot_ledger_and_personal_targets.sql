-- Existing quantity is retained as the opening holding before recorded transactions.
alter table public.v2_spot_assets add column if not exists opening_unit_cost numeric;
alter table public.v2_spot_assets add column if not exists template_id uuid references public.v2_templates(id) on delete restrict;

-- Recover an unambiguous existing template association without changing saved answers.
update public.v2_spot_assets a set template_id = chosen.template_id
from (
  select v.asset_id, (array_agg(distinct s.template_id))[1] as template_id
  from public.v2_spot_values v join public.v2_template_fields f on f.id = v.field_id
  join public.v2_template_sections s on s.id = f.section_id
  group by v.asset_id having count(distinct s.template_id) = 1
) chosen where a.id = chosen.asset_id and a.template_id is null;

create or replace function public.v2_validate_spot_asset()
returns trigger language plpgsql set search_path = public, pg_temp as $$
begin
  if new.quantity < 0 or new.quantity::text in ('NaN','Infinity','-Infinity')
    or (new.opening_unit_cost is not null and (new.opening_unit_cost < 0 or new.opening_unit_cost::text in ('NaN','Infinity','-Infinity'))) then
    raise exception 'spot opening quantity and cost must be finite and nonnegative' using errcode = '23514';
  end if;
  if new.template_id is not null and not exists(select 1 from public.v2_templates where id = new.template_id and workspace_id = new.workspace_id and kind = 'spot') then
    raise exception 'spot template has wrong workspace or kind' using errcode = '23514';
  end if;
  if tg_op = 'UPDATE' and new.template_id is distinct from old.template_id
    and exists(select 1 from public.v2_spot_values where asset_id = old.id) then
    raise exception 'cannot change spot template after answers exist' using errcode = '23514';
  end if;
  return new;
end $$;
drop trigger if exists v2_spot_asset_runtime_guard on public.v2_spot_assets;
create trigger v2_spot_asset_runtime_guard before insert or update on public.v2_spot_assets for each row execute function public.v2_validate_spot_asset();

create or replace function public.v2_validate_spot_value_template()
returns trigger language plpgsql set search_path = public, pg_temp as $$
begin
  if not exists(select 1 from public.v2_spot_assets a join public.v2_template_sections s on s.template_id = a.template_id
    join public.v2_template_fields f on f.section_id = s.id where a.id = new.asset_id and f.id = new.field_id) then
    raise exception 'spot answer must belong to the asset template' using errcode = '23514';
  end if;
  return new;
end $$;
drop trigger if exists v2_spot_value_template_guard on public.v2_spot_values;
create trigger v2_spot_value_template_guard before insert or update on public.v2_spot_values for each row execute function public.v2_validate_spot_value_template();

-- Serialize edits to an asset ledger before checking its full chronological path.
create or replace function public.v2_lock_spot_ledger()
returns trigger language plpgsql set search_path = public, pg_temp as $$
declare target_id uuid;
begin
  if tg_op = 'UPDATE' and new.asset_id is distinct from old.asset_id then
    raise exception 'a recorded spot transaction cannot move between assets' using errcode = '23514';
  end if;
  if tg_op <> 'DELETE' and (new.quantity::text in ('NaN','Infinity','-Infinity') or new.price::text in ('NaN','Infinity','-Infinity') or new.fee::text in ('NaN','Infinity','-Infinity')) then
    raise exception 'spot transaction numbers must be finite' using errcode = '23514';
  end if;
  target_id := case when tg_op = 'DELETE' then old.asset_id else new.asset_id end;
  perform id from public.v2_spot_assets where id = target_id for update;
  if tg_op = 'DELETE' then return old; end if;
  new.lot_key := nullif(btrim(new.lot_key), '');
  return new;
end $$;
drop trigger if exists v2_spot_ledger_lock on public.v2_spot_transactions;
create trigger v2_spot_ledger_lock before insert or update or delete on public.v2_spot_transactions for each row execute function public.v2_lock_spot_ledger();

create or replace function public.v2_check_spot_ledger()
returns trigger language plpgsql set search_path = public, pg_temp as $$
declare target_id uuid; opening_quantity numeric;
begin
  if tg_table_name = 'v2_spot_assets' then target_id := new.id;
  elsif tg_op = 'DELETE' then target_id := old.asset_id;
  else target_id := new.asset_id; end if;
  select quantity into opening_quantity from public.v2_spot_assets where id = target_id;
  if not found then return null; end if;
  if exists (
    select 1 from (
      select coalesce(nullif(btrim(lot_key),''),'') as cluster,
        sum(case when side = 'buy' then quantity else -quantity end) over (
          partition by coalesce(nullif(btrim(lot_key),''),'') order by occurred_at, created_at, id rows unbounded preceding
        ) as balance
      from public.v2_spot_transactions where asset_id = target_id
    ) ledger where balance + case when cluster = '' then opening_quantity else 0 end < 0
  ) then
    raise exception 'spot sale exceeds the available quantity in its cluster at that time' using errcode = '23514';
  end if;
  return null;
end $$;
drop trigger if exists v2_spot_ledger_balance_guard on public.v2_spot_transactions;
create trigger v2_spot_ledger_balance_guard after insert or update or delete on public.v2_spot_transactions for each row execute function public.v2_check_spot_ledger();
drop trigger if exists v2_spot_opening_balance_guard on public.v2_spot_assets;
create trigger v2_spot_opening_balance_guard after update of quantity on public.v2_spot_assets for each row execute function public.v2_check_spot_ledger();

create table if not exists public.v2_spot_target_acknowledgements (
  target_id uuid not null references public.v2_spot_targets(id) on delete cascade,
  user_id uuid not null default auth.uid() references auth.users(id) on delete cascade,
  acknowledged_price numeric check (acknowledged_price >= 0 and acknowledged_price::text not in ('NaN','Infinity','-Infinity')),
  acknowledged_at timestamptz not null default now(),
  primary key (target_id, user_id)
);
alter table public.v2_spot_target_acknowledgements enable row level security;
drop policy if exists v2_spot_ack_own_read on public.v2_spot_target_acknowledgements;
create policy v2_spot_ack_own_read on public.v2_spot_target_acknowledgements for select to authenticated
  using (user_id = auth.uid() and exists(select 1 from public.v2_spot_targets t where t.id = target_id and public.v2_is_workspace_member(t.workspace_id)));
drop policy if exists v2_spot_ack_own_write on public.v2_spot_target_acknowledgements;
create policy v2_spot_ack_own_write on public.v2_spot_target_acknowledgements for insert to authenticated
  with check (user_id = auth.uid() and exists(select 1 from public.v2_spot_targets t where t.id = target_id and public.v2_is_workspace_member(t.workspace_id)));
drop policy if exists v2_spot_ack_own_update on public.v2_spot_target_acknowledgements;
create policy v2_spot_ack_own_update on public.v2_spot_target_acknowledgements for update to authenticated
  using (user_id = auth.uid()) with check (user_id = auth.uid() and exists(select 1 from public.v2_spot_targets t where t.id = target_id and public.v2_is_workspace_member(t.workspace_id)));
revoke all on public.v2_spot_target_acknowledgements from anon;
grant select,insert,update on public.v2_spot_target_acknowledgements to authenticated;

-- Retain prior acknowledgements without exposing them to other users.
insert into public.v2_spot_target_acknowledgements(target_id,user_id)
select t.id,u.id from public.v2_spot_targets t
join auth.users u on t.acknowledged_by @> jsonb_build_array(u.id::text)
on conflict do nothing;

create or replace function public.v2_reset_spot_target_acknowledgements()
returns trigger language plpgsql security definer set search_path = public, pg_temp as $$
begin
  if new.direction is distinct from old.direction or new.target_price is distinct from old.target_price
    or (new.active and not old.active) then
    delete from public.v2_spot_target_acknowledgements where target_id = new.id;
    new.acknowledged_by := '[]'::jsonb;
  end if;
  return new;
end $$;
drop trigger if exists v2_spot_target_ack_reset on public.v2_spot_targets;
create trigger v2_spot_target_ack_reset before update on public.v2_spot_targets for each row execute function public.v2_reset_spot_target_acknowledgements();

revoke all on function public.v2_validate_spot_asset(), public.v2_validate_spot_value_template(), public.v2_lock_spot_ledger(), public.v2_check_spot_ledger(), public.v2_reset_spot_target_acknowledgements() from public, anon, authenticated;
