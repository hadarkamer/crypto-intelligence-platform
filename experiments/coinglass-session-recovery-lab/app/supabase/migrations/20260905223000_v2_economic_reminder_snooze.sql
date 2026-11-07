-- Snooze belongs to the person responding to a reminder. Neither shared event
-- times nor personal/default reminder offsets are changed by this operation.
create table if not exists public.v2_economic_reminder_snoozes (
  reminder_id uuid not null references public.economic_event_reminders(id) on delete cascade,
  user_id uuid not null default auth.uid() references auth.users(id) on delete cascade,
  snoozed_until timestamptz not null,
  original_due_at timestamptz not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key(reminder_id,user_id)
);
alter table public.v2_economic_reminder_snoozes enable row level security;
revoke all on public.v2_economic_reminder_snoozes from public,anon,authenticated;
grant select on public.v2_economic_reminder_snoozes to authenticated;
drop policy if exists v2_economic_snoozes_read on public.v2_economic_reminder_snoozes;
create policy v2_economic_snoozes_read on public.v2_economic_reminder_snoozes
for select to authenticated using(user_id=auth.uid() and exists(
  select 1 from public.economic_event_reminders r where r.id=reminder_id
));

create or replace function public.v2_reset_economic_reminder_snoozes()
returns trigger language plpgsql security definer set search_path=public,pg_temp as $$
begin
  if new.due_at is distinct from old.due_at or new.event_id is distinct from old.event_id then
    delete from public.v2_economic_reminder_snoozes where reminder_id=new.id;
  end if;
  return new;
end $$;
drop trigger if exists v2_economic_reminder_snooze_reset on public.economic_event_reminders;
create trigger v2_economic_reminder_snooze_reset after update on public.economic_event_reminders
for each row execute function public.v2_reset_economic_reminder_snoozes();
revoke all on function public.v2_reset_economic_reminder_snoozes() from public,anon,authenticated;

create or replace function public.v2_snooze_economic_reminder(p_reminder_id uuid,p_minutes integer default 10)
returns timestamptz language plpgsql security definer set search_path=public,pg_temp as $$
declare r public.economic_event_reminders%rowtype; due timestamptz;
begin
  if p_minutes is null or p_minutes not between 1 and 15 then
    raise exception 'snooze must be between 1 and 15 minutes' using errcode='23514';
  end if;
  select * into r from public.economic_event_reminders where id=p_reminder_id for update;
  if auth.uid() is null or not found
     or not public.v2_can_configure_reminders(r.event_id,null)
     or not (r.user_id is not distinct from auth.uid() or (r.user_id is null and not public.v2_has_personal_reminder_choice(r.event_id))) then
    raise exception 'reminder access required' using errcode='42501';
  end if;
  if r.due_at > now() then
    raise exception 'reminder is not due' using errcode='23514';
  end if;
  if exists(select 1 from public.economic_reminder_acknowledgements a
    where a.reminder_id=r.id and a.user_id=auth.uid()) then
    raise exception 'reminder already acknowledged' using errcode='23514';
  end if;
  due := now()+make_interval(mins=>p_minutes);
  insert into public.v2_economic_reminder_snoozes(reminder_id,user_id,snoozed_until,original_due_at)
  values(r.id,auth.uid(),due,r.due_at)
  on conflict(reminder_id,user_id) do update set snoozed_until=excluded.snoozed_until,
    original_due_at=excluded.original_due_at,updated_at=now();
  return due;
end $$;

create or replace function public.v2_acknowledge_economic_reminder(p_reminder_id uuid)
returns void language plpgsql security definer set search_path=public,pg_temp as $$
declare r public.economic_event_reminders%rowtype;
begin
  select * into r from public.economic_event_reminders where id=p_reminder_id for update;
  if auth.uid() is null or not found
     or not public.v2_can_configure_reminders(r.event_id,null)
     or not (r.user_id is not distinct from auth.uid() or (r.user_id is null and not public.v2_has_personal_reminder_choice(r.event_id))) then
    raise exception 'reminder access required' using errcode='42501';
  end if;
  insert into public.economic_reminder_acknowledgements(reminder_id,user_id)
  values(r.id,auth.uid()) on conflict(reminder_id,user_id) do nothing;
  delete from public.v2_economic_reminder_snoozes where reminder_id=r.id and user_id=auth.uid();
end $$;

-- The existing RLS on events, reminders, acknowledgements and snoozes applies.
-- A valid snooze is delivered even after its event starts. Changing the event
-- time invalidates a snooze through original_due_at, without rewriting history.
create or replace function public.v2_economic_reminder_state()
returns jsonb language sql stable security invoker set search_path=public,pg_temp as $$
  with pending as (
    select r.id,coalesce(s.snoozed_until,r.due_at) as effective_due_at,
      to_jsonb(r)||jsonb_build_object(
        'due_at',coalesce(s.snoozed_until,r.due_at),
        'event',to_jsonb(e)||jsonb_build_object('reminders','[]'::jsonb)
      ) as reminder
    from public.economic_event_reminders r
    join public.economic_events e on e.id=r.event_id
    left join public.v2_economic_reminder_snoozes s
      on s.reminder_id=r.id and s.user_id=auth.uid() and s.original_due_at=r.due_at
    where auth.uid() is not null and (e.event_at>now() or s.reminder_id is not null)
      and not exists(select 1 from public.economic_reminder_acknowledgements a
        where a.reminder_id=r.id and a.user_id=auth.uid())
  )
  select jsonb_build_object(
    'due',coalesce(jsonb_agg(reminder order by effective_due_at,id)
      filter(where effective_due_at<=now()),'[]'::jsonb),
    'next_due_at',min(effective_due_at) filter(where effective_due_at>now())
  ) from pending;
$$;

revoke all on function public.v2_snooze_economic_reminder(uuid,integer),
  public.v2_acknowledge_economic_reminder(uuid),public.v2_economic_reminder_state() from public,anon;
grant execute on function public.v2_snooze_economic_reminder(uuid,integer),
  public.v2_acknowledge_economic_reminder(uuid),public.v2_economic_reminder_state() to authenticated;
