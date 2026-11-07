-- Returning after several offsets elapsed presents one reminder per event.
-- Reads stay SECURITY INVOKER and do not acknowledge anything merely fetched.
-- An explicit acknowledgement/snooze consumes only the due offsets represented
-- by that popup, for auth.uid(); future offsets and future snoozes survive.
begin;

create or replace function app_private.lock_economic_reminder_due_group(p_reminder_id uuid)
returns uuid[] language plpgsql security definer
set search_path=pg_catalog,public,app_private,pg_temp as $$
declare
  target_event uuid;
  r public.economic_event_reminders%rowtype;
  cutoff timestamptz;
  result_ids uuid[];
begin
  if auth.uid() is null then
    raise exception 'reminder access required' using errcode='42501';
  end if;
  select event_id into target_event from public.economic_event_reminders where id=p_reminder_id;
  if not found or not public.v2_can_configure_reminders(target_event,null) then
    raise exception 'reminder access required' using errcode='42501';
  end if;
  -- Match event deletion/rescheduling order. Different tabs responding to
  -- different offsets of this event acquire the same lock before reminder rows.
  perform 1 from public.economic_events where id=target_event for update;
  if not found then
    raise exception 'reminder access required' using errcode='42501';
  end if;
  perform 1 from public.economic_event_reminders candidate
    where candidate.event_id=target_event
      and (candidate.user_id=auth.uid() or (candidate.user_id is null
        and not public.v2_has_personal_reminder_choice(target_event)))
    order by candidate.id for update;
  select * into r from public.economic_event_reminders
    where id=p_reminder_id and event_id=target_event;
  if not found or not public.v2_can_configure_reminders(r.event_id,null)
     or not (r.user_id is not distinct from auth.uid() or (r.user_id is null
       and not public.v2_has_personal_reminder_choice(r.event_id))) then
    raise exception 'reminder access required' using errcode='42501';
  end if;
  -- Repeating an old response must not consume a later reminder of this event.
  if exists(select 1 from public.economic_reminder_acknowledgements a
    where a.reminder_id=r.id and a.user_id=auth.uid()) then
    return array[r.id];
  end if;
  select coalesce(s.snoozed_until,r.due_at) into cutoff
    from (select 1) anchor left join public.v2_economic_reminder_snoozes s
      on s.reminder_id=r.id and s.user_id=auth.uid() and s.original_due_at=r.due_at;
  if cutoff>now() then return array[r.id]; end if;
  select array_agg(candidate.id order by candidate.id) into result_ids
    from public.economic_event_reminders candidate
    left join public.v2_economic_reminder_snoozes s
      on s.reminder_id=candidate.id and s.user_id=auth.uid()
        and s.original_due_at=candidate.due_at
    where candidate.event_id=r.event_id
      and (candidate.user_id=auth.uid() or (candidate.user_id is null
        and not public.v2_has_personal_reminder_choice(candidate.event_id)))
      and coalesce(s.snoozed_until,candidate.due_at)<=cutoff
      and not exists(select 1 from public.economic_reminder_acknowledgements a
        where a.reminder_id=candidate.id and a.user_id=auth.uid());
  return coalesce(result_ids,array[r.id]);
end $$;
revoke all on function app_private.lock_economic_reminder_due_group(uuid)
  from public,anon,authenticated;

create or replace function public.v2_acknowledge_economic_reminder(p_reminder_id uuid)
returns void language plpgsql security definer set search_path=public,pg_temp as $$
declare reminder_ids uuid[];
begin
  reminder_ids:=app_private.lock_economic_reminder_due_group(p_reminder_id);
  insert into public.economic_reminder_acknowledgements(reminder_id,user_id)
  select id,auth.uid() from unnest(reminder_ids) id
  on conflict(reminder_id,user_id) do nothing;
  delete from public.v2_economic_reminder_snoozes
    where reminder_id=any(reminder_ids) and user_id=auth.uid();
end $$;

create or replace function public.v2_snooze_economic_reminder(p_reminder_id uuid,p_minutes integer default 10)
returns timestamptz language plpgsql security definer set search_path=public,pg_temp as $$
declare r public.economic_event_reminders%rowtype; due timestamptz; reminder_ids uuid[];
begin
  if p_minutes is null or p_minutes not between 1 and 15 then
    raise exception 'snooze must be between 1 and 15 minutes' using errcode='23514';
  end if;
  reminder_ids:=app_private.lock_economic_reminder_due_group(p_reminder_id);
  select * into r from public.economic_event_reminders where id=p_reminder_id;
  if r.due_at>now() then
    raise exception 'reminder is not due' using errcode='23514';
  end if;
  if exists(select 1 from public.economic_reminder_acknowledgements a
    where a.reminder_id=r.id and a.user_id=auth.uid()) then
    raise exception 'reminder already acknowledged' using errcode='23514';
  end if;
  insert into public.economic_reminder_acknowledgements(reminder_id,user_id)
  select id,auth.uid() from unnest(reminder_ids) id where id<>r.id
  on conflict(reminder_id,user_id) do nothing;
  delete from public.v2_economic_reminder_snoozes
    where reminder_id=any(reminder_ids) and reminder_id<>r.id and user_id=auth.uid();
  due:=now()+make_interval(mins=>p_minutes);
  insert into public.v2_economic_reminder_snoozes(reminder_id,user_id,snoozed_until,original_due_at)
  values(r.id,auth.uid(),due,r.due_at)
  on conflict(reminder_id,user_id) do update set snoozed_until=excluded.snoozed_until,
    original_due_at=excluded.original_due_at,updated_at=now();
  return due;
end $$;

create or replace function public.v2_economic_reminder_state()
returns jsonb language sql stable security invoker set search_path=public,pg_temp as $$
  with pending as (
    select r.id,r.event_id,coalesce(s.snoozed_until,r.due_at) as effective_due_at,
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
  ), due_events as (
    select distinct on(event_id) id,effective_due_at,reminder
    from pending where effective_due_at<=now()
    order by event_id,effective_due_at desc,id
  )
  select jsonb_build_object(
    'due',coalesce((select jsonb_agg(reminder order by effective_due_at,id)
      from due_events),'[]'::jsonb),
    'next_due_at',(select min(effective_due_at) from pending where effective_due_at>now())
  );
$$;
revoke all on function public.v2_snooze_economic_reminder(uuid,integer),
  public.v2_acknowledge_economic_reminder(uuid),public.v2_economic_reminder_state() from public,anon;
grant execute on function public.v2_snooze_economic_reminder(uuid,integer),
  public.v2_acknowledge_economic_reminder(uuid),public.v2_economic_reminder_state() to authenticated;

commit;
