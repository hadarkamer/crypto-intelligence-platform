-- Missed calendar events never produce a late alert, including snoozed ones.
-- Preserve all event/reminder/read history and the existing per-user RLS and
-- one-popup-per-event coalescing. Snoozes reaching the event start are silent.
begin;

CREATE OR REPLACE FUNCTION public.v2_economic_reminder_state()
 RETURNS jsonb
 LANGUAGE sql
 STABLE
 SET search_path TO 'public', 'pg_temp'
AS $function$
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
    where auth.uid() is not null and e.event_at>now()
      and coalesce(s.snoozed_until,r.due_at)<e.event_at
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
$function$;

-- CREATE OR REPLACE retains the released authenticated/service-role grants.
-- No acknowledgement, snooze, reminder or event data is changed.
commit;

