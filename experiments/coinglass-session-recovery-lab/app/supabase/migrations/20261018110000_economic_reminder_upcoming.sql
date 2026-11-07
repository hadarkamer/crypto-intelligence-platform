-- List the signed-in user's real next alerts alongside each due event.
-- Reuse the released invoker/RLS pending set, including acknowledgements,
-- personal settings, snooze timestamps and the strict event-start cutoff.
begin;

create or replace function public.v2_economic_reminder_state()
returns jsonb language sql stable set search_path=public,pg_temp as $function$
  with pending as (
    select r.id,r.event_id,r.offset_minutes,
      coalesce(s.snoozed_until,r.due_at) as effective_due_at,
      s.snoozed_until is not null as snoozed,
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
    select distinct on(event_id) id,event_id,effective_due_at,reminder
    from pending where effective_due_at<=now()
    order by event_id,effective_due_at desc,id
  )
  select jsonb_build_object(
    'due',coalesce((select jsonb_agg(
      d.reminder||jsonb_build_object('upcoming_reminders',coalesce((
        select jsonb_agg(jsonb_build_object(
          'id',p.id,'event_id',p.event_id,'offset_minutes',p.offset_minutes,
          'due_at',p.effective_due_at,'snoozed',p.snoozed
        ) order by p.effective_due_at,p.id)
        from pending p where p.event_id=d.event_id and p.effective_due_at>now()
      ),'[]'::jsonb)) order by d.effective_due_at,d.id
    ) from due_events d),'[]'::jsonb),
    'next_due_at',(select min(effective_due_at) from pending where effective_due_at>now())
  );
$function$;

-- CREATE OR REPLACE preserves the existing authenticated grants and invoker mode.
-- No events, reminder preferences, acknowledgements or snoozes are changed.
commit;
