-- Restore reminder-state reads without granting access to other users' reminders.
-- Keep v2_economic_reminder_state SECURITY INVOKER and preserve all existing RLS policies.
CREATE OR REPLACE FUNCTION app_private.can_access_reminder(_reminder_id uuid)
RETURNS boolean
LANGUAGE sql
STABLE SECURITY DEFINER
SET search_path TO 'pg_catalog', 'public', 'app_private', 'pg_temp'
AS $function$
  SELECT auth.uid() IS NOT NULL AND EXISTS (
    SELECT 1
    FROM public.economic_event_reminders r
    JOIN public.economic_events e ON e.id = r.event_id
    WHERE r.id = _reminder_id
      -- The NULL-workspace case uses the existing shared-calendar membership rule.
      AND public.v2_calendar_can_read(e.workspace_id)
      AND (
        r.user_id = auth.uid()
        OR (r.user_id IS NULL
            AND NOT public.v2_has_personal_reminder_choice(r.event_id))
      )
  );
$function$;
REVOKE EXECUTE ON FUNCTION app_private.can_access_reminder(uuid) FROM PUBLIC, anon;
GRANT EXECUTE ON FUNCTION app_private.can_access_reminder(uuid) TO authenticated;
