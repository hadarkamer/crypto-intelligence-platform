
-- 1. Helper: verify the caller may access the workspace behind a reminder
CREATE OR REPLACE FUNCTION app_private.can_access_reminder(_reminder_id uuid)
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public, app_private
AS $$
  SELECT EXISTS (
    SELECT 1
    FROM public.economic_event_reminders r
    JOIN public.economic_events e ON e.id = r.event_id
    WHERE r.id = _reminder_id
      AND (e.workspace_id IS NULL OR public.v2_calendar_can_read(e.workspace_id))
  )
$$;

REVOKE ALL ON FUNCTION app_private.can_access_reminder(uuid) FROM PUBLIC, anon, authenticated;

-- 2. economic_reminder_acknowledgements: add workspace verification
DROP POLICY IF EXISTS "economic ack read own" ON public.economic_reminder_acknowledgements;
DROP POLICY IF EXISTS "economic ack insert own" ON public.economic_reminder_acknowledgements;
DROP POLICY IF EXISTS "economic ack update own" ON public.economic_reminder_acknowledgements;
DROP POLICY IF EXISTS "economic ack delete own" ON public.economic_reminder_acknowledgements;

CREATE POLICY "economic ack read own" ON public.economic_reminder_acknowledgements
  FOR SELECT TO authenticated
  USING (user_id = auth.uid() AND app_private.can_access_reminder(reminder_id));

CREATE POLICY "economic ack insert own" ON public.economic_reminder_acknowledgements
  FOR INSERT TO authenticated
  WITH CHECK (user_id = auth.uid() AND app_private.can_access_reminder(reminder_id));

CREATE POLICY "economic ack update own" ON public.economic_reminder_acknowledgements
  FOR UPDATE TO authenticated
  USING (user_id = auth.uid() AND app_private.can_access_reminder(reminder_id))
  WITH CHECK (user_id = auth.uid() AND app_private.can_access_reminder(reminder_id));

CREATE POLICY "economic ack delete own" ON public.economic_reminder_acknowledgements
  FOR DELETE TO authenticated
  USING (user_id = auth.uid() AND app_private.can_access_reminder(reminder_id));

-- 3. v2_economic_reminder_snoozes: workspace-verified read + owner-scoped writes
DROP POLICY IF EXISTS "v2_economic_snoozes_read" ON public.v2_economic_reminder_snoozes;
DROP POLICY IF EXISTS "v2_economic_snoozes_insert" ON public.v2_economic_reminder_snoozes;
DROP POLICY IF EXISTS "v2_economic_snoozes_update" ON public.v2_economic_reminder_snoozes;
DROP POLICY IF EXISTS "v2_economic_snoozes_delete" ON public.v2_economic_reminder_snoozes;

CREATE POLICY "v2_economic_snoozes_read" ON public.v2_economic_reminder_snoozes
  FOR SELECT TO authenticated
  USING (user_id = auth.uid() AND app_private.can_access_reminder(reminder_id));

CREATE POLICY "v2_economic_snoozes_insert" ON public.v2_economic_reminder_snoozes
  FOR INSERT TO authenticated
  WITH CHECK (user_id = auth.uid() AND app_private.can_access_reminder(reminder_id));

CREATE POLICY "v2_economic_snoozes_update" ON public.v2_economic_reminder_snoozes
  FOR UPDATE TO authenticated
  USING (user_id = auth.uid() AND app_private.can_access_reminder(reminder_id))
  WITH CHECK (user_id = auth.uid() AND app_private.can_access_reminder(reminder_id));

CREATE POLICY "v2_economic_snoozes_delete" ON public.v2_economic_reminder_snoozes
  FOR DELETE TO authenticated
  USING (user_id = auth.uid() AND app_private.can_access_reminder(reminder_id));

-- 4. profiles: explicit owner-scoped write policies
DROP POLICY IF EXISTS "insert own profile" ON public.profiles;
DROP POLICY IF EXISTS "update own profile" ON public.profiles;

CREATE POLICY "insert own profile" ON public.profiles
  FOR INSERT TO authenticated
  WITH CHECK (id = auth.uid());

CREATE POLICY "update own profile" ON public.profiles
  FOR UPDATE TO authenticated
  USING (id = auth.uid())
  WITH CHECK (id = auth.uid());

-- 5. Revoke direct execution of internal SECURITY DEFINER functions
REVOKE ALL ON FUNCTION public.tg_assign_primary_trade_cycle() FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.clone_version_to_template(uuid, uuid) FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.v2_seed_default_templates(uuid) FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.v2_import_complete_legacy_template(uuid, text) FROM PUBLIC, anon, authenticated;
