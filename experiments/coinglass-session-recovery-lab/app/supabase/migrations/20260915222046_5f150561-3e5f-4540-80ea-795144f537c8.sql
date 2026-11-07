-- 1. Narrower reader check for shared financial (spot) data
CREATE OR REPLACE FUNCTION app_private.is_shared_workspace_reader()
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path TO 'public', 'app_private'
AS $function$
  SELECT auth.uid() IS NOT NULL AND (
    EXISTS (
      SELECT 1 FROM public.project_members pm
      WHERE pm.user_id = auth.uid()
    )
    OR EXISTS (
      SELECT 1 FROM public.user_roles ur
      WHERE ur.user_id = auth.uid() AND ur.role = 'admin'
    )
  );
$function$;

DROP POLICY IF EXISTS workspace_read ON public.spot_positions;
CREATE POLICY workspace_read ON public.spot_positions
  FOR SELECT TO authenticated
  USING (app_private.is_shared_workspace_reader());

DROP POLICY IF EXISTS workspace_read ON public.spot_transactions;
CREATE POLICY workspace_read ON public.spot_transactions
  FOR SELECT TO authenticated
  USING (app_private.is_shared_workspace_reader());

DROP POLICY IF EXISTS workspace_read ON public.spot_price_alerts;
CREATE POLICY workspace_read ON public.spot_price_alerts
  FOR SELECT TO authenticated
  USING (app_private.is_shared_workspace_reader());

DROP POLICY IF EXISTS workspace_read ON public.spot_accounts;
CREATE POLICY workspace_read ON public.spot_accounts
  FOR SELECT TO authenticated
  USING (app_private.is_shared_workspace_reader());

DROP POLICY IF EXISTS workspace_read ON public.spot_assets;
CREATE POLICY workspace_read ON public.spot_assets
  FOR SELECT TO authenticated
  USING (app_private.is_shared_workspace_reader());

DROP POLICY IF EXISTS spot_ack_read_own ON public.spot_alert_acknowledgements;
CREATE POLICY spot_ack_read_own ON public.spot_alert_acknowledgements
  FOR SELECT TO authenticated
  USING (user_id = auth.uid() AND app_private.is_shared_workspace_reader());

-- 2. Economic event reminders: explicit authenticated scoping for shared rows
DROP POLICY IF EXISTS "economic reminders read" ON public.economic_event_reminders;
CREATE POLICY "economic reminders read" ON public.economic_event_reminders
  FOR SELECT TO authenticated
  USING (
    auth.uid() IS NOT NULL
    AND (
      user_id = auth.uid()
      OR (user_id IS NULL AND NOT public.v2_has_personal_reminder_choice(event_id))
    )
    AND EXISTS (
      SELECT 1 FROM public.economic_events e
      WHERE e.id = economic_event_reminders.event_id
        AND public.v2_calendar_can_read(e.workspace_id)
    )
  );

-- 3. Workspace membership inserts must specify a non-owner role
DROP POLICY IF EXISTS v2_workspace_members_insert ON public.v2_workspace_members;
CREATE POLICY v2_workspace_members_insert ON public.v2_workspace_members
  FOR INSERT TO authenticated
  WITH CHECK (
    public.v2_is_workspace_owner(workspace_id)
    AND role IN ('editor', 'viewer')
    AND user_id <> auth.uid()
  );

DROP POLICY IF EXISTS v2_workspace_members_update ON public.v2_workspace_members;
CREATE POLICY v2_workspace_members_update ON public.v2_workspace_members
  FOR UPDATE TO authenticated
  USING (public.v2_is_workspace_owner(workspace_id))
  WITH CHECK (
    public.v2_is_workspace_owner(workspace_id)
    AND role IN ('editor', 'viewer')
    AND user_id <> auth.uid()
  );
