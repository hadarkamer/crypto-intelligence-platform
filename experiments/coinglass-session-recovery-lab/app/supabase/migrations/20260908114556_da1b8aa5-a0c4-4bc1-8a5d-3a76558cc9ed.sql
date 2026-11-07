DO $$
BEGIN
  IF to_regclass('public.spot_alert_acknowledgements') IS NOT NULL
     AND NOT EXISTS (
       SELECT 1 FROM pg_policies
       WHERE schemaname='public' AND tablename='spot_alert_acknowledgements' AND cmd='UPDATE'
     ) THEN
    EXECUTE $p$
      CREATE POLICY spot_ack_update_own ON public.spot_alert_acknowledgements
      FOR UPDATE TO authenticated
      USING ((user_id = auth.uid()) AND app_private.is_app_member())
      WITH CHECK ((user_id = auth.uid()) AND app_private.is_app_member())
    $p$;
  END IF;
END $$;

GRANT SELECT, INSERT, UPDATE, DELETE ON public.spot_alert_acknowledgements TO authenticated;
GRANT ALL ON public.spot_alert_acknowledgements TO service_role;