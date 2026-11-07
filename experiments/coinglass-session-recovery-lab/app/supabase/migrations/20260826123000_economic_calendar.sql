-- Shared economic calendar + per-user reminder acknowledgements.
-- Events are global to authenticated app members; acknowledgement state is personal.

CREATE TABLE IF NOT EXISTS public.economic_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name text NOT NULL CHECK (char_length(btrim(name)) > 0),
  event_at timestamptz NOT NULL,
  importance smallint NOT NULL DEFAULT 1 CHECK (importance BETWEEN 1 AND 3),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  created_by uuid NOT NULL DEFAULT auth.uid(),
  updated_by uuid NOT NULL DEFAULT auth.uid()
);

CREATE TABLE IF NOT EXISTS public.economic_event_reminders (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  event_id uuid NOT NULL REFERENCES public.economic_events(id) ON DELETE CASCADE,
  offset_minutes integer NOT NULL CHECK (offset_minutes > 0),
  due_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (event_id, offset_minutes)
);

CREATE TABLE IF NOT EXISTS public.economic_reminder_acknowledgements (
  reminder_id uuid NOT NULL REFERENCES public.economic_event_reminders(id) ON DELETE CASCADE,
  user_id uuid NOT NULL DEFAULT auth.uid(),
  acknowledged_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (reminder_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_economic_events_event_at
  ON public.economic_events(event_at);
CREATE INDEX IF NOT EXISTS idx_economic_reminders_due_at
  ON public.economic_event_reminders(due_at);
CREATE INDEX IF NOT EXISTS idx_economic_reminders_event
  ON public.economic_event_reminders(event_id);
CREATE INDEX IF NOT EXISTS idx_economic_ack_user
  ON public.economic_reminder_acknowledgements(user_id, acknowledged_at);

-- An app member is someone who owns a project or belongs to at least one project.
CREATE OR REPLACE FUNCTION app_private.is_app_member()
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path TO 'public','app_private'
AS $$
  SELECT auth.uid() IS NOT NULL AND (
    EXISTS (SELECT 1 FROM public.projects p WHERE p.user_id = auth.uid())
    OR EXISTS (SELECT 1 FROM public.project_members pm WHERE pm.user_id = auth.uid())
  );
$$;

-- Shared calendar editing is allowed to project owners and users who are editor/owner somewhere.
CREATE OR REPLACE FUNCTION app_private.can_edit_shared_calendar()
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path TO 'public','app_private'
AS $$
  SELECT auth.uid() IS NOT NULL AND (
    EXISTS (SELECT 1 FROM public.projects p WHERE p.user_id = auth.uid())
    OR EXISTS (
      SELECT 1 FROM public.project_members pm
      WHERE pm.user_id = auth.uid() AND pm.role IN ('owner','editor')
    )
  );
$$;

REVOKE ALL ON FUNCTION app_private.is_app_member() FROM PUBLIC, anon;
REVOKE ALL ON FUNCTION app_private.can_edit_shared_calendar() FROM PUBLIC, anon;
GRANT EXECUTE ON FUNCTION app_private.is_app_member() TO authenticated;
GRANT EXECUTE ON FUNCTION app_private.can_edit_shared_calendar() TO authenticated;

-- Keep event metadata authoritative on the server.
CREATE OR REPLACE FUNCTION public.tg_touch_economic_event()
RETURNS trigger
LANGUAGE plpgsql
SET search_path TO 'public'
AS $$
BEGIN
  NEW.updated_at := now();
  NEW.updated_by := COALESCE(auth.uid(), NEW.updated_by);
  RETURN NEW;
END;
$$;

-- Reminder due_at is derived from the parent event time and its offset.
CREATE OR REPLACE FUNCTION public.tg_set_economic_reminder_due()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $$
DECLARE
  _event_at timestamptz;
BEGIN
  SELECT event_at INTO _event_at FROM public.economic_events WHERE id = NEW.event_id;
  IF _event_at IS NULL THEN
    RAISE EXCEPTION 'economic event not found';
  END IF;
  NEW.due_at := _event_at - make_interval(mins => NEW.offset_minutes);
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION public.tg_reschedule_economic_reminders()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $$
BEGIN
  IF NEW.event_at IS DISTINCT FROM OLD.event_at THEN
    UPDATE public.economic_event_reminders
    SET due_at = NEW.event_at - make_interval(mins => offset_minutes),
        updated_at = now()
    WHERE event_id = NEW.id;
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS t_economic_events_touch ON public.economic_events;
CREATE TRIGGER t_economic_events_touch
BEFORE UPDATE ON public.economic_events
FOR EACH ROW EXECUTE FUNCTION public.tg_touch_economic_event();

DROP TRIGGER IF EXISTS t_economic_reminder_due ON public.economic_event_reminders;
CREATE TRIGGER t_economic_reminder_due
BEFORE INSERT OR UPDATE OF event_id, offset_minutes ON public.economic_event_reminders
FOR EACH ROW EXECUTE FUNCTION public.tg_set_economic_reminder_due();

DROP TRIGGER IF EXISTS t_economic_event_reschedule ON public.economic_events;
CREATE TRIGGER t_economic_event_reschedule
AFTER UPDATE OF event_at ON public.economic_events
FOR EACH ROW EXECUTE FUNCTION public.tg_reschedule_economic_reminders();

REVOKE ALL ON FUNCTION public.tg_touch_economic_event() FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.tg_set_economic_reminder_due() FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.tg_reschedule_economic_reminders() FROM PUBLIC, anon, authenticated;

ALTER TABLE public.economic_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.economic_event_reminders ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.economic_reminder_acknowledgements ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "economic events read" ON public.economic_events;
CREATE POLICY "economic events read" ON public.economic_events
FOR SELECT TO authenticated
USING (app_private.is_app_member());

DROP POLICY IF EXISTS "economic events insert" ON public.economic_events;
CREATE POLICY "economic events insert" ON public.economic_events
FOR INSERT TO authenticated
WITH CHECK (app_private.can_edit_shared_calendar() AND created_by = auth.uid() AND updated_by = auth.uid());

DROP POLICY IF EXISTS "economic events update" ON public.economic_events;
CREATE POLICY "economic events update" ON public.economic_events
FOR UPDATE TO authenticated
USING (app_private.can_edit_shared_calendar())
WITH CHECK (app_private.can_edit_shared_calendar());

DROP POLICY IF EXISTS "economic events delete" ON public.economic_events;
CREATE POLICY "economic events delete" ON public.economic_events
FOR DELETE TO authenticated
USING (app_private.can_edit_shared_calendar());

DROP POLICY IF EXISTS "economic reminders read" ON public.economic_event_reminders;
CREATE POLICY "economic reminders read" ON public.economic_event_reminders
FOR SELECT TO authenticated
USING (app_private.is_app_member());

DROP POLICY IF EXISTS "economic reminders insert" ON public.economic_event_reminders;
CREATE POLICY "economic reminders insert" ON public.economic_event_reminders
FOR INSERT TO authenticated
WITH CHECK (
  app_private.can_edit_shared_calendar()
  AND EXISTS (SELECT 1 FROM public.economic_events e WHERE e.id = event_id)
);

DROP POLICY IF EXISTS "economic reminders update" ON public.economic_event_reminders;
CREATE POLICY "economic reminders update" ON public.economic_event_reminders
FOR UPDATE TO authenticated
USING (app_private.can_edit_shared_calendar())
WITH CHECK (app_private.can_edit_shared_calendar());

DROP POLICY IF EXISTS "economic reminders delete" ON public.economic_event_reminders;
CREATE POLICY "economic reminders delete" ON public.economic_event_reminders
FOR DELETE TO authenticated
USING (app_private.can_edit_shared_calendar());

DROP POLICY IF EXISTS "economic ack read own" ON public.economic_reminder_acknowledgements;
CREATE POLICY "economic ack read own" ON public.economic_reminder_acknowledgements
FOR SELECT TO authenticated
USING (user_id = auth.uid() AND app_private.is_app_member());

DROP POLICY IF EXISTS "economic ack insert own" ON public.economic_reminder_acknowledgements;
CREATE POLICY "economic ack insert own" ON public.economic_reminder_acknowledgements
FOR INSERT TO authenticated
WITH CHECK (user_id = auth.uid() AND app_private.is_app_member());

DROP POLICY IF EXISTS "economic ack delete own" ON public.economic_reminder_acknowledgements;
CREATE POLICY "economic ack delete own" ON public.economic_reminder_acknowledgements
FOR DELETE TO authenticated
USING (user_id = auth.uid() AND app_private.is_app_member());

GRANT SELECT, INSERT, UPDATE, DELETE ON public.economic_events TO authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.economic_event_reminders TO authenticated;
GRANT SELECT, INSERT, DELETE ON public.economic_reminder_acknowledgements TO authenticated;
