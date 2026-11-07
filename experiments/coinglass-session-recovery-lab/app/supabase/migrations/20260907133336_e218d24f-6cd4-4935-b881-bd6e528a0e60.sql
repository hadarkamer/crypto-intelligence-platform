CREATE TABLE IF NOT EXISTS public.home_reminders (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id uuid NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  body text NOT NULL DEFAULT '',
  frequency text NOT NULL DEFAULT 'normal' CHECK (frequency IN ('rare','normal','high')),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS home_reminders_user_idx ON public.home_reminders(user_id, created_at);

GRANT SELECT, INSERT, UPDATE, DELETE ON public.home_reminders TO authenticated;
GRANT ALL ON public.home_reminders TO service_role;

ALTER TABLE public.home_reminders ENABLE ROW LEVEL SECURITY;

CREATE POLICY "home reminders select own" ON public.home_reminders
  FOR SELECT TO authenticated USING (user_id = auth.uid());
CREATE POLICY "home reminders insert own" ON public.home_reminders
  FOR INSERT TO authenticated WITH CHECK (user_id = auth.uid());
CREATE POLICY "home reminders update own" ON public.home_reminders
  FOR UPDATE TO authenticated USING (user_id = auth.uid()) WITH CHECK (user_id = auth.uid());
CREATE POLICY "home reminders delete own" ON public.home_reminders
  FOR DELETE TO authenticated USING (user_id = auth.uid());

CREATE TRIGGER home_reminders_touch
  BEFORE UPDATE ON public.home_reminders
  FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();