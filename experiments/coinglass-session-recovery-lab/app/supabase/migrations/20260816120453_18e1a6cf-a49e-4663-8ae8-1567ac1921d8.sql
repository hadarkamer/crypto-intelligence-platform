-- 1) העדפות משתמש (צבע כתיבה אישי)
CREATE TABLE public.user_preferences (
  user_id uuid PRIMARY KEY,
  default_text_class text NOT NULL DEFAULT 'rt-color-default',
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
GRANT SELECT, INSERT, UPDATE, DELETE ON public.user_preferences TO authenticated;
GRANT ALL ON public.user_preferences TO service_role;
ALTER TABLE public.user_preferences ENABLE ROW LEVEL SECURITY;
CREATE POLICY "own preferences" ON public.user_preferences
  FOR ALL TO authenticated
  USING (user_id = auth.uid())
  WITH CHECK (user_id = auth.uid());
CREATE TRIGGER t_user_preferences_updated_at
  BEFORE UPDATE ON public.user_preferences
  FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

-- 2) תשובות נעוצות (snapshot)
CREATE TABLE public.pinned_answers (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  pinned_by uuid NOT NULL DEFAULT auth.uid(),
  question_key text NOT NULL,
  question_type text NOT NULL DEFAULT 'long_text',
  question_title text,
  source_project_id uuid,
  source_project_name text,
  answer_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb,
  image_paths jsonb NOT NULL DEFAULT '[]'::jsonb,
  author_label text,
  answer_updated_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX pinned_answers_key_idx ON public.pinned_answers (pinned_by, question_key);
GRANT SELECT, INSERT, UPDATE, DELETE ON public.pinned_answers TO authenticated;
GRANT ALL ON public.pinned_answers TO service_role;
ALTER TABLE public.pinned_answers ENABLE ROW LEVEL SECURITY;
CREATE POLICY "own pins" ON public.pinned_answers
  FOR ALL TO authenticated
  USING (pinned_by = auth.uid())
  WITH CHECK (pinned_by = auth.uid());
CREATE TRIGGER t_pinned_answers_updated_at
  BEFORE UPDATE ON public.pinned_answers
  FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

-- 3) מי עדכן תשובה לאחרונה (metadata מינימלי, לא audit)
ALTER TABLE public.project_answers ADD COLUMN IF NOT EXISTS last_edited_by uuid;