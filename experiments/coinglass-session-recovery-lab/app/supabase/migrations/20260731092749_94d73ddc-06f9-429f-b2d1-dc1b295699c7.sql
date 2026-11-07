
CREATE OR REPLACE FUNCTION public.set_updated_at() RETURNS TRIGGER AS $$
BEGIN NEW.updated_at = now(); RETURN NEW; END; $$ LANGUAGE plpgsql SET search_path = public;

CREATE TABLE public.templates (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name text NOT NULL,
  description text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE public.template_versions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  template_id uuid NOT NULL REFERENCES public.templates(id) ON DELETE CASCADE,
  version_number int NOT NULL DEFAULT 1,
  status text NOT NULL DEFAULT 'draft',
  created_at timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz
);

CREATE TABLE public.template_stages (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  template_version_id uuid NOT NULL REFERENCES public.template_versions(id) ON DELETE CASCADE,
  title text NOT NULL,
  description text,
  position int NOT NULL DEFAULT 0,
  active boolean NOT NULL DEFAULT true
);

CREATE TABLE public.template_groups (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  stage_id uuid NOT NULL REFERENCES public.template_stages(id) ON DELETE CASCADE,
  title text NOT NULL,
  description text,
  position int NOT NULL DEFAULT 0,
  collapsible boolean NOT NULL DEFAULT true,
  active boolean NOT NULL DEFAULT true
);

CREATE TABLE public.template_questions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  group_id uuid NOT NULL REFERENCES public.template_groups(id) ON DELETE CASCADE,
  title text NOT NULL,
  description text,
  question_type text NOT NULL DEFAULT 'short_text',
  position int NOT NULL DEFAULT 0,
  required boolean NOT NULL DEFAULT false,
  active boolean NOT NULL DEFAULT true,
  options_json jsonb,
  visibility_rule_json jsonb,
  help_text text,
  source_url text
);

CREATE TABLE public.app_settings (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  app_name text NOT NULL DEFAULT 'סביבת העבודה למסחר',
  active_template_version_id uuid REFERENCES public.template_versions(id),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE public.projects (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  template_version_id uuid REFERENCES public.template_versions(id),
  name text NOT NULL,
  symbol text,
  direction text NOT NULL DEFAULT 'undecided',
  timeframe text,
  opportunity_source text,
  initial_note text,
  status text NOT NULL DEFAULT 'draft',
  current_stage_id uuid,
  progress_percent int NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  archived_at timestamptz
);

CREATE TABLE public.project_answers (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id uuid NOT NULL REFERENCES public.projects(id) ON DELETE CASCADE,
  question_id uuid NOT NULL REFERENCES public.template_questions(id) ON DELETE CASCADE,
  text_value text,
  number_value numeric,
  boolean_value boolean,
  choice_value_json jsonb,
  date_value timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (project_id, question_id)
);

CREATE TABLE public.project_images (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id uuid NOT NULL REFERENCES public.projects(id) ON DELETE CASCADE,
  question_id uuid REFERENCES public.template_questions(id) ON DELETE CASCADE,
  storage_path text NOT NULL,
  file_name text,
  mime_type text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE public.trades (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id uuid NOT NULL UNIQUE REFERENCES public.projects(id) ON DELETE CASCADE,
  symbol text,
  direction text,
  planned_entry numeric,
  actual_entry numeric,
  stop_loss numeric,
  take_profit_json jsonb,
  position_size numeric,
  opened_at timestamptz,
  closed_at timestamptz,
  exit_price numeric,
  pnl_amount numeric,
  pnl_percent numeric,
  result text,
  summary text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TRIGGER t1 BEFORE UPDATE ON public.templates FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();
CREATE TRIGGER t2 BEFORE UPDATE ON public.projects FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();
CREATE TRIGGER t3 BEFORE UPDATE ON public.project_answers FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();
CREATE TRIGGER t4 BEFORE UPDATE ON public.trades FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();
CREATE TRIGGER t5 BEFORE UPDATE ON public.app_settings FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY['templates','template_versions','template_stages','template_groups','template_questions','app_settings','projects','project_answers','project_images','trades']
  LOOP
    EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON public.%I TO anon, authenticated', t);
    EXECUTE format('GRANT ALL ON public.%I TO service_role', t);
    EXECUTE format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY', t);
    EXECUTE format('CREATE POLICY "open_access" ON public.%I FOR ALL TO anon, authenticated USING (true) WITH CHECK (true)', t);
  END LOOP;
END $$;

-- Seed initial template
DO $$
DECLARE
  tpl uuid; ver uuid; st uuid; grp uuid;
  stage_names text[] := ARRAY['תמונת מצב','בניית תרחיש','אימות והפרכה','מוכנות למסחר','החלטה','תחקור ותוצאה'];
  i int;
BEGIN
  INSERT INTO public.templates(name, description) VALUES ('תבנית ניתוח עסקה', 'תבנית ברירת מחדל') RETURNING id INTO tpl;
  INSERT INTO public.template_versions(template_id, version_number, status, published_at) VALUES (tpl, 1, 'active', now()) RETURNING id INTO ver;
  INSERT INTO public.app_settings(active_template_version_id) VALUES (ver);

  FOR i IN 1..6 LOOP
    INSERT INTO public.template_stages(template_version_id, title, description, position)
      VALUES (ver, stage_names[i], 'שלב ' || i, i) RETURNING id INTO st;
    INSERT INTO public.template_groups(stage_id, title, position) VALUES (st, 'סעיפים כלליים', 1) RETURNING id INTO grp;

    IF i = 1 THEN
      INSERT INTO public.template_questions(group_id, title, question_type, position, required) VALUES
        (grp, 'בימי תנודתיות קיצונית אין לסחור בתנאי אי־ודאות.', 'info', 1, false),
        (grp, 'בדקתי את מצב הביטקוין', 'checkbox', 2, true),
        (grp, 'צילום מסך של הגרף', 'image', 3, false);
    ELSIF i = 2 THEN
      INSERT INTO public.template_questions(group_id, title, question_type, position, required, options_json) VALUES
        (grp, 'תיאור התרחיש המרכזי', 'long_text', 1, true, NULL),
        (grp, 'האם קיים תרחיש נגדי ברור?', 'yes_no_na', 2, true, NULL),
        (grp, 'טווח הזמן של התרחיש', 'single_choice', 3, false, '["סקאלפ","אינטראדיי","סווינג","ארוך טווח"]'::jsonb);
    ELSIF i = 3 THEN
      INSERT INTO public.template_questions(group_id, title, question_type, position, required, options_json) VALUES
        (grp, 'אילו ראיות תומכות בתרחיש?', 'multi_choice', 1, false, '["מבנה מחיר","נפח","אינדיקטור","חדשות","קורלציה"]'::jsonb),
        (grp, 'מה עשוי להפריך את התרחיש?', 'long_text', 2, true, NULL),
        (grp, 'קישור למקור חיצוני', 'link', 3, false, NULL);
    ELSIF i = 4 THEN
      INSERT INTO public.template_questions(group_id, title, question_type, position, required) VALUES
        (grp, 'מחיר כניסה מתוכנן', 'number', 1, true),
        (grp, 'מחיר סטופ', 'number', 2, true),
        (grp, 'מועד מתוכנן לכניסה', 'datetime', 3, false);
    ELSIF i = 5 THEN
      INSERT INTO public.template_questions(group_id, title, question_type, position, required, options_json) VALUES
        (grp, 'נימוק קצר להחלטה', 'short_text', 1, true, NULL),
        (grp, 'מסקנת השלב', 'stage_conclusion', 2, true, '["המשך","המתן","פסול","נדרשת בדיקה נוספת"]'::jsonb);
    ELSE
      INSERT INTO public.template_questions(group_id, title, question_type, position, required, options_json) VALUES
        (grp, 'מה עבד ומה לא?', 'long_text', 1, false, NULL),
        (grp, 'תוצאת העסקה', 'single_choice', 2, false, '["רווח","הפסד","איזון","לא נכנסנו","בוטלה"]'::jsonb),
        (grp, 'צילום מסך של הסגירה', 'image', 3, false, NULL);
    END IF;
  END LOOP;
END $$;
