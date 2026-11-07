ALTER TABLE public.template_questions
  ADD COLUMN IF NOT EXISTS links_json jsonb NOT NULL DEFAULT '[]'::jsonb;