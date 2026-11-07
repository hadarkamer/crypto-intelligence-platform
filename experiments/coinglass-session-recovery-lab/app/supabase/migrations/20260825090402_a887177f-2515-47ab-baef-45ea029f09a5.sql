ALTER TABLE public.template_questions
  ADD COLUMN IF NOT EXISTS default_text_align text NOT NULL DEFAULT 'right';

ALTER TABLE public.template_questions
  DROP CONSTRAINT IF EXISTS template_questions_default_text_align_check;

ALTER TABLE public.template_questions
  ADD CONSTRAINT template_questions_default_text_align_check
  CHECK (default_text_align IN ('right','left','center'));