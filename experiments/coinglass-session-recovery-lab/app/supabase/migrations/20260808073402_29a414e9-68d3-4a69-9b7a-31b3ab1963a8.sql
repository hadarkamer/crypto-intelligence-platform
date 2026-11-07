ALTER TABLE public.template_questions ADD COLUMN IF NOT EXISTS question_key text;

UPDATE public.template_questions q
SET question_key = COALESCE(
  NULLIF(q.field_key, ''),
  lower(btrim(s.title)) || '/' || lower(btrim(g.title)) || '/' || lower(btrim(q.title))
)
FROM public.template_groups g, public.template_stages s
WHERE q.group_id = g.id
  AND g.stage_id = s.id
  AND (q.question_key IS NULL OR q.question_key = '');

CREATE INDEX IF NOT EXISTS template_questions_question_key_idx
  ON public.template_questions (question_key);