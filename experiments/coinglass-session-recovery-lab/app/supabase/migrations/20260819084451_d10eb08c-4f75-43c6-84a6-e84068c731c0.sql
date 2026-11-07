ALTER TABLE public.pinned_answers
  ADD COLUMN IF NOT EXISTS source_question_id uuid,
  ADD COLUMN IF NOT EXISTS source_group_id uuid,
  ADD COLUMN IF NOT EXISTS source_stage_id uuid;

-- backfill חד-משמעי בלבד: pin עם פרויקט מקור שבו יש בדיוק שאלה אחת עם אותו question_key
WITH resolved AS (
  SELECT p.id AS pin_id,
         (array_agg(q.id))[1]  AS question_id,
         (array_agg(g.id))[1]  AS group_id,
         (array_agg(s.id))[1]  AS stage_id
    FROM public.pinned_answers p
    JOIN public.projects pr ON pr.id = p.source_project_id
    JOIN public.template_stages s ON s.template_version_id = pr.template_version_id
    JOIN public.template_groups g ON g.stage_id = s.id
    JOIN public.template_questions q ON q.group_id = g.id AND q.question_key = p.question_key
   WHERE p.source_question_id IS NULL
   GROUP BY p.id
  HAVING count(*) = 1
)
UPDATE public.pinned_answers p
   SET source_question_id = r.question_id,
       source_group_id = r.group_id,
       source_stage_id = r.stage_id
  FROM resolved r
 WHERE p.id = r.pin_id;