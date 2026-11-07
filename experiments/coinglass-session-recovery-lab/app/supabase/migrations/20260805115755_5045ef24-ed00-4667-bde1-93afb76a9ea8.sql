UPDATE public.template_questions q
SET field_key = CASE q.title WHEN 'מסקנות' THEN 'journal_conclusions' ELSE 'journal_notes' END
FROM public.template_groups g, public.template_stages s
WHERE g.id = q.group_id AND s.id = g.stage_id
  AND s.title = 'יומן'
  AND q.title IN ('מסקנות','הערות')
  AND q.field_key IS NULL;