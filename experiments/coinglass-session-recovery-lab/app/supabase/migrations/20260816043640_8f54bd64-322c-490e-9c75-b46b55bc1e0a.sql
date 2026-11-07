ALTER TABLE public.projects ADD COLUMN IF NOT EXISTS scenario_ideas text;
ALTER TABLE public.project_images ALTER COLUMN question_id DROP NOT NULL;