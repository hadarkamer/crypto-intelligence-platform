CREATE TABLE public.project_user_state (
  user_id uuid NOT NULL DEFAULT auth.uid(),
  project_id uuid NOT NULL REFERENCES public.projects(id) ON DELETE CASCADE,
  last_stage_id uuid,
  stage_seen jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, project_id)
);

GRANT SELECT, INSERT, UPDATE, DELETE ON public.project_user_state TO authenticated;
GRANT ALL ON public.project_user_state TO service_role;

ALTER TABLE public.project_user_state ENABLE ROW LEVEL SECURITY;

CREATE POLICY "own project state" ON public.project_user_state
  FOR ALL TO authenticated
  USING (user_id = auth.uid())
  WITH CHECK (user_id = auth.uid());

CREATE TRIGGER t_project_user_state_updated_at
  BEFORE UPDATE ON public.project_user_state
  FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();