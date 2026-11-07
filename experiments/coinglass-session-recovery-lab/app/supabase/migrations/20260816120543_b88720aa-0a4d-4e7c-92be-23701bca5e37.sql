-- חברות בפרויקט
CREATE TABLE public.project_members (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id uuid NOT NULL REFERENCES public.projects(id) ON DELETE CASCADE,
  user_id uuid NOT NULL,
  role text NOT NULL DEFAULT 'editor',
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (project_id, user_id)
);
CREATE INDEX project_members_user_idx ON public.project_members (user_id);

-- backfill: הבעלים הקיים נשמר
INSERT INTO public.project_members (project_id, user_id, role)
SELECT p.id, p.user_id, 'owner' FROM public.projects p
WHERE p.user_id IS NOT NULL
ON CONFLICT (project_id, user_id) DO NOTHING;

GRANT SELECT, INSERT, UPDATE, DELETE ON public.project_members TO authenticated;
GRANT ALL ON public.project_members TO service_role;
ALTER TABLE public.project_members ENABLE ROW LEVEL SECURITY;

-- פונקציות עזר (SECURITY DEFINER, ללא הרשאת הרצה לציבור)
CREATE OR REPLACE FUNCTION app_private.project_role(_project_id uuid)
RETURNS text LANGUAGE sql STABLE SECURITY DEFINER SET search_path TO 'public', 'app_private' AS $$
  SELECT CASE
    WHEN EXISTS (SELECT 1 FROM public.projects p WHERE p.id = _project_id AND p.user_id = auth.uid()) THEN 'owner'
    ELSE (SELECT m.role FROM public.project_members m WHERE m.project_id = _project_id AND m.user_id = auth.uid() LIMIT 1)
  END
$$;

CREATE OR REPLACE FUNCTION app_private.can_access_project(_project_id uuid)
RETURNS boolean LANGUAGE sql STABLE SECURITY DEFINER SET search_path TO 'public', 'app_private' AS $$
  SELECT auth.uid() IS NOT NULL AND app_private.project_role(_project_id) IS NOT NULL
$$;

CREATE OR REPLACE FUNCTION app_private.can_edit_project(_project_id uuid)
RETURNS boolean LANGUAGE sql STABLE SECURITY DEFINER SET search_path TO 'public', 'app_private' AS $$
  SELECT app_private.project_role(_project_id) IN ('owner','editor')
$$;

REVOKE ALL ON FUNCTION app_private.project_role(uuid) FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION app_private.can_access_project(uuid) FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION app_private.can_edit_project(uuid) FROM PUBLIC, anon, authenticated;

-- מדיניות על חברי פרויקט
CREATE POLICY "members read own project members" ON public.project_members
  FOR SELECT TO authenticated USING (app_private.can_access_project(project_id));
CREATE POLICY "owner manages members" ON public.project_members
  FOR ALL TO authenticated
  USING (EXISTS (SELECT 1 FROM public.projects p WHERE p.id = project_id AND p.user_id = auth.uid()))
  WITH CHECK (EXISTS (SELECT 1 FROM public.projects p WHERE p.id = project_id AND p.user_id = auth.uid()));

-- פרויקטים
DROP POLICY IF EXISTS "own projects" ON public.projects;
CREATE POLICY "read accessible projects" ON public.projects
  FOR SELECT TO authenticated USING (user_id = auth.uid() OR app_private.can_access_project(id));
CREATE POLICY "insert own projects" ON public.projects
  FOR INSERT TO authenticated WITH CHECK (user_id = auth.uid());
CREATE POLICY "edit accessible projects" ON public.projects
  FOR UPDATE TO authenticated
  USING (user_id = auth.uid() OR app_private.can_edit_project(id))
  WITH CHECK (user_id = auth.uid() OR app_private.can_edit_project(id));
CREATE POLICY "owner deletes projects" ON public.projects
  FOR DELETE TO authenticated USING (user_id = auth.uid());

-- תשובות
DROP POLICY IF EXISTS "own answers" ON public.project_answers;
CREATE POLICY "read accessible answers" ON public.project_answers
  FOR SELECT TO authenticated USING (app_private.can_access_project(project_id));
CREATE POLICY "write accessible answers" ON public.project_answers
  FOR ALL TO authenticated
  USING (app_private.can_edit_project(project_id))
  WITH CHECK (app_private.can_edit_project(project_id));

-- תמונות
DROP POLICY IF EXISTS "own images" ON public.project_images;
CREATE POLICY "read accessible images" ON public.project_images
  FOR SELECT TO authenticated USING (app_private.can_access_project(project_id));
CREATE POLICY "write accessible images" ON public.project_images
  FOR ALL TO authenticated
  USING (app_private.can_edit_project(project_id))
  WITH CHECK (app_private.can_edit_project(project_id));

-- עסקאות
DROP POLICY IF EXISTS "own trades" ON public.trades;
CREATE POLICY "read accessible trades" ON public.trades
  FOR SELECT TO authenticated USING (app_private.can_access_project(project_id));
CREATE POLICY "write accessible trades" ON public.trades
  FOR ALL TO authenticated
  USING (app_private.can_edit_project(project_id))
  WITH CHECK (app_private.can_edit_project(project_id));

-- תאימות: הפונקציה הישנה ממשיכה לעבוד ומכבדת חברות
CREATE OR REPLACE FUNCTION app_private.owns_project(_project_id uuid)
RETURNS boolean LANGUAGE sql STABLE SECURITY DEFINER SET search_path TO 'public', 'app_private' AS $$
  SELECT app_private.can_edit_project(_project_id)
$$;