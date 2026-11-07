-- 1. profiles -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.profiles (
  id uuid PRIMARY KEY,
  display_name text,
  email text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
GRANT SELECT ON public.profiles TO authenticated;
GRANT ALL ON public.profiles TO service_role;
ALTER TABLE public.profiles ENABLE ROW LEVEL SECURITY;

CREATE OR REPLACE FUNCTION app_private.shares_project(_other uuid)
RETURNS boolean LANGUAGE sql STABLE SECURITY DEFINER SET search_path TO 'public','app_private' AS $$
  SELECT EXISTS (
    SELECT 1 FROM public.projects p
    WHERE (p.user_id = auth.uid()
           OR EXISTS (SELECT 1 FROM public.project_members m WHERE m.project_id = p.id AND m.user_id = auth.uid()))
      AND (p.user_id = _other
           OR EXISTS (SELECT 1 FROM public.project_members m2 WHERE m2.project_id = p.id AND m2.user_id = _other))
  )
$$;
REVOKE ALL ON FUNCTION app_private.shares_project(uuid) FROM PUBLIC, anon;
GRANT EXECUTE ON FUNCTION app_private.shares_project(uuid) TO authenticated;

DROP POLICY IF EXISTS "read visible profiles" ON public.profiles;
CREATE POLICY "read visible profiles" ON public.profiles FOR SELECT TO authenticated
  USING (id = auth.uid() OR app_private.shares_project(id));

CREATE OR REPLACE FUNCTION public.sync_my_profile()
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'public','app_private' AS $$
DECLARE uid uuid := auth.uid();
BEGIN
  IF uid IS NULL THEN RETURN; END IF;
  INSERT INTO public.profiles (id, display_name, email)
  SELECT u.id,
         COALESCE(NULLIF(u.raw_user_meta_data->>'full_name',''),
                  NULLIF(u.raw_user_meta_data->>'name',''),
                  split_part(u.email, '@', 1)),
         u.email
  FROM auth.users u WHERE u.id = uid
  ON CONFLICT (id) DO UPDATE
    SET display_name = COALESCE(EXCLUDED.display_name, public.profiles.display_name),
        email = EXCLUDED.email,
        updated_at = now();
END; $$;
REVOKE ALL ON FUNCTION public.sync_my_profile() FROM PUBLIC, anon;
GRANT EXECUTE ON FUNCTION public.sync_my_profile() TO authenticated;

INSERT INTO public.profiles (id, display_name, email)
SELECT u.id,
       COALESCE(NULLIF(u.raw_user_meta_data->>'full_name',''),
                NULLIF(u.raw_user_meta_data->>'name',''),
                split_part(u.email, '@', 1)),
       u.email
FROM auth.users u
ON CONFLICT (id) DO NOTHING;

CREATE TRIGGER t_profiles_updated_at BEFORE UPDATE ON public.profiles
  FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

-- 2. project_members role constraint ---------------------------------------
UPDATE public.project_members SET role = 'editor' WHERE role NOT IN ('owner','editor','viewer');
ALTER TABLE public.project_members DROP CONSTRAINT IF EXISTS project_members_role_check;
ALTER TABLE public.project_members ADD CONSTRAINT project_members_role_check
  CHECK (role IN ('owner','editor','viewer'));
CREATE INDEX IF NOT EXISTS idx_project_members_user ON public.project_members(user_id);

-- 3. answer state: "no new / not relevant" ----------------------------------
ALTER TABLE public.project_answers ADD COLUMN IF NOT EXISTS not_relevant boolean NOT NULL DEFAULT false;

-- 4. per-user checkbox marks (נפשי) -----------------------------------------
CREATE TABLE IF NOT EXISTS public.question_user_checks (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id uuid NOT NULL REFERENCES public.projects(id) ON DELETE CASCADE,
  question_id uuid NOT NULL REFERENCES public.template_questions(id) ON DELETE CASCADE,
  user_id uuid NOT NULL DEFAULT auth.uid(),
  checked boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (project_id, question_id, user_id)
);
GRANT SELECT, INSERT, UPDATE, DELETE ON public.question_user_checks TO authenticated;
GRANT ALL ON public.question_user_checks TO service_role;
ALTER TABLE public.question_user_checks ENABLE ROW LEVEL SECURITY;
CREATE POLICY "read project checks" ON public.question_user_checks FOR SELECT TO authenticated
  USING (app_private.can_access_project(project_id));
CREATE POLICY "write own checks" ON public.question_user_checks FOR ALL TO authenticated
  USING (user_id = auth.uid() AND app_private.can_edit_project(project_id))
  WITH CHECK (user_id = auth.uid() AND app_private.can_edit_project(project_id));
CREATE INDEX IF NOT EXISTS idx_question_user_checks_project ON public.question_user_checks(project_id);
CREATE TRIGGER t_question_user_checks_updated_at BEFORE UPDATE ON public.question_user_checks
  FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

-- 5. template group behavior marker -----------------------------------------
ALTER TABLE public.template_groups ADD COLUMN IF NOT EXISTS behavior text;
UPDATE public.template_groups SET behavior = 'mental'
 WHERE behavior IS NULL AND (title LIKE 'נפשי%' OR title LIKE '%נפשי');

-- 6. activity ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.project_activity (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id uuid NOT NULL REFERENCES public.projects(id) ON DELETE CASCADE,
  question_id uuid,
  group_id uuid,
  stage_id uuid,
  question_key text,
  actor_id uuid,
  kind text NOT NULL DEFAULT 'answer',
  created_at timestamptz NOT NULL DEFAULT now()
);
GRANT SELECT ON public.project_activity TO authenticated;
GRANT ALL ON public.project_activity TO service_role;
ALTER TABLE public.project_activity ENABLE ROW LEVEL SECURITY;
CREATE POLICY "read project activity" ON public.project_activity FOR SELECT TO authenticated
  USING (app_private.can_access_project(project_id));
CREATE INDEX IF NOT EXISTS idx_project_activity_project ON public.project_activity(project_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_project_activity_question ON public.project_activity(project_id, question_id);

CREATE OR REPLACE FUNCTION app_private.log_activity(_project_id uuid, _question_id uuid, _kind text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'public','app_private' AS $$
DECLARE _actor uuid := auth.uid(); _group uuid; _stage uuid; _key text;
BEGIN
  IF _actor IS NULL THEN RETURN; END IF;
  IF EXISTS (SELECT 1 FROM public.project_activity a
             WHERE a.project_id = _project_id
               AND a.question_id IS NOT DISTINCT FROM _question_id
               AND a.actor_id = _actor
               AND a.created_at > now() - interval '90 seconds') THEN
    UPDATE public.project_activity SET created_at = now()
     WHERE id = (SELECT id FROM public.project_activity a
                  WHERE a.project_id = _project_id
                    AND a.question_id IS NOT DISTINCT FROM _question_id
                    AND a.actor_id = _actor
                  ORDER BY a.created_at DESC LIMIT 1);
    RETURN;
  END IF;
  SELECT q.group_id, g.stage_id, COALESCE(q.question_key, q.field_key)
    INTO _group, _stage, _key
    FROM public.template_questions q
    JOIN public.template_groups g ON g.id = q.group_id
   WHERE q.id = _question_id;
  INSERT INTO public.project_activity (project_id, question_id, group_id, stage_id, question_key, actor_id, kind)
  VALUES (_project_id, _question_id, _group, _stage, _key, _actor, COALESCE(_kind,'answer'));
END; $$;
REVOKE ALL ON FUNCTION app_private.log_activity(uuid, uuid, text) FROM PUBLIC, anon, authenticated;

CREATE OR REPLACE FUNCTION public.tg_answer_activity()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'public','app_private' AS $$
BEGIN
  IF TG_OP = 'UPDATE' AND NOT (
      NEW.text_value IS DISTINCT FROM OLD.text_value OR
      NEW.number_value IS DISTINCT FROM OLD.number_value OR
      NEW.boolean_value IS DISTINCT FROM OLD.boolean_value OR
      NEW.choice_value_json IS DISTINCT FROM OLD.choice_value_json OR
      NEW.date_value IS DISTINCT FROM OLD.date_value OR
      NEW.not_relevant IS DISTINCT FROM OLD.not_relevant) THEN
    RETURN NEW;
  END IF;
  PERFORM app_private.log_activity(NEW.project_id, NEW.question_id, 'answer');
  RETURN NEW;
END; $$;
DROP TRIGGER IF EXISTS t_answer_activity ON public.project_answers;
CREATE TRIGGER t_answer_activity AFTER INSERT OR UPDATE ON public.project_answers
  FOR EACH ROW EXECUTE FUNCTION public.tg_answer_activity();

CREATE OR REPLACE FUNCTION public.tg_image_activity()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'public','app_private' AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    PERFORM app_private.log_activity(OLD.project_id, OLD.question_id, 'image');
    RETURN OLD;
  END IF;
  PERFORM app_private.log_activity(NEW.project_id, NEW.question_id, 'image');
  RETURN NEW;
END; $$;
DROP TRIGGER IF EXISTS t_image_activity ON public.project_images;
CREATE TRIGGER t_image_activity AFTER INSERT OR UPDATE OR DELETE ON public.project_images
  FOR EACH ROW EXECUTE FUNCTION public.tg_image_activity();

CREATE OR REPLACE FUNCTION public.tg_check_activity()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'public','app_private' AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    PERFORM app_private.log_activity(OLD.project_id, OLD.question_id, 'check');
    RETURN OLD;
  END IF;
  PERFORM app_private.log_activity(NEW.project_id, NEW.question_id, 'check');
  RETURN NEW;
END; $$;
DROP TRIGGER IF EXISTS t_check_activity ON public.question_user_checks;
CREATE TRIGGER t_check_activity AFTER INSERT OR UPDATE OR DELETE ON public.question_user_checks
  FOR EACH ROW EXECUTE FUNCTION public.tg_check_activity();

-- 7. per-user read state -----------------------------------------------------
ALTER TABLE public.project_user_state ADD COLUMN IF NOT EXISTS first_opened_at timestamptz;
UPDATE public.project_user_state SET first_opened_at = COALESCE(first_opened_at, created_at);

CREATE TABLE IF NOT EXISTS public.project_question_seen (
  user_id uuid NOT NULL DEFAULT auth.uid(),
  project_id uuid NOT NULL REFERENCES public.projects(id) ON DELETE CASCADE,
  question_id uuid NOT NULL,
  seen_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, project_id, question_id)
);
GRANT SELECT, INSERT, UPDATE, DELETE ON public.project_question_seen TO authenticated;
GRANT ALL ON public.project_question_seen TO service_role;
ALTER TABLE public.project_question_seen ENABLE ROW LEVEL SECURITY;
CREATE POLICY "own question seen" ON public.project_question_seen FOR ALL TO authenticated
  USING (user_id = auth.uid() AND app_private.can_access_project(project_id))
  WITH CHECK (user_id = auth.uid() AND app_private.can_access_project(project_id));
CREATE INDEX IF NOT EXISTS idx_project_question_seen_project ON public.project_question_seen(user_id, project_id);

-- 8. pinned answers: no duplicate pin of the same source answer --------------
CREATE UNIQUE INDEX IF NOT EXISTS uq_pinned_answers_source
  ON public.pinned_answers (pinned_by, COALESCE(source_project_id, '00000000-0000-0000-0000-000000000000'::uuid), question_key);

-- 9. storage policies --------------------------------------------------------
DROP POLICY IF EXISTS "own project images read" ON storage.objects;
DROP POLICY IF EXISTS "own project images insert" ON storage.objects;
DROP POLICY IF EXISTS "own project images update" ON storage.objects;
DROP POLICY IF EXISTS "own project images delete" ON storage.objects;

CREATE POLICY "project images read" ON storage.objects FOR SELECT TO authenticated
  USING (bucket_id = 'project-images' AND app_private.can_access_project(((storage.foldername(name))[1])::uuid));
CREATE POLICY "project images insert" ON storage.objects FOR INSERT TO authenticated
  WITH CHECK (bucket_id = 'project-images' AND app_private.can_edit_project(((storage.foldername(name))[1])::uuid));
CREATE POLICY "project images update" ON storage.objects FOR UPDATE TO authenticated
  USING (bucket_id = 'project-images' AND app_private.can_edit_project(((storage.foldername(name))[1])::uuid))
  WITH CHECK (bucket_id = 'project-images' AND app_private.can_edit_project(((storage.foldername(name))[1])::uuid));
CREATE POLICY "project images delete" ON storage.objects FOR DELETE TO authenticated
  USING (bucket_id = 'project-images' AND app_private.can_edit_project(((storage.foldername(name))[1])::uuid));