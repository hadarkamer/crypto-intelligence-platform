-- 1) last editor on project
ALTER TABLE public.projects ADD COLUMN IF NOT EXISTS last_edited_by uuid;

CREATE OR REPLACE FUNCTION public.tg_activity_touch_project()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  IF NEW.actor_id IS NOT NULL THEN
    UPDATE public.projects
       SET last_edited_by = NEW.actor_id,
           updated_at = now()
     WHERE id = NEW.project_id;
  END IF;
  RETURN NEW;
END; $$;

DROP TRIGGER IF EXISTS t_activity_touch_project ON public.project_activity;
CREATE TRIGGER t_activity_touch_project
AFTER INSERT OR UPDATE ON public.project_activity
FOR EACH ROW EXECUTE FUNCTION public.tg_activity_touch_project();

-- 2) race-safe first open baseline + personal last stage
CREATE OR REPLACE FUNCTION public.open_project(_project_id uuid, _stage_id uuid DEFAULT NULL)
RETURNS timestamptz
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = public
AS $$
DECLARE _uid uuid := auth.uid(); _first timestamptz;
BEGIN
  IF _uid IS NULL THEN RETURN NULL; END IF;
  INSERT INTO public.project_user_state (user_id, project_id, last_stage_id, first_opened_at)
  VALUES (_uid, _project_id, _stage_id, now())
  ON CONFLICT (user_id, project_id) DO UPDATE
    SET last_stage_id = COALESCE(EXCLUDED.last_stage_id, public.project_user_state.last_stage_id),
        first_opened_at = COALESCE(public.project_user_state.first_opened_at, now())
  RETURNING first_opened_at INTO _first;
  RETURN _first;
END; $$;

REVOKE ALL ON FUNCTION public.open_project(uuid, uuid) FROM PUBLIC, anon;
GRANT EXECUTE ON FUNCTION public.open_project(uuid, uuid) TO authenticated;

-- 3) unread activity of OTHER users, one row per question, RLS-safe
CREATE OR REPLACE FUNCTION public.my_unread_questions(_project_id uuid DEFAULT NULL)
RETURNS TABLE (
  project_id uuid,
  stage_id uuid,
  group_id uuid,
  question_id uuid,
  question_key text,
  actor_id uuid,
  kind text,
  latest_at timestamptz
)
LANGUAGE sql
STABLE
SECURITY INVOKER
SET search_path = public
AS $$
  SELECT DISTINCT ON (a.project_id, a.question_id)
         a.project_id, a.stage_id, a.group_id, a.question_id,
         a.question_key, a.actor_id, a.kind, a.created_at AS latest_at
    FROM public.project_activity a
    JOIN public.project_user_state s
      ON s.project_id = a.project_id AND s.user_id = auth.uid()
    LEFT JOIN public.project_question_seen v
      ON v.project_id = a.project_id AND v.question_id = a.question_id AND v.user_id = auth.uid()
   WHERE auth.uid() IS NOT NULL
     AND a.actor_id IS NOT NULL
     AND a.actor_id <> auth.uid()
     AND a.question_id IS NOT NULL
     AND s.first_opened_at IS NOT NULL
     AND a.created_at > s.first_opened_at
     AND (v.seen_at IS NULL OR a.created_at > v.seen_at)
     AND (_project_id IS NULL OR a.project_id = _project_id)
   ORDER BY a.project_id, a.question_id, a.created_at DESC
$$;

REVOKE ALL ON FUNCTION public.my_unread_questions(uuid) FROM PUBLIC, anon;
GRANT EXECUTE ON FUNCTION public.my_unread_questions(uuid) TO authenticated;

-- 4) latest activity per question (for "עודכן ע״י") — batch, RLS-safe
CREATE OR REPLACE FUNCTION public.project_latest_activity(_project_id uuid)
RETURNS TABLE (question_id uuid, actor_id uuid, kind text, latest_at timestamptz)
LANGUAGE sql
STABLE
SECURITY INVOKER
SET search_path = public
AS $$
  SELECT DISTINCT ON (a.question_id)
         a.question_id, a.actor_id, a.kind, a.created_at AS latest_at
    FROM public.project_activity a
   WHERE a.project_id = _project_id
     AND a.question_id IS NOT NULL
   ORDER BY a.question_id, a.created_at DESC
$$;

REVOKE ALL ON FUNCTION public.project_latest_activity(uuid) FROM PUBLIC, anon;
GRANT EXECUTE ON FUNCTION public.project_latest_activity(uuid) TO authenticated;

-- 5) owner-only sharing by email of an already-registered user
CREATE OR REPLACE FUNCTION public.add_project_member_by_email(
  _project_id uuid, _email text, _role text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE _uid uuid := auth.uid(); _owner uuid; _target uuid; _label text;
BEGIN
  IF _uid IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'message', 'נדרשת התחברות');
  END IF;
  IF _role NOT IN ('editor','viewer') THEN
    RETURN jsonb_build_object('ok', false, 'message', 'תפקיד לא חוקי');
  END IF;

  SELECT p.user_id INTO _owner FROM public.projects p WHERE p.id = _project_id;
  IF _owner IS NULL OR _owner <> _uid THEN
    RETURN jsonb_build_object('ok', false, 'message', 'רק בעל הפרויקט יכול לשתף אותו');
  END IF;

  SELECT u.id INTO _target
    FROM auth.users u
   WHERE lower(u.email) = lower(btrim(_email))
   LIMIT 1;

  IF _target IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'message', 'המשתמש עדיין לא רשום במערכת');
  END IF;
  IF _target = _owner THEN
    RETURN jsonb_build_object('ok', false, 'message', 'בעל הפרויקט כבר בעל גישה מלאה');
  END IF;

  INSERT INTO public.profiles (id, display_name, email)
  SELECT u.id,
         COALESCE(NULLIF(u.raw_user_meta_data->>'full_name',''),
                  NULLIF(u.raw_user_meta_data->>'name',''),
                  split_part(u.email,'@',1)),
         u.email
    FROM auth.users u WHERE u.id = _target
  ON CONFLICT (id) DO UPDATE
    SET email = EXCLUDED.email,
        display_name = COALESCE(public.profiles.display_name, EXCLUDED.display_name);

  INSERT INTO public.project_members (project_id, user_id, role)
  VALUES (_project_id, _target, _role)
  ON CONFLICT (project_id, user_id) DO UPDATE SET role = EXCLUDED.role;

  SELECT COALESCE(pr.display_name, split_part(pr.email,'@',1))
    INTO _label FROM public.profiles pr WHERE pr.id = _target;

  RETURN jsonb_build_object('ok', true, 'user_id', _target, 'label', _label, 'role', _role);
END; $$;

REVOKE ALL ON FUNCTION public.add_project_member_by_email(uuid, text, text) FROM PUBLIC, anon;
GRANT EXECUTE ON FUNCTION public.add_project_member_by_email(uuid, text, text) TO authenticated;

-- 6) realtime for activity badges (idempotent)
ALTER TABLE public.project_activity REPLICA IDENTITY FULL;
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'supabase_realtime')
     AND NOT EXISTS (
       SELECT 1 FROM pg_publication_tables
        WHERE pubname = 'supabase_realtime' AND schemaname = 'public' AND tablename = 'project_activity'
     ) THEN
    EXECUTE 'ALTER PUBLICATION supabase_realtime ADD TABLE public.project_activity';
  END IF;
END $$;