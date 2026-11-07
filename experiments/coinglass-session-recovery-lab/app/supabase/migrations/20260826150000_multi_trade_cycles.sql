-- Multiple trades inside one project. Pre-existing data becomes Trade 1.

CREATE TABLE IF NOT EXISTS public.project_trade_cycles (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id uuid NOT NULL REFERENCES public.projects(id) ON DELETE CASCADE,
  cycle_number integer NOT NULL CHECK (cycle_number > 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  created_by uuid DEFAULT auth.uid(),
  UNIQUE (project_id, cycle_number)
);
CREATE INDEX IF NOT EXISTS idx_project_trade_cycles_project
  ON public.project_trade_cycles(project_id, cycle_number);

ALTER TABLE public.project_trade_cycles ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "trade cycles read" ON public.project_trade_cycles;
CREATE POLICY "trade cycles read" ON public.project_trade_cycles
FOR SELECT TO authenticated USING (app_private.can_access_project(project_id));
DROP POLICY IF EXISTS "trade cycles insert" ON public.project_trade_cycles;
CREATE POLICY "trade cycles insert" ON public.project_trade_cycles
FOR INSERT TO authenticated WITH CHECK (app_private.can_edit_project(project_id));
DROP POLICY IF EXISTS "trade cycles update" ON public.project_trade_cycles;
CREATE POLICY "trade cycles update" ON public.project_trade_cycles
FOR UPDATE TO authenticated USING (app_private.can_edit_project(project_id)) WITH CHECK (app_private.can_edit_project(project_id));
DROP POLICY IF EXISTS "trade cycles delete" ON public.project_trade_cycles;
CREATE POLICY "trade cycles delete" ON public.project_trade_cycles
FOR DELETE TO authenticated USING (app_private.can_edit_project(project_id));
GRANT SELECT,INSERT,UPDATE,DELETE ON public.project_trade_cycles TO authenticated;

-- Backfill Trade 1 for every existing project.
INSERT INTO public.project_trade_cycles(project_id, cycle_number, created_by)
SELECT p.id, 1, p.user_id
FROM public.projects p
ON CONFLICT (project_id, cycle_number) DO NOTHING;

-- New projects always receive Trade 1.
CREATE OR REPLACE FUNCTION public.tg_create_primary_trade_cycle()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $$
BEGIN
  INSERT INTO public.project_trade_cycles(project_id, cycle_number, created_by)
  VALUES (NEW.id, 1, COALESCE(auth.uid(), NEW.user_id))
  ON CONFLICT (project_id, cycle_number) DO NOTHING;
  RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS create_primary_trade_cycle ON public.projects;
CREATE TRIGGER create_primary_trade_cycle
AFTER INSERT ON public.projects
FOR EACH ROW EXECUTE FUNCTION public.tg_create_primary_trade_cycle();
REVOKE ALL ON FUNCTION public.tg_create_primary_trade_cycle() FROM PUBLIC, anon, authenticated;

CREATE OR REPLACE FUNCTION public.create_trade_cycle(_project_id uuid)
RETURNS SETOF public.project_trade_cycles
LANGUAGE plpgsql
SET search_path TO 'public','app_private'
AS $$
DECLARE _next integer; _row public.project_trade_cycles;
BEGIN
  IF NOT app_private.can_edit_project(_project_id) THEN
    RAISE EXCEPTION 'project edit denied';
  END IF;
  PERFORM 1 FROM public.projects WHERE id = _project_id FOR UPDATE;
  SELECT COALESCE(MAX(cycle_number),0) + 1 INTO _next
  FROM public.project_trade_cycles WHERE project_id = _project_id;
  INSERT INTO public.project_trade_cycles(project_id,cycle_number,created_by)
  VALUES (_project_id,_next,auth.uid())
  RETURNING * INTO _row;
  RETURN NEXT _row;
END;
$$;
GRANT EXECUTE ON FUNCTION public.create_trade_cycle(uuid) TO authenticated;

-- Add cycle identity to per-trade content.
ALTER TABLE public.project_answers ADD COLUMN IF NOT EXISTS trade_cycle_id uuid;
ALTER TABLE public.project_images ADD COLUMN IF NOT EXISTS trade_cycle_id uuid;
ALTER TABLE public.trades ADD COLUMN IF NOT EXISTS trade_cycle_id uuid;
ALTER TABLE public.question_user_checks ADD COLUMN IF NOT EXISTS trade_cycle_id uuid;
ALTER TABLE public.project_activity ADD COLUMN IF NOT EXISTS trade_cycle_id uuid;
ALTER TABLE public.project_question_seen ADD COLUMN IF NOT EXISTS trade_cycle_id uuid;
ALTER TABLE public.project_user_state ADD COLUMN IF NOT EXISTS last_trade_cycle_id uuid;
ALTER TABLE public.pinned_answers ADD COLUMN IF NOT EXISTS source_trade_cycle_id uuid;

-- Backward-compatible safety net: legacy client paths that omit trade_cycle_id
-- are assigned to Trade 1. Current multi-trade UI always sends an explicit cycle.
CREATE OR REPLACE FUNCTION public.tg_assign_primary_trade_cycle()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $$
BEGIN
  IF NEW.trade_cycle_id IS NULL THEN
    SELECT id INTO NEW.trade_cycle_id
    FROM public.project_trade_cycles
    WHERE project_id = NEW.project_id AND cycle_number = 1
    LIMIT 1;
  END IF;
  IF NEW.trade_cycle_id IS NULL THEN
    RAISE EXCEPTION 'primary trade cycle missing for project %', NEW.project_id;
  END IF;
  RETURN NEW;
END;
$$;
REVOKE ALL ON FUNCTION public.tg_assign_primary_trade_cycle() FROM PUBLIC, anon;

DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY['project_answers','project_images','trades','question_user_checks','project_activity','project_question_seen']
  LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS assign_primary_trade_cycle ON public.%I', t);
    EXECUTE format('CREATE TRIGGER assign_primary_trade_cycle BEFORE INSERT ON public.%I FOR EACH ROW EXECUTE FUNCTION public.tg_assign_primary_trade_cycle()', t);
  END LOOP;
END $$;

-- Existing records belong to Trade 1.
UPDATE public.project_answers a SET trade_cycle_id = c.id
FROM public.project_trade_cycles c
WHERE c.project_id=a.project_id AND c.cycle_number=1 AND a.trade_cycle_id IS NULL;
UPDATE public.project_images a SET trade_cycle_id = c.id
FROM public.project_trade_cycles c
WHERE c.project_id=a.project_id AND c.cycle_number=1 AND a.trade_cycle_id IS NULL;
UPDATE public.trades a SET trade_cycle_id = c.id
FROM public.project_trade_cycles c
WHERE c.project_id=a.project_id AND c.cycle_number=1 AND a.trade_cycle_id IS NULL;
UPDATE public.question_user_checks a SET trade_cycle_id = c.id
FROM public.project_trade_cycles c
WHERE c.project_id=a.project_id AND c.cycle_number=1 AND a.trade_cycle_id IS NULL;
UPDATE public.project_activity a SET trade_cycle_id = c.id
FROM public.project_trade_cycles c
WHERE c.project_id=a.project_id AND c.cycle_number=1 AND a.trade_cycle_id IS NULL;
UPDATE public.project_question_seen a SET trade_cycle_id = c.id
FROM public.project_trade_cycles c
WHERE c.project_id=a.project_id AND c.cycle_number=1 AND a.trade_cycle_id IS NULL;
UPDATE public.project_user_state a SET last_trade_cycle_id = c.id
FROM public.project_trade_cycles c
WHERE c.project_id=a.project_id AND c.cycle_number=1 AND a.last_trade_cycle_id IS NULL;
UPDATE public.pinned_answers a SET source_trade_cycle_id = c.id
FROM public.project_trade_cycles c
WHERE c.project_id=a.source_project_id AND c.cycle_number=1 AND a.source_project_id IS NOT NULL AND a.source_trade_cycle_id IS NULL;

ALTER TABLE public.project_answers ALTER COLUMN trade_cycle_id SET NOT NULL;
ALTER TABLE public.project_images ALTER COLUMN trade_cycle_id SET NOT NULL;
ALTER TABLE public.trades ALTER COLUMN trade_cycle_id SET NOT NULL;
ALTER TABLE public.question_user_checks ALTER COLUMN trade_cycle_id SET NOT NULL;
ALTER TABLE public.project_activity ALTER COLUMN trade_cycle_id SET NOT NULL;
ALTER TABLE public.project_question_seen ALTER COLUMN trade_cycle_id SET NOT NULL;

DO $$ BEGIN
  ALTER TABLE public.project_answers ADD CONSTRAINT project_answers_trade_cycle_id_fkey FOREIGN KEY (trade_cycle_id) REFERENCES public.project_trade_cycles(id) ON DELETE CASCADE;
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN
  ALTER TABLE public.project_images ADD CONSTRAINT project_images_trade_cycle_id_fkey FOREIGN KEY (trade_cycle_id) REFERENCES public.project_trade_cycles(id) ON DELETE CASCADE;
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN
  ALTER TABLE public.trades ADD CONSTRAINT trades_trade_cycle_id_fkey FOREIGN KEY (trade_cycle_id) REFERENCES public.project_trade_cycles(id) ON DELETE CASCADE;
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN
  ALTER TABLE public.question_user_checks ADD CONSTRAINT question_user_checks_trade_cycle_id_fkey FOREIGN KEY (trade_cycle_id) REFERENCES public.project_trade_cycles(id) ON DELETE CASCADE;
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN
  ALTER TABLE public.project_activity ADD CONSTRAINT project_activity_trade_cycle_id_fkey FOREIGN KEY (trade_cycle_id) REFERENCES public.project_trade_cycles(id) ON DELETE CASCADE;
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN
  ALTER TABLE public.project_question_seen ADD CONSTRAINT project_question_seen_trade_cycle_id_fkey FOREIGN KEY (trade_cycle_id) REFERENCES public.project_trade_cycles(id) ON DELETE CASCADE;
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN
  ALTER TABLE public.project_user_state ADD CONSTRAINT project_user_state_last_trade_cycle_id_fkey FOREIGN KEY (last_trade_cycle_id) REFERENCES public.project_trade_cycles(id) ON DELETE SET NULL;
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN
  ALTER TABLE public.pinned_answers ADD CONSTRAINT pinned_answers_source_trade_cycle_id_fkey FOREIGN KEY (source_trade_cycle_id) REFERENCES public.project_trade_cycles(id) ON DELETE SET NULL;
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

ALTER TABLE public.project_answers DROP CONSTRAINT IF EXISTS project_answers_project_id_question_id_key;
ALTER TABLE public.project_answers ADD CONSTRAINT project_answers_project_question_cycle_key UNIQUE(project_id,question_id,trade_cycle_id);
ALTER TABLE public.trades DROP CONSTRAINT IF EXISTS trades_project_id_key;
ALTER TABLE public.trades ADD CONSTRAINT trades_project_cycle_key UNIQUE(project_id,trade_cycle_id);
ALTER TABLE public.question_user_checks DROP CONSTRAINT IF EXISTS question_user_checks_project_id_question_id_user_id_key;
ALTER TABLE public.question_user_checks ADD CONSTRAINT question_user_checks_project_question_user_cycle_key UNIQUE(project_id,question_id,user_id,trade_cycle_id);
ALTER TABLE public.project_question_seen DROP CONSTRAINT IF EXISTS project_question_seen_pkey;
ALTER TABLE public.project_question_seen ADD CONSTRAINT project_question_seen_pkey PRIMARY KEY(user_id,project_id,question_id,trade_cycle_id);

CREATE INDEX IF NOT EXISTS idx_answers_project_cycle ON public.project_answers(project_id,trade_cycle_id);
CREATE INDEX IF NOT EXISTS idx_images_project_cycle ON public.project_images(project_id,trade_cycle_id);
CREATE INDEX IF NOT EXISTS idx_activity_project_cycle ON public.project_activity(project_id,trade_cycle_id,created_at DESC);

-- Activity logger with cycle awareness. The 3-argument version remains for shared structured
-- data and resolves to Trade 1 automatically.
CREATE OR REPLACE FUNCTION app_private.log_activity(_project_id uuid, _question_id uuid, _kind text, _trade_cycle_id uuid)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public','app_private'
AS $$
DECLARE
  _actor uuid := auth.uid();
  _group uuid;
  _stage uuid;
  _key text;
  _cycle uuid := _trade_cycle_id;
BEGIN
  IF _actor IS NULL THEN RETURN; END IF;
  IF _cycle IS NULL THEN
    SELECT id INTO _cycle FROM public.project_trade_cycles WHERE project_id=_project_id AND cycle_number=1;
  END IF;
  IF _cycle IS NULL THEN RETURN; END IF;

  IF EXISTS (
    SELECT 1 FROM public.project_activity a
    WHERE a.project_id=_project_id
      AND a.question_id IS NOT DISTINCT FROM _question_id
      AND a.trade_cycle_id=_cycle
      AND a.actor_id=_actor
      AND a.created_at > now() - interval '90 seconds'
  ) THEN
    UPDATE public.project_activity SET created_at=now(), kind=COALESCE(_kind,'answer')
    WHERE id=(
      SELECT id FROM public.project_activity a
      WHERE a.project_id=_project_id
        AND a.question_id IS NOT DISTINCT FROM _question_id
        AND a.trade_cycle_id=_cycle
        AND a.actor_id=_actor
      ORDER BY a.created_at DESC LIMIT 1
    );
    RETURN;
  END IF;

  SELECT q.group_id,g.stage_id,COALESCE(q.question_key,q.field_key)
    INTO _group,_stage,_key
  FROM public.template_questions q
  JOIN public.template_groups g ON g.id=q.group_id
  WHERE q.id=_question_id;

  INSERT INTO public.project_activity(project_id,question_id,group_id,stage_id,question_key,actor_id,kind,trade_cycle_id)
  VALUES(_project_id,_question_id,_group,_stage,_key,_actor,COALESCE(_kind,'answer'),_cycle);
END;
$$;
REVOKE ALL ON FUNCTION app_private.log_activity(uuid,uuid,text,uuid) FROM PUBLIC,anon;
GRANT EXECUTE ON FUNCTION app_private.log_activity(uuid,uuid,text,uuid) TO authenticated;

CREATE OR REPLACE FUNCTION app_private.log_activity(_project_id uuid, _question_id uuid, _kind text)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public','app_private'
AS $$
DECLARE _cycle uuid;
BEGIN
  SELECT id INTO _cycle FROM public.project_trade_cycles WHERE project_id=_project_id AND cycle_number=1;
  PERFORM app_private.log_activity(_project_id,_question_id,_kind,_cycle);
END;
$$;

CREATE OR REPLACE FUNCTION public.tg_answer_activity()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'public','app_private'
AS $$
BEGIN
  IF TG_OP='UPDATE' AND NOT (
    NEW.text_value IS DISTINCT FROM OLD.text_value OR
    NEW.number_value IS DISTINCT FROM OLD.number_value OR
    NEW.boolean_value IS DISTINCT FROM OLD.boolean_value OR
    NEW.choice_value_json IS DISTINCT FROM OLD.choice_value_json OR
    NEW.date_value IS DISTINCT FROM OLD.date_value OR
    NEW.not_relevant IS DISTINCT FROM OLD.not_relevant
  ) THEN RETURN NEW; END IF;
  PERFORM app_private.log_activity(NEW.project_id,NEW.question_id,'answer',NEW.trade_cycle_id);
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION public.tg_image_activity()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'public','app_private'
AS $$
BEGIN
  IF TG_OP='DELETE' THEN
    PERFORM app_private.log_activity(OLD.project_id,OLD.question_id,'image',OLD.trade_cycle_id); RETURN OLD;
  END IF;
  PERFORM app_private.log_activity(NEW.project_id,NEW.question_id,'image',NEW.trade_cycle_id); RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION public.tg_check_activity()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'public','app_private'
AS $$
BEGIN
  IF TG_OP='DELETE' THEN
    PERFORM app_private.log_activity(OLD.project_id,OLD.question_id,'check',OLD.trade_cycle_id); RETURN OLD;
  END IF;
  PERFORM app_private.log_activity(NEW.project_id,NEW.question_id,'check',NEW.trade_cycle_id); RETURN NEW;
END;
$$;

-- Store both last base-stage and last trade cycle.
CREATE OR REPLACE FUNCTION public.open_project(_project_id uuid, _stage_id uuid, _trade_cycle_id uuid)
RETURNS timestamptz
LANGUAGE plpgsql
SET search_path TO 'public','app_private'
AS $$
DECLARE _uid uuid:=auth.uid(); _first timestamptz; _cycle uuid:=_trade_cycle_id;
BEGIN
  IF _uid IS NULL THEN RETURN NULL; END IF;
  IF NOT app_private.can_access_project(_project_id) THEN RAISE EXCEPTION 'project access denied'; END IF;
  IF _cycle IS NULL THEN SELECT id INTO _cycle FROM public.project_trade_cycles WHERE project_id=_project_id AND cycle_number=1; END IF;
  INSERT INTO public.project_user_state(user_id,project_id,last_stage_id,last_trade_cycle_id,first_opened_at)
  VALUES(_uid,_project_id,_stage_id,_cycle,now())
  ON CONFLICT(user_id,project_id) DO UPDATE SET
    last_stage_id=COALESCE(EXCLUDED.last_stage_id,public.project_user_state.last_stage_id),
    last_trade_cycle_id=COALESCE(EXCLUDED.last_trade_cycle_id,public.project_user_state.last_trade_cycle_id),
    first_opened_at=COALESCE(public.project_user_state.first_opened_at,now())
  RETURNING first_opened_at INTO _first;
  RETURN _first;
END;
$$;
GRANT EXECUTE ON FUNCTION public.open_project(uuid,uuid,uuid) TO authenticated;

-- Cycle-aware unread and latest-activity APIs.
DROP FUNCTION IF EXISTS public.my_unread_questions(uuid);
CREATE FUNCTION public.my_unread_questions(_project_id uuid DEFAULT NULL)
RETURNS TABLE(project_id uuid,stage_id uuid,group_id uuid,question_id uuid,question_key text,actor_id uuid,kind text,trade_cycle_id uuid,latest_at timestamptz)
LANGUAGE sql STABLE SET search_path TO 'public'
AS $$
  SELECT DISTINCT ON (a.project_id,a.question_id,a.trade_cycle_id)
    a.project_id,a.stage_id,a.group_id,a.question_id,a.question_key,a.actor_id,a.kind,a.trade_cycle_id,a.created_at
  FROM public.project_activity a
  JOIN public.project_user_state s ON s.project_id=a.project_id AND s.user_id=auth.uid()
  LEFT JOIN public.project_question_seen v
    ON v.project_id=a.project_id AND v.question_id=a.question_id
   AND v.trade_cycle_id=a.trade_cycle_id AND v.user_id=auth.uid()
  WHERE auth.uid() IS NOT NULL
    AND a.actor_id IS NOT NULL AND a.actor_id<>auth.uid()
    AND a.question_id IS NOT NULL
    AND s.first_opened_at IS NOT NULL AND a.created_at>s.first_opened_at
    AND (v.seen_at IS NULL OR a.created_at>v.seen_at)
    AND (_project_id IS NULL OR a.project_id=_project_id)
  ORDER BY a.project_id,a.question_id,a.trade_cycle_id,a.created_at DESC
$$;
GRANT EXECUTE ON FUNCTION public.my_unread_questions(uuid) TO authenticated;

DROP FUNCTION IF EXISTS public.project_latest_activity(uuid);
CREATE FUNCTION public.project_latest_activity(_project_id uuid)
RETURNS TABLE(question_id uuid,actor_id uuid,kind text,trade_cycle_id uuid,latest_at timestamptz)
LANGUAGE sql STABLE SET search_path TO 'public'
AS $$
  SELECT DISTINCT ON (a.question_id,a.trade_cycle_id)
    a.question_id,a.actor_id,a.kind,a.trade_cycle_id,a.created_at
  FROM public.project_activity a
  WHERE a.project_id=_project_id AND a.question_id IS NOT NULL
  ORDER BY a.question_id,a.trade_cycle_id,a.created_at DESC
$$;
GRANT EXECUTE ON FUNCTION public.project_latest_activity(uuid) TO authenticated;
