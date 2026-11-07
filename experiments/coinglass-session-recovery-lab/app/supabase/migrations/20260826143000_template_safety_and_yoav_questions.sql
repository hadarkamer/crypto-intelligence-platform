-- One-time workflow additions + hard protection for the published template.
-- The two new questions are intentionally added to the currently published version so
-- existing projects using that version receive them. After that, active-template content
-- becomes immutable and future edits must happen in a draft.

DO $$
DECLARE
  _active_version uuid;
  _market_stage uuid;
  _news_group uuid;
  _target_group uuid;
  _after_position integer;
  _new_position integer;
BEGIN
  SELECT active_template_version_id INTO _active_version
  FROM public.app_settings
  ORDER BY id
  LIMIT 1;

  IF _active_version IS NULL THEN
    RETURN;
  END IF;

  -- Normalize the historical duplicate-active condition before installing the unique index.
  UPDATE public.template_versions
     SET status = 'archived'
   WHERE status = 'active'
     AND id <> _active_version;
  UPDATE public.template_versions
     SET status = 'active'
   WHERE id = _active_version;

  SELECT id INTO _market_stage
  FROM public.template_stages
  WHERE template_version_id = _active_version
    AND title = 'סריקת שוק'
  ORDER BY position
  LIMIT 1;

  IF _market_stage IS NOT NULL THEN
    SELECT id INTO _news_group
    FROM public.template_groups
    WHERE stage_id = _market_stage
      AND title = 'חדשות'
    ORDER BY position
    LIMIT 1;

    IF _news_group IS NOT NULL
       AND NOT EXISTS (
         SELECT 1 FROM public.template_questions
         WHERE group_id = _news_group
           AND question_key = 'market_scan_crypto_university_saturday_post'
       ) THEN
      SELECT position INTO _after_position
      FROM public.template_questions
      WHERE group_id = _news_group
        AND title = 'תקציר סמוחה'
      ORDER BY position
      LIMIT 1;

      IF _after_position IS NULL THEN
        SELECT COALESCE(MAX(position), 0) INTO _after_position
        FROM public.template_questions
        WHERE group_id = _news_group;
      END IF;

      _new_position := _after_position + 1;
      UPDATE public.template_questions
         SET position = position + 1
       WHERE group_id = _news_group
         AND position >= _new_position;

      INSERT INTO public.template_questions (
        group_id, title, question_type, position, required, active,
        question_key, default_text_align
      ) VALUES (
        _news_group,
        'פוסט שבת באוניברסיטת הקריפטו + אווירה שם',
        'long_text',
        _new_position,
        false,
        true,
        'market_scan_crypto_university_saturday_post',
        'right'
      );
    END IF;

    -- Last box of Market Scan, immediately before the Trade Scan sheet.
    SELECT id INTO _target_group
    FROM public.template_groups
    WHERE stage_id = _market_stage
      AND active = true
    ORDER BY position DESC
    LIMIT 1;

    IF _target_group IS NOT NULL
       AND NOT EXISTS (
         SELECT 1 FROM public.template_questions
         WHERE group_id = _target_group
           AND question_key = 'market_scan_concrete_scenarios'
       ) THEN
      SELECT COALESCE(MAX(position), 0) + 1 INTO _new_position
      FROM public.template_questions
      WHERE group_id = _target_group;

      INSERT INTO public.template_questions (
        group_id, title, question_type, position, required, active,
        question_key, default_text_align
      ) VALUES (
        _target_group,
        'מה יכול להיות כשמשער מה קורה? תרחישים ממש',
        'long_text',
        _new_position,
        false,
        true,
        'market_scan_concrete_scenarios',
        'right'
      );
    END IF;
  END IF;
END $$;

-- Only one active version per template.
CREATE UNIQUE INDEX IF NOT EXISTS ux_template_versions_one_active
  ON public.template_versions(template_id)
  WHERE status = 'active';

-- Active version content is immutable at the database layer.
CREATE OR REPLACE FUNCTION public.tg_guard_active_template_stage()
RETURNS trigger
LANGUAGE plpgsql
SET search_path TO 'public'
AS $$
DECLARE
  _active uuid;
  _old_version uuid;
  _new_version uuid;
BEGIN
  SELECT active_template_version_id INTO _active FROM public.app_settings ORDER BY id LIMIT 1;
  IF TG_OP <> 'INSERT' THEN _old_version := OLD.template_version_id; END IF;
  IF TG_OP <> 'DELETE' THEN _new_version := NEW.template_version_id; END IF;
  IF _active IS NOT NULL AND (_old_version = _active OR _new_version = _active) THEN
    RAISE EXCEPTION 'active template is immutable; create a draft first';
  END IF;
  RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END;
$$;

CREATE OR REPLACE FUNCTION public.tg_guard_active_template_group()
RETURNS trigger
LANGUAGE plpgsql
SET search_path TO 'public'
AS $$
DECLARE
  _active uuid;
  _old_version uuid;
  _new_version uuid;
BEGIN
  SELECT active_template_version_id INTO _active FROM public.app_settings ORDER BY id LIMIT 1;
  IF TG_OP <> 'INSERT' THEN
    SELECT template_version_id INTO _old_version FROM public.template_stages WHERE id = OLD.stage_id;
  END IF;
  IF TG_OP <> 'DELETE' THEN
    SELECT template_version_id INTO _new_version FROM public.template_stages WHERE id = NEW.stage_id;
  END IF;
  IF _active IS NOT NULL AND (_old_version = _active OR _new_version = _active) THEN
    RAISE EXCEPTION 'active template is immutable; create a draft first';
  END IF;
  RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END;
$$;

CREATE OR REPLACE FUNCTION public.tg_guard_active_template_question()
RETURNS trigger
LANGUAGE plpgsql
SET search_path TO 'public'
AS $$
DECLARE
  _active uuid;
  _old_version uuid;
  _new_version uuid;
BEGIN
  SELECT active_template_version_id INTO _active FROM public.app_settings ORDER BY id LIMIT 1;
  IF TG_OP <> 'INSERT' THEN
    SELECT s.template_version_id INTO _old_version
    FROM public.template_groups g JOIN public.template_stages s ON s.id = g.stage_id
    WHERE g.id = OLD.group_id;
  END IF;
  IF TG_OP <> 'DELETE' THEN
    SELECT s.template_version_id INTO _new_version
    FROM public.template_groups g JOIN public.template_stages s ON s.id = g.stage_id
    WHERE g.id = NEW.group_id;
  END IF;
  IF _active IS NOT NULL AND (_old_version = _active OR _new_version = _active) THEN
    RAISE EXCEPTION 'active template is immutable; create a draft first';
  END IF;
  RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END;
$$;

DROP TRIGGER IF EXISTS guard_active_template_stage ON public.template_stages;
CREATE TRIGGER guard_active_template_stage
BEFORE INSERT OR UPDATE OR DELETE ON public.template_stages
FOR EACH ROW EXECUTE FUNCTION public.tg_guard_active_template_stage();

DROP TRIGGER IF EXISTS guard_active_template_group ON public.template_groups;
CREATE TRIGGER guard_active_template_group
BEFORE INSERT OR UPDATE OR DELETE ON public.template_groups
FOR EACH ROW EXECUTE FUNCTION public.tg_guard_active_template_group();

DROP TRIGGER IF EXISTS guard_active_template_question ON public.template_questions;
CREATE TRIGGER guard_active_template_question
BEFORE INSERT OR UPDATE OR DELETE ON public.template_questions
FOR EACH ROW EXECUTE FUNCTION public.tg_guard_active_template_question();

-- Atomic section copy: a PostgreSQL function runs as one transaction, so a failed child insert
-- cannot leave a half-created section behind.
CREATE OR REPLACE FUNCTION public.copy_template_group_atomic(
  _source_group_id uuid,
  _target_stage_id uuid,
  _title text,
  _keep_history_key boolean DEFAULT false
)
RETURNS uuid
LANGUAGE plpgsql
SET search_path TO 'public'
AS $$
DECLARE
  _new_group_id uuid;
  _next_position integer;
  _target_version uuid;
  _active_version uuid;
BEGIN
  SELECT template_version_id INTO _target_version
  FROM public.template_stages
  WHERE id = _target_stage_id;
  IF _target_version IS NULL THEN RAISE EXCEPTION 'target stage not found'; END IF;

  SELECT active_template_version_id INTO _active_version FROM public.app_settings ORDER BY id LIMIT 1;
  IF _target_version = _active_version THEN
    RAISE EXCEPTION 'active template is immutable; create a draft first';
  END IF;

  SELECT COALESCE(MAX(position), 0) + 1 INTO _next_position
  FROM public.template_groups WHERE stage_id = _target_stage_id;

  INSERT INTO public.template_groups(stage_id,title,description,position,collapsible,active,behavior)
  SELECT _target_stage_id,
         COALESCE(NULLIF(btrim(_title), ''), title),
         description,
         _next_position,
         collapsible,
         active,
         behavior
  FROM public.template_groups
  WHERE id = _source_group_id
  RETURNING id INTO _new_group_id;

  IF _new_group_id IS NULL THEN RAISE EXCEPTION 'source group not found'; END IF;

  INSERT INTO public.template_questions(
    group_id,title,description,question_type,position,required,active,
    options_json,visibility_rule_json,help_text,source_url,links_json,
    field_key,question_key,default_text_align
  )
  SELECT _new_group_id,title,description,question_type,position,required,active,
         options_json,visibility_rule_json,help_text,source_url,links_json,
         CASE WHEN _keep_history_key THEN field_key ELSE NULL END,
         CASE WHEN _keep_history_key THEN question_key ELSE gen_random_uuid()::text END,
         default_text_align
  FROM public.template_questions
  WHERE group_id = _source_group_id
  ORDER BY position;

  RETURN _new_group_id;
END;
$$;

GRANT EXECUTE ON FUNCTION public.copy_template_group_atomic(uuid,uuid,text,boolean) TO authenticated;
