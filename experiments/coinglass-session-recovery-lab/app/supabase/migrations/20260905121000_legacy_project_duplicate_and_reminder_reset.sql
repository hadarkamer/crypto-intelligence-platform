-- Copy legacy projects atomically, including all their trade cycles and content.
-- Storage bytes stay immutable and are shared by reference. Only this RPC can
-- grant the copy access to a source path; editable image metadata is not a grant.
CREATE TABLE IF NOT EXISTS app_private.legacy_project_image_grants (
  project_id uuid NOT NULL REFERENCES public.projects(id) ON DELETE CASCADE,
  storage_path text NOT NULL,
  PRIMARY KEY (project_id, storage_path)
);
REVOKE ALL ON app_private.legacy_project_image_grants FROM PUBLIC, anon, authenticated;

CREATE OR REPLACE FUNCTION app_private.can_read_copied_legacy_image(_path text)
RETURNS boolean LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, public, app_private
AS $$
  SELECT auth.uid() IS NOT NULL AND EXISTS (
    SELECT 1 FROM app_private.legacy_project_image_grants g
    WHERE g.storage_path = _path AND app_private.can_access_project(g.project_id)
  );
$$;
REVOKE ALL ON FUNCTION app_private.can_read_copied_legacy_image(text) FROM PUBLIC, anon;
GRANT EXECUTE ON FUNCTION app_private.can_read_copied_legacy_image(text) TO authenticated;

-- The optional project exclusion is used only for inline cleanup: its own
-- metadata must not prevent an inline image removed from HTML being cleaned up.
-- Other projects, ordinary attachments, originals and snapshots always count.
CREATE OR REPLACE FUNCTION app_private.legacy_image_is_referenced(
  _path text, _inline_cleanup_project uuid DEFAULT NULL
)
RETURNS boolean LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, public, app_private
AS $$
  SELECT _path IS NOT NULL AND _path <> '' AND (
    EXISTS (
      SELECT 1 FROM public.project_images i
      WHERE (i.storage_path = _path OR i.original_storage_path = _path)
        AND NOT (
          _inline_cleanup_project IS NOT NULL
          AND i.project_id = _inline_cleanup_project
          AND i.storage_path = _path AND strpos(_path, '-inline-') > 0
          AND i.original_storage_path IS DISTINCT FROM _path
        )
    )
    OR EXISTS (SELECT 1 FROM public.project_answers a
      WHERE strpos(coalesce(a.text_value, ''), _path) > 0
         OR strpos(coalesce(a.choice_value_json::text, ''), _path) > 0)
    OR EXISTS (SELECT 1 FROM public.projects p
      WHERE strpos(coalesce(p.scenario_ideas, ''), _path) > 0
         OR strpos(coalesce(p.intuition, ''), _path) > 0
         OR strpos(coalesce(p.initial_note, ''), _path) > 0)
    OR EXISTS (SELECT 1 FROM public.project_news_rows n WHERE strpos(n.body, _path) > 0)
    OR EXISTS (SELECT 1 FROM public.trades t WHERE strpos(coalesce(t.summary, ''), _path) > 0)
    OR EXISTS (SELECT 1 FROM public.pinned_answers p
      WHERE strpos(p.answer_snapshot::text, _path) > 0
         OR strpos(p.image_paths::text, _path) > 0)
  );
$$;
REVOKE ALL ON FUNCTION app_private.legacy_image_is_referenced(text, uuid) FROM PUBLIC, anon;
GRANT EXECUTE ON FUNCTION app_private.legacy_image_is_referenced(text, uuid) TO authenticated;

DROP POLICY IF EXISTS "read copied legacy project images" ON storage.objects;
CREATE POLICY "read copied legacy project images" ON storage.objects
FOR SELECT TO authenticated
USING (bucket_id = 'project-images' AND app_private.can_read_copied_legacy_image(name));

-- Restrictive policies also protect callers that bypass the UI cleanup helper.
-- Replacing an edited image uses a new random path, leaving referenced bytes intact.
DROP POLICY IF EXISTS "retain referenced legacy images on delete" ON storage.objects;
CREATE POLICY "retain referenced legacy images on delete" ON storage.objects
AS RESTRICTIVE FOR DELETE TO authenticated
USING (bucket_id <> 'project-images' OR NOT app_private.legacy_image_is_referenced(name));
DROP POLICY IF EXISTS "retain referenced legacy images on update" ON storage.objects;
CREATE POLICY "retain referenced legacy images on update" ON storage.objects
AS RESTRICTIVE FOR UPDATE TO authenticated
USING (bucket_id <> 'project-images' OR NOT app_private.legacy_image_is_referenced(name))
WITH CHECK (bucket_id <> 'project-images' OR NOT app_private.legacy_image_is_referenced(name));

CREATE OR REPLACE FUNCTION public.inline_paths_referenced(_project_id uuid, _paths text[])
RETURNS SETOF text LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, public, app_private
AS $$
  SELECT DISTINCT p FROM unnest(_paths) AS p
  WHERE auth.uid() IS NOT NULL AND app_private.can_access_project(_project_id)
    AND app_private.legacy_image_is_referenced(p, _project_id);
$$;
REVOKE ALL ON FUNCTION public.inline_paths_referenced(uuid, text[]) FROM PUBLIC, anon;
GRANT EXECUTE ON FUNCTION public.inline_paths_referenced(uuid, text[]) TO authenticated;

-- Keep ordinary activity logging unchanged while allowing parent cascades.
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
  -- Cascading child deletes run after their project row has gone. Recording
  -- activity at that point would recreate an invalid child and block deletion.
  IF NOT EXISTS (SELECT 1 FROM public.projects WHERE id = _project_id) THEN
    RETURN;
  END IF;
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

CREATE OR REPLACE FUNCTION public.duplicate_legacy_project(_project_id uuid)
RETURNS uuid LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, public, app_private
AS $$
DECLARE
  _actor uuid := auth.uid();
  _source public.projects;
  _snapshot jsonb;
  _copy uuid;
  _cycle public.project_trade_cycles;
  _new_cycle uuid;
  _cycle_map jsonb := '{}'::jsonb;
  _answer public.project_answers;
  _image public.project_images;
  _trade public.trades;
  _row public.project_liquidity_rows;
  _new_row uuid;
  _path text;
  _folder text;
BEGIN
  IF _actor IS NULL OR NOT coalesce(app_private.can_access_project(_project_id), false) THEN
    RAISE EXCEPTION 'project access denied' USING ERRCODE = '42501';
  END IF;
  SELECT * INTO _source FROM public.projects WHERE id = _project_id FOR SHARE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'project not found' USING ERRCODE = 'P0002';
  END IF;

  -- Read every source collection in one statement snapshot. Concurrent edits
  -- cannot produce a copy assembled from different versions of its children.
  SELECT jsonb_build_object(
    'project', to_jsonb(p),
    'cycles', (SELECT coalesce(jsonb_agg(to_jsonb(c) ORDER BY c.cycle_number), '[]') FROM public.project_trade_cycles c WHERE c.project_id = p.id),
    'answers', (SELECT coalesce(jsonb_agg(to_jsonb(a)), '[]') FROM public.project_answers a WHERE a.project_id = p.id),
    'images', (SELECT coalesce(jsonb_agg(to_jsonb(i)), '[]') FROM public.project_images i WHERE i.project_id = p.id),
    'trades', (SELECT coalesce(jsonb_agg(to_jsonb(t)), '[]') FROM public.trades t WHERE t.project_id = p.id),
    'liquidity_rows', (SELECT coalesce(jsonb_agg(to_jsonb(r)), '[]') FROM public.project_liquidity_rows r WHERE r.project_id = p.id),
    'liquidity_levels', (SELECT coalesce(jsonb_agg(to_jsonb(l)), '[]') FROM public.project_liquidity_levels l WHERE l.project_id = p.id),
    'news', (SELECT coalesce(jsonb_agg(to_jsonb(n)), '[]') FROM public.project_news_rows n WHERE n.project_id = p.id)
  ) INTO _snapshot FROM public.projects p WHERE p.id = _project_id;

  INSERT INTO public.projects (
    name, symbol, direction, timeframe, opportunity_source, initial_note,
    template_version_id, scenario_ideas, intuition, status, user_id, last_edited_by
  ) VALUES (
    _source.name || ' (עותק)', _source.symbol, _source.direction, _source.timeframe,
    _source.opportunity_source, _source.initial_note, _source.template_version_id,
    _source.scenario_ideas, _source.intuition, 'draft', _actor, _actor
  ) RETURNING id INTO _copy;

  -- The projects INSERT trigger already created cycle 1. Reuse it, retaining
  -- source cycle numbers and assigning new ids to every copied relationship.
  FOR _cycle IN SELECT * FROM jsonb_populate_recordset(NULL::public.project_trade_cycles, _snapshot->'cycles') LOOP
    INSERT INTO public.project_trade_cycles(project_id, cycle_number, created_by)
    VALUES (_copy, _cycle.cycle_number, _actor)
    ON CONFLICT (project_id, cycle_number) DO UPDATE SET created_by = EXCLUDED.created_by
    RETURNING id INTO _new_cycle;
    _cycle_map := _cycle_map || jsonb_build_object(_cycle.id::text, _new_cycle);
  END LOOP;

  FOR _answer IN SELECT * FROM jsonb_populate_recordset(NULL::public.project_answers, _snapshot->'answers') LOOP
    _new_cycle := (_cycle_map->>_answer.trade_cycle_id::text)::uuid;
    IF _new_cycle IS NULL THEN RAISE EXCEPTION 'source answer has invalid trade cycle' USING ERRCODE = '23503'; END IF;
    INSERT INTO public.project_answers(project_id, trade_cycle_id, question_id, text_value, number_value, boolean_value, choice_value_json, date_value, not_relevant, last_edited_by)
    VALUES (_copy, _new_cycle, _answer.question_id, _answer.text_value, _answer.number_value, _answer.boolean_value, _answer.choice_value_json, _answer.date_value, _answer.not_relevant, _actor);
  END LOOP;

  FOR _image IN SELECT * FROM jsonb_populate_recordset(NULL::public.project_images, _snapshot->'images') LOOP
    _new_cycle := (_cycle_map->>_image.trade_cycle_id::text)::uuid;
    IF _new_cycle IS NULL THEN RAISE EXCEPTION 'source image has invalid trade cycle' USING ERRCODE = '23503'; END IF;
    INSERT INTO public.project_images(project_id, trade_cycle_id, question_id, storage_path, original_storage_path, file_name, mime_type, caption, description)
    VALUES (_copy, _new_cycle, _image.question_id, _image.storage_path, _image.original_storage_path, _image.file_name, _image.mime_type, _image.caption, _image.description);
  END LOOP;

  FOR _trade IN SELECT * FROM jsonb_populate_recordset(NULL::public.trades, _snapshot->'trades') LOOP
    _new_cycle := (_cycle_map->>_trade.trade_cycle_id::text)::uuid;
    IF _new_cycle IS NULL THEN RAISE EXCEPTION 'source trade has invalid trade cycle' USING ERRCODE = '23503'; END IF;
    INSERT INTO public.trades(project_id, trade_cycle_id, symbol, direction, planned_entry, actual_entry, stop_loss, take_profit_json, position_size, opened_at, closed_at, exit_price, pnl_amount, pnl_percent, result, summary)
    VALUES (_copy, _new_cycle, _trade.symbol, _trade.direction, _trade.planned_entry, _trade.actual_entry, _trade.stop_loss, _trade.take_profit_json, _trade.position_size, _trade.opened_at, _trade.closed_at, _trade.exit_price, _trade.pnl_amount, _trade.pnl_percent, _trade.result, _trade.summary);
  END LOOP;
  -- A project without trade rows still gets a usable blank trade in cycle 1.
  IF jsonb_array_length(_snapshot->'trades') = 0 THEN
    INSERT INTO public.trades(project_id, trade_cycle_id, symbol)
    SELECT _copy, c.id, _source.symbol FROM public.project_trade_cycles c
    WHERE c.project_id = _copy AND c.cycle_number = 1;
  END IF;

  FOR _row IN SELECT * FROM jsonb_populate_recordset(NULL::public.project_liquidity_rows, _snapshot->'liquidity_rows') LOOP
    INSERT INTO public.project_liquidity_rows(project_id, question_id, timeframe, position, updated_by)
    VALUES (_copy, _row.question_id, _row.timeframe, _row.position, _actor)
    RETURNING id INTO _new_row;
    INSERT INTO public.project_liquidity_levels(project_id, row_id, side, position, price_text, amount, taken, updated_by)
    SELECT _copy, _new_row, l.side, l.position, l.price_text, l.amount, l.taken, _actor
    FROM jsonb_populate_recordset(NULL::public.project_liquidity_levels, _snapshot->'liquidity_levels') l
    WHERE l.row_id = _row.id;
  END LOOP;
  INSERT INTO public.project_news_rows(project_id, question_id, position, body, importance, updated_by)
  SELECT _copy, n.question_id, n.position, n.body, n.importance, _actor
  FROM jsonb_populate_recordset(NULL::public.project_news_rows, _snapshot->'news') n;

  -- Grant only paths the actor could already read, including rich-text images
  -- without metadata. Do not trust a caller-editable storage_path as authority.
  FOR _path IN
    SELECT DISTINCT path FROM (
      SELECT i.storage_path AS path FROM jsonb_populate_recordset(NULL::public.project_images, _snapshot->'images') i
      UNION ALL
      SELECT i.original_storage_path FROM jsonb_populate_recordset(NULL::public.project_images, _snapshot->'images') i
      UNION ALL
      SELECT m[1] FROM regexp_matches(replace(_snapshot::text, E'\\"', '"'), $re$data-path\s*=\s*["']([^"']+)["']$re$, 'gi') m
    ) paths WHERE path IS NOT NULL AND path <> ''
  LOOP
    _folder := split_part(_path, '/', 1);
    IF NOT (
      CASE WHEN _folder ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        THEN coalesce(app_private.can_access_project(_folder::uuid), false) ELSE false END
      OR app_private.can_read_copied_legacy_image(_path)
    ) THEN
      RAISE EXCEPTION 'source image access denied' USING ERRCODE = '42501';
    END IF;
    INSERT INTO app_private.legacy_project_image_grants(project_id, storage_path)
    VALUES (_copy, _path) ON CONFLICT DO NOTHING;
  END LOOP;
  -- Memberships, read/check state, activity history and pinned snapshots belong
  -- to their original users/project; they are not fabricated or rewritten.
  RETURN _copy;
END;
$$;
REVOKE ALL ON FUNCTION public.duplicate_legacy_project(uuid) FROM PUBLIC, anon;
GRANT EXECUTE ON FUNCTION public.duplicate_legacy_project(uuid) TO authenticated;

-- Moving an event creates a new reminder occurrence for every member. Editing
-- its name/importance or re-submitting the same instant keeps acknowledgements.
CREATE OR REPLACE FUNCTION public.tg_reschedule_economic_reminders()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
  IF NEW.event_at IS DISTINCT FROM OLD.event_at THEN
    DELETE FROM public.economic_reminder_acknowledgements a
    USING public.economic_event_reminders r
    WHERE a.reminder_id = r.id AND r.event_id = NEW.id;
    UPDATE public.economic_event_reminders
    SET due_at = NEW.event_at - make_interval(mins => offset_minutes), updated_at = now()
    WHERE event_id = NEW.id;
  END IF;
  RETURN NEW;
END;
$$;
REVOKE ALL ON FUNCTION public.tg_reschedule_economic_reminders() FROM PUBLIC, anon, authenticated;
