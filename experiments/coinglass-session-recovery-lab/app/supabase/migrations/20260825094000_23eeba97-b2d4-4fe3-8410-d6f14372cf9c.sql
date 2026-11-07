-- ============ Phase 4B: structured question types + intuition ============

-- 1) תיבת "אינטואיציה" ברמת הפרויקט
ALTER TABLE public.projects ADD COLUMN IF NOT EXISTS intuition text;

-- 2) עזר: עדכון חותמות זמן ומחבר
CREATE OR REPLACE FUNCTION public.tg_touch_row()
RETURNS trigger LANGUAGE plpgsql SET search_path TO 'public' AS $$
BEGIN
  NEW.updated_at = now();
  NEW.updated_by = COALESCE(auth.uid(), NEW.updated_by);
  RETURN NEW;
END; $$;

-- 3) טבלת נזילות — שורות לפי טווח זמן
CREATE TABLE public.project_liquidity_rows (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id uuid NOT NULL REFERENCES public.projects(id) ON DELETE CASCADE,
  question_id uuid NOT NULL REFERENCES public.template_questions(id) ON DELETE CASCADE,
  timeframe text NOT NULL CHECK (timeframe IN ('12H','24H','48H','3D','1W','2W','1M','3M','6M','1Y','2Y')),
  position integer NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  updated_by uuid DEFAULT auth.uid(),
  UNIQUE (project_id, question_id, timeframe)
);
GRANT SELECT, INSERT, UPDATE, DELETE ON public.project_liquidity_rows TO authenticated;
GRANT ALL ON public.project_liquidity_rows TO service_role;
ALTER TABLE public.project_liquidity_rows ENABLE ROW LEVEL SECURITY;
CREATE POLICY "read liquidity rows" ON public.project_liquidity_rows FOR SELECT TO authenticated
  USING (app_private.can_access_project(project_id));
CREATE POLICY "write liquidity rows" ON public.project_liquidity_rows FOR INSERT TO authenticated
  WITH CHECK (app_private.can_edit_project(project_id));
CREATE POLICY "update liquidity rows" ON public.project_liquidity_rows FOR UPDATE TO authenticated
  USING (app_private.can_edit_project(project_id))
  WITH CHECK (app_private.can_edit_project(project_id));
CREATE POLICY "delete liquidity rows" ON public.project_liquidity_rows FOR DELETE TO authenticated
  USING (app_private.can_edit_project(project_id));
CREATE INDEX idx_liq_rows_project ON public.project_liquidity_rows(project_id, question_id, position);

-- 4) רמות מחיר בכל שורה
CREATE TABLE public.project_liquidity_levels (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  row_id uuid NOT NULL REFERENCES public.project_liquidity_rows(id) ON DELETE CASCADE,
  project_id uuid NOT NULL REFERENCES public.projects(id) ON DELETE CASCADE,
  side text NOT NULL CHECK (side IN ('above','below')),
  position integer NOT NULL DEFAULT 0,
  price_text text NOT NULL DEFAULT '',
  amount text NOT NULL DEFAULT 'many' CHECK (amount IN ('many','normal','few')),
  taken boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  updated_by uuid DEFAULT auth.uid()
);
GRANT SELECT, INSERT, UPDATE, DELETE ON public.project_liquidity_levels TO authenticated;
GRANT ALL ON public.project_liquidity_levels TO service_role;
ALTER TABLE public.project_liquidity_levels ENABLE ROW LEVEL SECURITY;
CREATE POLICY "read liquidity levels" ON public.project_liquidity_levels FOR SELECT TO authenticated
  USING (app_private.can_access_project(project_id));
CREATE POLICY "write liquidity levels" ON public.project_liquidity_levels FOR INSERT TO authenticated
  WITH CHECK (app_private.can_edit_project(project_id));
CREATE POLICY "update liquidity levels" ON public.project_liquidity_levels FOR UPDATE TO authenticated
  USING (app_private.can_edit_project(project_id))
  WITH CHECK (app_private.can_edit_project(project_id));
CREATE POLICY "delete liquidity levels" ON public.project_liquidity_levels FOR DELETE TO authenticated
  USING (app_private.can_edit_project(project_id));
CREATE INDEX idx_liq_levels_row ON public.project_liquidity_levels(row_id, side, position);
CREATE INDEX idx_liq_levels_project ON public.project_liquidity_levels(project_id);

-- 5) טבלת חדשות
CREATE TABLE public.project_news_rows (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id uuid NOT NULL REFERENCES public.projects(id) ON DELETE CASCADE,
  question_id uuid NOT NULL REFERENCES public.template_questions(id) ON DELETE CASCADE,
  position integer NOT NULL DEFAULT 0,
  body text NOT NULL DEFAULT '',
  importance text NOT NULL DEFAULT 'neutral'
    CHECK (importance IN ('very_bullish','bullish','positive','neutral','negative','bearish','very_bearish')),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  updated_by uuid DEFAULT auth.uid()
);
GRANT SELECT, INSERT, UPDATE, DELETE ON public.project_news_rows TO authenticated;
GRANT ALL ON public.project_news_rows TO service_role;
ALTER TABLE public.project_news_rows ENABLE ROW LEVEL SECURITY;
CREATE POLICY "read news rows" ON public.project_news_rows FOR SELECT TO authenticated
  USING (app_private.can_access_project(project_id));
CREATE POLICY "write news rows" ON public.project_news_rows FOR INSERT TO authenticated
  WITH CHECK (app_private.can_edit_project(project_id));
CREATE POLICY "update news rows" ON public.project_news_rows FOR UPDATE TO authenticated
  USING (app_private.can_edit_project(project_id))
  WITH CHECK (app_private.can_edit_project(project_id));
CREATE POLICY "delete news rows" ON public.project_news_rows FOR DELETE TO authenticated
  USING (app_private.can_edit_project(project_id));
CREATE INDEX idx_news_rows_project ON public.project_news_rows(project_id, question_id, position);

-- 6) חותמות עדכון
CREATE TRIGGER t_liq_rows_touch BEFORE UPDATE ON public.project_liquidity_rows
  FOR EACH ROW EXECUTE FUNCTION public.tg_touch_row();
CREATE TRIGGER t_liq_levels_touch BEFORE UPDATE ON public.project_liquidity_levels
  FOR EACH ROW EXECUTE FUNCTION public.tg_touch_row();
CREATE TRIGGER t_news_rows_touch BEFORE UPDATE ON public.project_news_rows
  FOR EACH ROW EXECUTE FUNCTION public.tg_touch_row();

-- 7) רישום פעילות (עם ה-dedupe הקיים של 90 שניות)
CREATE OR REPLACE FUNCTION public.tg_structured_activity()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'public','app_private' AS $$
DECLARE _rec record; _qid uuid; _pid uuid;
BEGIN
  IF TG_OP = 'DELETE' THEN _rec := OLD; ELSE _rec := NEW; END IF;
  _pid := _rec.project_id;
  IF TG_TABLE_NAME = 'project_liquidity_levels' THEN
    SELECT r.question_id INTO _qid FROM public.project_liquidity_rows r WHERE r.id = _rec.row_id;
  ELSE
    _qid := _rec.question_id;
  END IF;
  IF _qid IS NOT NULL THEN
    PERFORM app_private.log_activity(_pid, _qid, 'answer');
  END IF;
  IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END; $$;

CREATE TRIGGER t_liq_rows_activity AFTER INSERT OR UPDATE OR DELETE ON public.project_liquidity_rows
  FOR EACH ROW EXECUTE FUNCTION public.tg_structured_activity();
CREATE TRIGGER t_liq_levels_activity AFTER INSERT OR UPDATE OR DELETE ON public.project_liquidity_levels
  FOR EACH ROW EXECUTE FUNCTION public.tg_structured_activity();
CREATE TRIGGER t_news_rows_activity AFTER INSERT OR UPDATE OR DELETE ON public.project_news_rows
  FOR EACH ROW EXECUTE FUNCTION public.tg_structured_activity();

-- 8) בדיקת הפניות לתמונות מוטמעות — כוללת גם את תיבת האינטואיציה
CREATE OR REPLACE FUNCTION public.inline_paths_referenced(_project_id uuid, _paths text[])
RETURNS SETOF text LANGUAGE sql STABLE SECURITY DEFINER SET search_path TO 'public','app_private' AS $$
  SELECT p
  FROM unnest(_paths) AS p
  WHERE app_private.can_access_project(_project_id)
    AND (
      EXISTS (SELECT 1 FROM public.project_answers a WHERE a.text_value LIKE '%' || p || '%')
      OR EXISTS (SELECT 1 FROM public.projects pr
                  WHERE pr.scenario_ideas LIKE '%' || p || '%'
                     OR pr.intuition LIKE '%' || p || '%')
      OR EXISTS (
        SELECT 1 FROM public.pinned_answers pa
        WHERE pa.answer_snapshot::text LIKE '%' || p || '%'
           OR pa.image_paths::text LIKE '%' || p || '%'
      )
    );
$$;