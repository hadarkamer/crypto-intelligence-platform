CREATE OR REPLACE FUNCTION public.inline_paths_referenced(_project_id uuid, _paths text[])
RETURNS SETOF text
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public, app_private
AS $$
  SELECT p
  FROM unnest(_paths) AS p
  WHERE app_private.can_access_project(_project_id)
    AND (
      EXISTS (SELECT 1 FROM public.project_answers a WHERE a.text_value LIKE '%' || p || '%')
      OR EXISTS (SELECT 1 FROM public.projects pr WHERE pr.scenario_ideas LIKE '%' || p || '%')
      OR EXISTS (
        SELECT 1 FROM public.pinned_answers pa
        WHERE pa.answer_snapshot::text LIKE '%' || p || '%'
           OR pa.image_paths::text LIKE '%' || p || '%'
      )
    );
$$;

REVOKE ALL ON FUNCTION public.inline_paths_referenced(uuid, text[]) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.inline_paths_referenced(uuid, text[]) FROM anon;
GRANT EXECUTE ON FUNCTION public.inline_paths_referenced(uuid, text[]) TO authenticated;