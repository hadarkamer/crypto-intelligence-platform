CREATE OR REPLACE FUNCTION public.open_project(_project_id uuid, _stage_id uuid DEFAULT NULL::uuid)
 RETURNS timestamp with time zone
 LANGUAGE plpgsql
 SET search_path TO 'public', 'app_private'
AS $function$
DECLARE _uid uuid := auth.uid(); _first timestamptz;
BEGIN
  IF _uid IS NULL THEN RETURN NULL; END IF;
  IF NOT app_private.can_access_project(_project_id) THEN
    RAISE EXCEPTION 'project access denied';
  END IF;
  INSERT INTO public.project_user_state (user_id, project_id, last_stage_id, first_opened_at)
  VALUES (_uid, _project_id, _stage_id, now())
  ON CONFLICT (user_id, project_id) DO UPDATE
    SET last_stage_id = COALESCE(EXCLUDED.last_stage_id, public.project_user_state.last_stage_id),
        first_opened_at = COALESCE(public.project_user_state.first_opened_at, now())
  RETURNING first_opened_at INTO _first;
  RETURN _first;
END; $function$;

REVOKE ALL ON FUNCTION public.open_project(uuid, uuid) FROM PUBLIC, anon;
GRANT EXECUTE ON FUNCTION public.open_project(uuid, uuid) TO authenticated;