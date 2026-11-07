CREATE OR REPLACE FUNCTION public.auto_share_project()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  INSERT INTO public.project_members (project_id, user_id, role)
  SELECT NEW.id, u, 'editor'
  FROM unnest(ARRAY['2fbb3051-058a-4912-a70c-5eca850d7606'::uuid,'de1cc656-32ec-4efe-b254-3d7ab4f36871'::uuid]) AS u
  WHERE u IS DISTINCT FROM NEW.user_id
  ON CONFLICT DO NOTHING;
  RETURN NEW;
END;
$$;

REVOKE EXECUTE ON FUNCTION public.auto_share_project() FROM PUBLIC, anon, authenticated;

DROP TRIGGER IF EXISTS t_auto_share_project ON public.projects;
CREATE TRIGGER t_auto_share_project
AFTER INSERT ON public.projects
FOR EACH ROW EXECUTE FUNCTION public.auto_share_project();