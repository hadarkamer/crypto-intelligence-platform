CREATE SCHEMA IF NOT EXISTS app_private;
GRANT USAGE ON SCHEMA app_private TO authenticated;

ALTER FUNCTION public.has_role(uuid, public.app_role) SET SCHEMA app_private;
ALTER FUNCTION public.owns_project(uuid) SET SCHEMA app_private;
ALTER FUNCTION public.claim_admin() SET SCHEMA app_private;

ALTER FUNCTION app_private.has_role(uuid, public.app_role) SET search_path = public, app_private;
ALTER FUNCTION app_private.owns_project(uuid) SET search_path = public, app_private;
ALTER FUNCTION app_private.claim_admin() SET search_path = public, app_private;

REVOKE ALL ON FUNCTION app_private.claim_admin() FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION app_private.has_role(uuid, public.app_role) FROM PUBLIC, anon;
REVOKE ALL ON FUNCTION app_private.owns_project(uuid) FROM PUBLIC, anon;
GRANT EXECUTE ON FUNCTION app_private.has_role(uuid, public.app_role) TO authenticated;
GRANT EXECUTE ON FUNCTION app_private.owns_project(uuid) TO authenticated;