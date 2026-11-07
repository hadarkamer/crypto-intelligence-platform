GRANT EXECUTE ON FUNCTION app_private.project_role(uuid) TO authenticated, service_role;
GRANT EXECUTE ON FUNCTION app_private.can_access_project(uuid) TO authenticated, service_role;
GRANT EXECUTE ON FUNCTION app_private.can_edit_project(uuid) TO authenticated, service_role;
REVOKE EXECUTE ON FUNCTION app_private.project_role(uuid) FROM anon, public;
REVOKE EXECUTE ON FUNCTION app_private.can_access_project(uuid) FROM anon, public;
REVOKE EXECUTE ON FUNCTION app_private.can_edit_project(uuid) FROM anon, public;