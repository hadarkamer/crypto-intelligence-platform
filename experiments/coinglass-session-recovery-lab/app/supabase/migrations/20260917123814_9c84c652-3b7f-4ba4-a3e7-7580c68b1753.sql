REVOKE EXECUTE ON FUNCTION public.v2_save_manual_entries(uuid, uuid, timestamptz, text, text, text, jsonb, text, timestamptz) FROM anon;
REVOKE EXECUTE ON FUNCTION public.v2_save_manual_entries(uuid, uuid, timestamptz, text, text, text, jsonb, text, timestamptz) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.v2_save_manual_entries(uuid, uuid, timestamptz, text, text, text, jsonb, text, timestamptz) TO authenticated;
GRANT EXECUTE ON FUNCTION public.v2_save_manual_entries(uuid, uuid, timestamptz, text, text, text, jsonb, text, timestamptz) TO service_role;