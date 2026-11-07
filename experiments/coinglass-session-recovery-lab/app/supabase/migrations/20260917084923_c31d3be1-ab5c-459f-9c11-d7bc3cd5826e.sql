REVOKE ALL PRIVILEGES ON TABLE public.v2_report_archives FROM authenticated, anon, PUBLIC;
GRANT SELECT ON TABLE public.v2_report_archives TO authenticated;
GRANT ALL PRIVILEGES ON TABLE public.v2_report_archives TO service_role;

REVOKE ALL ON FUNCTION public.v2_set_report_archive(uuid, text, text, boolean) FROM PUBLIC, anon;
GRANT EXECUTE ON FUNCTION public.v2_set_report_archive(uuid, text, text, boolean) TO authenticated, service_role;