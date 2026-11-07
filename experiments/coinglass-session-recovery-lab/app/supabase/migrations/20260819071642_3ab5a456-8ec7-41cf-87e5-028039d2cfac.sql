REVOKE ALL ON FUNCTION public.tg_answer_activity() FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.tg_image_activity() FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.tg_check_activity() FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.sync_my_profile() FROM PUBLIC, anon;
GRANT EXECUTE ON FUNCTION public.sync_my_profile() TO authenticated;