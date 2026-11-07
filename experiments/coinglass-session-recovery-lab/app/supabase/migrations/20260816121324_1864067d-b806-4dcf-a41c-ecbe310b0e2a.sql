GRANT SELECT, INSERT, UPDATE, DELETE ON public.user_preferences TO authenticated;
GRANT ALL ON public.user_preferences TO service_role;

GRANT SELECT, INSERT, UPDATE, DELETE ON public.pinned_answers TO authenticated;
GRANT ALL ON public.pinned_answers TO service_role;

GRANT SELECT, INSERT, UPDATE, DELETE ON public.project_members TO authenticated;
GRANT ALL ON public.project_members TO service_role;