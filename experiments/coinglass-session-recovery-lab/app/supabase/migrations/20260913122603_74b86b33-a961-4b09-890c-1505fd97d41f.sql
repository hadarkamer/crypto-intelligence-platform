revoke execute on function public.v2_acquire_ai_lease(text, uuid, integer, integer) from anon;
revoke execute on function public.v2_release_ai_lease(text) from anon;
revoke execute on function public.v2_commit_liquidity_ai_run(uuid, uuid, timestamptz, jsonb, jsonb) from anon;
revoke execute on function public.v2_ai_liquidity_levels(text, jsonb) from anon;