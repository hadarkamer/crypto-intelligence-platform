-- Supabase's default table privileges can grant writes despite SELECT-only RLS.
-- The browser roles must not hold table write privileges.
REVOKE ALL PRIVILEGES ON TABLE public.v2_automated_trade_cards FROM anon, authenticated;
GRANT SELECT ON TABLE public.v2_automated_trade_cards TO authenticated;
