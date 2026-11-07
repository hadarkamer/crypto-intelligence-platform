revoke all on function public.tg_legacy_cycle_first_pnl() from public, anon, authenticated;
revoke all on function public.v2_trade_value_first_pnl_stamp() from public, anon, authenticated;
revoke all on function public.v2_trade_first_pnl_stamp() from public, anon, authenticated;
revoke all on function public.v2_sync_trade_pnl_value() from public, anon, authenticated;
drop function if exists public.v2_save_manual_entries(uuid,uuid,timestamptz,text,text,text,jsonb);