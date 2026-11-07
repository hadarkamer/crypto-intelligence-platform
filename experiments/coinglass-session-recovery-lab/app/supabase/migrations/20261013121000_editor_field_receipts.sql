-- A save receipt and its effective value come from the same transaction.
-- Keep the existing trade edit/template/locking/PnL checks in the original RPC.
create or replace function public.save_v2_trade_value_receipt(
  p_trade_id uuid, p_field_id uuid, p_value jsonb
)
returns setof public.v2_trade_values
language plpgsql security invoker set search_path=public,pg_temp as $$
begin
  perform public.save_v2_trade_value(p_trade_id,p_field_id,p_value);
  return query select v.* from public.v2_trade_values v
    where v.trade_id=p_trade_id and v.field_id=p_field_id;
  if not found then
    raise exception 'Trade save did not return a value receipt' using errcode='P0002';
  end if;
end $$;

revoke all on function public.save_v2_trade_value_receipt(uuid,uuid,jsonb) from public,anon;
grant execute on function public.save_v2_trade_value_receipt(uuid,uuid,jsonb) to authenticated;
