-- Return the effective, trigger-merged value in the same transaction as its
-- existing permission-checked save. The original void RPC remains unchanged.
create or replace function public.v2_save_collection_value_receipt(
  p_session_id uuid, p_field_id uuid, p_value jsonb
) returns setof public.v2_collection_values
language plpgsql volatile security invoker
set search_path = public, pg_temp
as $function$
begin
  perform public.v2_save_collection_value(p_session_id, p_field_id, p_value);
  return query
    select receipt.*
    from public.v2_collection_current_values(p_session_id) receipt
    where receipt.session_id = p_session_id and receipt.field_id = p_field_id;
  if not found then
    raise exception 'Saved collection value receipt missing' using errcode = '23514';
  end if;
end;
$function$;

revoke all on function public.v2_save_collection_value_receipt(uuid,uuid,jsonb) from public, anon;
grant execute on function public.v2_save_collection_value_receipt(uuid,uuid,jsonb) to authenticated;

-- The generic structured-row API remains unchanged for every other module.
create or replace function public.v2_patch_collection_rows_receipt(
  p_session_id uuid, p_field_id uuid, p_operation text, p_row_id text,
  p_patch jsonb default '{}'::jsonb, p_parent_row_id text default null
) returns setof public.v2_collection_values
language plpgsql volatile security invoker
set search_path = public, pg_temp
as $function$
begin
  perform public.patch_v2_entity_rows(
    'collection', p_session_id, p_field_id, p_operation, p_row_id, p_patch, p_parent_row_id
  );
  return query
    select receipt.*
    from public.v2_collection_current_values(p_session_id) receipt
    where receipt.session_id = p_session_id and receipt.field_id = p_field_id;
  if not found then
    raise exception 'Patched collection row receipt missing' using errcode = '23514';
  end if;
end;
$function$;

revoke all on function public.v2_patch_collection_rows_receipt(uuid,uuid,text,text,jsonb,text) from public, anon;
grant execute on function public.v2_patch_collection_rows_receipt(uuid,uuid,text,text,jsonb,text) to authenticated;
