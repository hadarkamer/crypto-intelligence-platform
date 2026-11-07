-- Compare the editor's existing base inside the same lock/transaction as the
-- save. No new tables: keep current permissions, managed snapshots and receipts.
drop function if exists public.v2_save_collection_value_receipt(uuid,uuid,jsonb);
create or replace function public.v2_save_collection_value_receipt(
  p_session_id uuid, p_field_id uuid, p_value jsonb, p_baseline jsonb default null
) returns setof public.v2_collection_values
language plpgsql volatile security definer
set search_path = public, pg_temp
as $function$
declare ws uuid; current_value jsonb; baseline jsonb; incoming jsonb; merged jsonb; k text;
begin
  select workspace_id into ws from public.v2_collection_sessions where id=p_session_id;
  if auth.uid() is null or ws is null or not coalesce(public.v2_can_edit_workspace(ws),false) then
    raise exception 'Collection edit permission required' using errcode='42501';
  end if;
  if jsonb_typeof(p_baseline) is distinct from 'object' then
    raise exception 'יש לרענן את העמוד לפני שמירה; העריכה נשארה בתיבה.' using errcode='40001';
  end if;
  perform 1 from public.v2_workspaces where id=ws for update;
  perform 1 from public.v2_collection_sessions where id=p_session_id for update;
  select value into current_value from public.v2_collection_current_values(p_session_id)
    where field_id=p_field_id;
  current_value:=coalesce(current_value,'{}'::jsonb);
  -- Normalize managed generated content against the same current snapshot, so
  -- a new automatic scan alone does not conflict with a user's manual text.
  baseline:=public.v2_collection_managed_snapshot_merge(current_value,p_baseline,ws);
  incoming:=public.v2_collection_managed_snapshot_merge(current_value,p_value,ws);
  merged:=current_value;
  for k in select jsonb_object_keys(baseline) union select jsonb_object_keys(incoming) loop
    if baseline->k is distinct from incoming->k then
      if current_value->k is distinct from baseline->k and current_value->k is distinct from incoming->k then
        raise exception 'התיבה עודכנה בחלון אחר. העריכה שלך נשארה בתיבה; יש להשוות לעדכון לפני שמירה.' using errcode='40001';
      end if;
      if incoming ? k then merged:=merged||jsonb_build_object(k,incoming->k);
      else merged:=merged-k; end if;
    end if;
  end loop;
  perform public.v2_save_collection_value(p_session_id,p_field_id,merged);
  return query select receipt.* from public.v2_collection_current_values(p_session_id) receipt
    where receipt.session_id=p_session_id and receipt.field_id=p_field_id;
  if not found then raise exception 'Saved collection value receipt missing' using errcode='23514'; end if;
end;
$function$;
revoke all on function public.v2_save_collection_value_receipt(uuid,uuid,jsonb,jsonb) from public,anon;
grant execute on function public.v2_save_collection_value_receipt(uuid,uuid,jsonb,jsonb) to authenticated;
