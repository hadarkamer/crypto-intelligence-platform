-- Independent server receipts for human notes and automatic snapshots.
-- Existing text, leases, clears, RLS and unknown historical note times are preserved.
begin;
CREATE OR REPLACE FUNCTION public.v2_collection_managed_snapshot_merge(p_current jsonb, p_incoming jsonb, p_workspace_id uuid, p_run_id uuid DEFAULT NULL::uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 VOLATILE
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare current_snapshot jsonb; incoming_snapshot jsonb; next_snapshot jsonb;
 next_value jsonb; editable_value jsonb; manual_html text; combined_html text;
begin
 p_current:=coalesce(p_current,'{}'::jsonb);
 if jsonb_typeof(p_current) is distinct from 'object'
 or jsonb_typeof(p_incoming) is distinct from 'object' then
  raise exception 'Invalid collection snapshot value' using errcode='23514';
 end if;

 if p_run_id is null and p_current ? '_clear_generation'
 and p_incoming->'_clear_generation' is distinct from p_current->'_clear_generation' then
  raise exception 'התיבה נוקתה; יש לרענן לפני שמירה.' using errcode='23514';end if;
 p_incoming:=p_incoming-'_clear_generation';
 if p_current ? '_clear_generation' then
  p_incoming:=p_incoming||jsonb_build_object('_clear_generation',p_current->'_clear_generation');end if;
 current_snapshot:=p_current->'_autofill_snapshot';
 incoming_snapshot:=p_incoming->'_autofill_snapshot';

 if p_run_id is null and not (p_current ? '_autofill_snapshot') then
  -- An ordinary editor may still save text, but only the server starts managed
  -- snapshots. Ignore a forged marker instead of changing ordinary save rules.
  return p_incoming-'_autofill_snapshot';
 end if;
 if p_current ? '_autofill_snapshot' and
 not public.v2_collection_managed_snapshot_valid(p_current,p_workspace_id) then
  raise exception 'Unsupported current collection snapshot; reload or review it' using errcode='23514';
 end if;

 if p_run_id is null then
  -- A stale editor owns only manual_html. Its old generated content, image,
  -- chart, provenance and existing snapshot metadata cannot replace newer data.
  if jsonb_typeof(incoming_snapshot) is distinct from 'object'
  or incoming_snapshot->'version' is distinct from '1'::jsonb
  or incoming_snapshot->'kind' is distinct from current_snapshot->'kind'
  or jsonb_typeof(incoming_snapshot->'manual_html') is distinct from 'string' then
   raise exception 'Invalid manual collection snapshot; reload before saving' using errcode='23514';
  end if;
  -- Ordinary user attachments, evidence and completion flags retain their
  -- usual last-writer behavior. Only generated aliases/provenance are guarded.
  select coalesce(jsonb_object_agg(key,value),'{}'::jsonb) into editable_value
  from jsonb_each(p_incoming) where key not in ('text','html','text_value','image_path','chart')
  and left(key,9)<>'_autofill';
  next_value:=p_current||editable_value;
  next_snapshot:=(incoming_snapshot-array['version','kind','generated_html','manual_html','image_path','chart'])
   ||current_snapshot||jsonb_build_object('manual_html',incoming_snapshot->'manual_html');
 else
  if not public.v2_collection_managed_snapshot_valid(p_incoming,p_workspace_id,p_run_id) then
   raise exception 'Invalid generated collection snapshot or image scope' using errcode='23514';
  end if;
  if p_current ? '_autofill_snapshot' and incoming_snapshot->'kind' is distinct from current_snapshot->'kind' then
   raise exception 'Collection snapshot kind changed' using errcode='23514';
  end if;
  -- Unclassified legacy text remains manual; it is never guessed to be AI.
  if p_current ? '_autofill_snapshot' then manual_html:=current_snapshot->>'manual_html';
  elsif jsonb_typeof(p_current->'text_value')='string' then manual_html:=p_current->>'text_value';
  elsif jsonb_typeof(p_current->'html')='string' then manual_html:=p_current->>'html';
  elsif jsonb_typeof(p_current->'text')='string' then manual_html:=p_current->>'text';
  else manual_html:='';end if;
  next_value:=p_current||p_incoming;
  -- Omitted optional generated assets clear the previous generated assets.
  next_snapshot:=(coalesce(current_snapshot,'{}'::jsonb)-'image_path'-'chart')
   ||incoming_snapshot||jsonb_build_object('manual_html',manual_html);
 end if;
 -- Receipt times are server-owned and independent for generated content and human notes.
 -- Restore trusted metadata from the locked value; callers cannot forge either clock.
 next_snapshot:=next_snapshot-array['manual_updated_at','manual_updated_by','generated_updated_at'];
 if current_snapshot ? 'manual_updated_at' then
  next_snapshot:=next_snapshot||jsonb_build_object('manual_updated_at',current_snapshot->'manual_updated_at');
 end if;
 if current_snapshot ? 'manual_updated_by' then
  next_snapshot:=next_snapshot||jsonb_build_object('manual_updated_by',current_snapshot->'manual_updated_by');
 end if;
 if p_run_id is null then
  if current_snapshot ? 'generated_updated_at' then
   next_snapshot:=next_snapshot||jsonb_build_object('generated_updated_at',current_snapshot->'generated_updated_at');
  end if;
  if next_snapshot->'manual_html' is distinct from current_snapshot->'manual_html' then
   next_snapshot:=next_snapshot||jsonb_build_object(
    'manual_updated_at',statement_timestamp(),'manual_updated_by',auth.uid());
  end if;
 else
  next_snapshot:=next_snapshot||jsonb_build_object('generated_updated_at',statement_timestamp());
 end if;
 combined_html:=concat_ws(E'\n',nullif(next_snapshot->>'generated_html',''),nullif(next_snapshot->>'manual_html',''));
 next_value:=next_value||jsonb_build_object('_autofill_snapshot',next_snapshot,'text',combined_html);
 if p_current ? 'html' or p_incoming ? 'html' then next_value:=next_value||jsonb_build_object('html',combined_html);end if;
 if p_current ? 'text_value' or p_incoming ? 'text_value' then next_value:=next_value||jsonb_build_object('text_value',combined_html);end if;
 return next_value;
end $function$
;
commit;
