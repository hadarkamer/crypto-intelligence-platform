-- Manual scans remain in the selected session; scheduled news keeps rollover.
-- Change only the reviewed routing snippets and retain RPC identity/permissions.
do $fix$
declare
  function_oid oid;
  original_definition text;
  definition text;
  expected_contract jsonb;
  current_contract jsonb;
  old_expression text;
  fixed_expression text;
  old_count integer;
  fixed_count integer;
  manual_guard constant text := $guard$-- Manual scans must still belong to their original selected session.
 if run.manual and not exists(
 select 1 from public.v2_collection_sessions selected
 join public.v2_template_sections selected_sec on selected_sec.template_id=selected.template_id and selected_sec.active
 join public.v2_template_fields selected_field on selected_field.section_id=selected_sec.id and selected_field.active
 where selected.id=run.session_id and selected.workspace_id=run.workspace_id
 and selected.deleted_at is null and selected.status<>'archived' and selected_field.id=run.field_id
 and public.v2_collection_sheet_key(to_jsonb(selected_sec))=run.sheet_key
 and public.v2_collection_field_logical_key(to_jsonb(selected_field))=run.logical_key
 ) then raise exception 'The selected manual scan target changed' using errcode='23514';end if;
 select * into f from public.v2_template_fields where id=run.field_id and active;$guard$;
begin
  for function_oid,old_expression,fixed_expression in
    select * from (values
      (to_regprocedure('public.v2_collection_autofill_claim(uuid,uuid,boolean)')::oid,
       $old$-- A manual button on an archived-in-history sheet still scans the current
 -- news field and takes its baseline, instructions and template revision.
 if sheet='news_macro' then$old$,
       $new$-- Scheduled news follows rollover; manual scans keep the selected session.
 if sheet='news_macro' and not p_manual then$new$),
      (to_regprocedure('public.v2_collection_autofill_apply(uuid,jsonb,jsonb,timestamptz)')::oid,
       $old$if run.sheet_key='news_macro' and (jsonb_array_length$old$,
       $new$if run.sheet_key='news_macro' and not run.manual and (jsonb_array_length$new$),
      (to_regprocedure('public.v2_collection_autofill_apply(uuid,jsonb,jsonb,timestamptz)')::oid,
       $old$select * into f from public.v2_template_fields where id=run.field_id and active;$old$,
       manual_guard)
    ) changes
  loop
    if function_oid is null then raise exception 'Existing collection autofill RPCs are required';end if;
    select pg_get_functiondef(p.oid),jsonb_build_object('oid',p.oid,'owner',p.proowner,
      'acl',p.proacl,'signature',p.oid::regprocedure::text,'security_definer',p.prosecdef,
      'volatility',p.provolatile,'settings',p.proconfig,'return_type',p.prorettype)
      into original_definition,expected_contract from pg_proc p where p.oid=function_oid;
    definition:=original_definition;
    fixed_count:=(length(definition)-length(replace(definition,fixed_expression,'')))/length(fixed_expression);
    -- The integrity replacement retains its original final SELECT. Remove the
    -- complete fixed block before counting an unapplied original expression.
    old_count:=(length(replace(definition,fixed_expression,''))-
      length(replace(replace(definition,fixed_expression,''),old_expression,'')))/length(old_expression);
    if old_count=1 and fixed_count=0 then
      definition:=replace(definition,old_expression,fixed_expression);
      execute definition;
    elsif old_count=0 and fixed_count=1 then
      null;
    else
      raise exception 'Unexpected manual routing snippet counts (old %, fixed %); review RPC',old_count,fixed_count;
    end if;
    select jsonb_build_object('oid',p.oid,'owner',p.proowner,
      'acl',p.proacl,'signature',p.oid::regprocedure::text,'security_definer',p.prosecdef,
      'volatility',p.provolatile,'settings',p.proconfig,'return_type',p.prorettype)
      into current_contract from pg_proc p where p.oid=function_oid;
    if current_contract is distinct from expected_contract or pg_get_functiondef(function_oid) is distinct from definition then
      raise exception 'Autofill identity, authorization or reviewed body changed';
    end if;
  end loop;
end;
$fix$;
