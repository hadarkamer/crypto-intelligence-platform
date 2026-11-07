-- Parenthesize four existing autofill extraction operands before concatenation.
-- Schema-only and idempotent. No workspace, session, cadence or content writes.
do $fix$
declare
  function_oid oid := to_regprocedure('public.v2_collection_autofill_apply(uuid,jsonb,jsonb,timestamptz)');
  original_definition text;
  definition text;
  current_contract jsonb;
  expected_contract jsonb;
  replacement jsonb;
  old_expression text;
  fixed_expression text;
  old_count integer;
  fixed_count integer;
  changed integer := 0;
  replacements constant jsonb := $replacements$[["current_value||p_payload->'value'","current_value||(p_payload->'value')"],["coalesce(current_value->>'text','')||p_payload#>>'{value,text}'","coalesce(current_value->>'text','')||(p_payload#>>'{value,text}')"],["coalesce(current_value->'dates','[]')||p_payload#>'{value,dates}'","coalesce(current_value->'dates','[]')||(p_payload#>'{value,dates}')"],["coalesce(current_value->'rows','[]')||p_payload#>'{value,rows}'","coalesce(current_value->'rows','[]')||(p_payload#>'{value,rows}')"]]$replacements$::jsonb;
begin
  if function_oid is null then
    raise exception 'Existing collection autofill apply function is required';
  end if;
  select pg_get_functiondef(p.oid),jsonb_build_object('oid',p.oid,'owner',p.proowner,
    'acl',p.proacl,'signature',p.oid::regprocedure::text,'security_definer',p.prosecdef,
    'volatility',p.provolatile,'settings',p.proconfig,'return_type',p.prorettype)
    into original_definition,expected_contract from pg_proc p where p.oid=function_oid;
  definition:=original_definition;
  for replacement in select value from jsonb_array_elements(replacements) loop
    old_expression:=replacement->>0;
    fixed_expression:=replacement->>1;
    old_count:=(length(definition)-length(replace(definition,old_expression,'')))/length(old_expression);
    fixed_count:=(length(definition)-length(replace(definition,fixed_expression,'')))/length(fixed_expression);
    if old_count=1 and fixed_count=0 then
      definition:=replace(definition,old_expression,fixed_expression);
      changed:=changed+1;
    elsif old_count=0 and fixed_count=1 then
      null; -- This exact operand was already parenthesized.
    else
      raise exception 'Unexpected autofill expression counts (old %, fixed %); review definition',
        old_count,fixed_count;
    end if;
  end loop;
  if changed>0 then execute definition; end if;
  select jsonb_build_object('oid',p.oid,'owner',p.proowner,
    'acl',p.proacl,'signature',p.oid::regprocedure::text,'security_definer',p.prosecdef,
    'volatility',p.provolatile,'settings',p.proconfig,'return_type',p.prorettype)
    into current_contract from pg_proc p where p.oid=function_oid;
  if current_contract is distinct from expected_contract
    or pg_get_functiondef(function_oid) is distinct from definition then
    raise exception 'Autofill identity, authorization or reviewed body changed';
  end if;
end;
$fix$;
