-- Autofill already stores images under this scope. Keep the existing workspace,
-- run/entity UUID and four-part checks so the current member-only RLS applies.
create or replace function public.v2_image_workspace(p_name text)
returns uuid
language plpgsql
immutable
set search_path to 'public', 'pg_temp'
as $function$
declare parts text[];
begin
  parts := string_to_array(p_name, '/');
  if cardinality(parts) <> 4
    or parts[2] not in ('collection', 'collection-autofill', 'scenario', 'trade', 'spot')
    or parts[1] !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or parts[3] !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or parts[4] = '' then return null; end if;
  return parts[1]::uuid;
end $function$;
