-- Supabase installs pgcrypto in extensions; isolated PostgreSQL commonly
-- installs it in public. Bind the trusted extension schema explicitly while
-- preserving the provenance trigger's restricted SECURITY DEFINER search path.
do $crypto_schema$
declare
  crypto_schema text;
  definition text;
  needle text := 'digest(i.item_key,''sha256'')';
  replacement text;
begin
  select n.nspname into crypto_schema from pg_catalog.pg_extension e
    join pg_catalog.pg_namespace n on n.oid=e.extnamespace where e.extname='pgcrypto';
  if crypto_schema is null then raise exception 'Required pgcrypto extension is missing';end if;
  replacement := format('%I.digest(i.item_key,''sha256'')',crypto_schema);
  definition := pg_catalog.pg_get_functiondef('public.v2_capture_collection_news_provenance()'::regprocedure);
  if strpos(definition,replacement)=0 then
    -- Rebinding also supports relocation of pgcrypto in isolated regressions.
    definition := regexp_replace(definition,'([a-zA-Z_][a-zA-Z_0-9]*|"[^"]+")\.digest\(i\.item_key,''sha256''\)',needle,'g');
    if (length(definition)-length(replace(definition,needle,'')))/length(needle)<>2 then
      raise exception 'Unexpected news provenance trigger; crypto binding was not installed';
    end if;
    execute replace(definition,needle,replacement);
  end if;
end $crypto_schema$;
notify pgrst,'reload schema';
