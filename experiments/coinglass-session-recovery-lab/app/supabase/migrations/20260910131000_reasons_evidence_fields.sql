-- Evidence is now an explicit, reusable template field type. Only definitions
-- change here: worksheet answers and existing scenario evidence stay untouched.
-- Referenced versions stay immutable. The client adapter recognizes their legacy
-- role fields; saving a new template version stores the explicit type.
update public.v2_template_fields f
set field_type = 'reasons_with_evidence',
    settings = coalesce(f.settings, '{}'::jsonb) || '{"evidence_type_migrated":true}'::jsonb
from public.v2_template_sections s
join public.v2_templates t on t.id = s.template_id
where f.section_id = s.id and t.kind = 'scenario'
  -- A migration has no end-user JWT. Inspect references directly instead of
  -- the member-scoped v2_template_in_use RPC, which hides non-member usage.
  and not exists(select 1 from public.v2_collection_sessions x where x.template_id=t.id)
  and not exists(select 1 from public.v2_trades x where x.template_id=t.id)
  and not exists(select 1 from public.v2_scenarios x where x.template_id=t.id)
  and not exists(select 1 from public.v2_spot_assets x where x.template_id=t.id)
  and (f.field_key in ('scenario_reasons','scenario_invalidation')
       or f.settings->>'scenario_role' in ('reasons','invalidation'))
  and f.field_type in ('rich_text','long_text','dynamic_text','reasons_with_evidence')
  and f.settings->>'evidence_type_migrated' is distinct from 'true';

-- Patch the current definitions so their permission checks and any unrelated
-- installed fixes survive. Fail instead of replacing an unexpected definition.
do $migration$
declare definition text; needle text; replacement text; fn text;
begin
  definition := pg_get_functiondef('public.v2_seed_default_templates(uuid)'::regprocedure);
  foreach needle in array array[
    '''scenario_reasons'',''סיבות ואינדיקציות'',''rich_text''',
    '''scenario_invalidation'',''מה יבטל את התרחיש'',''rich_text'''
  ] loop
    replacement := replace(needle, '''rich_text''', '''reasons_with_evidence''');
    if strpos(definition, needle) > 0 then
      definition := replace(definition, needle, replacement);
    elsif strpos(definition, replacement) = 0 then
      raise exception 'Unexpected scenario seed definition: %', needle;
    end if;
  end loop;
  execute definition;

  foreach fn in array array['public.v2_field_complete(text,jsonb)', 'public.v2_collection_value_filled(text,jsonb)'] loop
    definition := pg_get_functiondef(fn::regprocedure);
    if strpos(definition, 'reasons_with_evidence') > 0 then continue; end if;
    needle := case when fn like '%v2_field_complete%'
      then '  if v->>''completion''=''na'' then return true; end if;'
      else '  if p_value is null or p_value=''null''::jsonb then return false; end if;' end;
    if strpos(definition, needle) = 0 then
      raise exception 'Unexpected evidence completeness definition: %', fn;
    end if;
    replacement := $body$
  if p_type='reasons_with_evidence' and jsonb_typeof(p_value->'evidence')='array' then
    if exists (select 1 from jsonb_array_elements(p_value->'evidence') item
      where jsonb_typeof(item->'snapshot')='object' and nullif(item->>'id','') is not null)
    then return true; end if;
  end if;
$body$ || needle;
    execute replace(definition, needle, replacement);
  end loop;
end $migration$;
