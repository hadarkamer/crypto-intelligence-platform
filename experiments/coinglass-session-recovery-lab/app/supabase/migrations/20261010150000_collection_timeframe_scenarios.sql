-- Add compact manual scenarios without changing any existing answer content.
do $types$
declare definition text; expression text;
begin
  select pg_get_constraintdef(oid) into definition from pg_constraint
    where conrelid='public.v2_template_fields'::regclass and conname='v2_template_fields_field_type_check';
  if definition is null then raise exception 'Missing template field type constraint'; end if;
  if position('timeframe_scenarios' in definition)=0 then
    expression:=substring(definition from '^CHECK [(](.*)[)]$');
    if expression is null then raise exception 'Unexpected template field type constraint'; end if;
    alter table public.v2_template_fields drop constraint v2_template_fields_field_type_check;
    execute format('alter table public.v2_template_fields add constraint v2_template_fields_field_type_check check ((%s) or field_type=%L)', expression, 'timeframe_scenarios');
  end if;
end $types$;

create or replace function public.v2_valid_timeframe_scenarios(p_value jsonb)
returns boolean language plpgsql immutable set search_path=public,pg_temp as $valid$
declare item jsonb; seen text[]:='{}'; identity text;
begin
  if jsonb_typeof(p_value) is distinct from 'object' then return false; end if;
  -- Unchanged pre-upgrade manual text and cleared cells are valid.
  if not p_value ? 'structured' or p_value->'structured'='null'::jsonb then return true; end if;
  if jsonb_typeof(p_value->'structured') is distinct from 'object'
    or p_value#>>'{structured,type}' is distinct from 'timeframe_scenarios'
    or jsonb_typeof(p_value#>'{structured,rows}') is distinct from 'array'
    then return false; end if;
  if jsonb_array_length(p_value#>'{structured,rows}')>128 then return false; end if;
  for item in select value from jsonb_array_elements(p_value#>'{structured,rows}') loop
    identity:=item->>'id';
    if jsonb_typeof(item) is distinct from 'object'
      or jsonb_typeof(item->'id') is distinct from 'string' or nullif(btrim(identity),'') is null
      or identity=any(seen)
      or jsonb_typeof(item->'timeframe') is distinct from 'string'
      or not coalesce(item->>'timeframe'=any(array['15m','1H','4H','1D','1W','1M']),false)
      or jsonb_typeof(item->'text') is distinct from 'string' or length(item->>'text')>20000
      then return false; end if;
    seen:=array_append(seen,identity);
  end loop;
  return true;
end $valid$;
revoke all on function public.v2_valid_timeframe_scenarios(jsonb) from public,anon;
grant execute on function public.v2_valid_timeframe_scenarios(jsonb) to authenticated,service_role;

create or replace function public.v2_validate_collection_timeframe_scenarios()
returns trigger language plpgsql set search_path=public,pg_temp as $validate$
declare typed boolean:=false;
begin
  if tg_table_name='v2_collection_values' then
    select field_type='timeframe_scenarios' into typed from public.v2_template_fields where id=new.field_id;
  end if;
  if (coalesce(typed,false) or new.value#>>'{structured,type}'='timeframe_scenarios')
    and not public.v2_valid_timeframe_scenarios(new.value) then
    raise exception 'Invalid timeframe scenarios' using errcode='22023';
  end if;
  return new;
end $validate$;
revoke all on function public.v2_validate_collection_timeframe_scenarios() from public,anon,authenticated;
drop trigger if exists v2_collection_timeframe_scenarios_valid on public.v2_collection_values;
create trigger v2_collection_timeframe_scenarios_valid before insert or update of value on public.v2_collection_values
  for each row execute function public.v2_validate_collection_timeframe_scenarios();
drop trigger if exists v2_collection_live_timeframe_scenarios_valid on public.v2_collection_live_values;
create trigger v2_collection_live_timeframe_scenarios_valid before insert or update of value on public.v2_collection_live_values
  for each row execute function public.v2_validate_collection_timeframe_scenarios();

CREATE OR REPLACE FUNCTION public.v2_collection_value_filled(p_type text, p_value jsonb)
 RETURNS boolean
 LANGUAGE plpgsql
 IMMUTABLE
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare row_value jsonb; level_value jsonb; content text;
begin

  if p_type='reasons_with_evidence' and jsonb_typeof(p_value->'evidence')='array' then
    if exists (select 1 from jsonb_array_elements(p_value->'evidence') item
      where jsonb_typeof(item->'snapshot')='object' and nullif(item->>'id','') is not null)
    then return true; end if;
  end if;
  if p_value is null or p_value='null'::jsonb then return false; end if;
  -- Presence differs from completion: an explicitly saved unchecked checkbox
  -- must supersede its older checked answer. The shared calendar has no local answer.
  if p_type='checkbox' then return coalesce(jsonb_typeof(p_value->'checked')='boolean',false); end if;
  if p_type='economic_calendar' then return false; end if;
  if p_type='image' then
    return (case when jsonb_typeof(p_value->'images')='array' then jsonb_array_length(p_value->'images')>0 else false end)
      or coalesce(p_value->>'text','') ~* '<img[[:space:]]'
      or coalesce(p_value->>'html','') ~* '<img[[:space:]]';
  end if;
  if p_value->>'completion'='na' then return true; end if;
  if p_type='yes_no_na' then return coalesce(p_value->>'choice','') in ('yes','no','na'); end if;
  if p_type in ('news_table','news_source_table','liquidity_table') then
    for row_value in select value from jsonb_array_elements(public.v2_normalize_structured_rows('00000000-0000-0000-0000-000000000000'::uuid,p_type,p_value)) loop
      if p_type in ('news_table','news_source_table') and nullif(btrim(coalesce(row_value->>'body',row_value->>'text','')),'') is not null then return true; end if;
      if p_type='liquidity_table' then
        for level_value in select value from jsonb_array_elements(coalesce(row_value->'levels','[]')) loop
          if nullif(btrim(coalesce(level_value->>'price_text',level_value->>'price','')),'') is not null then return true; end if;
        end loop;
      end if;
    end loop;
    return false;
  end if;
  if p_type='timeframe_scenarios' then
    if jsonb_typeof(p_value#>'{structured,rows}')='array' then
      for row_value in select value from jsonb_array_elements(p_value#>'{structured,rows}') loop
        if nullif(btrim(row_value->>'text'),'') is not null then return true; end if;
      end loop;
    end if;
    -- A migrated manual answer remains visible until its owner edits it.
    content:=coalesce(p_value->>'text_value',p_value->>'html',p_value->>'text','');
    content:=regexp_replace(content,'<[^>]*>','','g');
    content:=regexp_replace(content,'&(?:nbsp|#0*160|#x0*a0);','','gi');
    content:=regexp_replace(content,U&'[[:space:]\00A0\200B-\200D\FEFF]','','g');
    return content<>'';
  end if;
  if p_type='multi_choice' then
    return (jsonb_typeof(p_value->'choices')='array' and p_value->'choices'<>'[]'::jsonb)
      or nullif(btrim(coalesce(p_value->>'text','')),'') is not null;
  end if;
  content:=coalesce(p_value->>'html',p_value->>'text',p_value->>'choice',p_value->>'date','');
  if content ~* '<img[[:space:]]' then return true; end if;
  content:=regexp_replace(content,'<[^>]*>','','g');
  -- Empty editors may persist invisible Unicode or its HTML entity encoding.
  -- Strip both forms before ranking a cell as the latest meaningful answer.
  content:=regexp_replace(content,'&(?:nbsp|#0*160|#x0*a0|ZeroWidthSpace|zwnj|zwj|#0*820[3-5]|#x0*200[b-d]|#0*65279|#x0*feff);','','gi');
  content:=regexp_replace(content,U&'[[:space:]\00A0\200B-\200D\FEFF]','','g');
  return content<>'';
end $function$;

-- Only the original question in this workspace's technical collection sheets.
-- Its stable imported key spans all referenced versions; values stay untouched.
do $upgrade$
declare guard_state "char"; command text;
begin
  lock table public.v2_template_fields in access exclusive mode;
  select tgenabled into guard_state from pg_trigger
    where tgrelid='public.v2_template_fields'::regclass and tgname='v2_used_template_fields_guard' and not tgisinternal;
  if guard_state is not null then
    alter table public.v2_template_fields disable trigger v2_used_template_fields_guard;
  end if;
  update public.v2_template_fields f set field_type='timeframe_scenarios'
    from public.v2_template_sections s, public.v2_templates t
    where s.id=f.section_id and t.id=s.template_id
      and t.workspace_id='a6bb2bb8-2c40-4404-9142-1304620fcd31'::uuid and t.kind='collection'
      and public.v2_collection_sheet_key(to_jsonb(s))='data'
      and f.field_key='סריקת שוק/טכני/מה דעתך על הגרף במסגרות זמן שונות?:1f2a77a7-a804-4ca6-b986-7d8e4d6e6675'
      and f.field_type in ('long_text','rich_text','dynamic_text');
  if guard_state is not null then
    command:=case guard_state when 'O' then 'enable' when 'D' then 'disable'
      when 'R' then 'enable replica' when 'A' then 'enable always' else null end;
    if command is null then raise exception 'Unexpected template guard state'; end if;
    execute 'alter table public.v2_template_fields '||command||' trigger v2_used_template_fields_guard';
  end if;
end $upgrade$;
