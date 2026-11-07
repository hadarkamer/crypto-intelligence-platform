-- New semantic journal types. Existing template versions and worksheet answers
-- are intentionally untouched; the UI adapter recognizes their historic keys.
alter table public.v2_template_fields drop constraint if exists v2_template_fields_field_type_check;
alter table public.v2_template_fields add constraint v2_template_fields_field_type_check check(field_type in (
 'dynamic_text','rich_text','short_text','long_text','checkbox','yes_no_na','liquidity_table','news_table','news_source_table',
 'economic_calendar','number','pnl','journal','image','info','single_choice','multi_choice','datetime','link','stage_conclusion','button',
 'currency_select','reasons_with_evidence','notes','conclusions'));

create or replace function public.v2_legacy_field_type(p_question_type text,p_field_key text)
returns text language plpgsql immutable set search_path=public,pg_temp as $$
declare kind text:=lower(btrim(coalesce(p_question_type,''))); semantic_key text:=lower(btrim(coalesce(p_field_key,'')));
begin
  if kind in ('button','pnl','notes','conclusions') then return kind; end if;
  if semantic_key='trade_pnl' then return 'pnl'; end if;
  if kind in ('long_text','rich_text','dynamic_text','short_text','journal') then
    if semantic_key in ('notes','journal_notes') then return 'notes'; end if;
    if semantic_key in ('conclusions','journal_conclusions') then return 'conclusions'; end if;
  end if;
  if kind='news' then kind:='news_table'; end if;
  if kind not in ('dynamic_text','rich_text','short_text','long_text','checkbox','yes_no_na',
    'liquidity_table','news_table','news_source_table','economic_calendar','number','pnl','journal','image',
    'info','single_choice','multi_choice','datetime','link','stage_conclusion','currency_select','reasons_with_evidence','notes','conclusions') then
    raise exception 'Unsupported legacy field type: %',p_question_type using errcode='23514';
  end if;
  return kind;
end $$;
revoke all on function public.v2_legacy_field_type(text,text) from public,anon;
grant execute on function public.v2_legacy_field_type(text,text) to authenticated;

-- Change only future seed definitions and accepted legacy copy types. Keep the
-- current bodies' authorization, immutability, source-copy and button safeguards.
do $migration$
declare definition text; needle text; replacement text;
begin
  definition:=pg_get_functiondef('public.v2_seed_default_templates(uuid)'::regprocedure);
  foreach needle in array array[
    '''conclusions'', ''מסקנות'', ''long_text''',
    '''notes'', ''הערות'', ''long_text'''
  ] loop
    replacement:=replace(needle,'''long_text''',case when needle like '%conclusions%' then '''conclusions''' else '''notes''' end);
    if strpos(definition,needle)>0 then definition:=replace(definition,needle,replacement);
    elsif strpos(definition,replacement)=0 then raise exception 'Unexpected journal seed definition: %',needle;
    end if;
  end loop;
  execute definition;

  -- The empty-seed upgrader recognizes both historic and semantic journal types.
  -- All other exact structural checks continue to distinguish user templates.
  definition:=pg_get_functiondef('public.v2_is_generated_template(uuid)'::regprocedure);
  needle:='f.field_key,f.title,f.field_type,f.position,f.required';
  replacement:='f.field_key,f.title,case when f.field_key in (''notes'',''conclusions'') and f.field_type=f.field_key then ''long_text'' else f.field_type end,f.position,f.required';
  if strpos(definition,needle)>0 then definition:=replace(definition,needle,replacement);
  elsif strpos(definition,replacement)=0 then raise exception 'Unexpected generated template classifier';
  end if;
  execute definition;

  definition:=pg_get_functiondef('public.import_template_tree_atomic(uuid,uuid,uuid,jsonb)'::regprocedure);
  needle:='''button'',''currency_select'',''reasons_with_evidence'')';
  replacement:='''button'',''currency_select'',''reasons_with_evidence'',''notes'',''conclusions'',''pnl'')';
  if strpos(definition,needle)>0 then definition:=replace(definition,needle,replacement);
  elsif strpos(definition,replacement)=0 then raise exception 'Unexpected legacy template import type validator';
  end if;
  execute definition;
end $migration$;

-- PnL remains a single trade result: existing triggers synchronize any field
-- whose explicit type is pnl, independent of key/title. The latest written field
-- wins; deleting it restores the latest remaining value. Values are not summed.
notify pgrst,'reload schema';
