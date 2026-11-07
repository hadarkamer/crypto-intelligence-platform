-- Trade controls are template fields, with the same status guard for buttons and direct writes.
-- This migration changes metadata/contracts only; existing templates, answers and trades are untouched.
alter table public.v2_trades drop constraint if exists v2_trades_status_check;
alter table public.v2_trades add constraint v2_trades_status_check
 check (status in ('evaluating','limit','active','closed'));

alter table public.v2_template_fields drop constraint if exists v2_template_fields_field_type_check;
alter table public.v2_template_fields add constraint v2_template_fields_field_type_check check(field_type in (
 'dynamic_text','rich_text','short_text','long_text','checkbox','yes_no_na','liquidity_table','news_table','news_source_table',
 'economic_calendar','number','pnl','journal','image','info','single_choice','multi_choice','datetime','link','stage_conclusion','button'));
alter table public.v2_template_fields drop constraint if exists v2_template_fields_button_action_check;
alter table public.v2_template_fields add constraint v2_template_fields_button_action_check check (
 field_type<>'button' or (coalesce(settings->>'action',settings#>>'{options_json,action}') in ('activate','close','limit')) is true);

create or replace function public.v2_legacy_field_type(p_question_type text,p_field_key text)
returns text language plpgsql immutable set search_path=public,pg_temp as $$
declare kind text:=lower(btrim(coalesce(p_question_type,'')));
begin
  if kind='button' then return 'button'; end if;
  if lower(btrim(coalesce(p_field_key,'')))='trade_pnl' then return 'pnl'; end if;
  if kind='news' then kind:='news_table'; end if;
  if kind not in ('dynamic_text','rich_text','short_text','long_text','checkbox','yes_no_na',
    'liquidity_table','news_table','news_source_table','economic_calendar','number','pnl','journal','image',
    'info','single_choice','multi_choice','datetime','link','stage_conclusion') then
    raise exception 'Unsupported legacy field type: %',p_question_type using errcode='23514';
  end if;
  return kind;
end $$;
revoke all on function public.v2_legacy_field_type(text,text) from public,anon;
grant execute on function public.v2_legacy_field_type(text,text) to authenticated;

create or replace function public.v2_field_complete(p_type text,p_value jsonb)
returns boolean language plpgsql immutable set search_path=public,pg_temp as $$
declare v jsonb:=coalesce(p_value,'{}'); txt text; r jsonb; lev jsonb;
begin
  if v->>'completion'='na' then return true; end if;
  if p_type in ('info','button') then return true; end if;
  if p_type in ('single_choice','stage_conclusion') and jsonb_typeof(v->'choice')='string'
    and nullif(btrim(v->>'choice'),'') is not null then return true; end if;
  if p_type='multi_choice' then
    if jsonb_typeof(v->'choices')='array' then
      if exists(select 1 from jsonb_array_elements(v->'choices') choice
        where jsonb_typeof(choice)='string' and length(btrim(choice#>>'{}'))>0) then return true; end if;
    end if;
  end if;
  if p_type='datetime' and ((jsonb_typeof(v->'date')='string' and nullif(btrim(v->>'date'),'') is not null) or (jsonb_typeof(v->'datetime')='string' and nullif(btrim(v->>'datetime'),'') is not null)) then return true; end if;
  if p_type='image' and jsonb_typeof(v->'images')='array' and exists(select 1 from jsonb_array_elements(v->'images') img where jsonb_typeof(img->'storage_path')='string' and nullif(btrim(img->>'storage_path'),'') is not null) then return true; end if;
  if p_type='economic_calendar' then return coalesce(v->>'reviewed','false')='true'; end if;
  if p_type='checkbox' then return coalesce(v->>'checked','false')='true'; end if;
  if p_type='yes_no_na' then return coalesce(v->>'choice','') in ('yes','no','na'); end if;
  if p_type in ('number','pnl') then
    txt:=coalesce(v->>'text','');
    if txt !~ '^\s*[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)([eE][+-]?[0-9]+)?\s*$' then return false; end if;
    begin return abs(txt::numeric)<=1.7976931348623157e308::numeric;
    exception when numeric_value_out_of_range or invalid_text_representation then return false; end;
  end if;
  if p_type in ('news_table','news_source_table','liquidity_table') then
    if jsonb_typeof(v#>'{structured,rows}')='array' then v:=v||jsonb_build_object('rows',v#>'{structured,rows}');end if;
    if jsonb_typeof(v->'rows') is distinct from 'array' then return false; end if;
    for r in select value from jsonb_array_elements(v->'rows') loop
      if p_type='liquidity_table' then
        if length(trim(coalesce(r->>'price','')))>0 and r->>'amount' in ('many','normal','few') then return true; end if;
        for lev in select value from jsonb_array_elements(
          case when jsonb_typeof(r->'levels')='array' then r->'levels' else
          (case when jsonb_typeof(r->'above')='array' then r->'above' else '[]'::jsonb end)||
          (case when jsonb_typeof(r->'below')='array' then r->'below' else '[]'::jsonb end) end) loop
          if nullif(btrim(coalesce(lev->>'price_text',lev->>'price','')),'') is not null and lev->>'amount' in ('few','normal','many') then return true; end if;
        end loop;
      else
        txt:=coalesce(r->>'body',r->>'text','');
        txt:=regexp_replace(regexp_replace(txt,'<[^>]*>',' ','g'),'&(nbsp|#160|#x0*a0);',' ','gi');
        if nullif(btrim(txt),'') is not null and r->>'importance' is not null then return true; end if;
      end if;
    end loop;
    return false;
  end if;
  txt:=coalesce(v->>'text','');
  if txt ~* '<img\y[^>]*(data-path|src)=' then return true; end if;
  txt:=regexp_replace(regexp_replace(regexp_replace(txt,'<[^>]*>',' ','g'),'&(nbsp|#160|#x0*a0);',' ','gi'),'[\s​‌‍﻿]','','g');
  return length(txt)>0;
end $$;
create or replace function public.v2_guard_trade_workflow()
returns trigger language plpgsql security definer set search_path=public,pg_temp as $$
declare root_record public.v2_trades;
begin
  if tg_op='INSERT' then
    new.activated_at:=null; new.closed_at:=null;
    if new.status<>'evaluating' or new.pnl is not null then raise exception 'A trade starts in evaluation without PnL' using errcode='23514'; end if;
    if new.template_id is null or not exists(select 1 from public.v2_templates where id=new.template_id and workspace_id=new.workspace_id and kind='trade' and active) then raise exception 'An active trade template is required' using errcode='23514'; end if;
    if new.cycle_number<1 then raise exception 'Invalid cycle number' using errcode='23514'; end if;
    if new.root_trade_id is null and new.cycle_number<>1 then raise exception 'Root trade must be cycle one' using errcode='23514'; end if;
    if new.root_trade_id is not null then
      select * into root_record from public.v2_trades where id=new.root_trade_id for update;
      if not found or root_record.root_trade_id is not null or root_record.workspace_id<>new.workspace_id or root_record.asset_symbol<>new.asset_symbol or new.cycle_number<>(select coalesce(max(cycle_number),0)+1 from public.v2_trades where id=new.root_trade_id or root_trade_id=new.root_trade_id) or exists(select 1 from public.v2_trades where (id=new.root_trade_id or root_trade_id=new.root_trade_id) and status<>'closed') then raise exception 'Invalid trade cycle or previous cycle remains open' using errcode='23514'; end if;
    end if;
  else
    -- Lifecycle timestamps are server-owned, including while a limit is pending.
    new.activated_at:=old.activated_at; new.closed_at:=old.closed_at;
    if new.workspace_id<>old.workspace_id or new.asset_symbol<>old.asset_symbol or new.template_id is distinct from old.template_id or new.root_trade_id is distinct from old.root_trade_id or new.cycle_number<>old.cycle_number then raise exception 'Trade identity and template are immutable' using errcode='23514'; end if;
    if new.pnl is distinct from old.pnl and new.status<>'closed' then raise exception 'PnL is locked until trade closes' using errcode='23514'; end if;
    if new.status is distinct from old.status then
      if (old.status='evaluating' and new.status in ('limit','active')) or (old.status='limit' and new.status='active') then
        if new.template_id is null or not exists(select 1 from public.v2_template_fields f join public.v2_template_sections s on s.id=f.section_id where s.template_id=new.template_id and s.active and f.active) or exists(
          select 1 from public.v2_template_fields f join public.v2_template_sections s on s.id=f.section_id left join public.v2_trade_values v on v.field_id=f.id and v.trade_id=new.id
          where s.template_id=new.template_id and s.active and f.active and f.id in (select public.v2_visible_trade_fields(new.id)) and f.required and f.field_type not in ('info','button')
            and not public.v2_trade_field_is_post_trade(s.title,s.settings,f.field_type,f.field_key,f.settings)
            and not public.v2_field_complete(f.field_type,v.value)
        ) then raise exception 'Complete required fields before opening a limit or activating a trade' using errcode='23514'; end if;
        if new.status='active' then new.activated_at:=now(); else new.activated_at:=null; end if;
        new.closed_at:=null;
      elsif old.status='active' and new.status='closed' then new.closed_at:=now();
      else raise exception 'Allowed trade transitions: evaluating to limit or active, limit to active, and active to closed' using errcode='23514'; end if;
    end if;
  end if;
  if new.scenario_id is not null and (tg_op='INSERT' or new.scenario_id is distinct from old.scenario_id) and not exists(select 1 from public.v2_scenarios where id=new.scenario_id and workspace_id=new.workspace_id and asset_symbol=new.asset_symbol and status='active') then raise exception 'Select an active scenario of the same asset' using errcode='23514'; end if;
  return new;
end $$;
revoke all on function public.v2_guard_trade_workflow() from public,anon,authenticated;


create or replace function public.import_template_tree_atomic(_target_version_id uuid,_target_stage_id uuid default null,
 _target_group_id uuid default null,_stages jsonb default '[]'::jsonb)
returns uuid language plpgsql security definer set search_path=public,pg_temp as $$
declare st jsonb; gr jsonb; qu jsonb; sid uuid; gid uuid; parent_stage uuid; version_status text; spos integer; gpos integer; qpos integer; qtype text;
begin
 if auth.uid() is null or not coalesce(app_private.has_role(auth.uid(),'admin'),false) then raise exception 'Template administrator required' using errcode='42501'; end if;
 select status into version_status from public.template_versions where id=_target_version_id for update;
 if not found or version_status<>'draft' or exists(select 1 from public.app_settings where active_template_version_id=_target_version_id)
  or exists(select 1 from public.projects where template_version_id=_target_version_id) then
  raise exception 'Copy into an unused draft template version' using errcode='23514'; end if;
 if _target_stage_id is not null and not exists(select 1 from public.template_stages where id=_target_stage_id and template_version_id=_target_version_id) then
  raise exception 'Target sheet does not belong to this draft' using errcode='23514'; end if;
 if _target_group_id is not null then
  select g.stage_id into parent_stage from public.template_groups g join public.template_stages s on s.id=g.stage_id
   where g.id=_target_group_id and s.template_version_id=_target_version_id;
  if parent_stage is null or (_target_stage_id is not null and parent_stage<>_target_stage_id) then
   raise exception 'Target section does not belong to this draft sheet' using errcode='23514'; end if;
 end if;
 if jsonb_typeof(_stages) is distinct from 'array' or jsonb_array_length(_stages)>100 then raise exception 'Invalid sheets to copy' using errcode='22023'; end if;
 select coalesce(max(position)+1,0) into spos from public.template_stages where template_version_id=_target_version_id;
 for st in select value from jsonb_array_elements(_stages) loop
  if jsonb_typeof(st)<>'object' or jsonb_typeof(st->'groups') is distinct from 'array' then raise exception 'Invalid sheet tree' using errcode='22023'; end if;
  sid:=coalesce(_target_stage_id,parent_stage);
  if sid is null then
   if nullif(btrim(st->>'title'),'') is null then raise exception 'Sheet title required' using errcode='23514'; end if;
   insert into public.template_stages(template_version_id,title,description,position,active)
   values(_target_version_id,st->>'title',st->>'description',spos,coalesce((st->>'active')::boolean,true)) returning id into sid;spos:=spos+1;
  end if;
  select coalesce(max(position)+1,0) into gpos from public.template_groups where stage_id=sid;
  for gr in select value from jsonb_array_elements(st->'groups') loop
   if jsonb_typeof(gr)<>'object' or jsonb_typeof(gr->'questions') is distinct from 'array' then raise exception 'Invalid section tree' using errcode='22023'; end if;
   gid:=_target_group_id;
   if gid is null then
    if nullif(btrim(gr->>'title'),'') is null then raise exception 'Section title required' using errcode='23514'; end if;
    insert into public.template_groups(stage_id,title,description,position,collapsible,active,behavior)
    values(sid,gr->>'title',gr->>'description',gpos,coalesce((gr->>'collapsible')::boolean,true),coalesce((gr->>'active')::boolean,true),gr->>'behavior') returning id into gid;gpos:=gpos+1;
   end if;
   select coalesce(max(position)+1,0) into qpos from public.template_questions where group_id=gid;
   for qu in select value from jsonb_array_elements(gr->'questions') loop
    qtype:=qu->>'question_type';
    if qtype is null or qtype not in ('info','checkbox','yes_no_na','single_choice','multi_choice','short_text','long_text','number','datetime','image','link','stage_conclusion','liquidity_table','news_table','economic_calendar','button')
      or nullif(btrim(qu->>'title'),'') is null then raise exception 'Unsupported legacy question type or missing title: %',qtype using errcode='23514'; end if;
    if qtype='button' and coalesce(qu#>>'{options_json,action}','') not in ('activate','close','limit') then
     raise exception 'Button action must be activate, close or limit' using errcode='23514'; end if;
    insert into public.template_questions(group_id,title,description,question_type,position,required,active,options_json,visibility_rule_json,help_text,source_url,links_json,default_text_align,field_key,question_key)
    values(gid,qu->>'title',qu->>'description',qtype,qpos,coalesce((qu->>'required')::boolean,false),coalesce((qu->>'active')::boolean,true),
     nullif(qu->'options_json','null'::jsonb),nullif(qu->'visibility_rule_json','null'::jsonb),qu->>'help_text',qu->>'source_url',coalesce(nullif(qu->'links_json','null'::jsonb),'[]'::jsonb),
     coalesce(qu->>'default_text_align','right'),qu->>'field_key',coalesce(nullif(qu->>'question_key',''),gen_random_uuid()::text));qpos:=qpos+1;
   end loop;
  end loop;
 end loop;
 return _target_version_id;
end $$;
revoke all on function public.import_template_tree_atomic(uuid,uuid,uuid,jsonb) from public,anon;
grant execute on function public.import_template_tree_atomic(uuid,uuid,uuid,jsonb) to authenticated;


notify pgrst,'reload schema';
