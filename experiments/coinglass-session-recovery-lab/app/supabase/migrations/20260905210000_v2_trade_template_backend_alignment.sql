-- Additive backend alignment. Source templates and existing trade answers are never rewritten.
-- Legacy field types remain typed; a renderer can consume the preserved source metadata.
alter table public.v2_template_fields drop constraint if exists v2_template_fields_field_type_check;
alter table public.v2_template_fields add constraint v2_template_fields_field_type_check check (field_type in (
  'dynamic_text','rich_text','short_text','long_text','checkbox','yes_no_na','liquidity_table',
  'news_table','news_source_table','economic_calendar','number','pnl','image','info',
  'single_choice','multi_choice','datetime','link','stage_conclusion'
));

create or replace function public.v2_legacy_field_type(p_question_type text,p_field_key text)
returns text language plpgsql immutable set search_path=public,pg_temp as $$
declare kind text:=lower(btrim(coalesce(p_question_type,'')));
begin
  if lower(btrim(coalesce(p_field_key,'')))='trade_pnl' then return 'pnl'; end if;
  if kind='news' then kind:='news_table'; end if;
  if kind not in ('dynamic_text','rich_text','short_text','long_text','checkbox','yes_no_na',
    'liquidity_table','news_table','news_source_table','economic_calendar','number','pnl','image',
    'info','single_choice','multi_choice','datetime','link','stage_conclusion') then
    raise exception 'Unsupported legacy field type: %',p_question_type using errcode='23514';
  end if;
  return kind;
end $$;
revoke all on function public.v2_legacy_field_type(text,text) from public,anon;
grant execute on function public.v2_legacy_field_type(text,text) to authenticated;

-- Explicit stage roles are preferred; exact historical journal/debrief titles cover
-- already referenced versions without modifying their immutable required flags.
create or replace function public.v2_trade_field_is_post_trade(
  p_section_title text,p_section_settings jsonb,p_field_type text,p_field_key text,p_field_settings jsonb)
returns boolean language sql immutable set search_path=public,pg_temp as $$
  select coalesce(p_field_type='pnl',false)
    or lower(btrim(coalesce(p_field_key,'')))='trade_pnl'
    or lower(btrim(coalesce(p_field_settings->>'legacy_field_key','')))='trade_pnl'
    or lower(btrim(coalesce(p_field_settings->'legacy_question'->>'field_key','')))='trade_pnl'
    or coalesce(p_section_settings->>'workflow_role','') in ('journal','debrief','post_trade')
    or lower(btrim(coalesce(p_section_title,''))) in
      ('יומן','יומן טרייד','תחקור','תחקור ותוצאה','תחקור וסיכום','journal','debrief')
    or lower(btrim(coalesce(p_section_settings->>'legacy_stage_title',''))) in
      ('יומן','יומן טרייד','תחקור','תחקור ותוצאה','תחקור וסיכום','journal','debrief');
$$;
revoke all on function public.v2_trade_field_is_post_trade(text,jsonb,text,text,jsonb) from public,anon;
grant execute on function public.v2_trade_field_is_post_trade(text,jsonb,text,text,jsonb) to authenticated;

-- Full source imports are backend-ready drafts. Their typed fields, groups and
-- visibility rules require matching renderer support before explicit activation.
-- Automatic initialization must never switch an existing/default UI to this schema.
create or replace function public.v2_import_legacy_trade_template(p_workspace_id uuid)
returns uuid language plpgsql security definer set search_path=public,pg_temp as $$
declare
  source_version uuid; imported_id uuid; source_stage record; source_question record;
  new_section uuid; role_name text; position_number integer:=0; field_position integer;
  mapped_kind text; stage_ids uuid[]; role_count integer; selected_count integer;
  source_fingerprint text; group_snapshot jsonb; field_settings jsonb;
begin
  if auth.uid() is null or not coalesce(public.v2_can_edit_workspace(p_workspace_id),false) then
    raise exception 'Workspace edit permission required' using errcode='42501';
  end if;
  -- Same workspace row lock as normal initialization; serialize draft creation.
  perform 1 from public.v2_workspaces where id=p_workspace_id for update;
  if not found then raise exception 'Workspace not found' using errcode='42501'; end if;
  if to_regclass('public.app_settings') is null or to_regclass('public.template_stages') is null
    or to_regclass('public.template_groups') is null or to_regclass('public.template_questions') is null then return null; end if;
  select active_template_version_id into source_version from public.app_settings
    order by updated_at desc,id limit 1 for share;
  if source_version is null then return null; end if;

  select array_agg(id order by position,id),count(*),count(distinct btrim(title))
    into stage_ids,selected_count,role_count
  from public.template_stages where template_version_id=source_version and active
    and btrim(title) in ('סריקת טרייד','כניסה','ניהול טרייד','יומן');
  if selected_count<>4 or role_count<>4 then
    raise exception 'Trade import requires exactly the four stages סריקת טרייד, כניסה, ניהול טרייד, יומן; found % stages / % roles',selected_count,role_count
      using errcode='PT422';
  end if;
  perform 1 from public.template_stages where id=any(stage_ids) order by id for share;
  perform 1 from public.template_groups where stage_id=any(stage_ids) order by id for share;
  perform 1 from public.template_questions where group_id in
    (select id from public.template_groups where stage_id=any(stage_ids)) order by id for share;
  if exists(select 1 from public.template_stages s where s.id=any(stage_ids) and not exists(
    select 1 from public.template_groups g join public.template_questions q on q.group_id=g.id
    where g.stage_id=s.id and g.active and q.active)) then
    raise exception 'Every imported trade stage must contain active source questions' using errcode='PT422';
  end if;

  -- Validate all mappings before writes; preserve the source snapshot including hidden items.
  begin
    perform public.v2_legacy_field_type(q.question_type,q.field_key)
      from public.template_questions q join public.template_groups g on g.id=q.group_id
      where g.stage_id=any(stage_ids);
  exception when check_violation then
    raise exception 'Legacy trade source has an unsupported question type: %',sqlerrm using errcode='PT422';
  end;
  select md5(jsonb_build_object(
    'stages',(select jsonb_agg(to_jsonb(s) order by s.position,s.id) from public.template_stages s where s.id=any(stage_ids)),
    'groups',(select jsonb_agg(to_jsonb(g) order by g.stage_id,g.position,g.id) from public.template_groups g where g.stage_id=any(stage_ids)),
    'questions',(select jsonb_agg(to_jsonb(q) order by q.group_id,q.position,q.id) from public.template_questions q
      join public.template_groups g on g.id=q.group_id where g.stage_id=any(stage_ids)))::text)
    into source_fingerprint;
  select id into imported_id from public.v2_templates where workspace_id=p_workspace_id and kind='trade'
    and settings->>'origin'='yoav_legacy_trade_v1'
    and settings->>'legacy_template_version_id'=source_version::text
    and settings->>'legacy_source_fingerprint'=source_fingerprint
    order by created_at,id limit 1;
  if imported_id is not null then return imported_id; end if;

  insert into public.v2_templates(workspace_id,kind,name,active,is_default,settings,created_by)
  values(p_workspace_id,'trade','טרייד – מהגליונות הקיימים',false,false,jsonb_build_object(
    'origin','yoav_legacy_trade_v1','legacy_import_version',1,'legacy_template_version_id',source_version,
    'legacy_source_fingerprint',source_fingerprint,'imported_at',now(),
    'backend_import_ready',true,'activation_pending_ui',true),auth.uid()) returning id into imported_id;

  for source_stage in select * from public.template_stages where id=any(stage_ids)
    order by case btrim(title) when 'סריקת טרייד' then 0 when 'כניסה' then 1 when 'ניהול טרייד' then 2 else 3 end
  loop
    role_name:=case btrim(source_stage.title) when 'סריקת טרייד' then 'scan' when 'כניסה' then 'entry'
      when 'ניהול טרייד' then 'manage' else 'journal' end;
    select coalesce(jsonb_agg(to_jsonb(g) order by g.position,g.id),'[]'::jsonb) into group_snapshot
      from public.template_groups g where g.stage_id=source_stage.id;
    insert into public.v2_template_sections(template_id,title,position,active,settings)
    values(imported_id,source_stage.title,position_number,true,jsonb_build_object(
      'workflow_role',role_name,'legacy_stage_id',source_stage.id,'legacy_stage_title',source_stage.title,
      'legacy_stage',to_jsonb(source_stage),'legacy_groups',group_snapshot,
      'action',case role_name when 'entry' then 'activate' when 'journal' then 'close' else '' end))
    returning id into new_section;
    field_position:=0;
    for source_question in select q.*,g.title legacy_group_title,g.position legacy_group_position,
      g.active legacy_group_active,g.behavior legacy_group_behavior,g.collapsible legacy_group_collapsible
      from public.template_groups g join public.template_questions q on q.group_id=g.id
      where g.stage_id=source_stage.id order by g.position,g.id,q.position,q.id
    loop
      mapped_kind:=public.v2_legacy_field_type(source_question.question_type,source_question.field_key);
      field_settings:=jsonb_build_object(
        'legacy_question',to_jsonb(source_question)-array['legacy_group_title','legacy_group_position','legacy_group_active','legacy_group_behavior','legacy_group_collapsible'],
        'legacy_question_id',source_question.id,'legacy_question_key',source_question.question_key,
        'legacy_field_key',source_question.field_key,'legacy_group_id',source_question.group_id,
        'legacy_group_title',source_question.legacy_group_title,'legacy_group_position',source_question.legacy_group_position,
        'legacy_group_behavior',source_question.legacy_group_behavior,'legacy_group_collapsible',source_question.legacy_group_collapsible,
        'legacy_stage_title',source_stage.title,'options_json',source_question.options_json,
        'visibility_rule_json',source_question.visibility_rule_json,'links_json',source_question.links_json,
        'source_url',source_question.source_url,'description',source_question.description,
        'help_text',source_question.help_text,'legacy_default_text_align',source_question.default_text_align);
      insert into public.v2_template_fields(section_id,field_key,title,field_type,position,required,default_align,active,settings)
      values(new_section,coalesce(nullif(source_question.question_key,''),nullif(source_question.field_key,''),'legacy')||':'||source_question.id::text,
        source_question.title,mapped_kind,field_position,
        source_question.required and role_name<>'journal' and mapped_kind not in ('pnl','info'),
        case when source_question.default_text_align='left' then 'left' else 'right' end,
        source_question.active and source_question.legacy_group_active,field_settings);
      field_position:=field_position+1;
    end loop;
    position_number:=position_number+1;
  end loop;

  -- Preserve every generated/custom default and asset binding. A later UI
  -- release can explicitly activate this version after its renderers are ready.
  return imported_id;
end $$;
revoke all on function public.v2_import_legacy_trade_template(uuid) from public,anon;
grant execute on function public.v2_import_legacy_trade_template(uuid) to authenticated;

-- Structured legacy completion accepts typed values and retains text fallback for existing clients.
create or replace function public.v2_field_complete(p_type text,p_value jsonb)
returns boolean language plpgsql immutable set search_path=public,pg_temp as $$
declare v jsonb:=coalesce(p_value,'{}'); txt text;
begin
  if v->>'completion'='na' then return true; end if;
  if p_type='info' then return true; end if;
  if p_type in ('single_choice','stage_conclusion') and jsonb_typeof(v->'choice')='string'
    and nullif(btrim(v->>'choice'),'') is not null then return true; end if;
  if p_type='multi_choice' then
    if jsonb_typeof(v->'choices')='array' then
      if exists(select 1 from jsonb_array_elements(v->'choices') choice
        where jsonb_typeof(choice)='string' and length(btrim(choice#>>'{}'))>0) then return true; end if;
    end if;
  end if;
  if p_type='datetime' and jsonb_typeof(v->'date')='string'
    and nullif(btrim(v->>'date'),'') is not null then return true; end if;
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
    if jsonb_typeof(v->'rows') is distinct from 'array' then return false; end if;
    return exists(select 1 from jsonb_array_elements(v->'rows') r where case when p_type='liquidity_table' then
      length(trim(coalesce(r->>'price','')))>0 and r->>'amount' in ('many','normal','few')
      else length(trim(coalesce(r->>'text','')))>0 and r->>'importance' is not null end);
  end if;
  txt:=coalesce(v->>'text','');
  if txt ~* '<img\y[^>]*(data-path|src)=' then return true; end if;
  txt:=regexp_replace(regexp_replace(regexp_replace(txt,'<[^>]*>',' ','g'),'&(nbsp|#160|#x0*a0);',' ','gi'),'[\s​‌‍﻿]','','g');
  return length(txt)>0;
end $$;

-- Keep every existing lifecycle invariant; only post-trade completion is optional.
create or replace function public.v2_guard_trade_workflow()
returns trigger language plpgsql security definer set search_path=public,pg_temp as $$
declare root_record public.v2_trades;
begin
  if tg_op='INSERT' then
    if new.status<>'evaluating' or new.pnl is not null then raise exception 'A trade starts in evaluation without PnL' using errcode='23514'; end if;
    if new.template_id is null or not exists(select 1 from public.v2_templates where id=new.template_id and workspace_id=new.workspace_id and kind='trade' and active) then raise exception 'An active trade template is required' using errcode='23514'; end if;
    if new.cycle_number<1 then raise exception 'Invalid cycle number' using errcode='23514'; end if;
    if new.root_trade_id is null and new.cycle_number<>1 then raise exception 'Root trade must be cycle one' using errcode='23514'; end if;
    if new.root_trade_id is not null then
      select * into root_record from public.v2_trades where id=new.root_trade_id for update;
      if not found or root_record.root_trade_id is not null or root_record.workspace_id<>new.workspace_id or root_record.asset_symbol<>new.asset_symbol or new.cycle_number<>(select coalesce(max(cycle_number),0)+1 from public.v2_trades where id=new.root_trade_id or root_trade_id=new.root_trade_id) or exists(select 1 from public.v2_trades where (id=new.root_trade_id or root_trade_id=new.root_trade_id) and status<>'closed') then raise exception 'Invalid trade cycle or previous cycle remains open' using errcode='23514'; end if;
    end if;
  else
    if new.workspace_id<>old.workspace_id or new.asset_symbol<>old.asset_symbol or new.template_id is distinct from old.template_id or new.root_trade_id is distinct from old.root_trade_id or new.cycle_number<>old.cycle_number then raise exception 'Trade identity and template are immutable' using errcode='23514'; end if;
    if new.pnl is distinct from old.pnl and new.status<>'closed' then raise exception 'PnL is locked until trade closes' using errcode='23514'; end if;
    if new.status is distinct from old.status then
      if old.status='evaluating' and new.status='active' then
        if new.template_id is null or not exists(select 1 from public.v2_template_fields f join public.v2_template_sections s on s.id=f.section_id where s.template_id=new.template_id and s.active and f.active) or exists(
          select 1 from public.v2_template_fields f join public.v2_template_sections s on s.id=f.section_id left join public.v2_trade_values v on v.field_id=f.id and v.trade_id=new.id
          where s.template_id=new.template_id and s.active and f.active and f.required and f.field_type<>'info'
            and not public.v2_trade_field_is_post_trade(s.title,s.settings,f.field_type,f.field_key,f.settings)
            and not public.v2_field_complete(f.field_type,v.value)
        ) then raise exception 'Complete required fields across every trade sheet before activation' using errcode='23514'; end if;
        new.activated_at:=now();
      elsif old.status='active' and new.status='closed' then new.closed_at:=now();
      else raise exception 'Only evaluating to active and active to closed transitions are allowed' using errcode='23514'; end if;
    end if;
  end if;
  if new.scenario_id is not null and (tg_op='INSERT' or new.scenario_id is distinct from old.scenario_id) and not exists(select 1 from public.v2_scenarios where id=new.scenario_id and workspace_id=new.workspace_id and asset_symbol=new.asset_symbol and status='active') then raise exception 'Select an active scenario of the same asset' using errcode='23514'; end if;
  return new;
end $$;
revoke all on function public.v2_guard_trade_workflow() from public,anon,authenticated;

-- Future workspaces stage the same exact import without changing active UI defaults.
create or replace function public.v2_seed_default_templates(p_workspace_id uuid)
returns void
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  collection_template uuid;
  trade_template uuid;
  section_news uuid;
  section_data uuid;
  section_scan uuid;
  section_entry uuid;
  section_manage uuid;
  section_journal uuid;
  extra_template uuid;
  extra_section uuid;
  template_kind text;
begin
  if not public.v2_can_edit_workspace(p_workspace_id) then
    raise exception 'not allowed';
  end if;

  -- Serialize initialization from multiple browser tabs.
  perform 1 from public.v2_workspaces where id = p_workspace_id for update;

  select id into collection_template
  from public.v2_templates
  where workspace_id = p_workspace_id and kind = 'collection' and is_default and active and asset_symbol is null
  limit 1;

  if collection_template is null then
    insert into public.v2_templates(workspace_id, kind, name, is_default, created_by)
    values (p_workspace_id, 'collection', 'איסוף מידע – ברירת מחדל', true, auth.uid())
    returning id into collection_template;

    insert into public.v2_template_sections(template_id, title, position)
    values (collection_template, 'חדשות ומאקרו', 0)
    returning id into section_news;
    insert into public.v2_template_sections(template_id, title, position)
    values (collection_template, 'נזילות, טכני ונתונים', 1)
    returning id into section_data;

    insert into public.v2_template_fields(section_id, field_key, title, field_type, position)
    values
      (section_news, 'news', 'חדשות', 'news_source_table', 0),
      (section_news, 'macro', 'מאקרו', 'dynamic_text', 1),
      (section_data, 'liquidity', 'נזילות', 'liquidity_table', 0),
      (section_data, 'technical', 'טכני ונתונים', 'dynamic_text', 1);
  end if;

  select id into trade_template
  from public.v2_templates
  where workspace_id = p_workspace_id and kind = 'trade' and is_default and active and asset_symbol is null
  limit 1;

  if trade_template is null then
    insert into public.v2_templates(workspace_id, kind, name, is_default, created_by)
    values (p_workspace_id, 'trade', 'טרייד – ברירת מחדל', true, auth.uid())
    returning id into trade_template;

    insert into public.v2_template_sections(template_id, title, position)
    values (trade_template, 'סריקת טרייד', 0) returning id into section_scan;
    insert into public.v2_template_sections(template_id, title, position)
    values (trade_template, 'כניסה', 1) returning id into section_entry;
    insert into public.v2_template_sections(template_id, title, position)
    values (trade_template, 'ניהול טרייד', 2) returning id into section_manage;
    insert into public.v2_template_sections(template_id, title, position)
    values (trade_template, 'יומן', 3) returning id into section_journal;

    insert into public.v2_template_fields(section_id, field_key, title, field_type, position, required)
    values
      (section_scan, 'trade_thesis', 'תזה ותרחיש', 'long_text', 0, true),
      (section_scan, 'trade_risks', 'מה יבטל את התרחיש', 'long_text', 1, true),
      (section_entry, 'entry_plan', 'תכנית כניסה', 'long_text', 0, true),
      (section_entry, 'stop_loss', 'סטופ', 'short_text', 1, true),
      (section_manage, 'management_plan', 'תכנית ניהול', 'long_text', 0, true),
      (section_journal, 'conclusions', 'מסקנות', 'long_text', 0, false),
      (section_journal, 'notes', 'הערות', 'long_text', 1, false);
  end if;

  foreach template_kind in array array['scenario','spot'] loop
    select id into extra_template from public.v2_templates
    where workspace_id = p_workspace_id and kind = template_kind and is_default and active and asset_symbol is null limit 1;
    if extra_template is null then
      insert into public.v2_templates(workspace_id,kind,name,is_default,created_by)
      values(p_workspace_id,template_kind,case when template_kind = 'scenario' then 'תרחיש – ברירת מחדל' else 'יומן ספוט – ברירת מחדל' end,true,auth.uid())
      returning id into extra_template;
      insert into public.v2_template_sections(template_id,title,position,settings)
      values(extra_template,case when template_kind = 'scenario' then 'פרטי התרחיש' else 'תיעוד הנכס' end,0,
        case when template_kind = 'scenario' then '{"base_section":true}'::jsonb else '{}'::jsonb end)
      returning id into extra_section;
      if template_kind = 'scenario' then
        insert into public.v2_template_fields(section_id,field_key,title,field_type,position) values
          (extra_section,'scenario_body','התרחיש','rich_text',0),
          (extra_section,'scenario_reasons','סיבות ואינדיקציות','rich_text',1),
          (extra_section,'scenario_invalidation','מה יבטל את התרחיש','rich_text',2);
      else
        insert into public.v2_template_fields(section_id,field_key,title,field_type,position) values
          (extra_section,'spot_plan','תכנית ונקודות חשובות','rich_text',0),
          (extra_section,'spot_notes','הערות','dynamic_text',1);
      end if;
    end if;
  end loop;
  select s.id into section_journal from public.v2_template_sections s
  where s.template_id = trade_template and s.title = 'יומן' order by s.position limit 1;
  if section_journal is not null and not exists(select 1 from public.v2_template_fields where section_id = section_journal and (field_key = 'trade_pnl' or field_type='pnl'))
    and not exists(select 1 from public.v2_trades where template_id = trade_template) then
    insert into public.v2_template_fields(section_id,field_key,title,field_type,position)
    values(section_journal,'trade_pnl','PnL','pnl',coalesce((select max(position)+1 from public.v2_template_fields where section_id = section_journal),0));
  end if;
  insert into public.v2_asset_settings(workspace_id,symbol,display_name,default_collection_template_id,default_scenario_template_id,default_trade_template_id,created_by,updated_by)
  values(p_workspace_id,'BTC','Bitcoin',collection_template,
    (select id from public.v2_templates where workspace_id = p_workspace_id and kind = 'scenario' and active and is_default and asset_symbol is null limit 1),
    trade_template,auth.uid(),auth.uid())
  on conflict(workspace_id,symbol) do nothing;

  -- Stage on ordinary authorized initialization once a complete source workflow
  -- exists. Partial/no-source workspaces keep working; the explicit importer
  -- reports a precise validation failure when asked to import an incomplete source.
  if to_regclass('public.app_settings') is not null and to_regclass('public.template_stages') is not null then
  if (select count(distinct btrim(s.title))=4 from public.template_stages s
      where s.template_version_id=(select active_template_version_id from public.app_settings order by updated_at desc,id limit 1)
      and s.active and btrim(s.title) in ('סריקת טרייד','כניסה','ניהול טרייד','יומן')) then
    begin
      perform public.v2_import_legacy_trade_template(p_workspace_id);
    exception when sqlstate 'PT422' then
      -- A malformed source must not prevent workspace access. Only the
      -- importer's source-validation failures are recoverable here; permission,
      -- integrity, serialization and other database errors still propagate.
      raise notice 'Automatic trade template import deferred: %',sqlerrm;
    end;
  end if;
  end if;
end;
$$;

revoke all on function public.v2_seed_default_templates(uuid) from public,anon;
grant execute on function public.v2_seed_default_templates(uuid) to authenticated;
notify pgrst,'reload schema';
