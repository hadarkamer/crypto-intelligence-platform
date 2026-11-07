alter table public.v2_template_fields drop constraint if exists v2_template_fields_field_type_check;
alter table public.v2_template_fields add constraint v2_template_fields_field_type_check check(field_type in (
 'dynamic_text','rich_text','short_text','long_text','checkbox','yes_no_na','liquidity_table','news_table','news_source_table',
 'economic_calendar','number','pnl','journal','image','info','single_choice','multi_choice','datetime','link','stage_conclusion'));
-- Complete editable sheets: source versions and all existing entity answers remain intact.
-- Canonical V2 hierarchy: template -> section (sheet) -> settings.groups -> fields.
create or replace function public.v2_is_generated_template(p_template_id uuid)
returns boolean language plpgsql stable security definer set search_path=public,pg_temp as $$
declare t public.v2_templates; actual jsonb; expected jsonb;
begin
 select * into t from public.v2_templates where id=p_template_id;
 if not found or t.settings<>'{}' or t.asset_symbol is not null then return false; end if;
 if exists(select 1 from public.v2_template_sections s where s.template_id=t.id and (not s.active or s.settings<>'{}'))
 or exists(select 1 from public.v2_template_fields f join public.v2_template_sections s on s.id=f.section_id where s.template_id=t.id
   and (not f.active or f.settings<>'{}' or f.default_align<>'right' or f.pinned_scope<>'none')) then return false; end if;
 select jsonb_agg(jsonb_build_array(s.title,s.position,f.field_key,f.title,f.field_type,f.position,f.required) order by s.position,f.position)
 into actual from public.v2_template_sections s join public.v2_template_fields f on f.section_id=s.id where s.template_id=t.id;
 if t.kind='collection' and t.name='איסוף מידע – ברירת מחדל' and (select count(*) from public.v2_template_sections where template_id=t.id)=2 then
  expected:='[["חדשות ומאקרו",0,"news","חדשות","news_source_table",0,false],["חדשות ומאקרו",0,"macro","מאקרו","dynamic_text",1,false],["נזילות, טכני ונתונים",1,"liquidity","נזילות","liquidity_table",0,false],["נזילות, טכני ונתונים",1,"technical","טכני ונתונים","dynamic_text",1,false]]';
 elsif t.kind='trade' and t.name='טרייד – ברירת מחדל' and (select count(*) from public.v2_template_sections where template_id=t.id)=4 then
  expected:='[["סריקת טרייד",0,"trade_thesis","תזה ותרחיש","long_text",0,true],["סריקת טרייד",0,"trade_risks","מה יבטל את התרחיש","long_text",1,true],["כניסה",1,"entry_plan","תכנית כניסה","long_text",0,true],["כניסה",1,"stop_loss","סטופ","short_text",1,true],["ניהול טרייד",2,"management_plan","תכנית ניהול","long_text",0,true],["יומן",3,"conclusions","מסקנות","long_text",0,false],["יומן",3,"notes","הערות","long_text",1,false],["יומן",3,"trade_pnl","PnL","pnl",2,false]]';
  if jsonb_array_length(actual)=7 then expected:=expected-7; end if;
 else return false; end if;
 return actual=expected;
end $$;
revoke all on function public.v2_is_generated_template(uuid) from public,anon,authenticated;

create or replace function public.v2_import_complete_legacy_template(p_workspace_id uuid,p_kind text)
returns uuid language plpgsql security definer set search_path=public,pg_temp as $$
declare source_version uuid; source_sheet uuid; source_ids uuid[]; stage_ids uuid[]; tid uuid; previous_default uuid;
 s record; sid uuid; groups_json jsonb; fingerprint text; role_name text; sheet_index integer;
 group_ids uuid[]; actual_count integer; category_count integer; key_name text;
begin
 if auth.uid() is null or not coalesce(public.v2_can_edit_workspace(p_workspace_id),false) then
  raise exception 'Workspace edit permission required' using errcode='42501'; end if;
 if p_kind not in ('collection','trade') then raise exception 'Unsupported import kind' using errcode='22023'; end if;
 perform 1 from public.v2_workspaces where id=p_workspace_id for update;
 if not found then raise exception 'Workspace not found' using errcode='42501'; end if;
 if to_regclass('public.app_settings') is null or to_regclass('public.template_questions') is null then return null; end if;
 select active_template_version_id into source_version from public.app_settings order by updated_at desc,id limit 1 for share;
 if source_version is null then return null; end if;
 if p_kind='collection' then
  -- Restrict to the original first collection sheet, avoiding similarly named groups elsewhere.
  select st.id into source_sheet from public.template_stages st where st.template_version_id=source_version and st.active
   and exists(select 1 from public.template_groups g where g.stage_id=st.id and g.active and btrim(g.title)='חדשות')
   and exists(select 1 from public.template_groups g where g.stage_id=st.id and g.active and btrim(g.title)='מאקרו')
   order by st.position,st.id limit 1;
  select array_agg(id order by position,id),count(*),count(distinct category) into source_ids,actual_count,category_count from (
   select g.*,case btrim(g.title) when 'חדשות' then 'news' when 'מאקרו' then 'macro' when 'נזילות' then 'liquidity'
    else 'technical' end category from public.template_groups g where g.stage_id=source_sheet and g.active
    and (btrim(g.title) in ('חדשות','מאקרו','נזילות') or btrim(g.title) ~ '^(טכני( ונתונים| / נתונים)?|נתונים וטכני|טכני, נתונים)$')) selected;
  if actual_count<>4 or category_count<>4 then raise exception 'Collection source must contain exactly the active news, macro, liquidity and technical groups in its first sheet' using errcode='PT422'; end if;
  stage_ids:=array[source_sheet];
 else
  select array_agg(id order by position,id),count(*),count(distinct btrim(title)) into stage_ids,actual_count,category_count
   from public.template_stages where template_version_id=source_version and active and btrim(title) in ('כניסה','ניהול טרייד','יומן');
  if actual_count<>3 or category_count<>3 then raise exception 'Trade source requires the three sheets כניסה, ניהול טרייד, יומן' using errcode='PT422'; end if;
  select array_agg(id order by stage_id,position,id) into source_ids from public.template_groups where stage_id=any(stage_ids);
 end if;
 perform 1 from public.template_stages where id=any(stage_ids) order by id for share;
 perform 1 from public.template_groups where id=any(source_ids) order by id for share;
 perform 1 from public.template_questions where group_id=any(source_ids) order by id for share;
 if exists(select 1 from public.template_stages st where st.id=any(stage_ids) and not exists(
   select 1 from public.template_groups g join public.template_questions q on q.group_id=g.id
   where g.id=any(source_ids) and g.stage_id=st.id and g.active and q.active)) then
  raise exception 'Source sheets must contain active questions' using errcode='PT422'; end if;
 begin perform public.v2_legacy_field_type(question_type,field_key) from public.template_questions where group_id=any(source_ids);
 exception when check_violation then raise exception 'Unsupported source field type: %',sqlerrm using errcode='PT422'; end;
 select md5(jsonb_build_object('stages',(select jsonb_agg(to_jsonb(st) order by st.position,st.id) from public.template_stages st where st.id=any(stage_ids)),
  'groups',(select jsonb_agg(to_jsonb(g) order by g.stage_id,g.position,g.id) from public.template_groups g where g.id=any(source_ids)),
  'questions',(select jsonb_agg(to_jsonb(q) order by q.group_id,q.position,q.id) from public.template_questions q where q.group_id=any(source_ids)))::text) into fingerprint;
 select id into tid from public.v2_templates where workspace_id=p_workspace_id and kind=p_kind
  and settings->>'origin'='yoav_complete_sheets_v2' and settings->>'legacy_source_fingerprint'=fingerprint
  and settings->>'legacy_template_version_id'=source_version::text order by created_at,id limit 1;
 if tid is not null then return tid; end if;
 select id into previous_default from public.v2_templates where workspace_id=p_workspace_id and kind=p_kind and is_default and active and asset_symbol is null for update;
 insert into public.v2_templates(workspace_id,kind,name,active,is_default,settings,created_by)
 values(p_workspace_id,p_kind,case p_kind when 'collection' then 'איסוף מידע – מהגליונות המקוריים' else 'טרייד – כניסה, ניהול ויומן' end,true,false,
  jsonb_build_object('origin','yoav_complete_sheets_v2','legacy_import',true,'legacy_import_version',4,'hierarchy_version',2,
   'legacy_template_version_id',source_version,'legacy_source_fingerprint',fingerprint,'backend_import_ready',true,'activation_pending_ui',false),auth.uid()) returning id into tid;
 for sheet_index in 0..case when p_kind='collection' then 1 else 2 end loop
  if p_kind='collection' then
   key_name:=case sheet_index when 0 then 'news_macro' else 'data' end;
   select array_agg(g.id order by case btrim(g.title) when 'חדשות' then 0 when 'מאקרו' then 1 when 'נזילות' then 2 else 3 end,g.id)
    into group_ids from public.template_groups g where g.id=any(source_ids) and (btrim(g.title) in ('חדשות','מאקרו'))=(sheet_index=0);
   select * into s from public.template_stages where id=source_sheet; role_name:=null;
  else
   role_name:=(array['entry','manage','journal'])[sheet_index+1];
   select * into s from public.template_stages where id=any(stage_ids) and btrim(title)=(array['כניסה','ניהול טרייד','יומן'])[sheet_index+1];
   select array_agg(g.id order by g.position,g.id) into group_ids from public.template_groups g where g.stage_id=s.id; key_name:=role_name;
  end if;
  select coalesce(jsonb_agg(to_jsonb(g) order by array_position(group_ids,g.id)),'[]') into groups_json from public.template_groups g where g.id=any(group_ids);
  insert into public.v2_template_sections(template_id,title,position,active,settings)
  values(tid,case when p_kind='trade' then s.title when sheet_index=0 then 'חדשות' else 'נזילות, טכני ונתונים' end,sheet_index,true,
   jsonb_build_object('hierarchy_version',2,'sheet_key',key_name,'workflow_role',role_name,'description',s.description,
    'groups',groups_json,'legacy_groups',groups_json,'legacy_stage_id',s.id,'legacy_stage_title',s.title,'legacy_stage',to_jsonb(s),
    'action',case role_name when 'entry' then 'activate' when 'manage' then 'close' else '' end)) returning id into sid;
  insert into public.v2_template_fields(section_id,field_key,title,field_type,position,required,default_align,pinned_scope,active,settings)
  select sid,coalesce(nullif(q.question_key,''),nullif(q.field_key,''),'legacy')||':'||q.id::text,q.title,public.v2_legacy_field_type(q.question_type,q.field_key),
   (row_number() over(order by array_position(group_ids,q.group_id),q.position,q.id)-1)::integer,
   q.required and not(p_kind='trade' and (role_name='journal' or public.v2_legacy_field_type(q.question_type,q.field_key) in ('pnl','info'))),
   case when q.default_text_align='left' then 'left' else 'right' end,
   case when to_jsonb(q)->>'pinned_scope' in ('all','template') then 'template' when to_jsonb(q)->>'pinned_scope' in ('stage','section') then 'section' else 'none' end,
   q.active,jsonb_build_object('group_id',q.group_id,'legacy_group_id',q.group_id,'legacy_question_id',q.id,'legacy_question_key',q.question_key,
    'question_key',q.question_key,'legacy_field_key',q.field_key,'legacy_question',to_jsonb(q),'original_required',q.required,
    'description',q.description,'help_text',q.help_text,'links',q.links_json,'links_json',q.links_json,'source_url',q.source_url,
    'options_json',q.options_json,'visibility_rule_json',q.visibility_rule_json,'legacy_default_text_align',q.default_text_align)
   from public.template_questions q where q.group_id=any(group_ids);
 end loop;
 -- A generated default may already hold answers; only future entity selection changes.
 if previous_default is null or public.v2_is_generated_template(previous_default) then
  update public.v2_templates set is_default=false where id=previous_default;
  update public.v2_templates set is_default=true where id=tid;
  if p_kind='collection' then update public.v2_asset_settings set default_collection_template_id=tid where workspace_id=p_workspace_id and default_collection_template_id=previous_default;
  else update public.v2_asset_settings set default_trade_template_id=tid where workspace_id=p_workspace_id and default_trade_template_id=previous_default; end if;
 end if;
 return tid;
end $$;
revoke all on function public.v2_import_complete_legacy_template(uuid,text) from public,anon;
grant execute on function public.v2_import_complete_legacy_template(uuid,text) to authenticated;
create or replace function public.v2_import_legacy_collection_template(p_workspace_id uuid)
returns uuid language sql security invoker set search_path=public,pg_temp as $$ select public.v2_import_complete_legacy_template(p_workspace_id,'collection') $$;
create or replace function public.v2_import_legacy_trade_template(p_workspace_id uuid)
returns uuid language sql security invoker set search_path=public,pg_temp as $$ select public.v2_import_complete_legacy_template(p_workspace_id,'trade') $$;

alter table public.v2_trades add column if not exists archived_at timestamptz;
create index if not exists v2_trades_trash_idx on public.v2_trades(workspace_id,archived_at) where archived_at is not null;
drop policy if exists v2_trades_delete on public.v2_trades;
create policy v2_trades_delete on public.v2_trades for delete to authenticated
 using (archived_at is not null and public.v2_can_edit_workspace(workspace_id));
create or replace function public.v2_archive_trade(p_trade_id uuid)
returns void language plpgsql security invoker set search_path=public,pg_temp as $$
begin
 update public.v2_trades set archived_at=coalesce(archived_at,now()),updated_by=auth.uid()
 where id=p_trade_id and public.v2_can_edit_workspace(workspace_id);
 if not found then raise exception 'Trade edit permission required' using errcode='42501'; end if;
end $$;
create or replace function public.v2_restore_trade(p_trade_id uuid)
returns void language plpgsql security invoker set search_path=public,pg_temp as $$
begin
 update public.v2_trades set archived_at=null,updated_by=auth.uid() where id=p_trade_id and public.v2_can_edit_workspace(workspace_id);
 if not found then raise exception 'Trade edit permission required' using errcode='42501'; end if;
end $$;
create or replace function public.v2_purge_trade(p_trade_id uuid)
returns void language plpgsql security invoker set search_path=public,pg_temp as $$
declare t public.v2_trades;
begin
 select * into t from public.v2_trades where id=p_trade_id and public.v2_can_edit_workspace(workspace_id) for update;
 if not found then raise exception 'Trade edit permission required' using errcode='42501'; end if;
 if t.archived_at is null then raise exception 'Move the trade to trash before deleting it' using errcode='23514'; end if;
 if exists(select 1 from public.v2_trades where root_trade_id=t.id) then
  raise exception 'Delete the subsequent trade cycles from trash before deleting their root trade' using errcode='23514'; end if;
 delete from public.v2_trades where id=t.id;
end $$;
revoke all on function public.v2_archive_trade(uuid),public.v2_restore_trade(uuid),public.v2_purge_trade(uuid) from public,anon;
grant execute on function public.v2_archive_trade(uuid),public.v2_restore_trade(uuid),public.v2_purge_trade(uuid) to authenticated;

create or replace function public.v2_normalize_structured_rows(p_field_id uuid,p_type text,p_value jsonb)
returns jsonb language plpgsql immutable set search_path=public,pg_temp as $$
declare source_rows jsonb; result jsonb:='[]'; r jsonb; base jsonb; lev jsonb; levels_json jsonb; idx integer:=0; n integer; existing_idx integer;
 timeframe text; rid text; side_name text; target jsonb;
begin
 source_rows:=case when jsonb_typeof(p_value#>'{structured,rows}')='array' then p_value#>'{structured,rows}' else coalesce(p_value->'rows','[]') end;
 if jsonb_typeof(source_rows)<>'array' then raise exception 'Invalid existing rows' using errcode='23514'; end if;
 for r in select value from jsonb_array_elements(source_rows) loop
  if jsonb_typeof(r)<>'object' then raise exception 'Existing row is not an object' using errcode='23514'; end if;
  if p_type<>'liquidity_table' then
   rid:=case when jsonb_typeof(r->'id')='string' then r->>'id' else p_field_id::text||':news:'||idx::text end;
   result:=result||jsonb_build_array(r||jsonb_build_object('id',rid,'body',coalesce(r->>'body',r->>'text',''),'importance',coalesce(r->>'importance','neutral')));
  else
   timeframe:=case when jsonb_typeof(r->'timeframe')='string' then r->>'timeframe' else '24H' end;
   rid:=case when jsonb_typeof(r->'id')='string' then r->>'id' else p_field_id::text||':'||timeframe end;
   base:=r||jsonb_build_object('id',rid,'timeframe',timeframe,'position',coalesce(r->'position',to_jsonb(idx)));
   levels_json:='[]';
   if jsonb_typeof(r->'levels')='array' then
    n:=0;
    for lev in select value from jsonb_array_elements(r->'levels') loop
     levels_json:=levels_json||jsonb_build_array(lev||jsonb_build_object('id',case when jsonb_typeof(lev->'id')='string' then lev->>'id' else rid||':level:'||n::text end,
      'row_id',rid,'position',coalesce(lev->'position',to_jsonb(n)),'price_text',coalesce(lev->>'price_text',lev->>'price','')));n:=n+1;
    end loop;
   elsif jsonb_typeof(r->'above')='array' or jsonb_typeof(r->'below')='array' then
    foreach side_name in array array['above','below'] loop
     n:=0;
     for lev in select value from jsonb_array_elements(case when jsonb_typeof(r->side_name)='array' then r->side_name else '[]'::jsonb end) loop
      levels_json:=levels_json||jsonb_build_array(lev||jsonb_build_object('id',case when jsonb_typeof(lev->'id')='string' then lev->>'id' else rid||':'||side_name||':'||n::text end,
       'row_id',rid,'side',side_name,'position',n,'price_text',coalesce(lev->>'price_text',lev->>'price',''),'taken',coalesce(lev->'taken'='true'::jsonb,false)));n:=n+1;
     end loop;
    end loop;
   elsif r ? 'price' or r ? 'price_text' then
    select ordinality::integer-1,value into existing_idx,target from jsonb_array_elements(result) with ordinality where value->>'timeframe'=timeframe limit 1;
    target:=coalesce(target,base||jsonb_build_object('levels','[]'::jsonb));
    lev:=r||jsonb_build_object('id',case when jsonb_typeof(r->'id')='string' then r->>'id' else rid||':'||idx::text end,
     'row_id',target->>'id','side',case when r->>'side'='below' then 'below' else 'above' end,'position',idx,
     'price_text',coalesce(r->>'price_text',r->>'price',''),'taken',coalesce(r->'taken'='true'::jsonb,false));
    target:=target||jsonb_build_object('levels',coalesce(target->'levels','[]')||jsonb_build_array(lev));
    if existing_idx is null then result:=result||jsonb_build_array(target);else result:=jsonb_set(result,array[existing_idx::text],target);end if;
    idx:=idx+1;continue;
   end if;
   result:=result||jsonb_build_array(base||jsonb_build_object('levels',levels_json));
  end if;
  idx:=idx+1;
 end loop;
 return result;
end $$;
revoke all on function public.v2_normalize_structured_rows(uuid,text,jsonb) from public,anon;
grant execute on function public.v2_normalize_structured_rows(uuid,text,jsonb) to authenticated;

create or replace function public.patch_v2_entity_rows(p_scope text,p_entity_id uuid,p_field_id uuid,p_operation text,
 p_row_id text,p_patch jsonb default '{}'::jsonb,p_parent_row_id text default null)
returns jsonb language plpgsql security invoker set search_path=public,pg_temp as $$
declare entity_table text; value_table text; entity_column text; wid uuid; tid uuid; v jsonb; rows_json jsonb;
 r jsonb; normalized jsonb:='[]'; result_rows jsonb:='[]'; levels_json jsonb; level jsonb; result_levels jsonb; matched boolean:=false; found_level boolean; field_type text; idx integer:=0;
begin
 if auth.uid() is null then raise exception 'Authentication required' using errcode='42501'; end if;
 if p_operation not in ('upsert','delete','upsert_level','delete_level','patch_meta') or nullif(btrim(p_row_id),'') is null
  or jsonb_typeof(p_patch) is distinct from 'object' then raise exception 'Invalid row operation' using errcode='22023'; end if;
 if p_operation in ('upsert_level','delete_level') and nullif(btrim(p_parent_row_id),'') is null then raise exception 'Parent row required' using errcode='22023'; end if;
 case p_scope when 'collection' then entity_table:='v2_collection_sessions';value_table:='v2_collection_values';entity_column:='session_id';
 when 'trade' then entity_table:='v2_trades';value_table:='v2_trade_values';entity_column:='trade_id';
 when 'scenario' then entity_table:='v2_scenarios';value_table:='v2_scenario_values';entity_column:='scenario_id';
 when 'spot' then entity_table:='v2_spot_assets';value_table:='v2_spot_values';entity_column:='asset_id';
 else raise exception 'Invalid scope' using errcode='22023'; end case;
 -- Lock the parent before reading: two first writes cannot both overwrite an absent value row.
 execute format('select workspace_id,template_id from public.%I where id=$1 for update',entity_table) into wid,tid using p_entity_id;
 if wid is null or not coalesce(public.v2_can_edit_workspace(wid),false) then raise exception 'Entity edit permission required' using errcode='42501'; end if;
 select f.field_type into field_type from public.v2_template_fields f join public.v2_template_sections s on s.id=f.section_id where f.id=p_field_id and s.template_id=tid;
 if field_type is null or field_type not in ('liquidity_table','news_table','news_source_table') then raise exception 'Structured field does not belong to this entity template' using errcode='23514'; end if;
 if p_operation in ('upsert_level','delete_level') and field_type<>'liquidity_table' then raise exception 'Only liquidity rows have levels' using errcode='23514'; end if;
 execute format('select value from public.%I where %I=$1 and field_id=$2 for update',value_table,entity_column) into v using p_entity_id,p_field_id;
 v:=coalesce(v,'{}');
 if jsonb_typeof(v)<>'object' or (v ? 'rows' and jsonb_typeof(v->'rows')<>'array') then raise exception 'Existing structured value has invalid shape' using errcode='23514'; end if;
 normalized:=public.v2_normalize_structured_rows(p_field_id,field_type,v);
 for r in select value from jsonb_array_elements(normalized) loop
  if p_operation<>'patch_meta' and r->>'id'=(case when p_operation in ('upsert_level','delete_level') then p_parent_row_id else p_row_id end) then
   matched:=true;
   if p_operation='delete' then continue;
   elsif p_operation='upsert' then r:=r||p_patch||jsonb_build_object('id',p_row_id);
   else
    if jsonb_typeof(coalesce(r->'levels','[]'))<>'array' then raise exception 'Existing levels are invalid' using errcode='23514'; end if;
    result_levels:='[]';found_level:=false;
    for level in select value from jsonb_array_elements(coalesce(r->'levels','[]')) loop
     if level->>'id'=p_row_id then found_level:=true;
      if p_operation='delete_level' then continue; end if;
      level:=level||p_patch||jsonb_build_object('id',p_row_id);
     end if; result_levels:=result_levels||jsonb_build_array(level);
    end loop;
    if not found_level and p_operation='upsert_level' then result_levels:=result_levels||jsonb_build_array(p_patch||jsonb_build_object('id',p_row_id)); end if;
    r:=r||jsonb_build_object('levels',result_levels);
   end if;
  end if;
  result_rows:=result_rows||jsonb_build_array(r);
 end loop;
 if p_operation='patch_meta' then
  if exists(select 1 from jsonb_object_keys(p_patch) key where key not in ('not_relevant','completion')) then raise exception 'Only completion metadata may be patched' using errcode='22023'; end if;
  if (p_patch ? 'not_relevant' and jsonb_typeof(p_patch->'not_relevant')<>'boolean') or (p_patch ? 'completion' and p_patch->'completion'<>'null'::jsonb and p_patch->>'completion'<>'na') then raise exception 'Invalid completion metadata' using errcode='22023'; end if;
  v:=v||p_patch;
 end if;
 if not matched then
  if p_operation='upsert' then result_rows:=result_rows||jsonb_build_array(p_patch||jsonb_build_object('id',p_row_id));
  elsif p_operation in ('upsert_level','delete_level') then raise exception 'Parent row not found' using errcode='23514'; end if;
 end if;
 v:=v||jsonb_build_object('rows',result_rows);
 if jsonb_typeof(v#>'{structured,rows}')='array' then v:=jsonb_set(v,'{structured,rows}',result_rows);end if;
 execute format('insert into public.%I(%I,field_id,value,updated_by) values($1,$2,$3,auth.uid()) on conflict(%I,field_id) do update set value=excluded.value,updated_by=auth.uid() returning value',value_table,entity_column,entity_column)
 into v using p_entity_id,p_field_id,v;
 return v;
end $$;
revoke all on function public.patch_v2_entity_rows(text,uuid,uuid,text,text,jsonb,text) from public,anon;
grant execute on function public.patch_v2_entity_rows(text,uuid,uuid,text,text,jsonb,text) to authenticated;

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
    if qtype is null or qtype not in ('info','checkbox','yes_no_na','single_choice','multi_choice','short_text','long_text','number','datetime','image','link','stage_conclusion','liquidity_table','news_table','economic_calendar')
      or nullif(btrim(qu->>'title'),'') is null then raise exception 'Unsupported legacy question type or missing title: %',qtype using errcode='23514'; end if;
    insert into public.template_questions(group_id,title,description,question_type,position,required,active,options_json,visibility_rule_json,help_text,source_url,links_json,default_text_align,field_key,question_key)
    values(gid,qu->>'title',qu->>'description',qtype,qpos,coalesce((qu->>'required')::boolean,false),coalesce((qu->>'active')::boolean,true),
     nullif(qu->'options_json','null'::jsonb),nullif(qu->'visibility_rule_json','null'::jsonb),qu->>'help_text',qu->>'source_url',nullif(qu->'links_json','null'::jsonb),
     coalesce(qu->>'default_text_align','right'),qu->>'field_key',coalesce(nullif(qu->>'question_key',''),gen_random_uuid()::text));qpos:=qpos+1;
   end loop;
  end loop;
 end loop;
 return _target_version_id;
end $$;
revoke all on function public.import_template_tree_atomic(uuid,uuid,uuid,jsonb) from public,anon;
grant execute on function public.import_template_tree_atomic(uuid,uuid,uuid,jsonb) to authenticated;

create or replace function public.v2_visible_trade_fields(p_trade_id uuid)
returns setof uuid language sql stable security invoker set search_path=public,pg_temp as $$
 with fields as (
  select f.id,f.field_type,f.settings,coalesce(f.settings->>'group_id',f.settings->>'legacy_group_id','') group_id,
   s.id section_id,f.position,v.value
  from public.v2_trades t join public.v2_template_sections s on s.template_id=t.template_id
  join public.v2_template_fields f on f.section_id=s.id
  left join public.v2_trade_values v on v.trade_id=t.id and v.field_id=f.id
  where t.id=p_trade_id and s.active and f.active
   and not exists(select 1 from jsonb_array_elements(case when jsonb_typeof(s.settings->'groups')='array' then s.settings->'groups' else '[]'::jsonb end) g
    where g->>'id'=coalesce(f.settings->>'group_id',f.settings->>'legacy_group_id') and g->>'active'='false')
 ), ordered as (
  select *,lag(field_type) over w previous_type,lag(value) over w previous_value from fields
  window w as(partition by section_id,group_id order by position,id)
 ) select id from ordered where coalesce(settings#>>'{visibility_rule_json,show_when}',settings#>>'{legacy_question,visibility_rule_json,show_when}','')<>'prev_yes'
  or case when previous_type='checkbox' then previous_value->'checked'='true'::jsonb
      else coalesce(previous_value->>'choice',previous_value->>'text')='yes' end;
$$;
revoke all on function public.v2_visible_trade_fields(uuid) from public,anon,authenticated;

create or replace function public.v2_save_template(
 p_workspace_id uuid,p_template_id uuid,p_kind text,p_name text,
 p_active boolean,p_default boolean,p_sections jsonb)
returns uuid language plpgsql security definer set search_path=public,pg_temp as $$
declare tid uuid; sec jsonb; fld jsonb; sid uuid; i integer:=0; j integer; old_kind text;
begin
 if not coalesce(public.v2_can_edit_workspace(p_workspace_id),false) then raise exception 'Workspace edit permission required'; end if;
 if p_kind not in ('collection','scenario','trade','spot') or nullif(btrim(p_name),'') is null then raise exception 'Invalid template'; end if;
 if p_sections is null or jsonb_typeof(p_sections)<>'array' or jsonb_array_length(p_sections)>100 then raise exception 'Invalid sections'; end if;
 perform pg_advisory_xact_lock(hashtextextended(p_workspace_id::text||p_kind,0));
 tid:=coalesce(p_template_id,gen_random_uuid());
 if p_template_id is not null then
  select kind into old_kind from v2_templates where id=tid and workspace_id=p_workspace_id for update;
  if not found or old_kind<>p_kind then raise exception 'Template does not belong to this workspace and kind'; end if;
  if public.v2_template_in_use(tid) then raise exception 'Create a new version before editing a used template'; end if;
 end if;
 if p_default and p_active then update v2_templates set is_default=false where workspace_id=p_workspace_id and kind=p_kind and asset_symbol is null; end if;
 insert into v2_templates(id,workspace_id,kind,name,active,is_default,created_by)
 values(tid,p_workspace_id,p_kind,btrim(p_name),p_active,p_default and p_active,auth.uid())
 on conflict(id) do update set name=excluded.name,active=excluded.active,is_default=excluded.is_default,updated_at=now();
 -- This template has no entities. Replacing its draft structure cannot remove answers.
 delete from v2_template_sections where template_id=tid;
 for sec in select value from jsonb_array_elements(p_sections) loop
  if nullif(btrim(sec->>'title'),'') is null then raise exception 'Section title is required'; end if;
  if jsonb_typeof(coalesce(sec->'settings','{}'::jsonb))<>'object' or jsonb_typeof(coalesce(sec->'fields','[]'::jsonb))<>'array' or jsonb_typeof(coalesce(sec#>'{settings,groups}','[]'::jsonb))<>'array' then raise exception 'Invalid sheet structure' using errcode='23514'; end if;
  sid:=gen_random_uuid();
  insert into v2_template_sections(id,template_id,title,position,active,settings)
  values(sid,tid,btrim(sec->>'title'),i,coalesce((sec->>'active')::boolean,true),coalesce(sec->'settings','{}'::jsonb));
  i:=i+1;j:=0;
  for fld in select value from jsonb_array_elements(coalesce(sec->'fields','[]'::jsonb)) loop
   if jsonb_typeof(coalesce(fld->'settings','{}'::jsonb))<>'object' then raise exception 'Invalid field settings' using errcode='23514'; end if;
   if nullif(btrim(fld->>'title'),'') is null then raise exception 'Field title is required'; end if;
   insert into v2_template_fields(section_id,field_key,title,field_type,position,required,default_align,pinned_scope,active,settings)
   values(sid,coalesce(nullif(fld->>'field_key',''),'field_'||gen_random_uuid()::text),btrim(fld->>'title'),fld->>'field_type',j,
    coalesce((fld->>'required')::boolean,false),coalesce(fld->>'default_align','right'),coalesce(fld->>'pinned_scope','none'),coalesce((fld->>'active')::boolean,true),coalesce(fld->'settings','{}'::jsonb));
   j:=j+1;
  end loop;
 end loop;
 return tid;
end $$;
revoke all on function public.v2_save_template(uuid,uuid,text,text,boolean,boolean,jsonb) from public,anon;
grant execute on function public.v2_save_template(uuid,uuid,text,text,boolean,boolean,jsonb) to authenticated;

alter table public.v2_scenarios add column if not exists extra_boxes jsonb not null default '[]'::jsonb;
alter table public.v2_scenarios drop constraint if exists v2_scenarios_extra_boxes_array;
alter table public.v2_scenarios add constraint v2_scenarios_extra_boxes_array check(jsonb_typeof(extra_boxes)='array');
alter table public.v2_scenarios add column if not exists supporting_evidence jsonb not null default '[]'::jsonb;
alter table public.v2_scenarios drop constraint if exists v2_scenarios_supporting_evidence_array;
alter table public.v2_scenarios add constraint v2_scenarios_supporting_evidence_array check(jsonb_typeof(supporting_evidence)='array');
alter table public.v2_scenarios add column if not exists invalidating_evidence jsonb not null default '[]'::jsonb;
alter table public.v2_scenarios drop constraint if exists v2_scenarios_invalidating_evidence_array;
alter table public.v2_scenarios add constraint v2_scenarios_invalidating_evidence_array check(jsonb_typeof(invalidating_evidence)='array');
create or replace function public.save_v2_scenario(p_scenario jsonb,p_values jsonb default '[]')
returns public.v2_scenarios language plpgsql security invoker set search_path=public,pg_temp as $$
declare sid uuid:=coalesce(nullif(p_scenario->>'id','')::uuid,gen_random_uuid()); wid uuid:=(p_scenario->>'workspace_id')::uuid; result public.v2_scenarios; entry jsonb; exists_id uuid;
begin
  if auth.uid() is null or not public.v2_can_edit_workspace(wid) then raise exception 'Workspace edit permission required' using errcode='42501'; end if;
  if nullif(trim(p_scenario->>'title'),'') is null or coalesce(p_scenario->>'asset_symbol','') !~ '^[A-Z0-9][A-Z0-9._-]{0,19}$' then raise exception 'Scenario title and valid asset are required' using errcode='23514'; end if;
  if jsonb_typeof(coalesce(p_scenario->'alternatives','[]'))<>'array' or jsonb_typeof(p_values)<>'array' then raise exception 'Invalid scenario values' using errcode='23514'; end if;
  select id into exists_id from public.v2_scenarios where id=sid and workspace_id=wid for update;
  if exists_id is null then
    insert into public.v2_scenarios(id,workspace_id,asset_symbol,title,template_id,author_name,scenario_html,reasons_html,invalidation_html,alternatives,extra_boxes,supporting_evidence,invalidating_evidence,created_by,updated_by)
    values(sid,wid,p_scenario->>'asset_symbol',trim(p_scenario->>'title'),nullif(p_scenario->>'template_id','')::uuid,p_scenario->>'author_name',p_scenario->>'scenario_html',p_scenario->>'reasons_html',p_scenario->>'invalidation_html',coalesce(p_scenario->'alternatives','[]'),coalesce(p_scenario->'extra_boxes','[]'),coalesce(p_scenario->'supporting_evidence','[]'),coalesce(p_scenario->'invalidating_evidence','[]'),auth.uid(),auth.uid()) returning * into result;
  else
    update public.v2_scenarios set title=trim(p_scenario->>'title'),asset_symbol=p_scenario->>'asset_symbol',template_id=nullif(p_scenario->>'template_id','')::uuid,author_name=p_scenario->>'author_name',scenario_html=p_scenario->>'scenario_html',reasons_html=p_scenario->>'reasons_html',invalidation_html=p_scenario->>'invalidation_html',alternatives=coalesce(p_scenario->'alternatives',alternatives),extra_boxes=coalesce(p_scenario->'extra_boxes',extra_boxes),supporting_evidence=coalesce(p_scenario->'supporting_evidence',supporting_evidence),invalidating_evidence=coalesce(p_scenario->'invalidating_evidence',invalidating_evidence),updated_by=auth.uid()
    where id=sid and workspace_id=wid returning * into result;
  end if;
  for entry in select value from jsonb_array_elements(p_values) loop
    insert into public.v2_scenario_values(scenario_id,field_id,value,updated_by) values(sid,(entry->>'field_id')::uuid,coalesce(entry->'value','{}'),auth.uid())
    on conflict(scenario_id,field_id) do update set value=excluded.value,updated_by=auth.uid();
  end loop;
  return result;
end $$;
create or replace function public.v2_field_complete(p_type text,p_value jsonb)
returns boolean language plpgsql immutable set search_path=public,pg_temp as $$
declare v jsonb:=coalesce(p_value,'{}'); txt text; r jsonb; lev jsonb;
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
          where s.template_id=new.template_id and s.active and f.active and f.id in (select public.v2_visible_trade_fields(new.id)) and f.required and f.field_type<>'info'
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


create or replace function public.v2_trade_field_is_post_trade(
  p_section_title text,p_section_settings jsonb,p_field_type text,p_field_key text,p_field_settings jsonb)
returns boolean language sql immutable set search_path=public,pg_temp as $$
  select coalesce(p_field_type in ('pnl','journal'),false)
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


notify pgrst,'reload schema';

create or replace function public.v2_collection_value_filled(p_type text,p_value jsonb)
returns boolean language plpgsql immutable set search_path=public,pg_temp as $$
declare row_value jsonb; level_value jsonb; content text;
begin
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
end $$;
revoke all on function public.v2_collection_value_filled(text,jsonb) from public,anon;
grant execute on function public.v2_collection_value_filled(text,jsonb) to authenticated;

