-- Pins are immutable whole-answer copies, independent of history pagination.
-- No source value, native news item, template or legacy project is changed.
begin;

create table if not exists public.v2_collection_previous_answer_pins (
 id uuid primary key default gen_random_uuid(),
 workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
 sheet_key text not null check(nullif(btrim(sheet_key),'') is not null),
 logical_key text not null,
 field_keys text[] not null,
 source_key text not null,
 source_title text not null,
 snapshot jsonb not null check(jsonb_typeof(snapshot)='object'),
 row_order jsonb not null default '[]' check(jsonb_typeof(row_order)='array'),
 pinned_at timestamptz not null default clock_timestamp(),
 pinned_by uuid references auth.users(id) on delete set null,
 unique(workspace_id,sheet_key,source_key)
);
alter table public.v2_collection_previous_answer_pins enable row level security;
revoke all on public.v2_collection_previous_answer_pins from public,anon,authenticated;
grant select on public.v2_collection_previous_answer_pins to authenticated;
drop policy if exists previous_answer_pins_read on public.v2_collection_previous_answer_pins;
create policy previous_answer_pins_read on public.v2_collection_previous_answer_pins for select to authenticated
 using(public.v2_is_workspace_member(workspace_id));

create or replace function app_private.collection_history_field_keys(p_field jsonb)
returns text[] language sql stable set search_path=pg_catalog,public,app_private,pg_temp as $$
 select coalesce(array_agg(distinct btrim(key)) filter(where nullif(btrim(key),'') is not null),'{}'::text[])
 from unnest(array[p_field->>'id',public.v2_collection_field_logical_key(p_field),
   p_field#>>'{settings,question_key}',p_field#>>'{settings,legacy_question_key}',
   p_field#>>'{settings,legacy_question,question_key}',p_field#>>'{settings,legacy_question,field_key}',
   p_field#>>'{settings,legacy_field_key}',p_field->>'field_key',
   'question:'||(p_field#>>'{settings,legacy_question_id}'),
   'question:'||(p_field#>>'{settings,legacy_question,id}')]) keys(key);
$$;
revoke all on function app_private.collection_history_field_keys(jsonb) from public,anon,authenticated;

create or replace function app_private.collection_history_pin_scope(p_session_id uuid,p_field_id uuid,p_edit boolean)
returns jsonb language plpgsql volatile security invoker set search_path=pg_catalog,public,app_private,pg_temp as $$
declare c public.v2_collection_sessions; f jsonb; sec jsonb; sheet text;
begin
 if p_edit then c:=public.v2_lock_collection_scenario_session(p_session_id);
 else select * into c from public.v2_collection_sessions where id=p_session_id for share; end if;
 if c.id is null or auth.uid() is null or c.deleted_at is not null
   or not public.v2_is_workspace_member(c.workspace_id)
   or (p_edit and (not public.v2_can_edit_workspace(c.workspace_id) or c.status='archived')) then
   raise exception 'אין הרשאה לתשובות בגליון זה.' using errcode='42501'; end if;
 select to_jsonb(field_row),to_jsonb(section_row) into f,sec
 from public.v2_template_fields field_row join public.v2_template_sections section_row on section_row.id=field_row.section_id
 where field_row.id=p_field_id and field_row.active and section_row.active and section_row.template_id=c.template_id for share;
 sheet:=public.v2_collection_sheet_key(sec);
 if f is null or sheet='data' or (sheet<>'news_macro' and coalesce(sec->>'title','') !~ '(חדשות|מאקרו)') then
   raise exception 'תיבת התשובות אינה בגליון שנבחר.' using errcode='23514'; end if;
 return jsonb_build_object('session',to_jsonb(c),'field',f,'section',sec,'sheet_key',sheet,
   'field_keys',to_jsonb(app_private.collection_history_field_keys(f)));
end $$;
revoke all on function app_private.collection_history_pin_scope(uuid,uuid,boolean) from public,anon,authenticated;

-- Legacy reads remain behind the same project permission helper as their RLS.
-- Keep every typed answer column, attachment, normalized news/liquidity row and
-- original metadata; the common frontend renderer derives its display shape.
create or replace function app_private.collection_history_legacy_source(p_project_id uuid,p_question_id uuid)
returns jsonb language plpgsql volatile security invoker set search_path=pg_catalog,public,app_private,pg_temp as $$
declare readable boolean; project_row jsonb; question_row jsonb; group_row jsonb; stage_row jsonb;
 answer jsonb; images jsonb; news jsonb; liquidity jsonb; structured jsonb; value jsonb; field jsonb; section jsonb;
 cycle_id uuid; latest_at timestamptz; latest_by text; logical_key text; sheet_key text; body text; entry jsonb;
begin
 if auth.uid() is null or to_regprocedure('app_private.can_access_project(uuid)') is null then
   raise exception 'אין הרשאה לתשובה הישנה.' using errcode='42501'; end if;
 execute 'select app_private.can_access_project($1)' into readable using p_project_id;
 if not coalesce(readable,false) then raise exception 'אין הרשאה לתשובה הישנה.' using errcode='42501'; end if;
 execute 'select to_jsonb(p),to_jsonb(q),to_jsonb(g),to_jsonb(s) from public.projects p
   join public.template_stages s on s.template_version_id=p.template_version_id
   join public.template_groups g on g.stage_id=s.id join public.template_questions q on q.group_id=g.id
   where p.id=$1 and q.id=$2 and s.id=(select s2.id from public.template_stages s2
     where s2.template_version_id=p.template_version_id order by s2.position,s2.id limit 1)'
   into project_row,question_row,group_row,stage_row using p_project_id,p_question_id;
 if question_row is null then raise exception 'התשובה הישנה אינה שייכת למקור זה.' using errcode='23514'; end if;
 sheet_key:=case when btrim(group_row->>'title') ~ '^(חדשות( מאקרו)?|מאקרו)$' then 'news_macro' else 'data' end;
 if sheet_key<>'news_macro' then raise exception 'אין היסטוריית חדשות במקור שנבחר.' using errcode='23514'; end if;
 execute 'select id from public.project_trade_cycles where project_id=$1 order by cycle_number,id limit 1' into cycle_id using p_project_id;
 execute 'select to_jsonb(a) from public.project_answers a where project_id=$1 and question_id=$2
   and ($3 is null or trade_cycle_id is null or trade_cycle_id=$3) order by id desc limit 1'
   into answer using p_project_id,p_question_id,cycle_id;
 execute 'select coalesce(jsonb_agg(to_jsonb(i) order by i.id),''[]'') from public.project_images i
   where project_id=$1 and question_id=$2 and ($3 is null or trade_cycle_id is null or trade_cycle_id=$3)'
   into images using p_project_id,p_question_id,cycle_id;
 execute 'select coalesce(jsonb_agg(to_jsonb(n) order by n.position,n.id),''[]'') from public.project_news_rows n where project_id=$1 and question_id=$2'
   into news using p_project_id,p_question_id;
 execute 'select coalesce(jsonb_agg(to_jsonb(r)||jsonb_build_object(''levels'',coalesce((select jsonb_agg(to_jsonb(l) order by l.position,l.id)
   from public.project_liquidity_levels l where l.row_id=r.id),''[]'')) order by r.position,r.id),''[]'')
   from public.project_liquidity_rows r where project_id=$1 and question_id=$2' into liquidity using p_project_id,p_question_id;
 value:=coalesce(answer,'{}');
 if answer->>'text_value' is not null then value:=value||jsonb_build_object('text',answer->'text_value','html',answer->'text_value'); end if;
 if jsonb_typeof(answer->'number_value')='number' then value:=value||jsonb_build_object('number',answer->'number_value'); end if;
 if jsonb_typeof(answer->'boolean_value')='boolean' then value:=value||jsonb_build_object('checked',answer->'boolean_value'); end if;
 if nullif(answer->>'date_value','') is not null then value:=value||jsonb_build_object('datetime',answer->'date_value'); end if;
 if jsonb_typeof(answer->'choice_value_json')='object' then value:=value||(answer->'choice_value_json');
 elsif jsonb_typeof(answer->'choice_value_json')='array' then value:=value||jsonb_build_object('choices',answer->'choice_value_json');
 elsif jsonb_typeof(answer->'choice_value_json')='string' then value:=value||jsonb_build_object('choice',answer->'choice_value_json'); end if;
 if jsonb_array_length(images)>0 then value:=value||jsonb_build_object('images',images); end if;
 if question_row->>'question_type'='news_table' or jsonb_array_length(news)>0 then
   body:=answer->>'text_value';
   if (body ~* '<img\y' or btrim(regexp_replace(replace(coalesce(body,''),'&nbsp;',' '),'<[^>]*>',' ','g'))<>'')
     and not exists(select 1 from jsonb_array_elements(news) n where n->>'body'=body) then
     news:=jsonb_build_array(jsonb_build_object('id','history-text:'||(answer->>'id'),'question_id',p_question_id,'body',body,
       'importance','neutral','position',-1,'updated_at',answer->'updated_at','updated_by',null))||news;
   end if;
   structured:=jsonb_build_object('type','news_table','rows',news);
 elsif question_row->>'question_type'='liquidity_table' then structured:=jsonb_build_object('type','liquidity_table','rows',liquidity); end if;
 if structured is not null then value:=value||jsonb_build_object('structured',structured,'rows',structured->'rows'); end if;
 latest_at:=(project_row->>'created_at')::timestamptz;
 for entry in select item from jsonb_array_elements(
   case when answer is null then '[]'::jsonb else jsonb_build_array(answer) end ||images||news||liquidity||
   coalesce((select jsonb_agg(l) from jsonb_array_elements(liquidity) r cross join lateral jsonb_array_elements(r->'levels') l),'[]')) item loop
   if coalesce(entry->>'updated_at',entry->>'created_at',project_row->>'created_at')::timestamptz>=latest_at then
     latest_at:=coalesce(entry->>'updated_at',entry->>'created_at',project_row->>'created_at')::timestamptz;
     latest_by:=coalesce(entry->>'updated_by',entry->>'last_edited_by');
   end if;
 end loop;
 section:=jsonb_build_object('id','legacy-group:'||(group_row->>'id'),'template_id','legacy:'||(project_row->>'template_version_id'),
   'title',group_row->'title','position',group_row->'position','active',true,'settings',jsonb_build_object('sheet_key',sheet_key,
   'legacy_stage_id',stage_row->'id','legacy_group_id',group_row->'id','legacy_group',group_row,'legacy_stage',stage_row,
   'description',group_row->'description','collapsible',group_row->'collapsible'));
 field:=jsonb_build_object('id','legacy-question:'||p_question_id::text,'section_id',section->'id',
   'field_key',coalesce(nullif(question_row->>'question_key',''),nullif(question_row->>'field_key',''),'legacy_'||p_question_id::text),
   'title',question_row->'title','field_type',question_row->'question_type','position',question_row->'position',
   'required',question_row->'required','default_align',case when question_row->>'default_text_align'='left' then 'left' else 'right' end,
   'pinned_scope','none','active',question_row->'active','settings',jsonb_build_object('legacy_question_id',p_question_id,
   'legacy_question_key',question_row->'question_key','question_key',question_row->'question_key','legacy_field_key',question_row->'field_key',
   'legacy_question',question_row,'description',question_row->'description','help_text',question_row->'help_text',
   'source_url',question_row->'source_url','links_json',question_row->'links_json','options',question_row->'options_json',
   'visibility_rule_json',question_row->'visibility_rule_json'));
 logical_key:=public.v2_collection_field_logical_key(field);
 return jsonb_build_object('source_title',project_row->'name','row_order','[]'::jsonb,'snapshot',jsonb_build_object(
   'snapshot_key',sheet_key||':'||logical_key,'sheet_key',sheet_key,'logical_key',logical_key,'field_id',field->'id','field',field,'section',section,
   'session_id','legacy:'||p_project_id::text,'session_created_at',project_row->'created_at','value',value,'news_items','[]'::jsonb,
   'updated_at',latest_at,'updated_by',latest_by,'read_only',true,'project_id',p_project_id,'project_name',project_row->'name',
   'origin_href','/projects/'||p_project_id::text||'?stage='||(stage_row->>'id')||'&group='||(group_row->>'id')||'&question='||p_question_id::text));
end $$;
revoke all on function app_private.collection_history_legacy_source(uuid,uuid) from public,anon,authenticated;

create or replace function app_private.collection_history_pin_source(p_scope jsonb,p_source_session_id text,p_source_field_id text)
returns jsonb language plpgsql volatile security invoker set search_path=pg_catalog,public,app_private,pg_temp as $$
declare source jsonb; source_field jsonb; sec jsonb; source_session public.v2_collection_sessions; saved public.v2_collection_values;
 posts jsonb; stamp timestamptz; actor uuid; row_order jsonb; logical_key text; source_title text; result jsonb; source_created_at timestamptz;
 source_session_uuid uuid; source_field_uuid uuid;
begin
 if p_source_session_id like 'legacy:%' and p_source_field_id like 'legacy-question:%' then
   result:=app_private.collection_history_legacy_source(substring(p_source_session_id from 8)::uuid,substring(p_source_field_id from 17)::uuid);
   source:=result->'snapshot'; source_title:=result->>'source_title'; row_order:=result->'row_order'; source_field:=source->'field';
 else
   source_session_uuid:=p_source_session_id::uuid; source_field_uuid:=p_source_field_id::uuid;
   select * into source_session from public.v2_collection_sessions where id=source_session_uuid for share;
   if not found or source_session.deleted_at is not null or source_session.workspace_id::text<>p_scope#>>'{session,workspace_id}' then
     raise exception 'אין הרשאה לגליון המקור.' using errcode='42501'; end if;
   select to_jsonb(f),to_jsonb(s) into source_field,sec from public.v2_template_fields f
     join public.v2_template_sections s on s.id=f.section_id where f.id=source_field_uuid and s.template_id=source_session.template_id for share;
   if source_field is null then raise exception 'תיבת המקור אינה שייכת לגליון.' using errcode='23514'; end if;
   select * into saved from public.v2_collection_values where session_id=source_session_uuid and v2_collection_values.field_id=source_field_uuid for share;
   perform 1 from public.v2_news_items where collection_session_id=source_session_uuid and v2_news_items.field_id=source_field_uuid for share;
   select coalesce(jsonb_agg(to_jsonb(n) order by coalesce((to_jsonb(n)->>'position')::numeric,0),n.occurred_at,n.created_at,n.id),'[]') into posts
     from public.v2_news_items n where n.workspace_id=source_session.workspace_id and n.collection_session_id=source_session_uuid and n.field_id=source_field_uuid;
   select u.updated_at,u.updated_by into stamp,actor from (
     select saved.id,saved.updated_at,saved.updated_by where saved.id is not null
     union all select n.id,n.updated_at,n.updated_by from public.v2_news_items n
       where n.workspace_id=source_session.workspace_id and n.collection_session_id=source_session_uuid and n.field_id=source_field_uuid
   ) u order by u.updated_at desc,u.id limit 1;
   select to_jsonb(o.row_ids) into row_order from public.v2_collection_news_orders o where o.session_id=source_session_uuid and o.field_id=source_field_uuid;
   logical_key:=public.v2_collection_field_logical_key(source_field); source_title:=source_session.title;
   source:=jsonb_build_object('snapshot_key',public.v2_collection_sheet_key(sec)||':'||logical_key||':'||source_field_uuid::text,
     'sheet_key',public.v2_collection_sheet_key(sec),'logical_key',logical_key,'field_id',source_field_uuid,'field',source_field,'section',sec,
     'session_id',source_session_uuid,'session_created_at',source_session.created_at,'value',coalesce(saved.value,'{}'),
     'news_items',posts,'updated_at',coalesce(stamp,source_session.created_at),'updated_by',actor);
   if saved.id is null and jsonb_array_length(posts)=0 then raise exception 'אין תוכן שמור בתשובה שנבחרה.' using errcode='23514'; end if;
 end if;
 source_created_at:=(source->>'session_created_at')::timestamptz;
 if source->>'sheet_key' is distinct from p_scope->>'sheet_key'
   or not (app_private.collection_history_field_keys(source_field) && app_private.collection_history_field_keys(p_scope->'field'))
   or not (source_created_at<(p_scope#>>'{session,created_at}')::timestamptz
     or (source_created_at=(p_scope#>>'{session,created_at}')::timestamptz and source->>'session_id'>p_scope#>>'{session,id}')) then
   raise exception 'התשובה שנבחרה אינה תשובה קודמת של מקור זה.' using errcode='23514'; end if;
 return jsonb_build_object('snapshot',source,'row_order',coalesce(row_order,'[]'),'source_title',source_title,
   'source_key',(source->>'session_id')||':'||(source->>'field_id'));
end $$;
revoke all on function app_private.collection_history_pin_source(jsonb,text,text) from public,anon,authenticated;

create or replace function public.v2_list_collection_previous_answer_pins(p_session_id uuid,p_field_id uuid)
returns setof public.v2_collection_previous_answer_pins language plpgsql volatile security definer
set search_path=pg_catalog,public,app_private,pg_temp as $$
declare scope jsonb; keys text[];
begin
 scope:=app_private.collection_history_pin_scope(p_session_id,p_field_id,false);
 keys:=app_private.collection_history_field_keys(scope->'field');
 return query select p.* from public.v2_collection_previous_answer_pins p where p.workspace_id=(scope#>>'{session,workspace_id}')::uuid
   and p.sheet_key=scope->>'sheet_key' and p.field_keys && keys order by p.pinned_at desc,p.id;
end $$;
revoke all on function public.v2_list_collection_previous_answer_pins(uuid,uuid) from public,anon;
grant execute on function public.v2_list_collection_previous_answer_pins(uuid,uuid) to authenticated;

create or replace function public.v2_prepare_collection_previous_answer_pin(
 p_session_id uuid,p_field_id uuid,p_source_session_id text,p_source_field_id text)
returns jsonb language plpgsql volatile security definer set search_path=pg_catalog,public,app_private,pg_temp as $$
declare scope jsonb; prepared jsonb; pin jsonb;
begin
 scope:=app_private.collection_history_pin_scope(p_session_id,p_field_id,true);
 prepared:=app_private.collection_history_pin_source(scope,p_source_session_id,p_source_field_id);
 select to_jsonb(p) into pin from public.v2_collection_previous_answer_pins p
   where p.workspace_id=(scope#>>'{session,workspace_id}')::uuid and p.sheet_key=scope->>'sheet_key' and p.source_key=prepared->>'source_key';
 return prepared||jsonb_build_object('existing_pin',pin);
end $$;
revoke all on function public.v2_prepare_collection_previous_answer_pin(uuid,uuid,text,text) from public,anon;
grant execute on function public.v2_prepare_collection_previous_answer_pin(uuid,uuid,text,text) to authenticated;

create or replace function public.v2_pin_collection_previous_answer(
 p_session_id uuid,p_field_id uuid,p_source_session_id text,p_source_field_id text,
 p_expected_snapshot jsonb,p_expected_row_order jsonb,p_image_copies jsonb default '[]')
returns public.v2_collection_previous_answer_pins language plpgsql volatile security definer
set search_path=pg_catalog,public,app_private,pg_temp as $$
declare scope jsonb; prepared jsonb; source jsonb; pin public.v2_collection_previous_answer_pins; originals text[];
 original text; copy jsonb; target text; original_path text; bucket text; readable boolean; project_id uuid;
begin
 scope:=app_private.collection_history_pin_scope(p_session_id,p_field_id,true);
 prepared:=app_private.collection_history_pin_source(scope,p_source_session_id,p_source_field_id);
 select * into pin from public.v2_collection_previous_answer_pins p where p.workspace_id=(scope#>>'{session,workspace_id}')::uuid
   and p.sheet_key=scope->>'sheet_key' and p.source_key=prepared->>'source_key';
 if found then return pin; end if;
 source:=prepared->'snapshot';
 if p_expected_snapshot is distinct from source or p_expected_row_order is distinct from prepared->'row_order' then
   raise exception 'תשובת המקור השתנתה. נסו לנעוץ שוב.' using errcode='40001'; end if;
 if jsonb_typeof(p_image_copies) is distinct from 'array' or jsonb_array_length(p_image_copies)>100 then
   raise exception 'רשימת תמונות התשובה אינה תקינה.' using errcode='23514'; end if;
 select coalesce(array_agg(distinct p order by p),'{}'::text[]) into originals
   from app_private.collection_evidence_image_paths(jsonb_build_object('value',source->'value','news_items',source->'news_items')) p;
 if jsonb_array_length(p_image_copies)<>cardinality(originals) or exists(
   select 1 from jsonb_array_elements(p_image_copies) e where jsonb_typeof(e) is distinct from 'object'
   or jsonb_typeof(e->'original') is distinct from 'string' or jsonb_typeof(e->'copied') is distinct from 'string'
   or not coalesce(e->>'original'=any(originals),false))
   or (select count(distinct e->>'original') from jsonb_array_elements(p_image_copies) e)<>cardinality(originals)
   or (select count(distinct e->>'copied') from jsonb_array_elements(p_image_copies) e)<>cardinality(originals) then
   raise exception 'נדרשת העתקה מלאה של תמונות התשובה.' using errcode='23514'; end if;
 foreach original in array originals loop
   select e into copy from jsonb_array_elements(p_image_copies) e where e->>'original'=original;
   target:=regexp_replace(copy->>'copied','^workspace-images:','');
   if not starts_with(copy->>'copied','workspace-images:'||(scope#>>'{session,workspace_id}')||'/collection/'||p_session_id::text||'/')
     or public.v2_image_workspace(target) is distinct from (scope#>>'{session,workspace_id}')::uuid
     or target ~ '(^|/)(\.|\.\.)(/|$)' or position(chr(92) in target)>0
     or not exists(select 1 from storage.objects o where o.bucket_id='workspace-images' and o.name=target) then
     raise exception 'תמונת הנעיצה אינה שייכת לגליון שנבחר.' using errcode='42501'; end if;
   bucket:=case when starts_with(original,'workspace-images:') then 'workspace-images' else 'project-images' end;
   original_path:=regexp_replace(original,'^workspace-images:',''); readable:=false;
   if bucket='workspace-images' then readable:=coalesce(public.v2_is_workspace_member(public.v2_image_workspace(original_path)),false);
   else
     if split_part(original_path,'/',1) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       and to_regprocedure('app_private.can_access_project(uuid)') is not null then
       project_id:=split_part(original_path,'/',1)::uuid;
       execute 'select app_private.can_access_project($1)' into readable using project_id;
     end if;
     if not coalesce(readable,false) and to_regprocedure('app_private.can_read_copied_legacy_image(text)') is not null then
       execute 'select app_private.can_read_copied_legacy_image($1)' into readable using original_path;
     end if;
   end if;
   if not coalesce(readable,false) or not exists(select 1 from storage.objects o where o.bucket_id=bucket and o.name=original_path) then
     raise exception 'אין הרשאה לתמונת המקור.' using errcode='42501'; end if;
 end loop;
 insert into public.v2_collection_previous_answer_pins(workspace_id,sheet_key,logical_key,field_keys,source_key,source_title,snapshot,row_order,pinned_by)
 values((scope#>>'{session,workspace_id}')::uuid,scope->>'sheet_key',public.v2_collection_field_logical_key(scope->'field'),
   app_private.collection_history_field_keys(scope->'field'),prepared->>'source_key',prepared->>'source_title',
   source||jsonb_build_object('value',app_private.replace_collection_evidence_images(source->'value',p_image_copies),
     'news_items',app_private.replace_collection_evidence_images(source->'news_items',p_image_copies)),prepared->'row_order',auth.uid())
 on conflict(workspace_id,sheet_key,source_key) do nothing returning * into pin;
 if pin.id is null then select * into pin from public.v2_collection_previous_answer_pins p
   where p.workspace_id=(scope#>>'{session,workspace_id}')::uuid and p.sheet_key=scope->>'sheet_key' and p.source_key=prepared->>'source_key'; end if;
 return pin;
end $$;
revoke all on function public.v2_pin_collection_previous_answer(uuid,uuid,text,text,jsonb,jsonb,jsonb) from public,anon;
grant execute on function public.v2_pin_collection_previous_answer(uuid,uuid,text,text,jsonb,jsonb,jsonb) to authenticated;

create or replace function public.v2_unpin_collection_previous_answer(p_session_id uuid,p_field_id uuid,p_pin_id uuid)
returns boolean language plpgsql volatile security definer set search_path=pg_catalog,public,app_private,pg_temp as $$
declare scope jsonb; removed uuid;
begin
 scope:=app_private.collection_history_pin_scope(p_session_id,p_field_id,true);
 delete from public.v2_collection_previous_answer_pins p where p.id=p_pin_id
   and p.workspace_id=(scope#>>'{session,workspace_id}')::uuid and p.sheet_key=scope->>'sheet_key'
   and p.field_keys && app_private.collection_history_field_keys(scope->'field') returning p.id into removed;
 return removed is not null;
end $$;
revoke all on function public.v2_unpin_collection_previous_answer(uuid,uuid,uuid) from public,anon;
grant execute on function public.v2_unpin_collection_previous_answer(uuid,uuid,uuid) to authenticated;

-- Ordinary member image deletion/rename cannot destroy an independent pin.
-- The existing storage allow policies still decide who can mutate all other
-- objects; these restrictive policies add only this new reference guard.
create or replace function public.v2_collection_previous_pin_image_retained(p_name text)
returns boolean language sql stable security definer set search_path=pg_catalog,public,app_private,pg_temp as $$
 select auth.uid() is not null and public.v2_is_workspace_member(public.v2_image_workspace(p_name))
 and exists(select 1 from public.v2_collection_previous_answer_pins pin
   where pin.workspace_id=public.v2_image_workspace(p_name) and exists(
     select 1 from app_private.collection_evidence_image_paths(jsonb_build_object(
       'value',pin.snapshot->'value','news_items',pin.snapshot->'news_items')) path
     where path='workspace-images:'||p_name));
$$;
revoke all on function public.v2_collection_previous_pin_image_retained(text) from public,anon;
grant execute on function public.v2_collection_previous_pin_image_retained(text) to authenticated;
drop policy if exists previous_answer_pin_image_delete on storage.objects;
create policy previous_answer_pin_image_delete on storage.objects as restrictive for delete to authenticated
 using(bucket_id<>'workspace-images' or not public.v2_collection_previous_pin_image_retained(name));
drop policy if exists previous_answer_pin_image_update on storage.objects;
create policy previous_answer_pin_image_update on storage.objects as restrictive for update to authenticated
 using(bucket_id<>'workspace-images' or not public.v2_collection_previous_pin_image_retained(name))
 with check(bucket_id<>'workspace-images' or not public.v2_collection_previous_pin_image_retained(name));

commit;
