-- Progress thresholds affect only explicitly configured buttons. Existing answers are untouched.
create or replace function public.v2_trade_button_threshold(p_settings jsonb)
returns numeric language plpgsql immutable security invoker set search_path=public,pg_temp as $$
declare raw jsonb:=coalesce(nullif(p_settings#>'{options_json,min_completion_percent}','null'::jsonb),nullif(p_settings->'min_completion_percent','null'::jsonb),nullif(p_settings#>'{legacy_question,options_json,min_completion_percent}','null'::jsonb)); n numeric;
begin
 if raw is null then return null; end if;
 if jsonb_typeof(raw)<>'number' then return null; end if;
 n:=(raw#>>'{}')::numeric;
 if n<0 or n>100 then return null; end if;
 return n;
end $$;
revoke all on function public.v2_trade_button_threshold(jsonb) from public,anon;
grant execute on function public.v2_trade_button_threshold(jsonb) to authenticated;

alter table public.v2_template_fields drop constraint if exists v2_template_fields_button_threshold_check;
alter table public.v2_template_fields add constraint v2_template_fields_button_threshold_check check (
 field_type<>'button' or coalesce(nullif(settings#>'{options_json,min_completion_percent}','null'::jsonb),nullif(settings->'min_completion_percent','null'::jsonb),nullif(settings#>'{legacy_question,options_json,min_completion_percent}','null'::jsonb)) is null
 or public.v2_trade_button_threshold(settings) is not null);

-- Normalize historic answer aliases the same way as the worksheet renderer. Pure data only.
create or replace function public.v2_trade_button_value(p_value jsonb)
returns jsonb language sql immutable security invoker set search_path=public,pg_temp as $$
 select coalesce(p_value,'{}'::jsonb)||jsonb_strip_nulls(jsonb_build_object(
  'text',coalesce(p_value->>'text',p_value->>'html',p_value->>'text_value',p_value->>'number',p_value->>'number_value'),
  'checked',coalesce(nullif(p_value->'checked','null'::jsonb),nullif(p_value->'boolean_value','null'::jsonb)),
  'choice',coalesce(p_value->>'choice',p_value#>>'{choice_value_json,choice}',p_value->>'text_value'),
  'date',coalesce(p_value->>'date',p_value->>'datetime',p_value->>'date_value'),
  'choices',coalesce(nullif(p_value->'choices','null'::jsonb),case when jsonb_typeof(p_value->'choice_value_json')='array' then p_value->'choice_value_json' end),
  'completion',case when p_value->>'not_relevant'='true' then 'na' else p_value->>'completion' end));
$$;
revoke all on function public.v2_trade_button_value(jsonb) from public,anon;
grant execute on function public.v2_trade_button_value(jsonb) to authenticated;

do $$ declare definition text; needle text:='s.id section_id,f.position,v.value'; begin
 select pg_get_functiondef('public.v2_visible_trade_fields(uuid)'::regprocedure) into definition;
 if strpos(definition,needle)=0 then raise exception 'Trade visibility contract changed; review normalization patch'; end if;
 definition:=replace(definition,needle,'s.id section_id,f.position,public.v2_trade_button_value(v.value) value');
 definition:=replace(definition,
  'then s.settings->''groups'' else ''[]''::jsonb end',
  'then s.settings->''groups'' when jsonb_typeof(s.settings->''legacy_groups'')=''array'' then s.settings->''legacy_groups'' else ''[]''::jsonb end');
 execute definition;
end $$;
-- The reader is invoker-only: existing trade/template RLS applies to every returned ID.
grant execute on function public.v2_visible_trade_fields(uuid) to authenticated;

create or replace function public.v2_trade_button_progress(p_trade_id uuid,p_field_id uuid)
returns table(required_percent numeric,completed bigint,total bigint,completion_percent numeric,eligible boolean)
language plpgsql stable security invoker set search_path=public,pg_temp as $$
declare tr public.v2_trades; target public.v2_template_fields; threshold numeric;
begin
 select * into tr from public.v2_trades where id=p_trade_id;
 if not found or auth.uid() is null or not coalesce(public.v2_is_workspace_member(tr.workspace_id),false) then
  raise exception 'Trade access required' using errcode='42501';
 end if;
 select f.* into target from public.v2_template_fields f join public.v2_template_sections s on s.id=f.section_id
  where f.id=p_field_id and s.template_id=tr.template_id and s.active and f.active and f.field_type='button';
 if not found or not exists(select 1 from public.v2_visible_trade_fields(p_trade_id) id where id=p_field_id) then
  raise exception 'A visible button in this trade template is required' using errcode='23514';
 end if;
 threshold:=public.v2_trade_button_threshold(target.settings);
 return query
 with sheet as (
  select s.id,case when jsonb_typeof(s.settings->'groups')='array' then s.settings->'groups'
    when jsonb_typeof(s.settings->'legacy_groups')='array' then s.settings->'legacy_groups' else '[]'::jsonb end groups
  from public.v2_template_sections s where s.id=target.section_id
 ), metadata as (
  select g.value,g.ordinality from sheet,jsonb_array_elements(sheet.groups) with ordinality g
 ), fields as (
  select f.*,coalesce(f.settings->>'group_id',f.settings->>'legacy_group_id',
   (select value->>'id' from metadata order by ordinality limit 1),f.section_id::text||':general') group_id
  from public.v2_template_fields f where f.section_id=target.section_id
 ), group_order as (
  select f.group_id,
   coalesce(max(case when jsonb_typeof(m.value->'position')='number' then (m.value->>'position')::numeric end),
    max(case when jsonb_typeof(f.settings->'legacy_group_position')='number' then (f.settings->>'legacy_group_position')::numeric end),0) group_position,
   coalesce(max(m.ordinality),1000000+min(f.position)) group_sequence
  from fields f left join metadata m on m.value->>'id'=f.group_id group by f.group_id
 ), ordered as (
  select f.*,row_number() over(order by g.group_position,g.group_sequence,f.position,f.id) display_order
  from fields f join group_order g using(group_id)
 ), preceding as (
  select f.field_type,public.v2_trade_button_value(v.value) value
   from ordered f left join public.v2_trade_values v on v.trade_id=p_trade_id and v.field_id=f.id
   where f.active and f.field_type not in ('info','button')
    and f.display_order<(select display_order from ordered where id=p_field_id)
    and f.id in(select public.v2_visible_trade_fields(p_trade_id))
 ), counts as (
  select count(*) filter(where public.v2_field_complete(field_type,value)) done,count(*) quantity from preceding
 ), result as (select done,quantity,case when quantity=0 then 100::numeric else done*100::numeric/quantity end percent from counts)
 select threshold,done,quantity,percent,threshold is not null and percent>=threshold from result;
end $$;
revoke all on function public.v2_trade_button_progress(uuid,uuid) from public,anon;
grant execute on function public.v2_trade_button_progress(uuid,uuid) to authenticated;

-- Direct status writes and header actions must satisfy the same configured buttons as field actions.
-- null means no configured button exists and the original required-field gate remains in force.
create or replace function public.v2_configured_trade_action_allowed(p_trade_id uuid,p_action text)
returns boolean language plpgsql stable security invoker set search_path=public,pg_temp as $$
declare candidate record; has_configured boolean:=false; allowed boolean:=false;
begin
 for candidate in
  select f.id from public.v2_trades t join public.v2_template_sections s on s.template_id=t.template_id
   join public.v2_template_fields f on f.section_id=s.id
   where t.id=p_trade_id and s.active and f.active and f.field_type='button'
    and coalesce(f.settings->>'action',f.settings#>>'{options_json,action}')=p_action
    and public.v2_trade_button_threshold(f.settings) is not null
 loop
  has_configured:=true;
  if candidate.id in(select public.v2_visible_trade_fields(p_trade_id)) then
   select allowed or p.eligible into allowed from public.v2_trade_button_progress(p_trade_id,candidate.id) p;
  end if;
 end loop;
 if not has_configured then return null; end if;
 return allowed;
end $$;
revoke all on function public.v2_configured_trade_action_allowed(uuid,text) from public,anon;
grant execute on function public.v2_configured_trade_action_allowed(uuid,text) to authenticated;

create or replace function public.execute_v2_trade_button(p_trade_id uuid,p_field_id uuid)
returns public.v2_trades language plpgsql security invoker set search_path=public,pg_temp as $$
declare tr public.v2_trades; action text; progress record; next_status text;
begin
 select * into tr from public.v2_trades where id=p_trade_id for update;
 if not found or auth.uid() is null or not coalesce(public.v2_can_edit_workspace(tr.workspace_id),false) then
  raise exception 'Trade edit permission required' using errcode='42501';
 end if;
 if tr.archived_at is not null then raise exception 'Restore trade before changing its status' using errcode='23514'; end if;
 select * into progress from public.v2_trade_button_progress(p_trade_id,p_field_id);
 if progress.required_percent is not null and not progress.eligible then
  raise exception 'Complete the configured percentage before using this button' using errcode='23514';
 end if;
 select coalesce(settings->>'action',settings#>>'{options_json,action}') into action from public.v2_template_fields where id=p_field_id;
 if progress.required_percent is null and action in ('activate','limit') and exists(
  select 1 from public.v2_template_fields f join public.v2_template_sections s on s.id=f.section_id
   left join public.v2_trade_values v on v.field_id=f.id and v.trade_id=tr.id
  where s.template_id=tr.template_id and s.active and f.active and f.required and f.field_type not in ('info','button')
   and f.id in(select public.v2_visible_trade_fields(tr.id))
   and not public.v2_trade_field_is_post_trade(s.title,s.settings,f.field_type,f.field_key,f.settings)
   and not public.v2_field_complete(f.field_type,public.v2_trade_button_value(v.value))
 ) then raise exception 'Complete required fields before opening a limit or activating a trade' using errcode='23514'; end if;
 next_status:=case action when 'activate' then 'active' when 'limit' then 'limit' when 'close' then 'closed' else null end;
 if next_status is null then raise exception 'Unsupported trade button action' using errcode='23514'; end if;
 if not ((tr.status='evaluating' and next_status in ('limit','active')) or (tr.status='limit' and next_status='active') or (tr.status='active' and next_status='closed')) then
  raise exception 'Button action is unavailable in the current trade state' using errcode='23514';
 end if;
 update public.v2_trades set status=next_status,updated_by=auth.uid(),updated_at=now() where id=p_trade_id returning * into tr;
 return tr;
end $$;
notify pgrst,'reload schema';
revoke all on function public.execute_v2_trade_button(uuid,uuid) from public,anon;
grant execute on function public.execute_v2_trade_button(uuid,uuid) to authenticated;

-- Replace only the lifecycle gate; retain immutable identity, scenario checks, cycle locking and PnL rules.
do $$
declare definition text; old_gate text; new_gate text; old_close text; new_close text;
begin
 select pg_get_functiondef('public.v2_guard_trade_workflow()'::regprocedure) into definition;
 old_gate:=$gate$if new.template_id is null or not exists(select 1 from public.v2_template_fields f join public.v2_template_sections s on s.id=f.section_id where s.template_id=new.template_id and s.active and f.active) or exists(
          select 1 from public.v2_template_fields f join public.v2_template_sections s on s.id=f.section_id left join public.v2_trade_values v on v.field_id=f.id and v.trade_id=new.id
          where s.template_id=new.template_id and s.active and f.active and f.id in (select public.v2_visible_trade_fields(new.id)) and f.required and f.field_type not in ('info','button')
            and not public.v2_trade_field_is_post_trade(s.title,s.settings,f.field_type,f.field_key,f.settings)
            and not public.v2_field_complete(f.field_type,v.value)
        ) then raise exception 'Complete required fields before opening a limit or activating a trade' using errcode='23514'; end if;$gate$;
 new_gate:=$gate$if public.v2_configured_trade_action_allowed(new.id,case when new.status='limit' then 'limit' else 'activate' end) is false then
          raise exception 'Complete the configured percentage before changing trade status' using errcode='23514';
        elsif public.v2_configured_trade_action_allowed(new.id,case when new.status='limit' then 'limit' else 'activate' end) is null then
          $gate$||old_gate||$gate$
        end if;$gate$;
 old_close:='elsif old.status=''active'' and new.status=''closed'' then new.closed_at:=now();';
 new_close:=$gate$elsif old.status='active' and new.status='closed' then
        if public.v2_configured_trade_action_allowed(new.id,'close') is false then
          raise exception 'Complete the configured percentage before closing this trade' using errcode='23514';
        end if;
        new.closed_at:=now();$gate$;
 if strpos(definition,old_gate)=0 or strpos(definition,old_close)=0 then
  raise exception 'Trade lifecycle contract changed; review button threshold patch';
 end if;
 execute replace(replace(definition,old_gate,new_gate),old_close,new_close);
end $$;
