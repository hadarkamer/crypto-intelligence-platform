-- Explicit done/not-done answers preserve the boolean and all existing answer data.
begin;

do $types$
declare definition text; expression text;
begin
 select pg_get_constraintdef(oid) into definition from pg_constraint
 where conrelid='public.v2_template_fields'::regclass and conname='v2_template_fields_field_type_check';
 if definition is null then raise exception 'Missing template field type constraint';end if;
 if position('done_not_done' in definition)=0 then
  expression:=substring(definition from '^CHECK [(](.*)[)]$');
  if expression is null then raise exception 'Unexpected template field type constraint';end if;
  alter table public.v2_template_fields drop constraint v2_template_fields_field_type_check;
  execute format('alter table public.v2_template_fields add constraint v2_template_fields_field_type_check check ((%s) or field_type=%L)',expression,'done_not_done');
 end if;
end $types$;

-- Extend the existing routines in place, retaining owners, grants and security.
do $routines$
declare signature text; routine oid; definition text; marker text:='done_not_done_contract_v1';
 needle text:=E'\nbegin\n'; replacement text; before_contract jsonb; after_contract jsonb;
begin
 foreach signature in array array['public.v2_legacy_field_type(text,text)','public.v2_field_complete(text,jsonb)','public.v2_collection_value_filled(text,jsonb)'] loop
  routine:=to_regprocedure(signature);
  if routine is null then raise exception 'Done/not-done prerequisite missing: %',signature;end if;
  select pg_get_functiondef(routine),jsonb_build_object('oid',p.oid,'owner',p.proowner,'acl',p.proacl,'security',p.prosecdef,'settings',p.proconfig,'result',p.prorettype,'volatility',p.provolatile)
  into definition,before_contract from pg_proc p where p.oid=routine;
  if position(marker in definition)>0 then continue;end if;
  if signature like '%v2_legacy_field_type(%' then
   replacement:=needle||E' -- done_not_done_contract_v1\n if lower(btrim(coalesce(p_question_type,'''')))=''done_not_done'' then return ''done_not_done'';end if;\n';
  else
   replacement:=needle||E' -- done_not_done_contract_v1\n if p_type=''done_not_done'' then return coalesce(jsonb_typeof(coalesce(nullif(p_value->''checked'',''null''::jsonb),p_value->''boolean_value''))=''boolean'',false) or coalesce(p_value->>''completion''=''na'',false);end if;\n';
  end if;
  if (length(definition)-length(replace(definition,needle,'')))/length(needle)<>1 then raise exception 'Unexpected done/not-done anchor: %',signature;end if;
  definition:=replace(definition,needle,replacement);execute definition;
  select jsonb_build_object('oid',p.oid,'owner',p.proowner,'acl',p.proacl,'security',p.prosecdef,'settings',p.proconfig,'result',p.prorettype,'volatility',p.provolatile)
   into after_contract from pg_proc p where p.oid=routine;
  if before_contract is distinct from after_contract or pg_get_functiondef(routine) is distinct from definition then raise exception 'Done/not-done changed routine security: %',signature;end if;
 end loop;
end $routines$;

-- Conditions after an explicit yes/no boolean keep their original behavior.
do $visibility$
declare fn oid:=to_regprocedure('public.v2_visible_trade_fields(uuid)');definition text;
 needle text:='previous_type=''checkbox''';
begin
 if fn is null then raise exception 'Missing trade visibility routine';end if;
 definition:=pg_get_functiondef(fn);
 if position('previous_type in (''checkbox'',''done_not_done'')' in definition)=0 then
  if (length(definition)-length(replace(definition,needle,'')))/length(needle)<>1 then raise exception 'Unexpected trade visibility anchor';end if;
  execute replace(definition,needle,'previous_type in (''checkbox'',''done_not_done'')');
 end if;
end $visibility$;

-- Retag only checkbox questions in explicitly mental trade groups. IDs,
-- positions, saved values and immutable template links remain unchanged.
do $upgrade$
declare guard_state "char"; command text;
begin
 lock table public.v2_template_fields in access exclusive mode;
 select tgenabled into guard_state from pg_trigger
  where tgrelid='public.v2_template_fields'::regclass and tgname='v2_used_template_fields_guard' and not tgisinternal;
 if guard_state is not null then alter table public.v2_template_fields disable trigger v2_used_template_fields_guard;end if;
 update public.v2_template_fields f set field_type='done_not_done'
 from public.v2_template_sections s,public.v2_templates t
 where s.id=f.section_id and t.id=s.template_id and t.kind='trade' and f.field_type='checkbox'
 and (f.settings->>'legacy_group_behavior'='mental' or s.settings->>'behavior'='mental' or exists(
  select 1 from jsonb_array_elements(coalesce(s.settings->'groups',s.settings->'legacy_groups','[]')) g
  where g->>'id'=coalesce(f.settings->>'group_id',f.settings->>'legacy_group_id') and g->>'behavior'='mental'));
 if guard_state is not null then
  command:=case guard_state when 'O' then 'enable' when 'D' then 'disable' when 'R' then 'enable replica' when 'A' then 'enable always' else null end;
  if command is null then raise exception 'Unexpected template guard state';end if;
  execute 'alter table public.v2_template_fields '||command||' trigger v2_used_template_fields_guard';
 end if;
end $upgrade$;

create or replace function public.v2_set_collection_news_read_marker(p_workspace_id uuid,p_session_id uuid,p_field_id uuid,p_read_until timestamptz)
returns void language plpgsql security definer set search_path=public,pg_temp as $$
declare c public.v2_collection_sessions; source_key text;
begin
  if auth.uid() is null or not public.v2_is_workspace_member(p_workspace_id) then
    raise exception 'אין הרשאה לסימון קריאה בגליון זה.' using errcode='42501'; end if;
  -- Personal progress is available to viewers, while writes always use auth.uid().
  perform id from public.v2_workspaces where id=p_workspace_id for share;
  perform user_id from public.v2_workspace_members where workspace_id=p_workspace_id and user_id=auth.uid() for share;
  if not found then raise exception 'אין הרשאה לסימון קריאה בגליון זה.' using errcode='42501'; end if;
  select * into c from public.v2_collection_sessions where id=p_session_id and workspace_id=p_workspace_id and deleted_at is null for share;
  if not found then raise exception 'הגליון אינו זמין לקריאה.' using errcode='42501'; end if;
  select source.logical_key into source_key
    from public.v2_read_collection_news_groups(p_workspace_id,p_session_id) source
    join public.v2_template_fields f on f.id=source.field_id
    join public.v2_template_sections sec on sec.id=f.section_id
    where source.field_id=p_field_id for share of f,sec;
  if source_key is null then raise exception 'קבוצת החדשות אינה שייכת לגליון זה.' using errcode='23514'; end if;
  -- NULL means the user's explicit "now" choice; use the authoritative clock.
  p_read_until:=coalesce(p_read_until,clock_timestamp());
  if not isfinite(p_read_until) or p_read_until>clock_timestamp()
    or p_read_until<clock_timestamp()-interval '8 days' then
    raise exception 'בחרו זמן קריאה מהיום או משבעת הימים האחרונים.' using errcode='23514'; end if;
  -- Keep the existing shared-source key so all previous personal markers survive.
  insert into public.v2_collection_news_read_markers(workspace_id,logical_key,user_id,read_until,updated_at)
    values(c.workspace_id,source_key,auth.uid(),p_read_until,clock_timestamp())
    on conflict(workspace_id,logical_key,user_id) do update set read_until=excluded.read_until,updated_at=excluded.updated_at;
end $$;
revoke all on function public.v2_set_collection_news_read_marker(uuid,uuid,uuid,timestamptz) from public,anon;
grant execute on function public.v2_set_collection_news_read_marker(uuid,uuid,uuid,timestamptz) to authenticated;

notify pgrst,'reload schema';

commit;
