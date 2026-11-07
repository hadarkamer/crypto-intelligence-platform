-- Collection-only live template editing. Answers and other template kinds are untouched.
-- Changes that could invalidate an open sheet fork a new version, including when
-- its current text is still an unsaved client draft and therefore invisible here.
alter table public.v2_templates add column if not exists editor_revision bigint not null default 0;

create table public.v2_collection_template_write_tokens (
 transaction_id bigint not null,
 template_id uuid not null references public.v2_templates(id) on delete cascade,
 actor_id uuid not null,
 primary key(transaction_id,template_id)
);
alter table public.v2_collection_template_write_tokens enable row level security;
revoke all on table public.v2_collection_template_write_tokens from public,anon,authenticated;

create or replace function public.v2_bump_collection_template_revision()
returns trigger language plpgsql security definer set search_path=public,pg_temp as $$
declare tid uuid;
begin
 if tg_table_name='v2_templates' then
  if new.kind='collection' then new.editor_revision:=old.editor_revision+1; end if;
  return new;
 end if;
 if tg_table_name='v2_template_sections' then
  tid:=case when tg_op='DELETE' then old.template_id else new.template_id end;
 else
  select template_id into tid from public.v2_template_sections
   where id=case when tg_op='DELETE' then old.section_id else new.section_id end;
 end if;
 update public.v2_templates set updated_at=now() where id=tid and kind='collection';
 if tg_op='DELETE' then return old; end if;
 return new;
end $$;
revoke all on function public.v2_bump_collection_template_revision() from public,anon,authenticated;
create trigger v2_collection_template_revision before update on public.v2_templates
 for each row execute function public.v2_bump_collection_template_revision();
create trigger v2_collection_section_revision after insert or update or delete on public.v2_template_sections
 for each row execute function public.v2_bump_collection_template_revision();
create trigger v2_collection_field_revision after insert or update or delete on public.v2_template_fields
 for each row execute function public.v2_bump_collection_template_revision();

-- The capability is a private table row, not a client-settable session variable.
-- Only the checked RPC below can create it, only for this transaction and actor.
create or replace function public.v2_guard_used_template_structure()
returns trigger language plpgsql security definer set search_path=public,pg_temp as $$
declare old_tid uuid; new_tid uuid;
begin
 if tg_table_name='v2_template_sections' then
  if tg_op<>'INSERT' then old_tid:=old.template_id; end if;
  if tg_op<>'DELETE' then new_tid:=new.template_id; end if;
 else
  if tg_op<>'INSERT' then select template_id into old_tid from public.v2_template_sections where id=old.section_id; end if;
  if tg_op<>'DELETE' then select template_id into new_tid from public.v2_template_sections where id=new.section_id; end if;
 end if;
 perform 1 from public.v2_templates where id=any(array[old_tid,new_tid]) order by id for update;
 if exists(select 1 from public.v2_collection_template_write_tokens c join public.v2_templates t on t.id=c.template_id
   where c.transaction_id=txid_current() and c.actor_id=auth.uid() and t.kind='collection'
    and (old_tid is null or old_tid=c.template_id) and (new_tid is null or new_tid=c.template_id)
    and not exists(select 1 from public.v2_trades where template_id=c.template_id)
    and not exists(select 1 from public.v2_scenarios where template_id=c.template_id)
    and not exists(select 1 from public.v2_spot_assets where template_id=c.template_id)) then
  if tg_op='DELETE' then return old; end if;
  return new;
 end if;
 if exists(select 1 from public.v2_collection_sessions where template_id=any(array[old_tid,new_tid]))
 or exists(select 1 from public.v2_trades where template_id=any(array[old_tid,new_tid]))
 or exists(select 1 from public.v2_scenarios where template_id=any(array[old_tid,new_tid]))
 or exists(select 1 from public.v2_spot_assets where template_id=any(array[old_tid,new_tid])) then
  raise exception 'Referenced template versions are immutable; create a new version';
 end if;
 if tg_op='DELETE' then return old; end if;
 return new;
end $$;
revoke all on function public.v2_guard_used_template_structure() from public,anon,authenticated;

create or replace function public.v2_collection_editor_snapshot(p_template_id uuid)
returns jsonb language sql stable security definer set search_path=public,pg_temp as $$
 select jsonb_build_object('revision',t.editor_revision,'template',to_jsonb(t),'sections',coalesce((
   select jsonb_agg(to_jsonb(s)||jsonb_build_object('fields',coalesce((
     select jsonb_agg(to_jsonb(f) order by f.position,f.id) from public.v2_template_fields f where f.section_id=s.id
   ),'[]'::jsonb)) order by s.position,s.id) from public.v2_template_sections s where s.template_id=t.id
 ),'[]'::jsonb))
 from public.v2_templates t where t.id=p_template_id and t.kind='collection' and public.v2_is_workspace_member(t.workspace_id);
$$;
revoke all on function public.v2_collection_editor_snapshot(uuid) from public,anon;
grant execute on function public.v2_collection_editor_snapshot(uuid) to authenticated;

create or replace function public.v2_collection_field_contract(p_settings jsonb)
returns jsonb language sql immutable set search_path=public,pg_temp as $$
 select jsonb_build_object(
  'options',coalesce(p_settings->'options_json',p_settings#>'{legacy_question,options_json}','[]'::jsonb),
  'visibility',coalesce(p_settings->'visibility_rule_json',p_settings#>'{legacy_question,visibility_rule_json}','null'::jsonb),
  'question_key',coalesce(p_settings->'question_key',p_settings->'legacy_question_key',p_settings#>'{legacy_question,question_key}','null'::jsonb));
$$;
revoke all on function public.v2_collection_field_contract(jsonb) from public,anon,authenticated;

create or replace function public.v2_save_collection_template_live(
 p_workspace_id uuid,p_template_id uuid,p_source_template_id uuid,p_expected_revision bigint,
 p_name text,p_active boolean,p_default boolean,p_sections jsonb)
returns jsonb language plpgsql security definer set search_path=public,pg_temp as $$
declare original public.v2_templates%rowtype; tid uuid; sid uuid; fid uuid;
 sec jsonb; fld jsonb; previous_field record; candidate jsonb; candidate_section jsonb;
 used boolean:=false; conflict boolean:=false; section_ids uuid[]:='{}'; field_ids uuid[]:='{}';
 si integer:=0; fi integer; id_map jsonb:='{}'; revision bigint;
begin
 if auth.uid() is null or not coalesce(public.v2_can_edit_workspace(p_workspace_id),false) then
  raise exception 'Workspace edit permission required' using errcode='42501';
 end if;
 if nullif(btrim(p_name),'') is null or p_sections is null or jsonb_typeof(p_sections)<>'array'
   or jsonb_array_length(p_sections)>100 then raise exception 'Invalid collection template' using errcode='23514'; end if;
 perform pg_advisory_xact_lock(hashtextextended(p_workspace_id::text||'collection',0));
 if coalesce(p_template_id,p_source_template_id) is not null then
  select * into original from public.v2_templates
   where id=coalesce(p_template_id,p_source_template_id) and workspace_id=p_workspace_id and kind='collection' for update;
  if not found then raise exception 'Collection template does not belong to this workspace' using errcode='42501'; end if;
 end if;
 if p_template_id is not null then
  if p_source_template_id is not null and p_template_id<>p_source_template_id then
   raise exception 'Existing template must retain its own source' using errcode='23514'; end if;
  if p_expected_revision is null or p_expected_revision<>original.editor_revision then
   raise exception 'התבנית עודכנה על ידי משתמש אחר. השינויים שלך נשמרו בטיוטה; ניתן לשמור אותם כגרסה נפרדת.' using errcode='40001';
  end if;
  used:=public.v2_template_in_use(p_template_id);
 end if;
 -- Validate complete payload before creating a capability or modifying anything.
 for sec in select value from jsonb_array_elements(p_sections) loop
  if nullif(btrim(sec->>'title'),'') is null or jsonb_typeof(coalesce(sec->'fields','[]'))<>'array'
   or jsonb_typeof(coalesce(sec->'settings','{}'))<>'object'
   or jsonb_typeof(coalesce(sec#>'{settings,groups}','[]'))<>'array' then raise exception 'Invalid sheet structure' using errcode='23514'; end if;
  for fld in select value from jsonb_array_elements(coalesce(sec->'fields','[]')) loop
   if nullif(btrim(fld->>'title'),'') is null or jsonb_typeof(coalesce(fld->'settings','{}'))<>'object' then
    raise exception 'Invalid field structure' using errcode='23514'; end if;
  end loop;
 end loop;
 if used then
  -- All existing fields must remain readable under the same answer contract.
  -- This protects persisted answers, news, info text and not-yet-saved client input.
  for previous_field in select f.*,s.active section_active,s.settings section_settings
    from public.v2_template_fields f join public.v2_template_sections s on s.id=f.section_id where s.template_id=p_template_id loop
   select f.value,s.value into candidate,candidate_section from jsonb_array_elements(p_sections) s
    cross join lateral jsonb_array_elements(coalesce(s.value->'fields','[]')) f where f.value->>'id'=previous_field.id::text;
   if candidate is null or candidate->>'field_type' is distinct from previous_field.field_type
    or candidate->>'field_key' is distinct from previous_field.field_key
    or (previous_field.active and not coalesce((candidate->>'active')::boolean,true))
    or (previous_field.section_active and not coalesce((candidate_section->>'active')::boolean,true))
    or public.v2_collection_field_contract(candidate->'settings') is distinct from public.v2_collection_field_contract(previous_field.settings)
    or (exists(select 1 from jsonb_array_elements(coalesce(candidate_section#>'{settings,groups}','[]')) g
      where g->>'id'=candidate#>>'{settings,group_id}' and g->>'active'='false')
      and not exists(select 1 from jsonb_array_elements(coalesce(previous_field.section_settings->'groups',previous_field.section_settings->'legacy_groups','[]')) g
       where g->>'id'=coalesce(previous_field.settings->>'group_id',previous_field.settings->>'legacy_group_id') and g->>'active'='false'))
    or (previous_field.field_type='info' and (
      candidate->>'title' is distinct from previous_field.title
      or coalesce(candidate#>'{settings,description}','null'::jsonb) is distinct from coalesce(previous_field.settings->'description',previous_field.settings#>'{legacy_question,description}','null'::jsonb)
      or coalesce(candidate#>'{settings,help_text}','null'::jsonb) is distinct from coalesce(previous_field.settings->'help_text',previous_field.settings#>'{legacy_question,help_text}','null'::jsonb))) then
    conflict:=true; exit;
   end if;
  end loop;
 end if;
 if p_template_id is null or conflict then
  tid:=public.v2_save_template_editor(p_workspace_id,null,coalesce(p_template_id,p_source_template_id),
    'collection',p_name,p_active,p_default,p_sections,null);
  for sec in select value from jsonb_array_elements(p_sections) loop
   select id into sid from public.v2_template_sections where template_id=tid and position=si;
   if sec->>'id' is not null then id_map:=id_map||jsonb_build_object(sec->>'id',sid); end if;
   fi:=0;
   for fld in select value from jsonb_array_elements(coalesce(sec->'fields','[]')) loop
    select id into fid from public.v2_template_fields where section_id=sid and position=fi;
    if fld->>'id' is not null then id_map:=id_map||jsonb_build_object(fld->>'id',fid); end if;
    fi:=fi+1;
   end loop;
   si:=si+1;
  end loop;
 else
  tid:=p_template_id;
  insert into public.v2_collection_template_write_tokens values(txid_current(),tid,auth.uid());
  -- Temporary unique positions/keys allow arbitrary reordering and cross-sheet moves.
  update public.v2_template_fields f set position=(-1000000-f.position),field_key='__editor_'||f.id
   from public.v2_template_sections s where s.id=f.section_id and s.template_id=tid;
  update public.v2_template_sections set position=(-1000000-position) where template_id=tid;
  for sec in select value from jsonb_array_elements(p_sections) loop
   sid:=case when coalesce(sec->>'id','')~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (sec->>'id')::uuid else gen_random_uuid() end;
   if sid=any(section_ids) or exists(select 1 from public.v2_template_sections where id=sid and template_id<>tid) then raise exception 'Duplicate or foreign sheet identity' using errcode='23514'; end if;
   section_ids:=array_append(section_ids,sid);
   if sec->>'id' is not null then id_map:=id_map||jsonb_build_object(sec->>'id',sid); end if;
   insert into public.v2_template_sections(id,template_id,title,position,active,settings)
    values(sid,tid,btrim(sec->>'title'),si,coalesce((sec->>'active')::boolean,true),coalesce(sec->'settings','{}'))
    on conflict(id) do update set title=excluded.title,position=excluded.position,active=excluded.active,settings=excluded.settings;
   fi:=0;
   for fld in select value from jsonb_array_elements(coalesce(sec->'fields','[]')) loop
    fid:=case when coalesce(fld->>'id','')~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (fld->>'id')::uuid else gen_random_uuid() end;
    if fid=any(field_ids) or exists(select 1 from public.v2_template_fields f join public.v2_template_sections s on s.id=f.section_id where f.id=fid and s.template_id<>tid) then raise exception 'Duplicate or foreign field identity' using errcode='23514'; end if;
    field_ids:=array_append(field_ids,fid);
    if fld->>'id' is not null then id_map:=id_map||jsonb_build_object(fld->>'id',fid); end if;
    insert into public.v2_template_fields(id,section_id,field_key,title,field_type,position,required,default_align,pinned_scope,active,settings)
     values(fid,sid,coalesce(nullif(fld->>'field_key',''),'field_'||fid),btrim(fld->>'title'),fld->>'field_type',fi,
      coalesce((fld->>'required')::boolean,false),coalesce(fld->>'default_align','right'),coalesce(fld->>'pinned_scope','none'),coalesce((fld->>'active')::boolean,true),coalesce(fld->'settings','{}'))
     on conflict(id) do update set section_id=excluded.section_id,field_key=excluded.field_key,title=excluded.title,
      field_type=excluded.field_type,position=excluded.position,required=excluded.required,default_align=excluded.default_align,
      pinned_scope=excluded.pinned_scope,active=excluded.active,settings=excluded.settings;
    fi:=fi+1;
   end loop;
   si:=si+1;
  end loop;
  delete from public.v2_template_fields f using public.v2_template_sections s where s.id=f.section_id and s.template_id=tid and not(f.id=any(field_ids));
  delete from public.v2_template_sections where template_id=tid and not(id=any(section_ids));
  if p_default and p_active then
   update public.v2_templates set is_default=false where workspace_id=p_workspace_id and kind='collection'
    and asset_symbol is not distinct from original.asset_symbol and id<>tid and is_default;
  end if;
  update public.v2_templates set name=btrim(p_name),active=p_active,is_default=p_active and p_default,
   settings=settings||jsonb_build_object('editor_customized',true,'hierarchy_version',2) where id=tid;
  delete from public.v2_collection_template_write_tokens where transaction_id=txid_current() and template_id=tid;
 end if;
 select editor_revision into revision from public.v2_templates where id=tid;
 return jsonb_build_object('template_id',tid,'revision',revision,'forked',conflict,'id_map',id_map);
end $$;
revoke all on function public.v2_save_collection_template_live(uuid,uuid,uuid,bigint,text,boolean,boolean,jsonb) from public,anon;
grant execute on function public.v2_save_collection_template_live(uuid,uuid,uuid,bigint,text,boolean,boolean,jsonb) to authenticated;
notify pgrst,'reload schema';
