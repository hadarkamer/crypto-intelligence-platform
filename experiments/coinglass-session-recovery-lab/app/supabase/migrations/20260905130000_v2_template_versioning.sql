-- Template editing is atomic; referenced versions remain immutable.
create or replace function public.v2_template_in_use(p_template_id uuid)
returns boolean language sql stable security definer set search_path=public,pg_temp as $$
 select public.v2_is_workspace_member(t.workspace_id) and (
 exists(select 1 from v2_collection_sessions s where s.template_id=t.id) or
 exists(select 1 from v2_trades s where s.template_id=t.id) or
 exists(select 1 from v2_scenarios s where s.template_id=t.id) or
 exists(select 1 from v2_spot_assets s where s.template_id=t.id))
 from v2_templates t where t.id=p_template_id
$$;
revoke all on function public.v2_template_in_use(uuid) from public,anon;
grant execute on function public.v2_template_in_use(uuid) to authenticated;

create or replace function public.v2_save_template(
 p_workspace_id uuid,p_template_id uuid,p_kind text,p_name text,
 p_active boolean,p_default boolean,p_sections jsonb)
returns uuid language plpgsql security definer set search_path=public,pg_temp as $$
declare tid uuid; sec jsonb; fld jsonb; sid uuid; i integer:=0; j integer; old_kind text;
begin
 if not coalesce(public.v2_can_edit_workspace(p_workspace_id),false) then raise exception 'Workspace edit permission required'; end if;
 if p_kind not in ('collection','scenario','trade','spot') or nullif(btrim(p_name),'') is null then raise exception 'Invalid template'; end if;
 if jsonb_typeof(p_sections)<>'array' or jsonb_array_length(p_sections)>100 then raise exception 'Invalid sections'; end if;
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
  sid:=gen_random_uuid();
  insert into v2_template_sections(id,template_id,title,position,active,settings)
  values(sid,tid,btrim(sec->>'title'),i,true,coalesce(sec->'settings','{}'::jsonb));
  i:=i+1;j:=0;
  for fld in select value from jsonb_array_elements(coalesce(sec->'fields','[]'::jsonb)) loop
   if nullif(btrim(fld->>'title'),'') is null then raise exception 'Field title is required'; end if;
   insert into v2_template_fields(section_id,field_key,title,field_type,position,required,default_align,pinned_scope,active,settings)
   values(sid,coalesce(nullif(fld->>'field_key',''),'field_'||gen_random_uuid()::text),btrim(fld->>'title'),fld->>'field_type',j,
    coalesce((fld->>'required')::boolean,false),coalesce(fld->>'default_align','right'),coalesce(fld->>'pinned_scope','none'),true,coalesce(fld->'settings','{}'::jsonb));
   j:=j+1;
  end loop;
 end loop;
 return tid;
end $$;
revoke all on function public.v2_save_template(uuid,uuid,text,text,boolean,boolean,jsonb) from public,anon;
grant execute on function public.v2_save_template(uuid,uuid,text,text,boolean,boolean,jsonb) to authenticated;

create or replace function public.v2_guard_used_template_structure()
returns trigger language plpgsql security definer set search_path=public,pg_temp as $$
declare old_tid uuid; new_tid uuid;
begin
 if tg_table_name='v2_template_sections' then
  if tg_op <> 'INSERT' then old_tid := old.template_id; end if;
  if tg_op <> 'DELETE' then new_tid := new.template_id; end if;
 else
  if tg_op <> 'INSERT' then
   select template_id into old_tid from v2_template_sections where id=old.section_id;
  end if;
  if tg_op <> 'DELETE' then
   select template_id into new_tid from v2_template_sections where id=new.section_id;
  end if;
 end if;
 -- Lock both versions in a stable order. Their foreign-key references acquire
 -- KEY SHARE locks, so an entity cannot attach concurrently between this check
 -- and the structural change. The check covers both ends of a move.
 perform 1 from v2_templates where id=any(array[old_tid,new_tid]) order by id for update;
 if exists(select 1 from v2_collection_sessions where template_id=any(array[old_tid,new_tid]))
 or exists(select 1 from v2_trades where template_id=any(array[old_tid,new_tid]))
 or exists(select 1 from v2_scenarios where template_id=any(array[old_tid,new_tid]))
 or exists(select 1 from v2_spot_assets where template_id=any(array[old_tid,new_tid])) then
  raise exception 'Referenced template versions are immutable; create a new version';
 end if;
 if tg_op='DELETE' then return old; end if;
 return new;
end $$;
revoke all on function public.v2_guard_used_template_structure() from public,anon,authenticated;
drop trigger if exists v2_used_template_sections_guard on public.v2_template_sections;
create trigger v2_used_template_sections_guard before insert or update or delete on public.v2_template_sections for each row execute function public.v2_guard_used_template_structure();
drop trigger if exists v2_used_template_fields_guard on public.v2_template_fields;
create trigger v2_used_template_fields_guard before insert or update or delete on public.v2_template_fields for each row execute function public.v2_guard_used_template_structure();
notify pgrst,'reload schema';

-- A parent DELETE otherwise nulls entity template_id through its FK before the
-- cascading child triggers can determine that this version was referenced.
create or replace function public.v2_guard_used_template_delete()
returns trigger language plpgsql security definer set search_path=public,pg_temp as $$
begin
 if exists(select 1 from v2_collection_sessions where template_id=old.id)
 or exists(select 1 from v2_trades where template_id=old.id)
 or exists(select 1 from v2_scenarios where template_id=old.id)
 or exists(select 1 from v2_spot_assets where template_id=old.id) then
  raise exception 'Referenced template versions cannot be deleted';
 end if;
 return old;
end $$;
revoke all on function public.v2_guard_used_template_delete() from public,anon,authenticated;
drop trigger if exists v2_used_template_delete_guard on public.v2_templates;
create trigger v2_used_template_delete_guard before delete on public.v2_templates
for each row execute function public.v2_guard_used_template_delete();


-- Protect the parent identity too: child-only guards do not prevent a kind
-- change from invalidating existing collection/scenario/trade/spot bindings.
-- Display name, active and default metadata deliberately remain editable.
create or replace function public.v2_guard_template_identity_update()
returns trigger language plpgsql security definer set search_path=public,pg_temp as $$
begin
 if new.id is not distinct from old.id and new.workspace_id is not distinct from old.workspace_id
    and new.kind is not distinct from old.kind and new.asset_symbol is not distinct from old.asset_symbol then
  return new;
 end if;
 -- Kind is creation-time identity, matching v2_save_template. Blocking it even
 -- before first use avoids racing a concurrent entity/default attachment.
 if new.kind is distinct from old.kind then
  raise exception 'Template kind is immutable; create a new template' using errcode='23514';
 end if;
 -- Serialize remaining identity changes with concurrent foreign-key attachment.
 perform id from public.v2_templates where id=old.id for update;
 if exists(select 1 from v2_collection_sessions where template_id=old.id)
 or exists(select 1 from v2_trades where template_id=old.id)
 or exists(select 1 from v2_scenarios where template_id=old.id)
 or exists(select 1 from v2_spot_assets where template_id=old.id)
 or exists(select 1 from v2_asset_settings where default_collection_template_id=old.id
   or default_scenario_template_id=old.id or default_trade_template_id=old.id) then
  raise exception 'Referenced template identity is immutable; create a new version' using errcode='23514';
 end if;
 return new;
end $$;
revoke all on function public.v2_guard_template_identity_update() from public,anon,authenticated;
drop trigger if exists v2_template_identity_update_guard on public.v2_templates;
create trigger v2_template_identity_update_guard before update of id,workspace_id,kind,asset_symbol on public.v2_templates
for each row execute function public.v2_guard_template_identity_update();
