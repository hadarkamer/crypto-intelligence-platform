begin;

-- The data sheet is permanent across news worksheets. A newly added field must
-- have a legal counterpart in every still-open worksheet's template; values
-- continue to use the existing shared logical-key storage.
create or replace function app_private.sync_collection_permanent_fields(p_source_template_id uuid)
returns integer language plpgsql security definer
set search_path=pg_catalog,public,app_private,pg_temp as $$
declare source public.v2_templates%rowtype; target public.v2_templates%rowtype;
 source_field record; source_section public.v2_template_sections%rowtype;
 target_section public.v2_template_sections%rowtype;
 source_group jsonb; target_group jsonb; target_groups jsonb; group_property text;
 source_group_id text; target_group_id text; field_settings jsonb;
 logical text; new_id uuid; target_section_id uuid; next_position integer;
 amount integer:=0; token_added integer; actor uuid:=auth.uid();
begin
 select * into source from public.v2_templates where id=p_source_template_id and kind='collection' and active and is_default;
 if not found or actor is null then return 0; end if;
 if not public.v2_can_edit_workspace(source.workspace_id) then
   raise exception 'Workspace edit permission required' using errcode='42501';
 end if;
 perform pg_advisory_xact_lock(hashtextextended(source.workspace_id::text||'collection',0));
 -- Re-read after waiting for an editor that may have changed the default.
 select * into source from public.v2_templates where id=p_source_template_id and kind='collection' and active and is_default;
 if not found then return 0; end if;
 for target in select t.* from public.v2_templates t
   where t.workspace_id=source.workspace_id and t.kind='collection' and t.id<>source.id
     and t.asset_symbol is not distinct from source.asset_symbol
     and exists(select 1 from public.v2_collection_sessions s where s.template_id=t.id
       and s.workspace_id=source.workspace_id and s.deleted_at is null and s.status<>'archived')
     and not exists(select 1 from public.v2_trades where template_id=t.id)
     and not exists(select 1 from public.v2_scenarios where template_id=t.id)
     and not exists(select 1 from public.v2_spot_assets where template_id=t.id)
   order by t.id for update
 loop
   -- Existing fields of any kind, including hidden ones, retain their identity,
   -- answer contracts and settings. Never resurrect or reinterpret them.
   if not exists(select 1 from public.v2_template_fields f
      join public.v2_template_sections sec on sec.id=f.section_id
      where sec.template_id=source.id and sec.active and f.active
        and public.v2_collection_sheet_key(to_jsonb(sec))='data'
        and not exists(select 1 from public.v2_template_fields prior
          join public.v2_template_sections prior_sec on prior_sec.id=prior.section_id
          where prior_sec.template_id=target.id
           and public.v2_collection_field_logical_key(to_jsonb(prior))=public.v2_collection_field_logical_key(to_jsonb(f))))
     then continue; end if;
   insert into public.v2_collection_template_write_tokens(transaction_id,template_id,actor_id)
     values(txid_current(),target.id,actor) on conflict do nothing;
   get diagnostics token_added=row_count;
   for source_field in select f.*,sec.id as source_section_id,
       public.v2_collection_field_logical_key(to_jsonb(f)) as logical_key
     from public.v2_template_fields f join public.v2_template_sections sec on sec.id=f.section_id
     where sec.template_id=source.id and sec.active and f.active
       and public.v2_collection_sheet_key(to_jsonb(sec))='data'
       and not exists(select 1 from jsonb_array_elements(coalesce(sec.settings->'groups',sec.settings->'legacy_groups','[]')) g
         where g->>'id'=coalesce(f.settings->>'group_id',f.settings->>'legacy_group_id') and g->>'active'='false')
     order by sec.position,sec.id,f.position,f.id
   loop
     logical:=source_field.logical_key;
     if logical is null or exists(select 1 from public.v2_template_fields prior
       join public.v2_template_sections prior_sec on prior_sec.id=prior.section_id
       where prior_sec.template_id=target.id and public.v2_collection_field_logical_key(to_jsonb(prior))=logical)
       then continue; end if;
     -- Ambiguous identities in the source must not become a second interpretation.
     if (select count(*) from public.v2_template_fields duplicate
        join public.v2_template_sections ds on ds.id=duplicate.section_id
        where ds.template_id=source.id and ds.active and duplicate.active
          and public.v2_collection_sheet_key(to_jsonb(ds))='data'
          and public.v2_collection_field_logical_key(to_jsonb(duplicate))=logical)<>1 then continue; end if;
     select * into source_section from public.v2_template_sections where id=source_field.source_section_id;
     select * into target_section from public.v2_template_sections sec
       where sec.template_id=target.id and sec.active and public.v2_collection_sheet_key(to_jsonb(sec))='data'
       order by (sec.title=source_section.title) desc,sec.position,sec.id limit 1;
     if not found then
       target_section_id:=gen_random_uuid();
       select coalesce(max(position),-1)+1 into next_position from public.v2_template_sections where template_id=target.id;
       insert into public.v2_template_sections(id,template_id,title,position,active,settings)
         values(target_section_id,target.id,source_section.title,next_position,true,
           source_section.settings||jsonb_build_object('sheet_key','data','permanent_source_section_id',source_section.id));
       select * into target_section from public.v2_template_sections where id=target_section_id;
     end if;
     target_section_id:=target_section.id;
     field_settings:=source_field.settings||jsonb_build_object('question_key',logical,
       'permanent_source_field_id',source_field.id,'permanent_source_template_id',source.id);
     -- Canonical sheets keep the corresponding group; old one-group sections
     -- need no group rewrite. Unknown groups are appended without changing peers.
     if jsonb_typeof(target_section.settings->'groups')='array' or jsonb_typeof(target_section.settings->'legacy_groups')='array' then
       group_property:=case when jsonb_typeof(target_section.settings->'groups')='array' then 'groups' else 'legacy_groups' end;
       target_groups:=target_section.settings->group_property;
       source_group_id:=coalesce(source_field.settings->>'group_id',source_field.settings->>'legacy_group_id');
       select g into source_group from jsonb_array_elements(coalesce(source_section.settings->'groups',source_section.settings->'legacy_groups','[]')) g
         where g->>'id'=source_group_id limit 1;
       select g into target_group from jsonb_array_elements(target_groups) g
         where (source_group_id is not null and g->>'id'=source_group_id)
          or (source_group->>'title' is not null and g->>'title'=source_group->>'title')
         order by (g->>'id'=source_group_id) desc limit 1;
       if target_group is null then
         target_group_id:=gen_random_uuid()::text;
         target_group:=coalesce(source_group,jsonb_build_object('title',coalesce(source_field.settings->>'group_title',source_field.settings->>'legacy_group_title','כללי')))
           ||jsonb_build_object('id',target_group_id,'position',jsonb_array_length(target_groups),'active',true);
         target_groups:=target_groups||jsonb_build_array(target_group);
         update public.v2_template_sections set settings=jsonb_set(settings,array[group_property],target_groups)
           where id=target_section_id;
       else
         -- An intentionally hidden target group stays hidden; do not add a new
         -- field into it or silently enable the whole group.
         if target_group->>'active'='false' then continue; end if;
         target_group_id:=target_group->>'id';
       end if;
       field_settings:=field_settings||jsonb_build_object('group_id',target_group_id);
     end if;
     select coalesce(max(position),-1)+1 into next_position from public.v2_template_fields where section_id=target_section_id;
     new_id:=gen_random_uuid();
     insert into public.v2_template_fields(id,section_id,field_key,title,field_type,position,required,default_align,pinned_scope,active,settings)
       values(new_id,target_section_id,'permanent_'||new_id::text,source_field.title,source_field.field_type,next_position,
         source_field.required,source_field.default_align,source_field.pinned_scope,true,field_settings);
     amount:=amount+1;
   end loop;
   if token_added>0 then
     delete from public.v2_collection_template_write_tokens where transaction_id=txid_current() and template_id=target.id and actor_id=actor;
   end if;
 end loop;
 return amount;
end $$;
revoke all on function app_private.sync_collection_permanent_fields(uuid) from public,anon,authenticated,service_role;

-- Defer until all fields/groups/default flags of an atomic editor save exist.
-- The source is re-read: an old default which was replaced in this transaction
-- must never propagate its obsolete shape.
create or replace function app_private.sync_collection_permanent_fields_after_template()
returns trigger language plpgsql security definer set search_path=pg_catalog,public,app_private,pg_temp as $$
begin
 if new.kind='collection' and auth.uid() is not null and exists(
   select 1 from public.v2_templates t where t.id=new.id and public.v2_can_edit_workspace(t.workspace_id)
 ) then perform app_private.sync_collection_permanent_fields(new.id); end if;
 return new;
end $$;
revoke all on function app_private.sync_collection_permanent_fields_after_template() from public,anon,authenticated,service_role;
drop trigger if exists v2_collection_permanent_schema on public.v2_templates;
create constraint trigger v2_collection_permanent_schema after insert or update on public.v2_templates
 deferrable initially deferred for each row execute function app_private.sync_collection_permanent_fields_after_template();

-- Repair additions already saved before this feature. Scope the capability to
-- each workspace owner only for this transaction, then restore the caller.
do $repair$
declare item record; prior_actor text:=current_setting('request.jwt.claim.sub',true);
begin
 for item in select t.id,w.owner_id from public.v2_templates t join public.v2_workspaces w on w.id=t.workspace_id
   where t.kind='collection' and t.active and t.is_default order by t.workspace_id,t.id
 loop
   perform set_config('request.jwt.claim.sub',item.owner_id::text,true);
   perform app_private.sync_collection_permanent_fields(item.id);
 end loop;
 perform set_config('request.jwt.claim.sub',coalesce(prior_actor,''),true);
end $repair$;
commit;

