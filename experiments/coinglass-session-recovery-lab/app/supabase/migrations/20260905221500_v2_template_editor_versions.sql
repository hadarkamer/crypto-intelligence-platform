-- Editor saves retain source identity metadata and update future per-asset defaults atomically.
-- Existing entities and answers stay bound to their immutable original template versions.
create or replace function public.v2_save_template_editor(
 p_workspace_id uuid,p_template_id uuid,p_source_template_id uuid,p_kind text,p_name text,
 p_active boolean,p_default boolean,p_sections jsonb,p_default_asset_symbol text default null)
returns uuid language plpgsql security definer set search_path=public,pg_temp as $$
declare source_template public.v2_templates%rowtype; saved_id uuid; source_id uuid;
 bind_symbol text; scope_symbol text; bound_template uuid; metadata jsonb:='{}'::jsonb;
begin
 if auth.uid() is null or not coalesce(public.v2_can_edit_workspace(p_workspace_id),false) then
  raise exception 'Workspace edit permission required' using errcode='42501';
 end if;
 if p_kind not in ('collection','scenario','trade','spot') then raise exception 'Invalid template kind'; end if;
 perform pg_advisory_xact_lock(hashtextextended(p_workspace_id::text||p_kind,0));
 source_id:=coalesce(p_source_template_id,p_template_id);
 if p_template_id is not null and p_source_template_id is not null and p_template_id<>p_source_template_id then
  raise exception 'An existing draft must retain its own source identity' using errcode='23514';
 end if;
 if source_id is not null then
  select * into source_template from public.v2_templates where id=source_id and workspace_id=p_workspace_id and kind=p_kind for update;
  if not found then raise exception 'Source template does not belong to this workspace and kind' using errcode='42501'; end if;
  scope_symbol:=source_template.asset_symbol;
  metadata:=coalesce(source_template.settings,'{}'::jsonb);
 end if;
 bind_symbol:=nullif(upper(btrim(p_default_asset_symbol)),'');
 if bind_symbol is null and p_active and p_template_id is null and source_id is not null then bind_symbol:=scope_symbol; end if;
 if bind_symbol is not null then
  if not p_active or p_kind='spot' then raise exception 'Only active collection, scenario or trade templates can become asset defaults' using errcode='23514'; end if;
  if scope_symbol is not null and upper(scope_symbol)<>bind_symbol then raise exception 'Template asset does not match the selected asset' using errcode='23514'; end if;
  select case p_kind when 'collection' then default_collection_template_id when 'scenario' then default_scenario_template_id else default_trade_template_id end
   into bound_template from public.v2_asset_settings where workspace_id=p_workspace_id and symbol=bind_symbol for update;
  if not found then raise exception 'Selected asset does not exist in this workspace' using errcode='23514'; end if;
  -- An inferred asset binding only follows a source already selected for that asset.
  if p_default_asset_symbol is null and bound_template is distinct from source_id then bind_symbol:=null;
  elsif source_id is not null and bound_template is distinct from source_id then
   raise exception 'The asset template changed. Reload its settings before saving' using errcode='40001';
  end if;
 end if;
 -- Avoid briefly assigning an asset-scoped template as the workspace-wide default.
 saved_id:=public.v2_save_template(p_workspace_id,p_template_id,p_kind,p_name,p_active,false,p_sections);
 update public.v2_templates set asset_symbol=scope_symbol,
   settings=metadata||coalesce(settings,'{}'::jsonb)||jsonb_build_object('hierarchy_version',2,'editor_customized',true)
      ||case when source_id is not null and saved_id<>source_id then jsonb_build_object('parent_template_id',source_id) else '{}'::jsonb end
   where id=saved_id;
 if p_default and p_active then
  update public.v2_templates set is_default=false
   where workspace_id=p_workspace_id and kind=p_kind and asset_symbol is not distinct from scope_symbol and id<>saved_id and is_default;
  update public.v2_templates set is_default=true where id=saved_id;
 end if;
 if bind_symbol is not null then
  update public.v2_asset_settings set
   default_collection_template_id=case when p_kind='collection' then saved_id else default_collection_template_id end,
   default_scenario_template_id=case when p_kind='scenario' then saved_id else default_scenario_template_id end,
   default_trade_template_id=case when p_kind='trade' then saved_id else default_trade_template_id end,
   updated_by=auth.uid(),updated_at=now()
  where workspace_id=p_workspace_id and symbol=bind_symbol;
 end if;
 return saved_id;
end $$;
revoke all on function public.v2_save_template_editor(uuid,uuid,uuid,text,text,boolean,boolean,jsonb,text) from public,anon;
grant execute on function public.v2_save_template_editor(uuid,uuid,uuid,text,text,boolean,boolean,jsonb,text) to authenticated;
