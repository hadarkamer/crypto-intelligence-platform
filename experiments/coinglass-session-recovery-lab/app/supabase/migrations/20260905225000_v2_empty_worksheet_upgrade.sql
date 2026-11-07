-- Repair only untouched entities attached to the exact original generated stubs.
-- Installation changes no application rows. An authorized detail-page open calls
-- prepare_v2_worksheet; filled, custom, completed and archived history stays bound.
create or replace function public.v2_empty_generated_worksheet(p_kind text,p_entity_id uuid,p_template_id uuid)
returns boolean language plpgsql stable security definer set search_path=public,pg_temp as $$
begin
 if not coalesce(public.v2_is_generated_template(p_template_id),false) then return false; end if;
 if p_kind='collection' then
  return exists(select 1 from public.v2_collection_sessions s where s.id=p_entity_id and s.template_id=p_template_id and s.status='draft')
   and not exists(select 1 from public.v2_collection_values where session_id=p_entity_id)
   and not exists(select 1 from public.v2_news_items where collection_session_id=p_entity_id);
 elsif p_kind='trade' then
  return exists(select 1 from public.v2_trades t where t.id=p_entity_id and t.template_id=p_template_id and t.status='evaluating'
   and t.activated_at is null and t.closed_at is null and t.archived_at is null and t.pnl is null and t.root_trade_id is null and t.cycle_number=1)
   and not exists(select 1 from public.v2_trade_values where trade_id=p_entity_id);
 end if;
 return false;
end $$;
revoke all on function public.v2_empty_generated_worksheet(text,uuid,uuid) from public,anon,authenticated;

-- Read the binding only after the entity row is locked. This covers direct
-- table writes as well as save/structured-row RPCs. If preparation wins a race,
-- a stale field write fails explicitly; if a value write wins, preparation skips.

create or replace function public.v2_validate_collection_value_field()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  session_workspace uuid;
  session_template uuid;
  field_workspace uuid;
  field_template uuid;
begin
  select s.workspace_id, s.template_id
  into session_workspace, session_template
  from public.v2_collection_sessions s
  where s.id = new.session_id for update;

  if not found then
    raise exception 'collection session does not exist'
      using errcode = '23503';
  end if;

  select t.workspace_id, t.id
  into field_workspace, field_template
  from public.v2_template_fields f
  join public.v2_template_sections sec on sec.id = f.section_id
  join public.v2_templates t on t.id = sec.template_id
  where f.id = new.field_id;

  if not found then
    raise exception 'collection field does not exist'
      using errcode = '23503';
  end if;

  if session_template is null
     or field_template <> session_template
     or field_workspace <> session_workspace then
    raise exception 'collection value field must belong to the session template and workspace'
      using errcode = '23514';
  end if;

  return new;
end;
$$;

revoke all on function public.v2_validate_collection_value_field() from public,anon,authenticated;

create or replace function public.v2_validate_trade_value_field()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  trade_workspace uuid;
  trade_template uuid;
  field_workspace uuid;
  field_template uuid;
begin
  select tr.workspace_id, tr.template_id
  into trade_workspace, trade_template
  from public.v2_trades tr
  where tr.id = new.trade_id for update;

  if not found then
    raise exception 'trade does not exist'
      using errcode = '23503';
  end if;

  select t.workspace_id, t.id
  into field_workspace, field_template
  from public.v2_template_fields f
  join public.v2_template_sections sec on sec.id = f.section_id
  join public.v2_templates t on t.id = sec.template_id
  where f.id = new.field_id;

  if not found then
    raise exception 'trade field does not exist'
      using errcode = '23503';
  end if;

  if trade_template is null
     or field_template <> trade_template
     or field_workspace <> trade_workspace then
    raise exception 'trade value field must belong to the trade template and workspace'
      using errcode = '23514';
  end if;

  return new;
end;
$$;

revoke all on function public.v2_validate_trade_value_field() from public,anon,authenticated;

create or replace function public.v2_validate_news_item_links()
returns trigger language plpgsql security definer set search_path = public, pg_temp as $$
begin
  -- Serialize legacy separate news rows with the collection binding repair too.
  if new.collection_session_id is not null then
    perform 1 from public.v2_collection_sessions where id=new.collection_session_id for update;
  end if;
  if new.collection_session_id is not null and not exists (
    select 1 from public.v2_collection_sessions s
    where s.id = new.collection_session_id and s.workspace_id = new.workspace_id
  ) then raise exception 'news collection must belong to its workspace' using errcode = '23514'; end if;
  if new.source_id is not null and not exists (
    select 1 from public.v2_news_sources src where src.id = new.source_id and src.workspace_id = new.workspace_id
  ) then raise exception 'news source must belong to its workspace' using errcode = '23514'; end if;
  if new.field_id is not null and not exists (
    select 1 from public.v2_template_fields f
    join public.v2_template_sections sec on sec.id = f.section_id
    join public.v2_collection_sessions s on s.template_id = sec.template_id
    where f.id = new.field_id and s.id = new.collection_session_id and s.workspace_id = new.workspace_id
      and f.field_type in ('news_table', 'news_source_table')
  ) then raise exception 'news field must belong to its collection template' using errcode = '23514'; end if;
  return new;
end; $$;

revoke all on function public.v2_validate_news_item_links() from public,anon,authenticated;

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
    if new.workspace_id<>old.workspace_id or new.asset_symbol<>old.asset_symbol or new.root_trade_id is distinct from old.root_trade_id or new.cycle_number<>old.cycle_number then raise exception 'Trade identity and template are immutable' using errcode='23514'; end if;
    if new.template_id is distinct from old.template_id then
      -- No caller-controlled bypass flag: direct writes must satisfy the same
      -- exact empty-seed and strict-source contract as prepare_v2_worksheet.
      if auth.uid() is null or not coalesce(public.v2_can_edit_workspace(old.workspace_id),false)
        or (to_jsonb(new)-array['template_id','updated_at','updated_by']) is distinct from (to_jsonb(old)-array['template_id','updated_at','updated_by'])
        or not public.v2_empty_generated_worksheet('trade',old.id,old.template_id) then
        raise exception 'Trade identity and template are immutable' using errcode='23514';
      end if;
      if new.template_id is null or new.template_id is distinct from public.v2_import_complete_legacy_template(old.workspace_id,'trade')
        or not exists(select 1 from public.v2_templates where id=new.template_id and workspace_id=old.workspace_id and kind='trade' and active) then
        raise exception 'Empty generated trades can only upgrade to the current complete source template' using errcode='23514';
      end if;
    end if;
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


create or replace function public.prepare_v2_worksheet(p_kind text,p_entity_id uuid)
returns uuid language plpgsql security definer set search_path=public,pg_temp as $$
declare entity_table text; wid uuid; tid uuid; target_id uuid;
begin
 if auth.uid() is null then raise exception 'Authentication required' using errcode='42501'; end if;
 case p_kind when 'collection' then entity_table:='v2_collection_sessions';
  when 'trade' then entity_table:='v2_trades';
  else raise exception 'Unsupported worksheet kind' using errcode='22023'; end case;
 execute format('select workspace_id,template_id from public.%I where id=$1 and public.v2_is_workspace_member(workspace_id)',entity_table)
  into wid,tid using p_entity_id;
 if wid is null then raise exception 'Worksheet access required' using errcode='42501'; end if;
 -- Viewing is never an implicit edit grant and must not break read-only access.
 if not coalesce(public.v2_can_edit_workspace(wid),false) then return tid; end if;
 -- Match value RPCs and direct trade UPDATEs: lock the entity before any
 -- import/workspace lock. The importer never locks collection/trade entities.
 execute format('select template_id from public.%I where id=$1 and workspace_id=$2 for update',entity_table)
  into tid using p_entity_id,wid;
 if tid is null or not public.v2_empty_generated_worksheet(p_kind,p_entity_id,tid) then return tid; end if;
 perform 1 from public.v2_workspaces where id=wid for update;
 perform 1 from public.v2_templates where id=tid for update;
 if not public.v2_empty_generated_worksheet(p_kind,p_entity_id,tid) then return tid; end if;
 target_id:=public.v2_import_complete_legacy_template(wid,p_kind);
 if target_id is null or target_id=tid then return tid; end if;
 perform 1 from public.v2_templates where id=target_id and workspace_id=wid and kind=p_kind and active for share;
 if not found then raise exception 'Complete worksheet template is unavailable' using errcode='23514'; end if;
 execute format('update public.%I set template_id=$1 where id=$2',entity_table) using target_id,p_entity_id;
 return target_id;
end $$;
revoke all on function public.prepare_v2_worksheet(text,uuid) from public,anon;
grant execute on function public.prepare_v2_worksheet(text,uuid) to authenticated;
comment on function public.prepare_v2_worksheet(text,uuid) is
 'Idempotent authorized repair for exact generated, entirely unanswered draft worksheets; readers and historical/custom worksheets remain unchanged.';
