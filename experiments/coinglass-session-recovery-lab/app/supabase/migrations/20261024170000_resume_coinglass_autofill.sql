-- Read a specific existing lease between short CoinGlass requests. The original
-- three-argument claim is unchanged: this overload can never create a run.
create or replace function public.v2_collection_autofill_claim(
 p_field_id uuid, p_session_id uuid, p_manual boolean, p_run_id uuid
) returns jsonb language plpgsql security definer set search_path=public,pg_temp as $$
declare
 r public.v2_collection_autofill_runs;
 st public.v2_collection_autofill_state;
 f public.v2_template_fields;
 sec public.v2_template_sections;
 s public.v2_collection_sessions;
 cfg jsonb;
begin
 select * into r from public.v2_collection_autofill_runs where id=p_run_id;
 if r.id is null or r.field_id is distinct from p_field_id
    or r.session_id is distinct from p_session_id then
  raise exception 'The existing scan does not belong to this request' using errcode='23514';
 end if;
 perform pg_advisory_xact_lock(hashtextextended(r.workspace_id::text||'collection',0));
 perform 1 from public.v2_workspaces where id=r.workspace_id for update;
 select * into r from public.v2_collection_autofill_runs where id=p_run_id;
 if r.status<>'running' then
  return jsonb_build_object('run_id',r.id,'status',r.status,'session_id',r.session_id,
    'count',r.count,'message',r.message);
 end if;
 select * into st from public.v2_collection_autofill_state
  where workspace_id=r.workspace_id and sheet_key=r.sheet_key and logical_key=r.logical_key for update;
 if st.lease_id is distinct from r.id or st.lease_until is null or st.lease_until<=clock_timestamp() then
  raise exception 'Expired automation lease' using errcode='23514';
 end if;
 -- Other source types keep their current single-request path.
 if r.sheet_key<>'data' or r.manual is distinct from p_manual then
  return jsonb_build_object('run_id',r.id,'status','running','session_id',r.session_id);
 end if;
 select * into f from public.v2_template_fields where id=r.field_id and active;
 select * into sec from public.v2_template_sections where id=f.section_id and active;
 select * into s from public.v2_collection_sessions where id=r.session_id
  and workspace_id=r.workspace_id and deleted_at is null and status<>'archived';
 if s.id is null or sec.template_id is distinct from s.template_id then
  raise exception 'The scan target changed' using errcode='23514';
 end if;
 cfg:=app_private.collection_autofill_current_config(f.id,s.id);
 f.settings:=coalesce(cfg->'settings',f.settings);
 return jsonb_build_object('run_id',r.id,'status','running','field',to_jsonb(f),
  'section',to_jsonb(sec),'session',to_jsonb(s),'baseline',r.baseline,
  'source_cursors',st.source_cursors,'last_scanned_at',st.last_scanned_at,
  'created_at',r.created_at,
  'configuration_current',app_private.collection_autofill_run_config_current(r.id));
end $$;
revoke all on function public.v2_collection_autofill_claim(uuid,uuid,boolean,uuid)
 from public,anon,authenticated;
grant execute on function public.v2_collection_autofill_claim(uuid,uuid,boolean,uuid) to service_role;
notify pgrst,'reload schema';
