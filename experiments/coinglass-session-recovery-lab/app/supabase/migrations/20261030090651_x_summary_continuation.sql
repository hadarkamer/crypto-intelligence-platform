-- Reuse the existing run, lease, apply transaction and client continuation.
-- No new tables, worker, schedule or provider request path.
begin;
do $migration$
declare definition text; needle text; replacement text;
begin
 definition:=pg_get_functiondef('public.v2_collection_autofill_apply(uuid,jsonb,jsonb,timestamptz)'::regprocedure);
 if position('x_summary_continuation_v1' in definition)=0 then
  needle:=' select * into state from public.v2_collection_autofill_state';
  if position(needle in definition)=0 then raise exception 'Autofill apply lock anchor missing';end if;
  definition:=replace(definition,needle,E' -- x_summary_continuation_v1: re-read after acquiring the shared lock.\n select * into run from public.v2_collection_autofill_runs where id=p_run_id;\n'||needle);
  needle:=' -- Manual scans must still belong to their original selected session.';
  replacement:=$patch$
 -- A late first-stage write cannot replace the already handed-off remainder.
 if state.source_cursors#>>'{__x_summary_retry,run_id}'=run.id::text and
   (state.source_cursors#>>'{__x_summary_retry,ready}' is distinct from 'false'
    or p_cursors->'__x_summary_retry' is distinct from state.source_cursors->'__x_summary_retry') then
  raise exception 'X summary phase changed' using errcode='23514';
 end if;
 -- Manual scans must still belong to their original selected session.$patch$;
  if position(needle in definition)=0 then raise exception 'Autofill apply phase anchor missing';end if;
  definition:=replace(definition,needle,replacement);
  needle:=' update public.v2_collection_autofill_runs set status=final_status';
  replacement:=$patch$
 -- Successful writes and the reduced X remainder commit in this transaction.
 if p_payload->>'x_summary_continue'='true' and final_status<>'conflict' then
  if run.sheet_key<>'news_macro' or p_payload->>'write_mode' is distinct from 'append'
    or p_cursors#>>'{__x_summary_retry,run_id}' is distinct from run.id::text
    or p_cursors#>>'{__x_summary_retry,ready}' is distinct from 'true'
    or (p_cursors#>>'{__x_summary_retry,scan_started_at}')::timestamptz is distinct from p_scan_started_at
    or state.source_cursors#>>'{__x_summary_retry,run_id}'=run.id::text
    or not exists(select 1 from jsonb_each(p_cursors) c
      where c.key ~ '^https://(www\.|mobile\.)?(x\.com|twitter\.com)/'
       and jsonb_typeof(c.value#>'{x_pending,items}')='array'
       and jsonb_array_length(c.value#>'{x_pending,items}')>0
       and not (c.value->'x_pending' ? 'pagination')) then
   raise exception 'Invalid X summary continuation' using errcode='23514';
  end if;
  final_status:='running';
 end if;
 amount:=amount+coalesce(run.count,0);
 if final_status='unchanged' and amount>0 then final_status:='completed';end if;
 update public.v2_collection_autofill_runs set status=final_status$patch$;
  if position(needle in definition)=0 then raise exception 'Autofill apply finalization anchor missing';end if;
  definition:=replace(definition,needle,replacement);
  definition:=replace(definition,'count=amount,finished_at=now(),',
    'count=amount,finished_at=case when final_status=''running'' then null else now() end,');
  definition:=replace(definition,'lease_id=null,lease_until=null,',
    'lease_id=case when final_status=''running'' then run.id else null end,lease_until=case when final_status=''running'' then lease_until else null end,');
  definition:=replace(definition,'last_scanned_at=case when final_status=''conflict''',
    'last_scanned_at=case when final_status in (''conflict'',''running'')');
  definition:=replace(definition,'else p_cursors end',
    'else case when final_status=''running'' then p_cursors else p_cursors-''__x_summary_retry'' end end');
  definition:=replace(definition,'return jsonb_build_object(''run_id'',run.id,''status'',final_status',
    'return (case when final_status=''running'' then ''{"continue":true}''::jsonb else ''{}''::jsonb end)||jsonb_build_object(''run_id'',run.id,''status'',final_status');
  execute definition;
 end if;

 definition:=pg_get_functiondef('public.v2_collection_autofill_claim(uuid,uuid,boolean,uuid)'::regprocedure);
 if position('x_summary_continuation_v1' in definition)=0 then
  needle:=$needle$ -- Other source types keep their current single-request path.
 if r.sheet_key<>'data' or r.manual is distinct from p_manual then
  return jsonb_build_object('run_id',r.id,'status','running','session_id',r.session_id);
 end if;$needle$;
  replacement:=$patch$ -- x_summary_continuation_v1: only one caller consumes the ready remainder.
 if r.manual is distinct from p_manual then
  return jsonb_build_object('run_id',r.id,'status','running','session_id',r.session_id);
 end if;
 if r.sheet_key='news_macro' then
  if st.source_cursors#>>'{__x_summary_retry,run_id}' is distinct from r.id::text then
   -- An observer may wait for the first request to expose its remainder.
   -- This only reads the existing run; it never starts another source scan.
   return jsonb_build_object('run_id',r.id,'status','running','session_id',r.session_id)||
    case when exists(select 1 from public.v2_template_fields field,
     unnest(app_private.collection_autofill_source_urls(field.settings)) url
     where field.id=r.field_id and url ~* '^https://(www\.|mobile\.)?(x\.com|twitter\.com)/')
    then '{"continue":true}'::jsonb else '{}'::jsonb end;
  end if;
  if st.source_cursors#>>'{__x_summary_retry,ready}' is distinct from 'true' then
   if (st.source_cursors#>>'{__x_summary_retry,claimed_at}')::timestamptz
        + interval '5 minutes' <= clock_timestamp() then
    -- Stop waiting beyond the request budget. Never reassign this paid attempt. Preserve
    -- successes and remainder, end it explicitly, and fence out late writes.
    return public.v2_collection_autofill_apply(r.id,
     jsonb_build_object('write_mode','append','source_failure_message',
      'המשך הסיכום לא הסתיים בזמן. התוצאות והפוסטים שנותרו נשמרו להפעלה הבאה.'),
     st.source_cursors,(st.source_cursors#>>'{__x_summary_retry,scan_started_at}')::timestamptz);
   end if;
   return jsonb_build_object('run_id',r.id,'status','running','session_id',r.session_id,'continue',true);
  end if;
  st.source_cursors:=jsonb_set(st.source_cursors,'{__x_summary_retry}',
   (st.source_cursors->'__x_summary_retry')||jsonb_build_object('ready',false,'claimed_at',clock_timestamp()));
  update public.v2_collection_autofill_state set source_cursors=st.source_cursors
   where lease_id=r.id;
  if st.lease_until<=clock_timestamp()+interval '5 minutes' then
   return public.v2_collection_autofill_apply(r.id,
    jsonb_build_object('write_mode','append','source_failure_message',
     'לא נותר זמן להמשך הסיכום בהפעלה זו. התוצאות והפוסטים שנותרו נשמרו להפעלה הבאה.'),
    st.source_cursors,(st.source_cursors#>>'{__x_summary_retry,scan_started_at}')::timestamptz);
  end if;
 elsif r.sheet_key<>'data' then
  return jsonb_build_object('run_id',r.id,'status','running','session_id',r.session_id);
 end if;$patch$;
  if position(needle in definition)=0 then raise exception 'Autofill resume gate anchor missing';end if;
  execute replace(definition,needle,replacement);
 end if;
end $migration$;
create or replace function public.v2_collection_autofill_fail(p_run_id uuid,p_message text)
returns void language plpgsql security definer set search_path=public,pg_temp as $$
declare r public.v2_collection_autofill_runs;
begin
 select * into r from public.v2_collection_autofill_runs where id=p_run_id;
 if r.id is null then return;end if;
 perform pg_advisory_xact_lock(hashtextextended(r.workspace_id::text||'collection',0));
 perform 1 from public.v2_workspaces where id=r.workspace_id for update;
 if exists(select 1 from public.v2_collection_autofill_state where lease_id=r.id
  and source_cursors#>>'{__x_summary_retry,run_id}'=r.id::text) then return;end if;
 update public.v2_collection_autofill_runs set status='failed',message=left(p_message,1000),finished_at=now()
  where id=p_run_id and status='running';
 update public.v2_collection_autofill_state set lease_id=null,lease_until=null where lease_id=p_run_id;
end $$;
-- Existing service-role-only grants are retained by CREATE OR REPLACE.
notify pgrst,'reload schema';
commit;
