-- A consensus screenshot is a current source snapshot beside, not instead of,
-- the source's existing manual and collected news.
create or replace function public.v2_collection_unbias_weekly_stored_valid(
 p_value jsonb,p_workspace_id uuid default null,p_run_id uuid default null
) returns boolean language plpgsql immutable set search_path=public,pg_temp as $$
declare snapshot jsonb:=p_value->'_autofill_consensus'; captured timestamptz; path text; as_of date;
begin
 if jsonb_typeof(snapshot) is distinct from 'object'
 or snapshot->>'version' is distinct from 'unbias_weekly.v1'
 or snapshot->>'period' is distinct from '7d'
 or snapshot->>'sourceUrl' is distinct from 'https://consensus.cryptoquant.com/consensus/btc?period=7d'
 or jsonb_typeof(snapshot->'capturedAt') is distinct from 'string'
 or snapshot->>'capturedAt' !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]+)?(Z|[+-][0-9]{2}:[0-9]{2})$'
 or jsonb_typeof(snapshot->'bullishPercent') is distinct from 'number'
 or (snapshot->>'bullishPercent')::numeric not between 0 and 100
 or jsonb_typeof(snapshot->'asOf') is distinct from 'string'
 or length(snapshot->>'asOf') not between 1 and 80
 or jsonb_typeof(snapshot->'width') is distinct from 'number'
 or (snapshot->>'width')::integer not between 1000 and 4096
 or jsonb_typeof(snapshot->'height') is distinct from 'number'
 or (snapshot->>'height')::integer not between 200 and 4000
 or jsonb_typeof(snapshot->'sourceWidth') is distinct from 'number'
 or (snapshot->>'sourceWidth')::integer not between 1000 and 4096
 or jsonb_typeof(snapshot->'sourceHeight') is distinct from 'number'
 or (snapshot->>'sourceHeight')::integer not between 500 and 4000
 or jsonb_typeof(snapshot->'cropX') is distinct from 'number' or (snapshot->>'cropX')::integer<0
 or jsonb_typeof(snapshot->'cropY') is distinct from 'number' or (snapshot->>'cropY')::integer<0
 or (snapshot->>'cropX')::integer+(snapshot->>'width')::integer>(snapshot->>'sourceWidth')::integer
 or (snapshot->>'cropY')::integer+(snapshot->>'height')::integer>(snapshot->>'sourceHeight')::integer
 or jsonb_typeof(snapshot->'bearishReasons') is distinct from 'array'
 or jsonb_typeof(snapshot->'bullishReasons') is distinct from 'array' then return false;end if;
 if jsonb_array_length(snapshot->'bearishReasons')>30 or jsonb_array_length(snapshot->'bullishReasons')>30
 or jsonb_array_length(snapshot->'bearishReasons')+jsonb_array_length(snapshot->'bullishReasons')=0
 or exists(select 1 from jsonb_array_elements((snapshot->'bearishReasons')||(snapshot->'bullishReasons')) reason
 where jsonb_typeof(reason) is distinct from 'string' or length(reason#>>'{}') not between 1 and 1000) then return false;end if;
 captured:=(snapshot->>'capturedAt')::timestamptz;
 if captured is null or not isfinite(captured) then return false;end if;
 if snapshot->>'asOf' !~ '^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) [0-9]{1,2}$' then return false;end if;
 as_of:=to_date((snapshot->>'asOf')||' '||extract(year from captured at time zone 'UTC')::text,'Mon DD YYYY');
 if to_char(as_of,'Mon')<>split_part(snapshot->>'asOf',' ',1) or extract(day from as_of)<>split_part(snapshot->>'asOf',' ',2)::integer then return false;end if;
 if as_of>(captured at time zone 'UTC')::date+1 then as_of:=(as_of-interval '1 year')::date;end if;
 if as_of<(captured at time zone 'UTC')::date-4 or as_of>(captured at time zone 'UTC')::date+1 then return false;end if;
 path:=snapshot->>'image_path';
 return path is not null and path ~ ('^workspace-images:'||coalesce(p_workspace_id::text,'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')||'/collection-autofill/'||
 coalesce(p_run_id::text,'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')||'/[0-9a-f]{24}\.png$');
exception when others then return false;
end $$;
revoke all on function public.v2_collection_unbias_weekly_stored_valid(jsonb,uuid,uuid) from public,anon;
grant execute on function public.v2_collection_unbias_weekly_stored_valid(jsonb,uuid,uuid) to authenticated,service_role;

-- Freshness belongs to accepting a new capture, never to reading saved history.
create or replace function public.v2_collection_unbias_weekly_valid(
 p_value jsonb,p_workspace_id uuid,p_run_id uuid default null
) returns boolean language plpgsql stable set search_path=public,pg_temp as $$
declare captured timestamptz;
begin
 if p_workspace_id is null or not public.v2_collection_unbias_weekly_stored_valid(p_value,p_workspace_id,p_run_id) then return false;end if;
 captured:=(p_value#>>'{_autofill_consensus,capturedAt}')::timestamptz;
 return captured<=now()+interval '1 minute' and captured>=now()-interval '10 minutes';
exception when others then return false;
end $$;
revoke all on function public.v2_collection_unbias_weekly_valid(jsonb,uuid,uuid) from public,anon,authenticated;
grant execute on function public.v2_collection_unbias_weekly_valid(jsonb,uuid,uuid) to service_role;

-- The generated image and its provenance belong to the latest verified scan.
-- A manual editor opened before that scan still owns its notes, never the old
-- screenshot. Reuse the installed guard's locked row and exact scan capability
-- so RPC saves and direct table writes enforce the same boundary.
do $weekly_guard$
declare fn oid:=to_regprocedure('public.v2_collection_managed_snapshot_write_guard()');
 definition text; original_definition text; old_text text; new_text text;
 before_contract jsonb; after_contract jsonb; pair jsonb; changes jsonb;
begin
 if fn is null or to_regclass('public.v2_collection_clear_tokens') is null then
  raise exception 'Managed snapshot and scoped clear guards required';end if;
 select pg_get_functiondef(p.oid),jsonb_build_object('oid',p.oid,'owner',p.proowner,'acl',p.proacl,
  'security',p.prosecdef,'volatility',p.provolatile,'settings',p.proconfig,'result',p.prorettype)
 into original_definition,before_contract from pg_proc p where p.oid=fn;
 if position('unbias_weekly_write_guard_v1' in original_definition)>0 then return;end if;
 definition:=original_definition;
 changes:=jsonb_build_array(
  jsonb_build_array(' if tg_op=''UPDATE'' and old.value ? ''_autofill_snapshot''',
   ' if tg_op=''UPDATE'' and (old.value ? ''_autofill_snapshot'' or old.value ? ''_autofill_consensus'')'),
  jsonb_build_array(' current_value:=coalesce(current_value,''{}''::jsonb);',
   $replacement$ current_value:=coalesce(current_value,'{}'::jsonb);
 -- Only a native clear's exact transaction/actor/target/value capability may
 -- discard this marker. Omission, null or client flags never mean clear.
 if current_value ? '_autofill_consensus' and exists(
  select 1 from public.v2_collection_clear_tokens token
  where token.transaction_id=txid_current() and token.workspace_id=ws
   and token.session_id=new.session_id and token.field_id=new.field_id
   and token.actor_id=auth.uid() and token.expected_value=new.value
 ) then return new;end if;$replacement$),
  jsonb_build_array(' if not (current_value ? ''_autofill_snapshot'') and not (new.value ? ''_autofill_snapshot'') then return new;end if;',
   $replacement$ -- unbias_weekly_write_guard_v1
 if transaction_run is not null and trusted_run is null
 and (current_value ? '_autofill_consensus' or new.value ? '_autofill_consensus') then
  raise exception 'Weekly consensus token does not match target' using errcode='23514';end if;
 if trusted_run is not null and new.value ? '_autofill_consensus'
 and new.value->'_autofill_consensus' is distinct from current_value->'_autofill_consensus' then
  if not public.v2_collection_unbias_weekly_valid(new.value,ws,trusted_run) then
   raise exception 'Invalid generated weekly consensus snapshot or image scope' using errcode='23514';end if;
 elsif current_value ? '_autofill_consensus' then
  -- Unrelated scans may carry the unchanged old snapshot. Do not bind an old
  -- image to their new run; keep its original provenance alongside it.
  new.value:=(new.value-'_autofill_consensus'-'_autofill')
   ||jsonb_build_object('_autofill_consensus',current_value->'_autofill_consensus');
  if current_value ? '_autofill' then
   new.value:=new.value||jsonb_build_object('_autofill',current_value->'_autofill');end if;
 else
  new.value:=new.value-'_autofill_consensus';
  if new.value#>>'{_autofill,kind}'='weekly_consensus' then
   new.value:=new.value-'_autofill';
   if current_value ? '_autofill' then
    new.value:=new.value||jsonb_build_object('_autofill',current_value->'_autofill');end if;
  end if;
 end if;
 if not (current_value ? '_autofill_snapshot') and not (new.value ? '_autofill_snapshot') then return new;end if;$replacement$)
 );
 for pair in select value from jsonb_array_elements(changes) loop
  old_text:=pair->>0;new_text:=pair->>1;
  if (length(definition)-length(replace(definition,old_text,'')))/length(old_text)<>1 then
   raise exception 'Unexpected weekly consensus write guard anchor';end if;
  definition:=replace(definition,old_text,new_text);
 end loop;
 execute definition;
 select jsonb_build_object('oid',p.oid,'owner',p.proowner,'acl',p.proacl,
  'security',p.prosecdef,'volatility',p.provolatile,'settings',p.proconfig,'result',p.prorettype)
 into after_contract from pg_proc p where p.oid=fn;
 if before_contract is distinct from after_contract or pg_get_functiondef(fn) is distinct from definition then
  raise exception 'Weekly consensus guard altered existing RPC contract';end if;
end $weekly_guard$;

-- Latest-filled and historical snapshots use this presence predicate before
-- serializing a source. Its image must survive even when it has no news rows.
do $weekly_presence$
declare fn oid:=to_regprocedure('public.v2_collection_value_filled(text,jsonb)');
 definition text; original_definition text; old_text text; new_text text;
 before_contract jsonb; after_contract jsonb;
begin
 if fn is null then raise exception 'Collection presence predicate required';end if;
 select pg_get_functiondef(p.oid),jsonb_build_object('oid',p.oid,'owner',p.proowner,'acl',p.proacl,
  'security',p.prosecdef,'volatility',p.provolatile,'settings',p.proconfig,'result',p.prorettype)
 into original_definition,before_contract from pg_proc p where p.oid=fn;
 if position('unbias_weekly_presence_v1' in original_definition)>0 then return;end if;
 definition:=original_definition;
 old_text:='  if p_value is null or p_value=''null''::jsonb then return false; end if;';
 new_text:=old_text||$replacement$
  -- unbias_weekly_presence_v1: stored source images do not expire in history.
  if p_type in ('news_table','news_source_table','news_group','unbiased_news')
    and public.v2_collection_unbias_weekly_stored_valid(p_value) then return true; end if;$replacement$;
 if (length(definition)-length(replace(definition,old_text,'')))/length(old_text)<>1 then
  raise exception 'Unexpected weekly consensus presence anchor';end if;
 definition:=replace(definition,old_text,new_text);
 execute definition;
 select jsonb_build_object('oid',p.oid,'owner',p.proowner,'acl',p.proacl,
  'security',p.prosecdef,'volatility',p.provolatile,'settings',p.proconfig,'result',p.prorettype)
 into after_contract from pg_proc p where p.oid=fn;
 if before_contract is distinct from after_contract or pg_get_functiondef(fn) is distinct from definition then
  raise exception 'Weekly consensus presence altered existing RPC contract';end if;
end $weekly_presence$;

do $$
declare definition text; needle text; matches integer:=0;
begin
 select pg_get_functiondef('public.v2_collection_autofill_apply(uuid,jsonb,jsonb,timestamptz)'::regprocedure) into definition;
 if position('unbias_weekly_snapshot_apply_v1' in definition)>0 then return;end if;
 foreach needle in array array[
  $n$ if f.field_type in ('news_table','news_source_table','unbiased_news') and run.sheet_key<>'data' then$n$,
  $n$ if f.field_type in ('news_table','news_source_table','news_group','unbiased_news') and run.sheet_key<>'data' then$n$
 ] loop
  if position(needle in definition)=0 then continue;end if;
  matches:=matches+1;
  definition:=replace(definition,needle,$replacement$
 -- unbias_weekly_snapshot_apply_v1
 if p_payload#>>'{value,_autofill_consensus,version}'='unbias_weekly.v1' then
  if f.field_type not in ('news_table','news_source_table','unbiased_news','news_group')
  or not public.v2_collection_unbias_weekly_valid(p_payload->'value',run.workspace_id,run.id) then
   raise exception 'Invalid weekly consensus snapshot';end if;
  next_value:=current_value||jsonb_build_object('_autofill_consensus',p_payload#>'{value,_autofill_consensus}',
   '_autofill',p_payload#>'{value,_autofill}','not_relevant',false,'completion',null);
  insert into public.v2_collection_values(session_id,field_id,value,updated_at) values(sid,fid,next_value,now())
  on conflict(session_id,field_id) do update set value=excluded.value,updated_at=excluded.updated_at;
  amount:=1;
 elsif$replacement$||substr(needle,4));
 end loop;
 if matches<>1 then raise exception 'Weekly consensus apply target changed';end if;
 execute definition;
end $$;
