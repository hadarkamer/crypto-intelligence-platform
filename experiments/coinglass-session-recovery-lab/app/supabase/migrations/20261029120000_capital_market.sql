-- Fixed public market observations and separately saved minute notes. Existing
-- template versions, worksheets, values and automation configuration stay intact.
begin;

do $types$
declare definition text; expression text;
begin
 select pg_get_constraintdef(oid) into definition from pg_constraint
 where conrelid='public.v2_template_fields'::regclass and conname='v2_template_fields_field_type_check';
 if definition is null then raise exception 'Missing template field type constraint';end if;
 if position('capital_market' in definition)=0 then
  expression:=substring(definition from '^CHECK [(](.*)[)]$');
  if expression is null then raise exception 'Unexpected template field type constraint';end if;
  alter table public.v2_template_fields drop constraint v2_template_fields_field_type_check;
  execute format('alter table public.v2_template_fields add constraint v2_template_fields_field_type_check check ((%s) or field_type=%L)',expression,'capital_market');
 end if;
end $types$;

create or replace function public.v2_capital_market_snapshot_valid(p_snapshot jsonb)
returns boolean language plpgsql immutable set search_path=public,pg_temp as $valid$
declare row_value jsonb; quote jsonb; correlation jsonb; row_id text; symbol text; unit_name text;
 proxy text; quote_url text; correlation_url text; scanned timestamptz; fetched timestamptz;
 observed timestamptz; ordinal integer:=0; key_name text;
begin
 if jsonb_typeof(p_snapshot) is distinct from 'object' or p_snapshot->'version' is distinct from '1'::jsonb
 or jsonb_typeof(p_snapshot->'rows') is distinct from 'array'
 or jsonb_array_length(p_snapshot->'rows')<>3
 or jsonb_typeof(p_snapshot->'scanned_at') is distinct from 'string'
 or p_snapshot->>'scanned_at' !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]+)?(Z|[+-][0-9]{2}:[0-9]{2})$'
 or exists(select 1 from jsonb_object_keys(p_snapshot) k where k not in ('version','rows','scanned_at')) then return false;end if;
 scanned:=(p_snapshot->>'scanned_at')::timestamptz;
 if not isfinite(scanned) then return false;end if;
 for row_value in select value from jsonb_array_elements(p_snapshot->'rows') loop
  ordinal:=ordinal+1;
  row_id:=(array['nasdaq','sp500','oil'])[ordinal];
  symbol:=(array['^NDX','^GSPC','CL=F'])[ordinal];
  unit_name:=case when row_id='oil' then 'usd_per_barrel' else 'points' end;
  proxy:=(array['QQQ','SPY','WTI'])[ordinal];
  quote_url:=(array['https://finance.yahoo.com/quote/%5ENDX/','https://finance.yahoo.com/quote/%5EGSPC/','https://finance.yahoo.com/quote/CL%3DF/'])[ordinal];
  correlation_url:=case when row_id='oil' then 'https://newhedge.io/bitcoin/crude-oil-correlation'
    else 'https://newhedge.io/bitcoin/bitcoin-correlations' end;
  if jsonb_typeof(row_value) is distinct from 'object' or row_value->>'id' is distinct from row_id
  or not (row_value ? 'quote' and row_value ? 'correlation')
  or exists(select 1 from jsonb_object_keys(row_value) k where k not in ('id','quote','correlation','quote_error','correlation_error')) then return false;end if;
  foreach key_name in array array['quote_error','correlation_error'] loop
   if row_value ? key_name and (jsonb_typeof(row_value->key_name) is distinct from 'string'
    or length(row_value->>key_name) not between 1 and 500) then return false;end if;
  end loop;
  quote:=row_value->'quote';correlation:=row_value->'correlation';
  if quote is distinct from 'null'::jsonb then
   if jsonb_typeof(quote) is distinct from 'object'
   or quote->>'symbol' is distinct from symbol or quote->>'unit' is distinct from unit_name
   or quote->>'currency' is distinct from 'USD' or quote->>'source_name' is distinct from 'Yahoo Finance'
   or quote->>'source_url' is distinct from quote_url or quote->>'delay' is distinct from 'may_be_delayed'
   or not coalesce(quote->>'market_status'=any(array['open','closed','unknown']),false)
   or jsonb_typeof(quote->'exchange_timezone') is distinct from 'string'
   or length(quote->>'exchange_timezone') not between 1 and 100
   or not (quote ? 'contract_label')
   or (quote->'contract_label'<>'null'::jsonb and (jsonb_typeof(quote->'contract_label') is distinct from 'string' or length(quote->>'contract_label')>120))
   or (row_id<>'oil' and quote->'contract_label'<>'null'::jsonb)
   or jsonb_typeof(quote->'price') is distinct from 'number'
   or jsonb_typeof(quote->'previous_close') is distinct from 'number'
   or jsonb_typeof(quote->'change_percent') is distinct from 'number'
   or (quote->>'price')::numeric<=0 or (quote->>'previous_close')::numeric<=0
   or abs((quote->>'change_percent')::numeric-
    (((quote->>'price')::numeric-(quote->>'previous_close')::numeric)/(quote->>'previous_close')::numeric)*100)>0.000001
   or exists(select 1 from jsonb_object_keys(quote) k where k not in
    ('price','previous_close','change_percent','currency','symbol','unit','contract_label','source_name','source_url','as_of','fetched_at','exchange_timezone','market_status','delay')) then return false;end if;
   foreach key_name in array array['as_of','fetched_at'] loop
    if jsonb_typeof(quote->key_name) is distinct from 'string'
    or quote->>key_name !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]+)?(Z|[+-][0-9]{2}:[0-9]{2})$' then return false;end if;
   end loop;
   observed:=(quote->>'as_of')::timestamptz;fetched:=(quote->>'fetched_at')::timestamptz;
   if not isfinite(observed) or not isfinite(fetched) or observed>fetched+interval '5 minutes'
   or fetched>scanned+interval '5 minutes' then return false;end if;
  end if;
  if correlation is distinct from 'null'::jsonb then
   if jsonb_typeof(correlation) is distinct from 'object'
   or jsonb_typeof(correlation->'coefficient') is distinct from 'number'
   or (correlation->>'coefficient')::numeric not between -1 and 1
   or correlation->>'proxy' is distinct from proxy or correlation->>'window' is distinct from '30d'
   or correlation->>'source_name' is distinct from 'Newhedge' or correlation->>'source_url' is distinct from correlation_url
   or correlation->'as_of' is distinct from 'null'::jsonb
   or jsonb_typeof(correlation->'fetched_at') is distinct from 'string'
   or correlation->>'fetched_at' !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]+)?(Z|[+-][0-9]{2}:[0-9]{2})$'
   or exists(select 1 from jsonb_object_keys(correlation) k where k not in
    ('coefficient','proxy','window','source_name','source_url','as_of','fetched_at')) then return false;end if;
   fetched:=(correlation->>'fetched_at')::timestamptz;
   if not isfinite(fetched) or fetched>scanned+interval '5 minutes' then return false;end if;
  end if;
 end loop;
 return true;
exception when others then return false;
end $valid$;
revoke all on function public.v2_capital_market_snapshot_valid(jsonb) from public,anon;
grant execute on function public.v2_capital_market_snapshot_valid(jsonb) to authenticated,service_role;

create or replace function public.v2_capital_market_value_filled(p_value jsonb)
returns boolean language plpgsql immutable set search_path=public,pg_temp as $filled$
declare note jsonb; content text; row_id text;
begin
 if jsonb_typeof(p_value) is distinct from 'object' then return false;end if;
 if public.v2_capital_market_snapshot_valid(p_value->'capital_market') and exists(
  select 1 from jsonb_array_elements(p_value#>'{capital_market,rows}') r
  where r->'quote'<>'null'::jsonb or r->'correlation'<>'null'::jsonb) then return true;end if;
 if jsonb_typeof(p_value->'capital_market_notes')='object' then
  foreach row_id in array array['nasdaq','sp500','oil'] loop
   note:=p_value#>array['capital_market_notes',row_id];
   if jsonb_typeof(note)='object' and jsonb_typeof(note->'text')='string' then
    content:=regexp_replace(note->>'text',U&'[[:space:]\00A0\200B-\200D\FEFF]','','g');
    if content<>'' then return true;end if;
   end if;
  end loop;
 end if;
 return false;
exception when others then return false;
end $filled$;
revoke all on function public.v2_capital_market_value_filled(jsonb) from public,anon;
grant execute on function public.v2_capital_market_value_filled(jsonb) to authenticated,service_role;

-- Presence/history accepts any verified cell or manual note. Completion is the
-- stronger worksheet contract: all three fixed rows have both automatic cells;
-- the optional minute notes do not substitute for a missing sourced observation.
create or replace function public.v2_capital_market_value_complete(p_value jsonb)
returns boolean language plpgsql immutable set search_path=public,pg_temp as $complete$
begin
 if not public.v2_capital_market_snapshot_valid(p_value->'capital_market') then return false;end if;
 return not exists(select 1 from jsonb_array_elements(p_value#>'{capital_market,rows}') r
  where r->'quote'='null'::jsonb or r->'correlation'='null'::jsonb);
exception when others then return false;
end $complete$;
revoke all on function public.v2_capital_market_value_complete(jsonb) from public,anon;
grant execute on function public.v2_capital_market_value_complete(jsonb) to authenticated,service_role;

-- Extend the actual import mapper/presence/completion routines rather than only
-- admitting a new table-constraint value. Keep their existing security contracts.
do $routines$
declare signature text; fn oid; definition text; before_contract jsonb; after_contract jsonb;
 needle text; replacement text;
begin
 foreach signature in array array['public.v2_legacy_field_type(text,text)',
  'public.v2_field_complete(text,jsonb)','public.v2_collection_value_filled(text,jsonb)'] loop
  fn:=to_regprocedure(signature);
  if fn is null then raise exception 'Required field routine missing: %',signature;end if;
  select pg_get_functiondef(fn),jsonb_build_object('oid',p.oid,'owner',p.proowner,'acl',p.proacl,
   'security',p.prosecdef,'settings',p.proconfig,'result',p.prorettype,'volatility',p.provolatile)
  into definition,before_contract from pg_proc p where p.oid=fn;
  if position('capital_market_field_contract_v1' in definition)>0 then
   -- Also repair an earlier local preflight installation of this new migration.
   if signature like '%v2_field_complete(%' and position('v2_capital_market_value_complete(p_value)' in definition)=0 then
    needle:='public.v2_capital_market_value_filled(p_value)';
    replacement:='public.v2_capital_market_value_complete(p_value)';
   else continue;end if;
  elsif signature like '%v2_legacy_field_type(%' then
   needle:='begin';
   replacement:=needle||E'\n  -- capital_market_field_contract_v1\n  if lower(btrim(coalesce(p_question_type,'''')))=''capital_market'' then return ''capital_market'';end if;';
  elsif signature like '%v2_field_complete(%' then
   needle:='  if v->>''completion''=''na'' then return true; end if;';
   replacement:=needle||E'\n  -- capital_market_field_contract_v1\n  if p_type=''capital_market'' then return public.v2_capital_market_value_complete(p_value);end if;';
  else
   needle:='  if p_value is null or p_value=''null''::jsonb then return false; end if;';
   replacement:=needle||E'\n  -- capital_market_field_contract_v1\n  if p_type=''capital_market'' then return public.v2_capital_market_value_filled(p_value) or coalesce(p_value->>''completion''=''na'',false);end if;';
  end if;
  if (length(definition)-length(replace(definition,needle,'')))/length(needle)<>1 then
   raise exception 'Unexpected capital market field routine anchor: %',signature;end if;
  definition:=replace(definition,needle,replacement);execute definition;
  select jsonb_build_object('oid',p.oid,'owner',p.proowner,'acl',p.proacl,'security',p.prosecdef,
   'settings',p.proconfig,'result',p.prorettype,'volatility',p.provolatile) into after_contract from pg_proc p where p.oid=fn;
  if before_contract is distinct from after_contract or pg_get_functiondef(fn) is distinct from definition then
   raise exception 'Capital market field contract changed RPC identity/security: %',signature;end if;
 end loop;
end $routines$;

-- Reuse the existing locked latest-value lookup, exact running-run capability,
-- configuration/lease checks and scoped-clear capability. The live table remains
-- read-only to ordinary clients and receives this guarded value through its mirror.
do $guard$
declare fn oid:=to_regprocedure('public.v2_collection_managed_snapshot_write_guard()');
 definition text; needle text; replacement text; before_contract jsonb; after_contract jsonb;
begin
 if fn is null or to_regclass('public.v2_collection_clear_tokens') is null then
  raise exception 'Managed snapshot and scoped clear guards required';end if;
 select pg_get_functiondef(fn),jsonb_build_object('oid',p.oid,'owner',p.proowner,'acl',p.proacl,
  'security',p.prosecdef,'settings',p.proconfig,'result',p.prorettype,'volatility',p.provolatile)
 into definition,before_contract from pg_proc p where p.oid=fn;
 if position('capital_market_write_guard_v1' in definition)>0 then return;end if;
 needle:='(old.value ? ''_autofill_snapshot'' or old.value ? ''_autofill_consensus'')';
 if (length(definition)-length(replace(definition,needle,'')))/length(needle)<>1 then
  raise exception 'Unexpected capital market identity guard anchor';end if;
 definition:=replace(definition,needle,'(old.value ? ''_autofill_snapshot'' or old.value ? ''_autofill_consensus'' or old.value ? ''capital_market'')');
 needle:=' if not (current_value ? ''_autofill_snapshot'') and not (new.value ? ''_autofill_snapshot'') then return new;end if;';
 replacement:=$replacement$
 -- capital_market_write_guard_v1
 if transaction_run is not null and trusted_run is null
 and (current_value ? 'capital_market' or new.value ? 'capital_market') then
  raise exception 'Capital market automation token does not match target' using errcode='23514';end if;
 if trusted_run is not null and new.value->'capital_market' is distinct from current_value->'capital_market' then
  if not exists(select 1 from public.v2_template_fields where id=new.field_id and field_type='capital_market')
  or not public.v2_capital_market_snapshot_valid(new.value->'capital_market') then
   raise exception 'Invalid generated capital market snapshot' using errcode='23514';end if;
 elsif current_value ? 'capital_market' then
  new.value:=(new.value-'capital_market')||jsonb_build_object('capital_market',current_value->'capital_market');
 else
  new.value:=new.value-'capital_market';
 end if;
$replacement$||needle;
 if (length(definition)-length(replace(definition,needle,'')))/length(needle)<>1 then
  raise exception 'Unexpected capital market generated-data guard anchor';end if;
 definition:=replace(definition,needle,replacement);execute definition;
 select jsonb_build_object('oid',p.oid,'owner',p.proowner,'acl',p.proacl,'security',p.prosecdef,
  'settings',p.proconfig,'result',p.prorettype,'volatility',p.provolatile) into after_contract from pg_proc p where p.oid=fn;
 if before_contract is distinct from after_contract or pg_get_functiondef(fn) is distinct from definition then
  raise exception 'Capital market guard changed existing RPC security contract';end if;
end $guard$;

create or replace function public.v2_collection_capital_market_apply(
 p_run_id uuid,p_snapshot jsonb,p_cursors jsonb,p_scan_started_at timestamptz
) returns jsonb language plpgsql security definer set search_path=public,pg_temp as $apply$
declare run public.v2_collection_autofill_runs; captured timestamptz; failed_cells integer; payload jsonb;
begin
 select * into run from public.v2_collection_autofill_runs where id=p_run_id;
 if run.id is null then raise exception 'Unknown capital market run' using errcode='23514';end if;
 if not exists(select 1 from public.v2_template_fields f
  join public.v2_template_sections sec on sec.id=f.section_id
  join public.v2_collection_sessions s on s.id=run.session_id and s.template_id=sec.template_id
  join public.v2_templates t on t.id=s.template_id and t.workspace_id=s.workspace_id and t.kind='collection'
  where f.id=run.field_id and f.field_type='capital_market' and f.active and sec.active
   and s.workspace_id=run.workspace_id and s.deleted_at is null and s.status<>'archived'
   and public.v2_collection_sheet_key(to_jsonb(sec))=run.sheet_key
   and public.v2_collection_field_logical_key(to_jsonb(f))=run.logical_key)
 or not public.v2_capital_market_snapshot_valid(p_snapshot) then
  raise exception 'Invalid capital market target or snapshot' using errcode='23514';end if;
 captured:=(p_snapshot->>'scanned_at')::timestamptz;
 if p_scan_started_at is null or not isfinite(p_scan_started_at)
 or captured>now()+interval '5 minutes' or captured<p_scan_started_at-interval '1 minute'
 or p_scan_started_at>now()+interval '5 minutes'
 or jsonb_typeof(p_cursors) is distinct from 'object' then
  raise exception 'Invalid capital market scan time or cursors' using errcode='23514';end if;
 -- Append merges only this automatic property into the current locked value.
 -- All current minute notes, sibling notes, images and unknown metadata survive.
 payload:=jsonb_build_object('write_mode','append','value',jsonb_build_object('capital_market',p_snapshot));
 select coalesce(sum((case when r ? 'quote_error' then 1 else 0 end)+(case when r ? 'correlation_error' then 1 else 0 end)),0)
 into failed_cells from jsonb_array_elements(p_snapshot->'rows') r;
 if failed_cells>0 then payload:=payload||jsonb_build_object('source_failure_message',
  format('נתוני השוק עודכנו חלקית; %s נתונים לא אומתו. נתונים קודמים נשמרו עם מועדי המקור שלהם.',failed_cells));end if;
 return public.v2_collection_autofill_apply(p_run_id,payload,p_cursors,p_scan_started_at);
end $apply$;
revoke all on function public.v2_collection_capital_market_apply(uuid,jsonb,jsonb,timestamptz) from public,anon,authenticated;
grant execute on function public.v2_collection_capital_market_apply(uuid,jsonb,jsonb,timestamptz) to service_role;

-- Remove the earlier four-argument overload: a default on the fifth argument
-- preserves pre-clear callers without leaving a route around the generation check.
drop function if exists public.v2_save_capital_market_note(uuid,uuid,text,text);
create or replace function public.v2_save_capital_market_note(
 p_session_id uuid,p_field_id uuid,p_row_id text,p_text text,p_clear_generation text default null
) returns jsonb language plpgsql security definer set search_path=public,pg_temp as $note$
declare session_row public.v2_collection_sessions; field_row public.v2_template_fields;
 workspace uuid; template uuid; sheet text; logical text; current_value jsonb; notes jsonb; note jsonb;
 receipt public.v2_collection_values; stamp timestamptz; actor uuid:=auth.uid();
begin
 if actor is null or p_row_id is null or p_row_id not in ('nasdaq','sp500','oil')
 or p_text is null or length(p_text)>2000 then
  raise exception 'Valid capital market row and short note required' using errcode='22023';end if;
 select * into session_row from public.v2_collection_sessions where id=p_session_id;
 if session_row.id is null or not coalesce(public.v2_can_edit_workspace(session_row.workspace_id),false) then
  raise exception 'Collection edit permission required' using errcode='42501';end if;
 workspace:=session_row.workspace_id;template:=session_row.template_id;
 perform pg_advisory_xact_lock(hashtextextended(workspace::text||'collection',0));
 perform 1 from public.v2_workspaces where id=workspace for update;
 select * into session_row from public.v2_collection_sessions where id=p_session_id for update;
 if session_row.id is null or session_row.workspace_id is distinct from workspace
 or session_row.template_id is distinct from template or session_row.deleted_at is not null or session_row.status='archived'
 or not exists(select 1 from public.v2_templates where id=template and workspace_id=workspace and kind='collection') then
  raise exception 'Active collection required' using errcode='23514';end if;
 if not coalesce(public.v2_can_edit_workspace(workspace),false) then
  raise exception 'Collection edit permission required' using errcode='42501';end if;
 select f.* into field_row from public.v2_template_fields f
 join public.v2_template_sections sec on sec.id=f.section_id
 where f.id=p_field_id and sec.template_id=template and f.active and sec.active and f.field_type='capital_market';
 if field_row.id is null then raise exception 'Active capital market field in this worksheet required' using errcode='23514';end if;
 select public.v2_collection_sheet_key(to_jsonb(sec)),public.v2_collection_field_logical_key(to_jsonb(field_row))
 into sheet,logical from public.v2_template_sections sec where sec.id=field_row.section_id;
 if sheet='data' then
  select value into current_value from public.v2_collection_live_values where workspace_id=workspace and logical_key=logical for update;
 end if;
 if current_value is null then
  select value into current_value from public.v2_collection_values where session_id=p_session_id and field_id=p_field_id for update;
 end if;
 current_value:=coalesce(current_value,'{}'::jsonb);
 -- Bind the queued draft to the generation the editor displayed. Re-reading
 -- the latest value must never upgrade an old draft across a native clear.
 if current_value->>'_clear_generation' is distinct from p_clear_generation then
  raise exception 'התיבה נוקתה; יש לרענן לפני שמירת ההערה.' using errcode='23514';end if;
 notes:=case when jsonb_typeof(current_value->'capital_market_notes')='object' then current_value->'capital_market_notes' else '{}'::jsonb end;
 note:=coalesce(notes->p_row_id,'{}'::jsonb);
 if jsonb_typeof(note) is distinct from 'object' then note:='{}'::jsonb;end if;
 stamp:=clock_timestamp();
 note:=note||jsonb_build_object('text',p_text,'updated_at',stamp,'updated_by',actor);
 notes:=notes||jsonb_build_object(p_row_id,note);
 perform public.v2_save_collection_value(p_session_id,p_field_id,current_value||jsonb_build_object('capital_market_notes',notes));
 select effective.* into receipt from public.v2_collection_current_values(p_session_id) effective where effective.field_id=p_field_id;
 if receipt.id is null then raise exception 'Saved capital market note receipt missing' using errcode='23514';end if;
 return jsonb_build_object('value',receipt.value,'revision',receipt.updated_at,
  'note',receipt.value#>array['capital_market_notes',p_row_id]);
end $note$;
revoke all on function public.v2_save_capital_market_note(uuid,uuid,text,text,text) from public,anon,service_role;
grant execute on function public.v2_save_capital_market_note(uuid,uuid,text,text,text) to authenticated;

notify pgrst,'reload schema';
commit;
