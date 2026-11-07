-- Durable, workspace-isolated expiry observations. Actual field saves and
-- their history are atomic; removing an expired active row never deletes history.
begin;

create table if not exists public.v2_collection_expiry_cohorts (
 id uuid primary key default gen_random_uuid(),
 workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
 logical_key text not null,
 expiry_date date not null,
 exchanges jsonb not null check(jsonb_typeof(exchanges)='array' and jsonb_array_length(exchanges)=3),
 origin text not null check(origin in ('verified','manual')),
 selected boolean not null default false,
 captured_at timestamptz not null,
 captured_after_expiry boolean not null default false,
 source_url text,
 first_captured_at timestamptz not null default now(),
 expired_at timestamptz,
 unique(workspace_id,logical_key,expiry_date)
);
create index if not exists v2_expiry_cohorts_history_idx
 on public.v2_collection_expiry_cohorts(workspace_id,logical_key,expiry_date desc) where selected;
create table if not exists public.v2_collection_expiry_observations (
 id uuid primary key default gen_random_uuid(),
 workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
 logical_key text not null,
 expiry_date date not null,
 exchanges jsonb not null check(jsonb_typeof(exchanges)='array' and jsonb_array_length(exchanges)=3),
 origin text not null check(origin in ('verified','manual')),
 selected boolean not null,
 captured_at timestamptz not null,
 captured_after_expiry boolean not null,
 source_url text,
 fingerprint text not null,
 created_at timestamptz not null default now(),
 unique(workspace_id,logical_key,expiry_date,captured_at,fingerprint)
);
create table if not exists public.v2_collection_expiry_market_days (
 day date primary key,
 status text not null default 'pending' check(status in ('pending','complete')),
 open numeric,high numeric,low numeric,close numeric,volume_btc numeric,
 opened_at timestamptz,closed_at timestamptz,captured_at timestamptz,source_url text,
 lease_id uuid,lease_until timestamptz,
 next_attempt_at timestamptz not null default now(),
 attempts integer not null default 0,
 check(status='pending' or (open is not null and high is not null and low is not null and close is not null and
   volume_btc is not null and opened_at is not null and closed_at is not null and captured_at is not null and source_url is not null and
   open>0 and high>=greatest(open,close,low) and
   low>0 and low<=least(open,close) and close>0 and volume_btc>=0 and
   opened_at=(day::timestamp at time zone 'UTC') and
   closed_at=((day+1)::timestamp at time zone 'UTC') and captured_at>=closed_at))
);
create table if not exists public.v2_collection_expiry_maintenance_tokens (
 transaction_id bigint not null,
 scope text not null check(scope in ('live','value')),
 value_id uuid not null,
 primary key(transaction_id,scope,value_id)
);
alter table public.v2_collection_expiry_cohorts enable row level security;
alter table public.v2_collection_expiry_observations enable row level security;
alter table public.v2_collection_expiry_market_days enable row level security;
alter table public.v2_collection_expiry_maintenance_tokens enable row level security;
drop policy if exists v2_expiry_cohorts_read on public.v2_collection_expiry_cohorts;
create policy v2_expiry_cohorts_read on public.v2_collection_expiry_cohorts for select to authenticated
 using(public.v2_is_workspace_member(workspace_id));
drop policy if exists v2_expiry_observations_read on public.v2_collection_expiry_observations;
create policy v2_expiry_observations_read on public.v2_collection_expiry_observations for select to authenticated
 using(public.v2_is_workspace_member(workspace_id));
revoke all on public.v2_collection_expiry_cohorts,public.v2_collection_expiry_observations,
 public.v2_collection_expiry_market_days,public.v2_collection_expiry_maintenance_tokens from public,anon,authenticated;
grant select on public.v2_collection_expiry_cohorts,public.v2_collection_expiry_observations to authenticated;
grant all on public.v2_collection_expiry_cohorts,public.v2_collection_expiry_observations,
 public.v2_collection_expiry_market_days to service_role;

create or replace function public.v2_expiry_valid_date(p_value text)
returns date language plpgsql immutable set search_path=public,pg_temp as $$
declare result date;
begin
 if p_value is null or p_value !~ '^\d{4}-\d{2}-\d{2}$' then return null;end if;
 begin result:=p_value::date;exception when datetime_field_overflow or invalid_datetime_format then return null;end;
 if to_char(result,'YYYY-MM-DD')<>p_value then return null;end if;
 return result;
end $$;
create or replace function public.v2_expiry_decimal(p_value text,p_price boolean default false)
returns numeric language plpgsql immutable set search_path=public,pg_temp as $$
declare text_value text; multiplier numeric:=1; result numeric;
begin
 if p_value is null or length(p_value)>64 then return null;end if;
 text_value:=upper(btrim(p_value));
 text_value:=regexp_replace(text_value,'^[$]|[$]$|\s*USD$','','g');
 text_value:=btrim(text_value);
 if right(text_value,1) in ('K','M','B') then
 multiplier:=case right(text_value,1) when 'K' then 1000 when 'M' then 1000000 else 1000000000 end;
 text_value:=left(text_value,length(text_value)-1);
 end if;
 if position(',' in text_value)>0 then
 if text_value !~ '^\d{1,3}(,\d{3})+(\.\d+)?$' then return null;end if;
 text_value:=replace(text_value,',','');
 end if;
 if text_value !~ '^(\d+(\.\d+)?|\.\d+)$' then return null;end if;
 result:=text_value::numeric*multiplier;
 if result<0 or result>10000000000000000 or (p_price and (result<=0 or result>1000000000)) then return null;end if;
 return result;
end $$;

-- Private constructor. Verified metadata is trusted only when the existing
-- capability-based origin trigger has marked the actual write as ChatGPT.
create or replace function public.v2_expiry_record_value(
 p_workspace_id uuid,p_logical_key text,p_value jsonb,p_updated_origin text,p_updated_at timestamptz)
returns void language plpgsql security definer set search_path=public,pg_temp as $$
declare rows_json jsonb;row_json jsonb;cell jsonb;exchanges_json jsonb;labels text[]:=array['דריביט','בייננס','בייביט'];
 label text;idx integer;date_value date;stamp timestamptz;source text;origin_value text;chosen boolean;late boolean;
 price numeric;amount numeric;verified boolean;fingerprint_value text;
begin
 if jsonb_typeof(p_value->'dates') is distinct from 'array' or jsonb_array_length(p_value->'dates')>366 then return;end if;
 verified:=p_updated_origin='chatgpt' and p_value#>>'{_autofill,kind}'='expiry';
 stamp:=p_updated_at;
 if verified then
  begin stamp:=(p_value#>>'{_autofill,captured_at}')::timestamptz;
  exception when others then stamp:=null;end;
  source:=p_value#>>'{_autofill,source_url}';
  rows_json:=coalesce(p_value#>'{_autofill,expiry_source_rows}',p_value#>'{_autofill,expiry_rows}');
  if stamp is null or stamp>now()+interval '5 minutes' or source is null or
   source !~ '^https://(www\.)?coinglass\.com/pro/options/max-pain/?$' or jsonb_typeof(rows_json) is distinct from 'array' then
   verified:=false;
  end if;
 end if;
 if not verified then rows_json:=p_value->'dates';stamp:=p_updated_at;source:=null;end if;
 origin_value:=case when verified then 'verified' else 'manual' end;
 if stamp is null or stamp>now()+interval '5 minutes' or jsonb_typeof(rows_json) is distinct from 'array' or
   jsonb_array_length(rows_json)>366 then return;end if;
 for row_json in select * from jsonb_array_elements(rows_json) loop
  date_value:=public.v2_expiry_valid_date(row_json->>'date');
  if date_value is null or jsonb_typeof(row_json->'exchanges') is distinct from 'array' then continue;end if;
  late:=stamp>=((date_value+1)::timestamp at time zone 'UTC');
  -- Late legacy observations remain explicitly marked, but never replace an
  -- existing pre-expiry cohort or count as a prospective statistical sample.
  chosen:=exists(select 1 from jsonb_array_elements(p_value->'dates') d where d->>'date'=date_value::text);
  exchanges_json:='[]';
  for idx in 1..3 loop
   label:=labels[idx];
   select e into cell from jsonb_array_elements(row_json->'exchanges') e where e->>'exchange'=label limit 1;
   if cell is null and not verified then cell:=row_json->'exchanges'->(idx-1);end if;
   price:=public.v2_expiry_decimal(cell->>'price',true);
   amount:=public.v2_expiry_decimal(case when verified then cell->>'amount_usd' else cell->>'amount' end,false);
   exchanges_json:=exchanges_json||jsonb_build_array(jsonb_build_object('exchange',label,'price',price,'amount_usd',amount));
  end loop;
  fingerprint_value:=md5(exchanges_json::text||origin_value||chosen::text||coalesce(source,''));
  insert into public.v2_collection_expiry_observations(workspace_id,logical_key,expiry_date,exchanges,origin,selected,
   captured_at,captured_after_expiry,source_url,fingerprint)
  values(p_workspace_id,p_logical_key,date_value,exchanges_json,origin_value,chosen,stamp,late,source,fingerprint_value)
  on conflict do nothing;
  insert into public.v2_collection_expiry_cohorts(workspace_id,logical_key,expiry_date,exchanges,origin,selected,
   captured_at,captured_after_expiry,source_url,first_captured_at)
  values(p_workspace_id,p_logical_key,date_value,exchanges_json,origin_value,chosen,stamp,late,source,stamp)
  on conflict(workspace_id,logical_key,expiry_date) do update set
   exchanges=case when not excluded.captured_after_expiry and (excluded.captured_at>=v2_collection_expiry_cohorts.captured_at or v2_collection_expiry_cohorts.captured_after_expiry)
     then excluded.exchanges else v2_collection_expiry_cohorts.exchanges end,
   origin=case when not excluded.captured_after_expiry and (excluded.captured_at>=v2_collection_expiry_cohorts.captured_at or v2_collection_expiry_cohorts.captured_after_expiry)
     then excluded.origin else v2_collection_expiry_cohorts.origin end,
   captured_at=case when not excluded.captured_after_expiry and (excluded.captured_at>=v2_collection_expiry_cohorts.captured_at or v2_collection_expiry_cohorts.captured_after_expiry)
     then excluded.captured_at else v2_collection_expiry_cohorts.captured_at end,
   captured_after_expiry=case when not excluded.captured_after_expiry and (excluded.captured_at>=v2_collection_expiry_cohorts.captured_at or v2_collection_expiry_cohorts.captured_after_expiry)
     then false else v2_collection_expiry_cohorts.captured_after_expiry end,
   source_url=case when not excluded.captured_after_expiry and (excluded.captured_at>=v2_collection_expiry_cohorts.captured_at or v2_collection_expiry_cohorts.captured_after_expiry)
     then excluded.source_url else v2_collection_expiry_cohorts.source_url end,
   selected=v2_collection_expiry_cohorts.selected or excluded.selected,
   first_captured_at=least(v2_collection_expiry_cohorts.first_captured_at,excluded.first_captured_at);
 end loop;
end $$;

create or replace function public.v2_collection_expiry_capture()
returns trigger language plpgsql security definer set search_path=public,pg_temp as $$
declare ws uuid;logical text;
begin
 if tg_op='UPDATE' and new.value->'dates' is not distinct from old.value->'dates' and
   new.value#>'{_autofill,captured_at}' is not distinct from old.value#>'{_autofill,captured_at}' then return null;end if;
 if tg_table_name='v2_collection_live_values' then
  ws:=new.workspace_id;logical:=new.logical_key;
  if not exists(select 1 from public.v2_template_fields f join public.v2_template_sections sec on sec.id=f.section_id
    join public.v2_templates t on t.id=sec.template_id where t.workspace_id=ws and t.kind='collection'
    and f.field_type='options_expiry' and public.v2_collection_field_logical_key(to_jsonb(f))=logical) then return null;end if;
 else
  select s.workspace_id,public.v2_collection_field_logical_key(to_jsonb(f)) into ws,logical
   from public.v2_collection_sessions s join public.v2_template_fields f on f.id=new.field_id
   join public.v2_template_sections sec on sec.id=f.section_id where s.id=new.session_id and sec.template_id=s.template_id
   and f.field_type='options_expiry' and public.v2_collection_sheet_key(to_jsonb(sec))<>'data';
  if ws is null then return null;end if;
 end if;
 perform public.v2_expiry_record_value(ws,logical,new.value,new.updated_origin,new.updated_at);
 return null;
end $$;
drop trigger if exists v2_collection_expiry_live_capture on public.v2_collection_live_values;
create trigger v2_collection_expiry_live_capture after insert or update on public.v2_collection_live_values
 for each row execute function public.v2_collection_expiry_capture();
drop trigger if exists v2_collection_expiry_value_capture on public.v2_collection_values;
create trigger v2_collection_expiry_value_capture after insert or update on public.v2_collection_values
 for each row execute function public.v2_collection_expiry_capture();

-- An expiry-only removal is not an edit of the remaining answer. This private
-- per-row capability restores attribution after existing BEFORE timestamp and
-- origin triggers, without giving clients a way to impersonate an actor.
create or replace function public.v2_collection_expiry_preserve_attribution()
returns trigger language plpgsql security definer set search_path=public,pg_temp as $$
begin
 if exists(select 1 from public.v2_collection_expiry_maintenance_tokens where transaction_id=txid_current()
   and value_id=old.id and scope=case when tg_table_name='v2_collection_live_values' then 'live' else 'value' end) then
  new.updated_at:=old.updated_at;new.updated_by:=old.updated_by;new.updated_origin:=old.updated_origin;
 end if;
 return new;
end $$;
drop trigger if exists zz_v2_collection_expiry_preserve on public.v2_collection_values;
create trigger zz_v2_collection_expiry_preserve before update on public.v2_collection_values
 for each row execute function public.v2_collection_expiry_preserve_attribution();
drop trigger if exists zz_v2_collection_expiry_preserve on public.v2_collection_live_values;
create trigger zz_v2_collection_expiry_preserve before update on public.v2_collection_live_values
 for each row execute function public.v2_collection_expiry_preserve_attribution();

-- Workspaces are locked before value rows, matching the existing save/apply
-- RPC order. Re-read and remove only past dates under that lock; all other JSON,
-- attribution and image metadata survive. Historical/archived sessions remain.
create or replace function public.v2_collection_expiry_maintain(p_limit integer default 100)
returns integer language plpgsql security definer set search_path=public,pg_temp as $$
declare entry record;current_value jsonb;current_origin text;current_stamp timestamptz;filtered jsonb;
 today date:=(now() at time zone 'Asia/Jerusalem')::date;
 removed integer:=0;cap integer:=least(greatest(coalesce(p_limit,100),1),100);
begin
 for entry in
  select 'live'::text as scope,l.id,l.workspace_id,l.logical_key,l.value,l.updated_origin,l.updated_at
  from public.v2_collection_live_values l
  where jsonb_typeof(l.value->'dates')='array' and exists(select 1 from jsonb_array_elements(l.value->'dates') d
   where public.v2_expiry_valid_date(d->>'date')<today)
  and exists(select 1 from public.v2_template_fields f join public.v2_template_sections sec on sec.id=f.section_id
   join public.v2_templates t on t.id=sec.template_id where t.workspace_id=l.workspace_id and t.kind='collection'
   and f.field_type='options_expiry' and public.v2_collection_field_logical_key(to_jsonb(f))=l.logical_key)
  union all
  select 'value',v.id,s.workspace_id,public.v2_collection_field_logical_key(to_jsonb(f)),v.value,v.updated_origin,v.updated_at
  from public.v2_collection_values v join public.v2_collection_sessions s on s.id=v.session_id
  join public.v2_template_fields f on f.id=v.field_id join public.v2_template_sections sec on sec.id=f.section_id
  where s.deleted_at is null and s.status<>'archived' and sec.template_id=s.template_id and f.field_type='options_expiry'
  and public.v2_collection_sheet_key(to_jsonb(sec))<>'data' and jsonb_typeof(v.value->'dates')='array'
  and exists(select 1 from jsonb_array_elements(v.value->'dates') d where public.v2_expiry_valid_date(d->>'date')<today)
  order by workspace_id,id limit cap
 loop
  perform 1 from public.v2_workspaces where id=entry.workspace_id for update skip locked;
  if not found then continue;end if;
  if entry.scope='live' then
   select value,updated_origin,updated_at into current_value,current_origin,current_stamp
    from public.v2_collection_live_values where id=entry.id for update skip locked;
  else select value,updated_origin,updated_at into current_value,current_origin,current_stamp
    from public.v2_collection_values where id=entry.id for update skip locked;end if;
  if not found or jsonb_typeof(current_value->'dates') is distinct from 'array' then continue;end if;
  perform public.v2_expiry_record_value(entry.workspace_id,entry.logical_key,current_value,current_origin,current_stamp);
  select coalesce(jsonb_agg(d order by position),'[]') into filtered
   from jsonb_array_elements(current_value->'dates') with ordinality as items(d,position)
   where public.v2_expiry_valid_date(d->>'date') is null or public.v2_expiry_valid_date(d->>'date')>=today;
  if filtered is distinct from current_value->'dates' then
   removed:=removed+jsonb_array_length(current_value->'dates')-jsonb_array_length(filtered);
   insert into public.v2_collection_expiry_maintenance_tokens(transaction_id,scope,value_id) values(txid_current(),entry.scope,entry.id);
   if entry.scope='live' then update public.v2_collection_live_values set value=jsonb_set(current_value,'{dates}',filtered) where id=entry.id;
   else update public.v2_collection_values set value=jsonb_set(current_value,'{dates}',filtered) where id=entry.id;end if;
   delete from public.v2_collection_expiry_maintenance_tokens where transaction_id=txid_current() and scope=entry.scope and value_id=entry.id;
  end if;
 end loop;
 update public.v2_collection_expiry_cohorts set expired_at=now() where id in (
  select id from public.v2_collection_expiry_cohorts where expiry_date<today and expired_at is null order by expiry_date,id limit 1000);
 insert into public.v2_collection_expiry_market_days(day)
  select distinct expiry_date from public.v2_collection_expiry_cohorts where selected and expiry_date<(now() at time zone 'UTC')::date
  and not exists(select 1 from public.v2_collection_expiry_market_days m where m.day=expiry_date) order by expiry_date limit 200
  on conflict do nothing;
 return removed;
end $$;

create or replace function public.v2_collection_expiry_market_claim(p_limit integer default 2)
returns jsonb language plpgsql security definer set search_path=public,pg_temp as $$
declare result jsonb;
begin
 with candidates as (select day from public.v2_collection_expiry_market_days where status='pending'
   and next_attempt_at<=now() and (lease_until is null or lease_until<now())
   and ((day+1)::timestamp at time zone 'UTC')+interval '2 minutes'<=now()
   order by next_attempt_at,day for update skip locked limit least(greatest(coalesce(p_limit,2),1),2)),
 claimed as (update public.v2_collection_expiry_market_days d set lease_id=gen_random_uuid(),lease_until=now()+interval '1 minute',
   attempts=attempts+1 from candidates c where d.day=c.day returning d.day,d.lease_id)
 select coalesce(jsonb_agg(jsonb_build_object('day',day,'lease_id',lease_id)),'[]') into result from claimed;
 return result;
end $$;

create or replace function public.v2_collection_expiry_market_finish(p_day date,p_lease_id uuid,p_candle jsonb)
returns boolean language plpgsql security definer set search_path=public,pg_temp as $$
declare entry public.v2_collection_expiry_market_days;open_value numeric;high_value numeric;low_value numeric;close_value numeric;
 volume_value numeric;captured timestamptz;source text;
begin
 select * into entry from public.v2_collection_expiry_market_days where day=p_day for update;
 if entry.day is null or entry.status<>'pending' or entry.lease_id is distinct from p_lease_id or entry.lease_until<now() then return false;end if;
 if p_candle is null then
  update public.v2_collection_expiry_market_days set lease_id=null,lease_until=null,next_attempt_at=now()+interval '1 hour' where day=p_day;
  return false;
 end if;
 if p_candle->>'day' is distinct from p_day::text or p_day>=(now() at time zone 'UTC')::date then raise exception 'Invalid completed market day';end if;
 open_value:=public.v2_expiry_decimal(p_candle->>'open',true);high_value:=public.v2_expiry_decimal(p_candle->>'high',true);
 low_value:=public.v2_expiry_decimal(p_candle->>'low',true);close_value:=public.v2_expiry_decimal(p_candle->>'close',true);
 volume_value:=public.v2_expiry_decimal(p_candle->>'volumeBtc',false);
 captured:=(p_candle->>'capturedAt')::timestamptz;source:=p_candle->>'sourceUrl';
 if open_value is null or high_value is null or low_value is null or close_value is null or volume_value is null or
   high_value<greatest(open_value,close_value,low_value) or low_value>least(open_value,close_value) or
   (p_candle->>'openedAt')::timestamptz is distinct from (p_day::timestamp at time zone 'UTC') or
   (p_candle->>'closedAt')::timestamptz is distinct from ((p_day+1)::timestamp at time zone 'UTC') or
   captured<((p_day+1)::timestamp at time zone 'UTC')+interval '2 minutes' or captured>now()+interval '5 minutes' or
   source is distinct from ('https://www.bitstamp.net/api/v2/ohlc/btcusd/?step=86400&limit=1&end='||
     (extract(epoch from ((p_day+1)::timestamp at time zone 'UTC'))::bigint-1)::text||'&exclude_current_candle=true') then
   raise exception 'Unverified market day';
 end if;
 update public.v2_collection_expiry_market_days set status='complete',open=open_value,high=high_value,low=low_value,close=close_value,
   volume_btc=volume_value,opened_at=(p_day::timestamp at time zone 'UTC'),closed_at=((p_day+1)::timestamp at time zone 'UTC'),
   captured_at=captured,source_url=source,lease_id=null,lease_until=null where day=p_day;
 return true;
end $$;

create or replace function public.v2_collection_expiry_statistics(p_workspace_id uuid,p_field_id uuid,p_limit integer default 200)
returns jsonb language plpgsql stable security definer set search_path=public,pg_temp as $$
declare logical text;result jsonb;
begin
 if auth.uid() is null or not coalesce(public.v2_is_workspace_member(p_workspace_id),false) then
  raise exception 'Workspace membership required' using errcode='42501';end if;
 select public.v2_collection_field_logical_key(to_jsonb(f)) into logical
 from public.v2_template_fields f join public.v2_template_sections sec on sec.id=f.section_id
 join public.v2_templates t on t.id=sec.template_id where f.id=p_field_id and t.workspace_id=p_workspace_id
 and t.kind='collection' and f.field_type='options_expiry';
 if logical is null then raise exception 'Expiry field does not belong to workspace' using errcode='42501';end if;
 select coalesce(jsonb_agg(to_jsonb(history) order by expiry_date desc),'[]') into result from (
  select c.id,c.expiry_date,c.selected,c.origin,c.captured_at,c.captured_after_expiry,c.source_url,c.exchanges,
   jsonb_build_object('status',coalesce(m.status,'pending'),'open',m.open,'high',m.high,'low',m.low,'close',m.close,
     'opened_at',m.opened_at,'closed_at',m.closed_at,'captured_at',m.captured_at,'source_url',m.source_url) as market
  from public.v2_collection_expiry_cohorts c left join public.v2_collection_expiry_market_days m on m.day=c.expiry_date
  where c.workspace_id=p_workspace_id and c.logical_key=logical and c.selected
  and c.expiry_date<(now() at time zone 'Asia/Jerusalem')::date order by c.expiry_date desc,c.id
  limit least(greatest(coalesce(p_limit,200),1),200)
 ) history;
 return result;
end $$;

revoke all on function public.v2_expiry_valid_date(text),public.v2_expiry_decimal(text,boolean),
 public.v2_expiry_record_value(uuid,text,jsonb,text,timestamptz),public.v2_collection_expiry_capture(),public.v2_collection_expiry_preserve_attribution(),
 public.v2_collection_expiry_maintain(integer),public.v2_collection_expiry_market_claim(integer),
 public.v2_collection_expiry_market_finish(date,uuid,jsonb),public.v2_collection_expiry_statistics(uuid,uuid,integer)
 from public,anon,authenticated;
grant execute on function public.v2_collection_expiry_maintain(integer),public.v2_collection_expiry_market_claim(integer),
 public.v2_collection_expiry_market_finish(date,uuid,jsonb) to service_role;
grant execute on function public.v2_collection_expiry_statistics(uuid,uuid,integer) to authenticated;

-- Backfill the actual saved snapshots, oldest first. Do not infer missing
-- observations from today's quote or replace a pre-expiry amount with a later
-- capture. Mirrored duplicate snapshots deduplicate by their exact fingerprint.
do $backfill$
declare entry record;
begin
 for entry in
  select s.workspace_id,public.v2_collection_field_logical_key(to_jsonb(f)) as logical_key,
   v.value,v.updated_origin,v.updated_at
  from public.v2_collection_values v join public.v2_collection_sessions s on s.id=v.session_id
  join public.v2_template_fields f on f.id=v.field_id join public.v2_template_sections sec on sec.id=f.section_id
  where sec.template_id=s.template_id and f.field_type='options_expiry' and jsonb_typeof(v.value->'dates')='array'
  union all
  select l.workspace_id,l.logical_key,l.value,l.updated_origin,l.updated_at from public.v2_collection_live_values l
  where jsonb_typeof(l.value->'dates')='array' and exists(select 1 from public.v2_template_fields f
   join public.v2_template_sections sec on sec.id=f.section_id join public.v2_templates t on t.id=sec.template_id
   where t.workspace_id=l.workspace_id and t.kind='collection' and f.field_type='options_expiry'
   and public.v2_collection_field_logical_key(to_jsonb(f))=l.logical_key)
  order by updated_at
 loop
  perform public.v2_expiry_record_value(entry.workspace_id,entry.logical_key,entry.value,entry.updated_origin,entry.updated_at);
 end loop;
end $backfill$;
notify pgrst,'reload schema';
commit;
