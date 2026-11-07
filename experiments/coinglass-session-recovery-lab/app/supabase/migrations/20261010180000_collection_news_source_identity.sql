-- Automatic source identity is independent of model wording, X account names,
-- worksheet/template copies and scan retries. Manual news rows stay unrestricted.
create or replace function public.v2_news_source_identity(p_source text)
returns text language plpgsql immutable set search_path=public,pg_temp as $$
declare parts text[]; host text; path text; query text; post_id text;
begin
  if p_source ~ '^x:status:[1-9][0-9]*$' then return p_source;end if;
  if p_source is null or length(p_source)>4096 then return null;end if;
  parts:=regexp_match(p_source,'^https://([^/?#]+)([^?#]*)(?:[?]([^#]*))?(?:#.*)?$','i');
  if parts is null then return null;end if;
  host:=regexp_replace(lower(parts[1]),'^www[.]','');
  host:=regexp_replace(host,':443$','');
  if host !~ '^[a-z0-9.-]+$' then return null;end if;
  path:=coalesce(nullif(parts[2],''),'/');
  if host in ('x.com','twitter.com','mobile.twitter.com','mobile.x.com') then
    post_id:=substring(path from '^/(?:[A-Za-z0-9_]{1,15}|i(?:/web)?)/status/([1-9][0-9]*)(?:/(?:photo|video)/[0-9]+)?/?$');
    return case when post_id is not null then 'x:status:'||post_id end;
  end if;
  path:=coalesce(nullif(regexp_replace(path,'/+$',''),''),'/');
  select string_agg(part,'&' order by part collate "C") into query
  from regexp_split_to_table(coalesce(parts[3],''),'&') part
  where part<>'' and part !~* '^(?:utm_[^=]*|fbclid|gclid)(?:=|$)';
  return 'https://'||host||path||case when query is not null then '?'||query else '' end;
end $$;
revoke all on function public.v2_news_source_identity(text) from public,anon,authenticated;
grant execute on function public.v2_news_source_identity(text) to service_role;

create table if not exists public.v2_collection_news_source_claims (
  workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
  source_identity text not null,
  source_url text not null,
  item_key text not null,
  sheet_key text not null,
  logical_key text not null,
  first_run_id uuid references public.v2_collection_autofill_runs(id) on delete set null,
  published_at timestamptz not null,
  primary key(workspace_id,source_identity)
);
alter table public.v2_collection_news_source_claims enable row level security;
revoke all on public.v2_collection_news_source_claims from public,anon,authenticated;
grant all on public.v2_collection_news_source_claims to service_role;

-- Only the service's durable item registry is evidence of an automatic scan.
-- Existing duplicates stay recoverable for reviewed cleanup; no manual content,
-- feedback, cursor, receipt or origin is changed by installing this guard.
insert into public.v2_collection_news_source_claims(workspace_id,source_identity,source_url,item_key,sheet_key,logical_key,published_at)
select distinct on(i.workspace_id,public.v2_news_source_identity(i.source_url))
  i.workspace_id,public.v2_news_source_identity(i.source_url),i.source_url,i.item_key,i.sheet_key,i.logical_key,i.published_at
from public.v2_collection_autofill_items i
where public.v2_news_source_identity(i.source_url) is not null
order by i.workspace_id,public.v2_news_source_identity(i.source_url),i.published_at,i.item_key
on conflict(workspace_id,source_identity) do nothing;

create or replace function public.v2_claim_collection_news_source(p_run_id uuid,p_item jsonb)
returns boolean language plpgsql security definer set search_path=public,pg_temp as $$
declare run public.v2_collection_autofill_runs; identity text; inserted integer;
begin
  select * into run from public.v2_collection_autofill_runs where id=p_run_id;
  if run.id is null or run.status<>'running' then raise exception 'Active news automation run required' using errcode='23514';end if;
  identity:=public.v2_news_source_identity(p_item->>'url');
  if identity is null then raise exception 'Automatic news requires a stable source URL' using errcode='23514';end if;
  insert into public.v2_collection_news_source_claims(workspace_id,source_identity,source_url,item_key,sheet_key,logical_key,first_run_id,published_at)
    values(run.workspace_id,identity,p_item->>'url',p_item->>'key',run.sheet_key,run.logical_key,run.id,(p_item->>'published_at')::timestamptz)
    on conflict(workspace_id,source_identity) do nothing;
  get diagnostics inserted=row_count;
  return inserted=1;
end $$;
revoke all on function public.v2_claim_collection_news_source(uuid,jsonb) from public,anon,authenticated,service_role;

-- Return the caller's original key, so old/new provider adapters can skip paid
-- work while aliases are matched against a shared workspace-wide source ID.
create or replace function public.v2_collection_autofill_known_items(p_run_id uuid,p_keys text[])
returns setof text language sql stable security definer set search_path=public,pg_temp as $$
  select distinct candidate from public.v2_collection_autofill_runs r cross join unnest(p_keys) candidate
  where r.id=p_run_id and (exists(select 1 from public.v2_collection_news_source_claims c
    where c.workspace_id=r.workspace_id and c.source_identity=public.v2_news_source_identity(candidate))
    or exists(select 1 from public.v2_collection_autofill_items i
      where i.workspace_id=r.workspace_id and i.item_key=candidate));
$$;
revoke all on function public.v2_collection_autofill_known_items(uuid,text[]) from public,anon,authenticated;
grant execute on function public.v2_collection_autofill_known_items(uuid,text[]) to service_role;

-- The uniqueness claim and native/embedded creation commit or roll back in the
-- same apply transaction. The existing workspace lock serializes all fields;
-- the primary key independently protects a simultaneous competing claimant.
do $patch$
declare definition text;
  needle text:=E'for item in select * from jsonb_array_elements(coalesce(p_payload->''items'',''[]'')) loop\n insert into public.v2_collection_autofill_items';
  replacement text:=E'for item in select * from jsonb_array_elements(coalesce(p_payload->''items'',''[]'')) loop\n /* v2_workspace_source_identity */ if not public.v2_claim_collection_news_source(run.id,item) then continue;end if;\n insert into public.v2_collection_autofill_items';
begin
  definition:=pg_get_functiondef('public.v2_collection_autofill_apply(uuid,jsonb,jsonb,timestamptz)'::regprocedure);
  if strpos(definition,'v2_workspace_source_identity')=0 then
    if (length(definition)-length(replace(definition,needle,'')))/length(needle)<>1 then
      raise exception 'Unexpected news apply definition; stable source identity was not installed';end if;
    definition:=replace(definition,needle,replacement);
    execute definition;
  end if;
end $patch$;
notify pgrst,'reload schema';
