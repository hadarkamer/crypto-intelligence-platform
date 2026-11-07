-- Publication timing is factual source metadata, kept separately from relevance
-- filtering. Only the authenticated application worker may register evidence or
-- publish a learned model. Normal members cannot invent timing observations.
begin;
create or replace function app_private.collection_autofill_source_urls(p_settings jsonb)
returns text[] language plpgsql immutable set search_path=pg_catalog,public,app_private,pg_temp as $$
declare links jsonb; source text; result text[];
begin
 if p_settings ? 'autofill_source_urls' then
  if jsonb_typeof(p_settings->'autofill_source_urls')<>'array' or exists(
   select 1 from jsonb_array_elements(p_settings->'autofill_source_urls') v where jsonb_typeof(v)<>'string') then return '{}'; end if;
  select coalesce(array_agg(distinct btrim(v#>>'{}') order by btrim(v#>>'{}')),'{}') into result
   from jsonb_array_elements(p_settings->'autofill_source_urls') v where nullif(btrim(v#>>'{}'),'') is not null;
  return result;
 end if;
 links:=case when p_settings ? 'links_json' then p_settings->'links_json' when p_settings ? 'links' then p_settings->'links'
  else p_settings#>'{legacy_question,links_json}' end;
 source:=case when p_settings ? 'source_url' then p_settings->>'source_url' else p_settings#>>'{legacy_question,source_url}' end;
 if jsonb_typeof(links)='array' then
  select coalesce(array_agg(distinct btrim(v->>'url') order by btrim(v->>'url')),'{}') into result
   from jsonb_array_elements(links) v where jsonb_typeof(v->'url')='string' and nullif(btrim(v->>'url'),'') is not null;
 end if;
 if coalesce(cardinality(result),0)=0 and nullif(btrim(source),'') is not null then result:=array[btrim(source)]; end if;
 return coalesce(result,'{}');
end $$;
revoke all on function app_private.collection_autofill_source_urls(jsonb) from public,anon,authenticated;
create or replace function app_private.collection_autofill_source_fingerprint(p_settings jsonb)
returns text language sql immutable set search_path=pg_catalog,public,app_private,pg_temp as $$
 select md5(to_jsonb(app_private.collection_autofill_source_urls(p_settings))::text);
$$;
revoke all on function app_private.collection_autofill_source_fingerprint(jsonb) from public,anon,authenticated;

create table if not exists public.v2_collection_autofill_publication_profiles (
 workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
 sheet_key text not null,logical_key text not null,source_fingerprint text not null,
 model jsonb not null default '{}',evidence_revision timestamptz not null default clock_timestamp(),
 model_revision timestamptz,updated_at timestamptz not null default clock_timestamp(),
 primary key(workspace_id,sheet_key,logical_key,source_fingerprint),check(jsonb_typeof(model)='object')
);
create table if not exists public.v2_collection_autofill_publications (
 workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
 sheet_key text not null,logical_key text not null,source_fingerprint text not null,
 source_url text not null,item_key text not null,item_url text not null,published_at timestamptz not null,
 observed_at timestamptz not null default clock_timestamp(),
 primary key(workspace_id,sheet_key,logical_key,source_fingerprint,source_url,item_key)
);
create table if not exists public.v2_collection_autofill_publication_coverage (
 workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
 sheet_key text not null,logical_key text not null,source_fingerprint text not null,
 source_url text not null,from_at timestamptz not null,to_at timestamptz not null,
 primary key(workspace_id,sheet_key,logical_key,source_fingerprint,source_url,from_at,to_at),check(to_at>from_at)
);
create index if not exists v2_collection_publications_scope_time
 on public.v2_collection_autofill_publications(workspace_id,sheet_key,logical_key,source_fingerprint,published_at desc);
create index if not exists v2_collection_publication_coverage_scope_time
 on public.v2_collection_autofill_publication_coverage(workspace_id,sheet_key,logical_key,source_fingerprint,to_at desc);
alter table public.v2_collection_autofill_publication_profiles enable row level security;
alter table public.v2_collection_autofill_publications enable row level security;
alter table public.v2_collection_autofill_publication_coverage enable row level security;
revoke all on public.v2_collection_autofill_publication_profiles,public.v2_collection_autofill_publications,
 public.v2_collection_autofill_publication_coverage from public,anon,authenticated;
grant all on public.v2_collection_autofill_publication_profiles,public.v2_collection_autofill_publications,
 public.v2_collection_autofill_publication_coverage to service_role;

create or replace function app_private.collection_publication_scope(p_field_id uuid)
returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,public,app_private,pg_temp as $$
declare f public.v2_template_fields; sec public.v2_template_sections; t public.v2_templates;
begin
 select * into f from public.v2_template_fields where id=p_field_id and active;
 select * into sec from public.v2_template_sections where id=f.section_id and active;
 select * into t from public.v2_templates where id=sec.template_id and kind='collection';
 if t.id is null then raise exception 'תיבת המקור אינה בתבנית איסוף פעילה.' using errcode='23514'; end if;
 return jsonb_build_object('field',to_jsonb(f),'workspace_id',t.workspace_id,'sheet_key',public.v2_collection_sheet_key(to_jsonb(sec)),
  'logical_key',public.v2_collection_field_logical_key(to_jsonb(f)),
  'source_fingerprint',app_private.collection_autofill_source_fingerprint(f.settings),
  'source_urls',to_jsonb(app_private.collection_autofill_source_urls(f.settings)));
end $$;
revoke all on function app_private.collection_publication_scope(uuid) from public,anon,authenticated;

create or replace function public.v2_collection_autofill_publication_profile(p_field_id uuid)
returns jsonb language plpgsql volatile security definer set search_path=pg_catalog,public,app_private,pg_temp as $$
declare scope jsonb; ws uuid; sheet text; logical text; fingerprint text; profile jsonb; publications jsonb; covered jsonb; accepted jsonb; truncated boolean;
begin
 scope:=app_private.collection_publication_scope(p_field_id);ws:=(scope->>'workspace_id')::uuid;
 sheet:=scope->>'sheet_key';logical:=scope->>'logical_key';fingerprint:=scope->>'source_fingerprint';
 insert into public.v2_collection_autofill_publication_profiles(workspace_id,sheet_key,logical_key,source_fingerprint)
  values(ws,sheet,logical,fingerprint) on conflict do nothing;
 select to_jsonb(p) into profile from public.v2_collection_autofill_publication_profiles p
  where p.workspace_id=ws and p.sheet_key=sheet and p.logical_key=logical and p.source_fingerprint=fingerprint;
 select coalesce(jsonb_agg(to_jsonb(p) order by p.published_at),'[]') into publications from (
  select source_url,item_key,item_url,published_at,'verified_scan'::text basis from public.v2_collection_autofill_publications p
  where p.workspace_id=ws and p.sheet_key=sheet and p.logical_key=logical and p.source_fingerprint=fingerprint
   and p.published_at>=now()-interval '56 days' order by p.published_at desc limit 5000) p;
 select count(*)>5000 into truncated from public.v2_collection_autofill_publications p
  where p.workspace_id=ws and p.sheet_key=sheet and p.logical_key=logical and p.source_fingerprint=fingerprint
   and p.published_at>=now()-interval '56 days';
 select coalesce(jsonb_agg(to_jsonb(p)),'[]') into covered from (
  select source_url,from_at,to_at from public.v2_collection_autofill_publication_coverage p
  where p.workspace_id=ws and p.sheet_key=sheet and p.logical_key=logical and p.source_fingerprint=fingerprint
   and p.to_at>=now()-interval '56 days' order by p.to_at desc limit 5000) p;
 select coalesce(jsonb_agg(to_jsonb(p)),'[]') into accepted from (
  select source_url,item_key,published_at from public.v2_collection_autofill_items p
  where p.workspace_id=ws and p.sheet_key=sheet and p.logical_key=logical and p.published_at>=now()-interval '56 days'
  order by p.published_at desc limit 1000) p;
 return scope||jsonb_build_object('profile',profile,'publications',publications,'coverage',case when truncated then '[]'::jsonb else covered end,
  'accepted_history',accepted,'truncated',truncated);
end $$;
revoke all on function public.v2_collection_autofill_publication_profile(uuid) from public,anon,authenticated;
grant execute on function public.v2_collection_autofill_publication_profile(uuid) to service_role;

create or replace function public.v2_collection_autofill_record_publications(
 p_field_id uuid,p_run_id uuid,p_source_url text,p_items jsonb,p_coverage_from timestamptz,p_scanned_at timestamptz)
returns jsonb language plpgsql volatile security definer set search_path=pg_catalog,public,app_private,pg_temp as $$
declare scope jsonb; ws uuid; sheet text; logical text; fingerprint text; entry jsonb; publication timestamptz;
begin
 scope:=app_private.collection_publication_scope(p_field_id);ws:=(scope->>'workspace_id')::uuid;
 sheet:=scope->>'sheet_key';logical:=scope->>'logical_key';fingerprint:=scope->>'source_fingerprint';
 if not exists(select 1 from jsonb_array_elements_text(scope->'source_urls') u where u=p_source_url)
  or p_scanned_at is null or not isfinite(p_scanned_at) or p_scanned_at>clock_timestamp()+interval '5 minutes' or p_scanned_at<clock_timestamp()-interval '1 day'
  or jsonb_typeof(p_items) is distinct from 'array' or jsonb_array_length(p_items)>5000 then
  raise exception 'ראיות פרסום אינן שייכות למקור הפעיל.' using errcode='23514'; end if;
 if p_run_id is not null and not exists(select 1 from public.v2_collection_autofill_runs r
  where r.id=p_run_id and r.field_id=p_field_id and r.workspace_id=ws and r.sheet_key=sheet and r.logical_key=logical and r.status='running') then
  raise exception 'ראיות פרסום דורשות סריקה פעילה תואמת.' using errcode='23514'; end if;
 perform 1 from public.v2_workspaces where id=ws for update;
 delete from public.v2_collection_autofill_publications p where p.workspace_id=ws and p.sheet_key=sheet
  and p.logical_key=logical and p.published_at<clock_timestamp()-interval '56 days';
 delete from public.v2_collection_autofill_publication_coverage p where p.workspace_id=ws and p.sheet_key=sheet
  and p.logical_key=logical and p.to_at<clock_timestamp()-interval '56 days';
 for entry in select value from jsonb_array_elements(p_items) loop
  if jsonb_typeof(entry->'key') is distinct from 'string' or length(entry->>'key') not between 1 and 2000
   or jsonb_typeof(entry->'publishedAt') is distinct from 'string' or jsonb_typeof(entry->'url') is distinct from 'string'
   or length(entry->>'url') not between 1 and 4000 then raise exception 'פרסום ללא מזהה או זמן מאומת.' using errcode='23514'; end if;
  publication:=(entry->>'publishedAt')::timestamptz;
  if not isfinite(publication) or publication>p_scanned_at or publication<p_scanned_at-interval '56 days' then
   raise exception 'מועד הפרסום מחוץ להיסטוריה שנבדקת.' using errcode='23514'; end if;
  insert into public.v2_collection_autofill_publications(workspace_id,sheet_key,logical_key,source_fingerprint,source_url,item_key,item_url,published_at)
   values(ws,sheet,logical,fingerprint,p_source_url,entry->>'key',entry->>'url',publication)
   on conflict(workspace_id,sheet_key,logical_key,source_fingerprint,source_url,item_key) do update
    set published_at=excluded.published_at,item_url=excluded.item_url,observed_at=clock_timestamp();
 end loop;
 if p_coverage_from is not null then
  if not isfinite(p_coverage_from) or p_coverage_from>=p_scanned_at then raise exception 'חלון סריקה אינו תקין.' using errcode='23514'; end if;
  insert into public.v2_collection_autofill_publication_coverage(workspace_id,sheet_key,logical_key,source_fingerprint,source_url,from_at,to_at)
   values(ws,sheet,logical,fingerprint,p_source_url,greatest(p_coverage_from,p_scanned_at-interval '56 days'),p_scanned_at) on conflict do nothing;
 end if;
 insert into public.v2_collection_autofill_publication_profiles(workspace_id,sheet_key,logical_key,source_fingerprint)
  values(ws,sheet,logical,fingerprint) on conflict(workspace_id,sheet_key,logical_key,source_fingerprint) do update
  set evidence_revision=clock_timestamp(),updated_at=clock_timestamp();
 return public.v2_collection_autofill_publication_profile(p_field_id);
end $$;
revoke all on function public.v2_collection_autofill_record_publications(uuid,uuid,text,jsonb,timestamptz,timestamptz) from public,anon,authenticated;
grant execute on function public.v2_collection_autofill_record_publications(uuid,uuid,text,jsonb,timestamptz,timestamptz) to service_role;

create or replace function public.v2_collection_autofill_save_publication_model(p_field_id uuid,p_evidence_revision timestamptz,p_model jsonb)
returns boolean language plpgsql volatile security definer set search_path=pg_catalog,public,app_private,pg_temp as $$
declare scope jsonb; updated integer;
begin
 scope:=app_private.collection_publication_scope(p_field_id);
 if p_model->>'version' is distinct from 'publication_schedule.v1' or coalesce(p_model->>'confidence','') not in ('insufficient','provisional','learned')
  or coalesce(p_model->>'first_hour','') !~ '^(?:[0-9]|1[0-9]|2[0-3])$' or coalesce(p_model->>'interval_hours','') !~ '^(?:[1-9]|1[0-9]|2[0-4])$'
  or jsonb_typeof(p_model->'eligible') is distinct from 'boolean' or jsonb_typeof(p_model->'weekly_windows') is distinct from 'array'
  or jsonb_array_length(p_model->'weekly_windows')>168 then raise exception 'מודל תזמון אינו תקין.' using errcode='23514'; end if;
 if exists(select 1 from jsonb_array_elements(p_model->'weekly_windows') w where jsonb_typeof(w)<>'object'
  or coalesce(w->>'day','') !~ '^[0-6]$' or coalesce(w->>'start_minute','') !~ '^[0-9]{1,4}$'
  or coalesce(w->>'end_minute','') !~ '^[0-9]{1,4}$' or (w->>'start_minute')::integer<0
  or (w->>'end_minute')::integer>1440 or (w->>'end_minute')::integer<=(w->>'start_minute')::integer) then
  raise exception 'חלון תזמון שבועי אינו תקין.' using errcode='23514'; end if;
 update public.v2_collection_autofill_publication_profiles p set model=p_model,model_revision=p.evidence_revision,updated_at=clock_timestamp()
 where p.workspace_id=(scope->>'workspace_id')::uuid and p.sheet_key=scope->>'sheet_key' and p.logical_key=scope->>'logical_key'
  and p.source_fingerprint=scope->>'source_fingerprint' and p.evidence_revision=p_evidence_revision;
 get diagnostics updated=row_count;return updated=1;
end $$;
revoke all on function public.v2_collection_autofill_save_publication_model(uuid,timestamptz,jsonb) from public,anon,authenticated;
grant execute on function public.v2_collection_autofill_save_publication_model(uuid,timestamptz,jsonb) to service_role;
commit;
