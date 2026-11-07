-- Explicit news votes and reversible deletion are shared workspace records.
-- Original data, provenance and actors come from locked server rows, never
-- from a browser-supplied body, source, model prediction or account ID.
create table if not exists public.v2_collection_news_assessments (
  workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
  sheet_key text not null,
  logical_key text not null,
  item_key text not null,
  source_url text not null,
  learning_source_url text not null,
  source_name text,
  published_at timestamptz not null,
  body text not null,
  model_evaluation jsonb,
  run_id uuid references public.v2_collection_autofill_runs(id) on delete set null,
  created_at timestamptz not null default now(),
  primary key(workspace_id,sheet_key,logical_key,item_key)
);
create table if not exists public.v2_collection_news_provenance (
  workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
  session_id uuid not null,
  field_id uuid not null,
  row_id text not null,
  storage text not null check(storage in ('native','embedded')),
  sheet_key text not null,
  logical_key text not null,
  item_key text not null,
  source_url text not null,
  published_at timestamptz not null,
  created_at timestamptz not null default now(),
  primary key(workspace_id,session_id,field_id,row_id)
);
create table if not exists public.v2_collection_news_feedback_bootstrap (
  singleton boolean primary key default true check(singleton),
  completed_at timestamptz not null default now()
);
create table if not exists public.v2_collection_news_feedback (
  workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
  sheet_key text not null,
  logical_key text not null,
  item_key text not null,
  actor_id uuid not null references auth.users(id) on delete cascade,
  session_id uuid not null,
  field_id uuid not null,
  row_id text not null,
  action text not null check(action in ('rate','delete','restored')),
  feedback_level text check(feedback_level in ('most_important','very_important','important','useful','borderline','irrelevant','completely_irrelevant')),
  score smallint check(score between -3 and 3),
  sentiment text,
  detail text check(length(detail)<=1000),
  body text not null,
  source_url text not null,
  learning_source_url text not null,
  source_name text,
  model_evaluation jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key(workspace_id,item_key,actor_id),
  check((action='restored' and score is null and feedback_level is null)
    or (action<>'restored' and score is not null and feedback_level is not null))
);
create table if not exists public.v2_collection_news_feedback_events (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
  actor_id uuid references auth.users(id) on delete set null,
  item_key text not null,
  event jsonb not null,
  occurred_at timestamptz not null default now()
);
create table if not exists public.v2_collection_news_recycle_bin (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
  session_id uuid not null,
  field_id uuid not null,
  row_id text not null,
  sheet_key text not null,
  logical_key text not null,
  item_key text not null,
  storage text not null check(storage in ('native','embedded')),
  snapshot jsonb not null,
  body text not null,
  source_url text not null,
  learning_source_url text not null,
  source_name text,
  importance text not null,
  model_evaluation jsonb,
  reason text not null check(reason in ('borderline','irrelevant','completely_irrelevant')),
  detail text check(length(detail)<=1000),
  deleted_by uuid references auth.users(id) on delete set null,
  deleted_at timestamptz not null default now(),
  restored_by uuid references auth.users(id) on delete set null,
  restored_at timestamptz
);
create unique index if not exists v2_collection_news_recycle_active_idx
  on public.v2_collection_news_recycle_bin(workspace_id,session_id,field_id,row_id) where restored_at is null;
create index if not exists v2_collection_news_feedback_source_idx
  on public.v2_collection_news_feedback(workspace_id,learning_source_url,updated_at desc);
create index if not exists v2_collection_news_recycle_read_idx
  on public.v2_collection_news_recycle_bin(workspace_id,deleted_at desc);

alter table public.v2_collection_news_assessments enable row level security;
alter table public.v2_collection_news_provenance enable row level security;
alter table public.v2_collection_news_feedback_bootstrap enable row level security;
alter table public.v2_collection_news_feedback enable row level security;
alter table public.v2_collection_news_feedback_events enable row level security;
alter table public.v2_collection_news_recycle_bin enable row level security;
revoke all on public.v2_collection_news_assessments,public.v2_collection_news_provenance,
  public.v2_collection_news_feedback_bootstrap,public.v2_collection_news_feedback,public.v2_collection_news_feedback_events,
  public.v2_collection_news_recycle_bin from public,anon,authenticated;
grant all on public.v2_collection_news_assessments,public.v2_collection_news_provenance,
  public.v2_collection_news_feedback_bootstrap,public.v2_collection_news_feedback,public.v2_collection_news_feedback_events,
  public.v2_collection_news_recycle_bin to service_role;
grant select on public.v2_collection_news_feedback,public.v2_collection_news_feedback_events to authenticated;
drop policy if exists v2_collection_news_feedback_read on public.v2_collection_news_feedback;
create policy v2_collection_news_feedback_read on public.v2_collection_news_feedback for select to authenticated
  using(public.v2_is_workspace_member(workspace_id));
drop policy if exists v2_collection_news_feedback_events_read on public.v2_collection_news_feedback_events;
create policy v2_collection_news_feedback_events_read on public.v2_collection_news_feedback_events for select to authenticated
  using(public.v2_is_workspace_member(workspace_id));

create or replace function public.v2_record_collection_news_feedback_event()
returns trigger language plpgsql security definer set search_path=public,pg_temp as $$
begin
  if tg_op='UPDATE' and row(new.action,new.score,new.feedback_level,new.sentiment,new.detail,new.body,new.model_evaluation)
    is not distinct from row(old.action,old.score,old.feedback_level,old.sentiment,old.detail,old.body,old.model_evaluation) then return new;end if;
  insert into public.v2_collection_news_feedback_events(workspace_id,actor_id,item_key,event)
    values(new.workspace_id,coalesce(auth.uid(),new.actor_id),new.item_key,to_jsonb(new)||jsonb_build_object('performed_by',coalesce(auth.uid(),new.actor_id)));
  return new;
end $$;
revoke all on function public.v2_record_collection_news_feedback_event() from public,anon,authenticated;
drop trigger if exists v2_collection_news_feedback_event on public.v2_collection_news_feedback;
create trigger v2_collection_news_feedback_event after insert or update on public.v2_collection_news_feedback
  for each row execute function public.v2_record_collection_news_feedback_event();

create or replace function public.v2_news_learning_source(p_url text)
returns text language plpgsql immutable set search_path=public,pg_temp as $$
declare host text; account text;
begin
  host:=lower(substring(p_url from '^https?://([^/?#:]+)'));
  if host is null or strpos(host,'@')>0 then return null;end if;
  host:=regexp_replace(host,'^www\.','');
  if host in ('x.com','twitter.com') then
    account:=lower(substring(p_url from '^https?://[^/?#]+/([^/?#]+)'));
    return case when account~'^[a-z0-9_]{1,20}$' and account<>'i' then 'https://x.com/'||account else 'https://x.com' end;
  end if;
  return 'https://'||host;
end;
$$;
revoke all on function public.v2_news_learning_source(text) from public,anon,authenticated;

create or replace function public.v2_register_collection_news_assessment(p_run_id uuid,p_item jsonb)
returns void language plpgsql security definer set search_path=public,pg_temp as $$
declare r public.v2_collection_autofill_runs; evaluation jsonb:=p_item->'bot_relevance'; learning text;
begin
  select * into r from public.v2_collection_autofill_runs where id=p_run_id;
  if r.id is null then raise exception 'Unknown automation run' using errcode='23514'; end if;
  if evaluation='null'::jsonb then evaluation:=null; end if;
  if evaluation is not null and (jsonb_typeof(evaluation)<>'object' or coalesce(evaluation->>'v','')<>'1'
    or jsonb_typeof(evaluation->'v') is distinct from 'number' or jsonb_typeof(evaluation->'score') is distinct from 'number'
    or coalesce(evaluation->>'score','')!~'^(100|[0-9]{1,2})$'
    or coalesce(evaluation->>'level','') not in ('most_important','very_important','important','useful','borderline','irrelevant','completely_irrelevant')
    or jsonb_typeof(evaluation->'reason') is distinct from 'string'
    or nullif(btrim(evaluation->>'reason'),'') is null or length(evaluation->>'reason')>160
    or (evaluation->>'reason')~'[<>[:cntrl:]]'
    or (select count(*) from jsonb_object_keys(evaluation))<>4
    or not ((evaluation->>'score')::integer between case evaluation->>'level'
      when 'most_important' then 90 when 'very_important' then 75 when 'important' then 60 when 'useful' then 40
      when 'borderline' then 20 when 'irrelevant' then 5 else 0 end and case evaluation->>'level'
      when 'most_important' then 100 when 'very_important' then 89 when 'important' then 74 when 'useful' then 59
      when 'borderline' then 39 when 'irrelevant' then 19 else 4 end)) then
    raise exception 'Invalid news model assessment' using errcode='23514'; end if;
  learning:=public.v2_news_learning_source(coalesce(nullif(p_item->>'learning_source_url',''),p_item->>'url'));
  if learning is null then raise exception 'Invalid learning source' using errcode='23514'; end if;
  insert into public.v2_collection_news_assessments(workspace_id,sheet_key,logical_key,item_key,source_url,
    learning_source_url,source_name,published_at,body,model_evaluation,run_id)
    values(r.workspace_id,r.sheet_key,r.logical_key,p_item->>'key',p_item->>'url',learning,p_item->>'source_name',
      (p_item->>'published_at')::timestamptz,p_item->>'html',evaluation,p_run_id) on conflict do nothing;
end $$;
revoke all on function public.v2_register_collection_news_assessment(uuid,jsonb) from public,anon,authenticated;

-- Bind immutable automatic origin to real stable row IDs at creation. Later
-- user edits may change updated_origin; that does not lose the trusted origin.
create or replace function public.v2_capture_collection_news_provenance()
returns trigger language plpgsql security definer set search_path=public,pg_temp as $$
declare s public.v2_collection_sessions; f public.v2_template_fields; sec public.v2_template_sections;
  row_json jsonb; fid uuid; sid uuid; current_run_id uuid;
begin
  select token.run_id into current_run_id from public.v2_collection_autofill_write_tokens token where transaction_id=txid_current();
  if current_run_id is null then return new; end if;
  if tg_table_name='v2_news_items' then sid:=new.collection_session_id;fid:=new.field_id;
  else sid:=new.session_id;fid:=new.field_id;end if;
  select * into s from public.v2_collection_sessions where id=sid;
  select * into f from public.v2_template_fields where id=fid;
  select * into sec from public.v2_template_sections where id=f.section_id;
  if f.field_type not in ('news_table','news_source_table','unbiased_news') then return new;end if;
  if tg_table_name='v2_news_items' then
    insert into public.v2_collection_news_provenance(workspace_id,session_id,field_id,row_id,storage,sheet_key,logical_key,item_key,source_url,published_at)
      select s.workspace_id,sid,fid,new.id::text,'native',i.sheet_key,i.logical_key,i.item_key,i.source_url,i.published_at
      from public.v2_collection_autofill_items i join public.v2_collection_news_assessments a
        on a.workspace_id=i.workspace_id and a.sheet_key=i.sheet_key and a.logical_key=i.logical_key and a.item_key=i.item_key
      where i.workspace_id=s.workspace_id and a.run_id=current_run_id and a.body=new.body
        and i.sheet_key=public.v2_collection_sheet_key(to_jsonb(sec))
        and i.logical_key=public.v2_collection_field_logical_key(to_jsonb(f))
        and i.source_url=new.source_url and i.published_at=new.occurred_at
        and strpos(new.body,'data-autofill-id="'||encode(digest(i.item_key,'sha256'),'hex')||'"')>0
      order by i.item_key limit 1 on conflict do nothing;
  else
    for row_json in select r from jsonb_array_elements(public.v2_collection_embedded_news_rows(new.value)) r loop
      -- A robot append may retain manual siblings. Only newly created row IDs
      -- can acquire origin, and their source/body must match this exact run.
      if tg_op='UPDATE' and exists(select 1 from jsonb_array_elements(public.v2_collection_embedded_news_rows(old.value)) previous
        where previous->>'id'=row_json->>'id') then continue;end if;
      insert into public.v2_collection_news_provenance(workspace_id,session_id,field_id,row_id,storage,sheet_key,logical_key,item_key,source_url,published_at)
        select s.workspace_id,sid,fid,row_json->>'id','embedded',i.sheet_key,i.logical_key,i.item_key,i.source_url,i.published_at
        from public.v2_collection_autofill_items i join public.v2_collection_news_assessments a
          on a.workspace_id=i.workspace_id and a.sheet_key=i.sheet_key and a.logical_key=i.logical_key and a.item_key=i.item_key
        where i.workspace_id=s.workspace_id and a.run_id=current_run_id and a.body=coalesce(row_json->>'body',row_json->>'text','')
          and i.sheet_key=public.v2_collection_sheet_key(to_jsonb(sec))
          and i.logical_key=public.v2_collection_field_logical_key(to_jsonb(f))
          and i.source_url=row_json->>'source_url'
          and strpos(coalesce(row_json->>'body',row_json->>'text',''),'data-autofill-id="'||encode(digest(i.item_key,'sha256'),'hex')||'"')>0
        order by i.item_key limit 1 on conflict do nothing;
    end loop;
  end if;
  return new;
end $$;
revoke all on function public.v2_capture_collection_news_provenance() from public,anon,authenticated;
drop trigger if exists v2_collection_news_provenance_native on public.v2_news_items;
create trigger v2_collection_news_provenance_native after insert on public.v2_news_items
  for each row execute function public.v2_capture_collection_news_provenance();
drop trigger if exists v2_collection_news_provenance_embedded on public.v2_collection_values;
create trigger v2_collection_news_provenance_embedded after insert or update on public.v2_collection_values
  for each row execute function public.v2_capture_collection_news_provenance();

-- One-time upgrade of existing generated records only. New browser inserts do
-- not run this backfill and cannot self-declare trusted automatic provenance.
do $bootstrap$ begin
if not exists(select 1 from public.v2_collection_news_feedback_bootstrap) then
insert into public.v2_collection_news_provenance(workspace_id,session_id,field_id,row_id,storage,sheet_key,logical_key,item_key,source_url,published_at)
select distinct on(n.id) n.workspace_id,n.collection_session_id,n.field_id,n.id::text,'native',i.sheet_key,i.logical_key,i.item_key,i.source_url,i.published_at
from public.v2_news_items n join public.v2_template_fields f on f.id=n.field_id
join public.v2_template_sections sec on sec.id=f.section_id
join public.v2_collection_autofill_items i on i.workspace_id=n.workspace_id
  and i.sheet_key=public.v2_collection_sheet_key(to_jsonb(sec)) and i.logical_key=public.v2_collection_field_logical_key(to_jsonb(f))
  and i.source_url=n.source_url and i.published_at=n.occurred_at
where n.collection_session_id is not null and (n.updated_origin='chatgpt'
  or strpos(n.body,'data-autofill-id="'||encode(digest(i.item_key,'sha256'),'hex')||'"')>0)
order by n.id,i.item_key on conflict do nothing;
insert into public.v2_collection_news_provenance(workspace_id,session_id,field_id,row_id,storage,sheet_key,logical_key,item_key,source_url,published_at)
select distinct on(v.id,r->>'id') s.workspace_id,v.session_id,v.field_id,r->>'id','embedded',i.sheet_key,i.logical_key,i.item_key,i.source_url,i.published_at
from public.v2_collection_values v join public.v2_collection_sessions s on s.id=v.session_id
join public.v2_template_fields f on f.id=v.field_id join public.v2_template_sections sec on sec.id=f.section_id
cross join lateral jsonb_array_elements(public.v2_collection_embedded_news_rows(v.value)) r
join public.v2_collection_autofill_items i on i.workspace_id=s.workspace_id
  and i.sheet_key=public.v2_collection_sheet_key(to_jsonb(sec)) and i.logical_key=public.v2_collection_field_logical_key(to_jsonb(f))
  and i.source_url=r->>'source_url'
where f.field_type in ('news_table','news_source_table','unbiased_news') and nullif(r->>'id','') is not null
  and strpos(coalesce(r->>'body',r->>'text',''),'data-autofill-id="'||encode(digest(i.item_key,'sha256'),'hex')||'"')>0
order by v.id,r->>'id',i.item_key on conflict do nothing;
insert into public.v2_collection_news_feedback_bootstrap(singleton) values(true);
end if;
end $bootstrap$;

-- Assessment registration runs only for newly accepted service-generated
-- items, in the same transaction as dedup and actual news creation.
do $patch$
declare definition text; needle text:='if inserted=1 then accepted:=accepted||jsonb_build_array(item);end if;';
  replacement text:='if inserted=1 then perform public.v2_register_collection_news_assessment(run.id,item); accepted:=accepted||jsonb_build_array(item);end if;';
  rows_needle text:='next_value:=current_value||jsonb_build_object(''rows'',case when replace_value then ''[]''::jsonb else coalesce(current_value->''rows'',''[]'') end||next_value);';
  rows_replacement text:='/* v2_feedback_canonical_news_rows */ if jsonb_typeof(current_value#>''{structured,rows}'')=''array'' then '
    ||'next_value:=jsonb_set(current_value,''{structured,rows}'',(case when replace_value then ''[]''::jsonb else current_value#>''{structured,rows}'' end)||next_value); '
    ||'else next_value:=current_value||jsonb_build_object(''rows'',(case when replace_value then ''[]''::jsonb else coalesce(current_value->''rows'',''[]'') end)||next_value);end if;';
begin
  definition:=pg_get_functiondef('public.v2_collection_autofill_apply(uuid,jsonb,jsonb,timestamptz)'::regprocedure);
  if strpos(definition,'v2_register_collection_news_assessment')=0 then
    if (length(definition)-length(replace(definition,needle,'')))/length(needle)<>1 then
      raise exception 'Unexpected automation apply body; assessment registration was not installed';end if;
    definition:=replace(definition,needle,replacement);
  end if;
  -- The same news field can still have the imported nested rows layout. Append
  -- into its visible canonical container rather than create hidden top-level
  -- siblings; captions, images and unrelated structured metadata survive.
  if strpos(definition,'v2_feedback_canonical_news_rows')=0 then
    if (length(definition)-length(replace(definition,rows_needle,'')))/length(rows_needle)<>1 then
      raise exception 'Unexpected embedded news append body; canonical layout fix was not installed';end if;
    definition:=replace(definition,rows_needle,rows_replacement);
  end if;
  execute definition;
end $patch$;

create or replace function public.v2_locked_collection_feedback_item(p_workspace_id uuid,p_session_id uuid,p_field_id uuid,p_row_id text)
returns jsonb language plpgsql security definer set search_path=public,pg_temp as $$
declare c public.v2_collection_sessions; p public.v2_collection_news_provenance;
  n public.v2_news_items; v public.v2_collection_values; f public.v2_template_fields; snapshot jsonb;
  r jsonb; pos integer; path text[]; a public.v2_collection_news_assessments;
begin
  c:=public.v2_lock_collection_scenario_session(p_session_id);
  if c.workspace_id is distinct from p_workspace_id then raise exception 'אין הרשאה לגליון.' using errcode='42501';end if;
  select tf.* into f from public.v2_template_fields tf join public.v2_template_sections sec on sec.id=tf.section_id
    where tf.id=p_field_id and tf.active and sec.active and sec.template_id=c.template_id
      and tf.field_type in ('news_table','news_source_table','unbiased_news');
  if not found then raise exception 'תיבת חדשות תקינה נדרשת.' using errcode='23514';end if;
  select * into p from public.v2_collection_news_provenance where workspace_id=p_workspace_id
    and session_id=p_session_id and field_id=p_field_id and row_id=p_row_id;
  if not found then raise exception 'הידיעה אינה מזוהה כהזנה אוטומטית.' using errcode='23514';end if;
  if p.storage='native' then
    select * into n from public.v2_news_items where workspace_id=p_workspace_id and collection_session_id=p_session_id
      and field_id=p_field_id and id::text=p_row_id for update;
    if not found then raise exception 'הידיעה כבר אינה קיימת. רעננו את הגליון.' using errcode='40001';end if;
    snapshot:=jsonb_build_object('native',to_jsonb(n),'importance',n.importance,'body',n.body);
  else
    select * into v from public.v2_collection_values where session_id=p_session_id and field_id=p_field_id for update;
    if v.id is null then raise exception 'הידיעה כבר אינה קיימת.' using errcode='40001';end if;
    path:=case when jsonb_typeof(v.value#>'{structured,rows}')='array' then array['structured','rows'] else array['rows'] end;
    if (select count(*) from jsonb_array_elements(v.value#>path) entry where entry->>'id'=p_row_id)<>1 then
      raise exception 'הידיעה חסרה או שקיימת כפילות; רעננו את הגליון.' using errcode='40001';end if;
    select entry,ordinality::integer-1 into r,pos from jsonb_array_elements(v.value#>path) with ordinality
      as entries(entry,ordinality) where entry->>'id'=p_row_id;
    if r is null then raise exception 'הידיעה כבר אינה קיימת.' using errcode='40001';end if;
    snapshot:=jsonb_build_object('embedded',r,'value_id',v.id,'path',to_jsonb(path),'position',pos,
      'importance',coalesce(r->>'importance','neutral'),'body',coalesce(r->>'body',r->>'text',''));
  end if;
  select * into a from public.v2_collection_news_assessments where workspace_id=p.workspace_id
    and sheet_key=p.sheet_key and logical_key=p.logical_key and item_key=p.item_key;
  return snapshot||to_jsonb(p)||jsonb_build_object('source_name',coalesce(a.source_name,f.title),
    'learning_source_url',coalesce(a.learning_source_url,public.v2_news_learning_source(p.source_url)),
    'model_evaluation',a.model_evaluation);
end $$;
revoke all on function public.v2_locked_collection_feedback_item(uuid,uuid,uuid,text) from public,anon,authenticated;

create or replace function public.v2_read_collection_news_feedback_origins(p_workspace_id uuid,p_session_id uuid,p_field_id uuid)
returns table(row_id text,model_evaluation jsonb) language plpgsql stable security definer set search_path=public,pg_temp as $$
begin
  if auth.uid() is null or not public.v2_is_workspace_member(p_workspace_id) or not exists(
    select 1 from public.v2_collection_sessions s join public.v2_template_sections sec on sec.template_id=s.template_id
    join public.v2_template_fields f on f.section_id=sec.id where s.id=p_session_id and s.workspace_id=p_workspace_id
      and s.deleted_at is null and f.id=p_field_id and f.field_type in ('news_table','news_source_table','unbiased_news')) then
    raise exception 'Workspace news access required' using errcode='42501';end if;
  return query select p.row_id,a.model_evaluation from public.v2_collection_news_provenance p
    left join public.v2_collection_news_assessments a on a.workspace_id=p.workspace_id and a.sheet_key=p.sheet_key
      and a.logical_key=p.logical_key and a.item_key=p.item_key
    where p.workspace_id=p_workspace_id and p.session_id=p_session_id and p.field_id=p_field_id
      and ((p.storage='native' and exists(select 1 from public.v2_news_items n where n.id::text=p.row_id
        and n.workspace_id=p_workspace_id and n.collection_session_id=p_session_id and n.field_id=p_field_id))
      or (p.storage='embedded' and exists(select 1 from public.v2_collection_values v
        cross join lateral jsonb_array_elements(public.v2_collection_embedded_news_rows(v.value)) r
        where v.session_id=p_session_id and v.field_id=p_field_id and r->>'id'=p.row_id)))
    order by p.row_id;
end $$;
revoke all on function public.v2_read_collection_news_feedback_origins(uuid,uuid,uuid) from public,anon;
grant execute on function public.v2_read_collection_news_feedback_origins(uuid,uuid,uuid) to authenticated;

create or replace function public.v2_store_collection_news_feedback(p_item jsonb,p_action text,p_score integer,p_level text,p_sentiment text,p_detail text)
returns void language sql security definer set search_path=public,pg_temp as $$
  insert into public.v2_collection_news_feedback(workspace_id,sheet_key,logical_key,item_key,actor_id,
    session_id,field_id,row_id,action,score,feedback_level,sentiment,detail,body,source_url,learning_source_url,source_name,model_evaluation)
  values((p_item->>'workspace_id')::uuid,p_item->>'sheet_key',p_item->>'logical_key',p_item->>'item_key',auth.uid(),
    (p_item->>'session_id')::uuid,(p_item->>'field_id')::uuid,p_item->>'row_id',p_action,p_score,p_level,p_sentiment,p_detail,
    p_item->>'body',p_item->>'source_url',p_item->>'learning_source_url',p_item->>'source_name',nullif(p_item->'model_evaluation','null'::jsonb))
  on conflict(workspace_id,item_key,actor_id) do update set session_id=excluded.session_id,
    sheet_key=excluded.sheet_key,logical_key=excluded.logical_key,field_id=excluded.field_id,row_id=excluded.row_id,
    action=excluded.action,score=excluded.score,feedback_level=excluded.feedback_level,
    sentiment=excluded.sentiment,detail=excluded.detail,body=excluded.body,source_url=excluded.source_url,
    learning_source_url=excluded.learning_source_url,source_name=excluded.source_name,model_evaluation=excluded.model_evaluation,updated_at=now();
$$;
revoke all on function public.v2_store_collection_news_feedback(jsonb,text,integer,text,text,text) from public,anon,authenticated;

create or replace function public.v2_rate_collection_news_feedback(p_workspace_id uuid,p_session_id uuid,p_field_id uuid,p_row_id text,p_sentiment text)
returns void language plpgsql security definer set search_path=public,pg_temp as $$
declare item jsonb; score integer; level text; path text[];
begin
  item:=public.v2_locked_collection_feedback_item(p_workspace_id,p_session_id,p_field_id,p_row_id);
  score:=case p_sentiment when 'very_bullish' then 3 when 'very_bearish' then 3 when 'bullish' then 2 when 'bearish' then 2
    when 'positive' then 1 when 'negative' then 1 when 'neutral' then 0 end;
  if score is null then raise exception 'דירוג אינו תקין.' using errcode='23514';end if;
  level:=case score when 3 then 'most_important' when 2 then 'very_important' when 1 then 'important' else 'useful' end;
  if item->>'storage'='native' then
    update public.v2_news_items set importance=p_sentiment,rating_pending=false,updated_by=auth.uid()
      where id=(item#>>'{native,id}')::uuid;
  else
    select array_agg(value) into path from jsonb_array_elements_text(item->'path');
    path:=path||array[item->>'position'];
    update public.v2_collection_values set value=jsonb_set(value,path,(item->'embedded')||
      jsonb_build_object('importance',p_sentiment,'rating_pending',false,'updated_by',auth.uid(),'updated_at',now())),updated_by=auth.uid()
      where id=(item->>'value_id')::uuid;
  end if;
  perform public.v2_store_collection_news_feedback(item,'rate',score,level,p_sentiment,null);
end $$;
revoke all on function public.v2_rate_collection_news_feedback(uuid,uuid,uuid,text,text) from public,anon;
grant execute on function public.v2_rate_collection_news_feedback(uuid,uuid,uuid,text,text) to authenticated;

create or replace function public.v2_archive_collection_news_feedback(p_workspace_id uuid,p_session_id uuid,p_field_id uuid,p_row_id text,p_reason text,p_detail text default null)
returns jsonb language plpgsql security definer set search_path=public,pg_temp as $$
declare item jsonb; score integer; trash uuid; path text[]; rows_json jsonb;
begin
  item:=public.v2_locked_collection_feedback_item(p_workspace_id,p_session_id,p_field_id,p_row_id);
  score:=case p_reason when 'borderline' then -1 when 'irrelevant' then -2 when 'completely_irrelevant' then -3 end;
  if score is null or length(p_detail)>1000 then raise exception 'סיבת המחיקה אינה תקינה.' using errcode='23514';end if;
  p_detail:=nullif(btrim(p_detail),'');
  insert into public.v2_collection_news_recycle_bin(workspace_id,session_id,field_id,row_id,sheet_key,logical_key,item_key,
    storage,snapshot,body,source_url,learning_source_url,source_name,importance,model_evaluation,reason,detail,deleted_by)
    values(p_workspace_id,p_session_id,p_field_id,p_row_id,item->>'sheet_key',item->>'logical_key',item->>'item_key',
      item->>'storage',item,item->>'body',item->>'source_url',item->>'learning_source_url',item->>'source_name',
      item->>'importance',nullif(item->'model_evaluation','null'::jsonb),p_reason,p_detail,auth.uid()) returning id into trash;
  perform public.v2_store_collection_news_feedback(item,'delete',score,p_reason,null,p_detail);
  if item->>'storage'='native' then
    delete from public.v2_news_items where id=(item#>>'{native,id}')::uuid;
  else
    select array_agg(value) into path from jsonb_array_elements_text(item->'path');
    select coalesce(jsonb_agg(entry order by ordinality),'[]'::jsonb) into rows_json
      from public.v2_collection_values v cross join lateral jsonb_array_elements(v.value#>path) with ordinality as entries(entry,ordinality)
      where v.id=(item->>'value_id')::uuid and entry->>'id' is distinct from p_row_id;
    update public.v2_collection_values set value=jsonb_set(value,path,rows_json),updated_by=auth.uid() where id=(item->>'value_id')::uuid;
  end if;
  -- Existing pin reads resolve live bodies, so removal never displays a stale
  -- copied answer. Old references are harmless and restoration restores them.
  return jsonb_build_object('trash_id',trash);
end $$;
revoke all on function public.v2_archive_collection_news_feedback(uuid,uuid,uuid,text,text,text) from public,anon;
grant execute on function public.v2_archive_collection_news_feedback(uuid,uuid,uuid,text,text,text) to authenticated;

create or replace function public.v2_read_collection_news_recycle_bin(p_workspace_id uuid,p_session_id uuid default null,p_field_id uuid default null)
returns table(id uuid,workspace_id uuid,session_id uuid,field_id uuid,row_id text,body text,source_name text,source_url text,
  importance text,reason text,detail text,deleted_by uuid,deleted_at timestamptz,restored_by uuid,restored_at timestamptz,model_evaluation jsonb)
language plpgsql stable security definer set search_path=public,pg_temp as $$
begin
  if auth.uid() is null or not public.v2_is_workspace_member(p_workspace_id) then
    raise exception 'Workspace access required' using errcode='42501';end if;
  return query select t.id,t.workspace_id,t.session_id,t.field_id,t.row_id,t.body,t.source_name,t.source_url,t.importance,
    t.reason,t.detail,t.deleted_by,t.deleted_at,t.restored_by,t.restored_at,t.model_evaluation
    from public.v2_collection_news_recycle_bin t where t.workspace_id=p_workspace_id and t.restored_at is null
      and (p_session_id is null or t.session_id=p_session_id) and (p_field_id is null or t.field_id=p_field_id)
    order by t.deleted_at desc,t.id desc;
end $$;
revoke all on function public.v2_read_collection_news_recycle_bin(uuid,uuid,uuid) from public,anon;
grant execute on function public.v2_read_collection_news_recycle_bin(uuid,uuid,uuid) to authenticated;

create or replace function public.v2_restore_collection_news_feedback(p_workspace_id uuid,p_trash_id uuid)
returns void language plpgsql security definer set search_path=public,pg_temp as $$
declare t public.v2_collection_news_recycle_bin; c public.v2_collection_sessions; item jsonb;
  n public.v2_news_items; v public.v2_collection_values; path text[]; rows_json jsonb; restored_rows jsonb;
begin
  select * into t from public.v2_collection_news_recycle_bin where id=p_trash_id and workspace_id=p_workspace_id;
  if not found then raise exception 'אין הרשאה לידיעה שנמחקה.' using errcode='42501';end if;
  c:=public.v2_lock_collection_scenario_session(t.session_id);
  if c.workspace_id is distinct from p_workspace_id then raise exception 'אין הרשאה לגליון.' using errcode='42501';end if;
  select * into t from public.v2_collection_news_recycle_bin where id=p_trash_id for update;
  if t.restored_at is not null then return;end if;
  if not exists(select 1 from public.v2_template_fields f join public.v2_template_sections sec on sec.id=f.section_id
    where f.id=t.field_id and sec.template_id=c.template_id and f.active and sec.active) then
    raise exception 'לא ניתן לשחזר לתיבה שאינה קיימת.' using errcode='23514';end if;
  item:=t.snapshot;
  if t.storage='native' then
    if exists(select 1 from public.v2_news_items where id=(item#>>'{native,id}')::uuid) then
      raise exception 'הידיעה כבר קיימת; השחזור לא ידרוס אותה.' using errcode='40001';end if;
    n:=jsonb_populate_record(null::public.v2_news_items,item->'native');
    insert into public.v2_news_items(id,workspace_id,collection_session_id,field_id,source_id,body,importance,occurred_at,
      created_by,updated_by,created_at,updated_at,source_url,source_name,category,rating_pending)
      values(n.id,p_workspace_id,c.id,t.field_id,n.source_id,n.body,n.importance,n.occurred_at,n.created_by,auth.uid(),n.created_at,
        now(),n.source_url,n.source_name,n.category,n.rating_pending);
  else
    select * into v from public.v2_collection_values where session_id=c.id and field_id=t.field_id for update;
    if v.id is null then raise exception 'לא ניתן לשחזר לתיבה שנמחקה.' using errcode='23514';end if;
    select array_agg(value) into path from jsonb_array_elements_text(item->'path');
    if (cardinality(path)=2 and jsonb_typeof(v.value->'rows')='array' and jsonb_typeof(v.value#>'{structured,rows}') is distinct from 'array')
      or (cardinality(path)=1 and jsonb_typeof(v.value#>'{structured,rows}')='array') then
      raise exception 'מבנה החדשות השתנה; השחזור לא יסתיר ידיעות חדשות.' using errcode='40001';end if;
    rows_json:=coalesce(v.value#>path,'[]'::jsonb);
    if jsonb_typeof(rows_json)<>'array' or exists(select 1 from jsonb_array_elements(rows_json) r where r->>'id'=t.row_id) then
      raise exception 'הידיעה כבר קיימת או שהתיבה השתנתה; לא בוצעה דריסה.' using errcode='40001';end if;
    select coalesce(jsonb_agg(entry order by position),'[]'::jsonb) into restored_rows from (
      select r entry,case when ordinality-1<(item->>'position')::integer then ordinality-1 else ordinality end position
        from jsonb_array_elements(rows_json) with ordinality as rows(r,ordinality)
      union all select item->'embedded',least((item->>'position')::integer,jsonb_array_length(rows_json))::bigint) entries;
    if cardinality(path)=2 and v.value->'structured' is null then
      v.value:=v.value||jsonb_build_object('structured','{}'::jsonb);
    elsif cardinality(path)=2 and jsonb_typeof(v.value->'structured')<>'object' then
      raise exception 'מבנה התיבה השתנה; לא בוצעה דריסה.' using errcode='40001';end if;
    update public.v2_collection_values set value=jsonb_set(v.value,path,restored_rows),updated_by=auth.uid() where id=v.id;
  end if;
  update public.v2_collection_news_recycle_bin set restored_at=now(),restored_by=auth.uid() where id=t.id;
  -- Restoring withdraws only the exact deletion vote. A later explicit rating
  -- by the deleter, or another user's vote, is preserved.
  update public.v2_collection_news_feedback set action='restored',score=null,feedback_level=null,updated_at=now()
    where workspace_id=t.workspace_id and sheet_key=t.sheet_key and logical_key=t.logical_key and item_key=t.item_key
      and actor_id=t.deleted_by and action='delete' and session_id=t.session_id and row_id=t.row_id
      and updated_at=t.deleted_at;
end $$;
revoke all on function public.v2_restore_collection_news_feedback(uuid,uuid) from public,anon;
grant execute on function public.v2_restore_collection_news_feedback(uuid,uuid) to authenticated;

-- Model learning uses all retained votes; only examples are bounded. Canonical
-- item consensus prevents repeat ratings or two-account votes multiplying the
-- source calibration sample. A useful dissent blocks a correct-discard count.
create or replace function public.v2_read_collection_news_learning(p_workspace_id uuid,p_field_id uuid,p_source_url text default null)
returns jsonb language plpgsql stable security definer set search_path=public,pg_temp as $$
declare logical text; learning_source text; result jsonb;
begin
  select public.v2_collection_field_logical_key(to_jsonb(f)) into logical from public.v2_template_fields f
    join public.v2_template_sections sec on sec.id=f.section_id join public.v2_templates t on t.id=sec.template_id
    where f.id=p_field_id and t.workspace_id=p_workspace_id;
  if logical is null then raise exception 'Invalid learning field' using errcode='23514';end if;
  learning_source:=public.v2_news_learning_source(p_source_url);
  with votes as (select * from public.v2_collection_news_feedback where workspace_id=p_workspace_id and action<>'restored'),
  items as (select item_key,learning_source_url,max(score) score,
    bool_or(model_evaluation->>'level'='borderline') borderline,avg(score) average_score from votes
    group by item_key,learning_source_url),
  source_groups as (select learning_source_url,count(*) total,count(distinct item_key) unique_items,
    avg(score) average_score,jsonb_build_object('most_important',count(*) filter(where score=3),'very_important',count(*) filter(where score=2),
    'important',count(*) filter(where score=1),'useful',count(*) filter(where score=0),'borderline',count(*) filter(where score=-1),
    'irrelevant',count(*) filter(where score=-2),'completely_irrelevant',count(*) filter(where score=-3)) counts from items group by learning_source_url),
  source_calibration as (select learning_source_url,count(*) filter(where borderline) predicted,
    count(*) filter(where borderline and score<0) discarded,count(*) filter(where borderline and score>=0) useful
    from items group by learning_source_url),
  examples as (select * from votes order by (learning_source_url=learning_source) desc nulls last,(logical_key=logical) desc,
    updated_at desc,item_key,actor_id limit 24),
  document_terms as (select v.item_key,avg(v.score) score,
    array_agg(distinct lower(token[1])) tokens from votes v
    cross join lateral regexp_matches(regexp_replace(v.body,'<[^>]*>',' ','g'),'([a-zA-Zא-ת][a-zA-Zא-ת0-9]{2,})','g') token
    where lower(token[1]) not in ('the','and','that','with','from','this','https','www','com','for','has','are','was','של','את','על','שלו','הוא','היא')
    group by v.item_key),
  terms as (select term,count(*) count,avg(d.score) score from document_terms d cross join lateral unnest(d.tokens) term
    group by term order by count(*) desc,abs(avg(d.score)) desc,term limit 80)
  select jsonb_build_object('version',1,'total',(select count(*) from items),'uniqueItems',(select count(*) from items),
    'averageScore',(select round(avg(score),4) from items),
    'counts',(select jsonb_build_object('most_important',count(*) filter(where score=3),'very_important',count(*) filter(where score=2),
      'important',count(*) filter(where score=1),'useful',count(*) filter(where score=0),'borderline',count(*) filter(where score=-1),
      'irrelevant',count(*) filter(where score=-2),'completely_irrelevant',count(*) filter(where score=-3)) from items),
    'bySource',coalesce((select jsonb_agg(jsonb_build_object('sourceUrl',g.learning_source_url,'total',g.total,'uniqueItems',g.unique_items,
      'counts',g.counts,'averageScore',round(g.average_score,4),'predictedBorderlineUniqueItems',c.predicted,
      'predictedBorderlineDiscardedItems',c.discarded,'predictedBorderlineUsefulItems',c.useful,
      'borderlineDiscardRate',case when c.predicted>0 then round(c.discarded::numeric/c.predicted,4) else null end)
      order by g.learning_source_url) from source_groups g join source_calibration c using(learning_source_url)),'[]'::jsonb),
    'terms',coalesce((select jsonb_agg(jsonb_build_object('term',term,'count',count,'averageScore',round(score,4)) order by count desc,term) from terms),'[]'::jsonb),
    'examples',coalesce((select jsonb_agg(jsonb_build_object('sourceUrl',learning_source_url,'summary',body,'feedbackLevel',feedback_level,
      'score',score,'note',detail,'model',model_evaluation,'updatedAt',updated_at) order by updated_at desc,item_key,actor_id) from examples),'[]'::jsonb)) into result;
  return result;
end $$;
revoke all on function public.v2_read_collection_news_learning(uuid,uuid,text) from public,anon,authenticated;
grant execute on function public.v2_read_collection_news_learning(uuid,uuid,text) to service_role;
notify pgrst,'reload schema';
