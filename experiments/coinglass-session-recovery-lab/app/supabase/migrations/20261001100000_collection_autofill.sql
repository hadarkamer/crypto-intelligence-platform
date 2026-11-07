-- Durable collection data and an atomic, service-only automation worker.
-- Keep every historical answer. Public writes cannot impersonate ChatGPT.
begin;

alter table public.v2_template_fields drop constraint if exists v2_template_fields_field_type_check;
alter table public.v2_template_fields add constraint v2_template_fields_field_type_check check(field_type in (
 'dynamic_text','rich_text','short_text','long_text','checkbox','yes_no_na','liquidity_table','news_table','news_source_table',
 'unbiased_news','options_expiry','correlation','economic_calendar','number','pnl','journal','image','info','single_choice',
 'multi_choice','datetime','link','stage_conclusion','button','currency_select','reasons_with_evidence','notes','conclusions'));

-- Only the requested named collection fields change type. Their stored values
-- remain intact and the new editor displays old text beside the new controls.
alter table public.v2_template_fields disable trigger v2_used_template_fields_guard;
update public.v2_template_fields f set field_type=case
 when lower(f.title) ~ 'unbiased' then 'unbiased_news'
 when f.title ~ '(אופציות.*יפקעו|מה.*פקיע)' then 'options_expiry'
 else 'correlation' end
from public.v2_template_sections sec join public.v2_templates t on t.id=sec.template_id
where f.section_id=sec.id and t.kind='collection' and
 (lower(f.title) ~ 'unbiased' or f.title ~ '(אופציות.*יפקעו|מה.*פקיע|[וו]יד[או]*[יי]*.*קורלציה רגילה)');
alter table public.v2_template_fields enable trigger v2_used_template_fields_guard;

create function public.v2_collection_sheet_key(p_section jsonb)
returns text language sql immutable set search_path=public,pg_temp as $$
 select case when nullif(btrim(p_section#>>'{settings,sheet_key}'),'') is not null then p_section#>>'{settings,sheet_key}'
 when jsonb_typeof(p_section#>'{settings,groups}')='array' or jsonb_typeof(p_section#>'{settings,legacy_groups}')='array'
 then 'sheet:'||(p_section->>'id') when p_section->>'title' ~ '(חדשות|מאקרו)' then 'news_macro' else 'data' end;
$$;
revoke all on function public.v2_collection_sheet_key(jsonb) from public,anon;
grant execute on function public.v2_collection_sheet_key(jsonb) to authenticated,service_role;

alter table public.v2_collection_values add column updated_origin text not null default 'user' check(updated_origin in ('user','chatgpt'));
alter table public.v2_news_items
 add column updated_origin text not null default 'user' check(updated_origin in ('user','chatgpt')),
 add column source_url text,
 add column source_name text,
 add column category text check(category in ('bullish','bearish')),
 add column rating_pending boolean not null default false;
alter table public.v2_collection_field_activity
 add column actor_origin text not null default 'user' check(actor_origin in ('user','chatgpt')),
 add column workspace_id uuid references public.v2_workspaces(id) on delete cascade,
 add column logical_key text,
 add column sheet_key text;
update public.v2_collection_field_activity e set workspace_id=s.workspace_id,
 logical_key=public.v2_collection_field_logical_key(to_jsonb(f)),sheet_key=public.v2_collection_sheet_key(to_jsonb(sec))
from public.v2_collection_sessions s,public.v2_template_fields f,public.v2_template_sections sec
where s.id=e.session_id and f.id=e.field_id and sec.id=f.section_id;
create index v2_collection_activity_logical_idx on public.v2_collection_field_activity(workspace_id,sheet_key,logical_key,version desc);

create table public.v2_collection_live_values (
 workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
 logical_key text not null,
 id uuid not null default gen_random_uuid(),
 value jsonb not null default '{}',
 updated_by uuid references auth.users(id) on delete set null,
 updated_origin text not null default 'user' check(updated_origin in ('user','chatgpt')),
 created_at timestamptz not null default now(),
 updated_at timestamptz not null default now(),
 primary key(workspace_id,logical_key)
);
alter table public.v2_collection_live_values enable row level security;
create policy v2_collection_live_read on public.v2_collection_live_values for select to authenticated
 using(public.v2_is_workspace_member(workspace_id));
revoke all on public.v2_collection_live_values from public,anon,authenticated;
grant select on public.v2_collection_live_values to authenticated;

insert into public.v2_collection_live_values(workspace_id,logical_key,value,updated_by,created_at,updated_at)
select distinct on(s.workspace_id,public.v2_collection_field_logical_key(to_jsonb(f)))
 s.workspace_id,public.v2_collection_field_logical_key(to_jsonb(f)),v.value,v.updated_by,v.created_at,v.updated_at
from public.v2_collection_values v join public.v2_collection_sessions s on s.id=v.session_id
 join public.v2_template_fields f on f.id=v.field_id join public.v2_template_sections sec on sec.id=f.section_id
where public.v2_collection_sheet_key(to_jsonb(sec))='data' and s.deleted_at is null and s.status<>'archived'
order by s.workspace_id,public.v2_collection_field_logical_key(to_jsonb(f)),v.updated_at desc,s.created_at desc,v.id desc;

create table public.v2_collection_live_seen (
 workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
 logical_key text not null,
 user_id uuid not null references auth.users(id) on delete cascade,
 seen_version bigint not null default 0,
 primary key(workspace_id,logical_key,user_id)
);
alter table public.v2_collection_live_seen enable row level security;
revoke all on public.v2_collection_live_seen from public,anon,authenticated;

create table public.v2_collection_autofill_state (
 workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
 sheet_key text not null,
 logical_key text not null,
 next_due_at timestamptz not null default now(),
 last_scanned_at timestamptz,
 source_cursors jsonb not null default '{}',
 lease_id uuid,
 lease_until timestamptz,
 primary key(workspace_id,sheet_key,logical_key)
);
create table public.v2_collection_autofill_runs (
 id uuid primary key default gen_random_uuid(),
 workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
 field_id uuid not null references public.v2_template_fields(id) on delete cascade,
 sheet_key text not null,
 logical_key text not null,
 manual boolean not null,
 status text not null default 'running' check(status in ('running','completed','unchanged','failed','conflict')),
 baseline jsonb not null default '{}',
 template_revision bigint not null,
 baseline_version bigint not null default 0,
 session_id uuid references public.v2_collection_sessions(id) on delete set null,
 count integer not null default 0,
 message text,
 created_at timestamptz not null default now(),
 finished_at timestamptz
);
create table public.v2_collection_autofill_items (
 workspace_id uuid not null references public.v2_workspaces(id) on delete cascade,
 sheet_key text not null,
 logical_key text not null,
 item_key text not null,
 source_url text not null,
 published_at timestamptz not null,
 primary key(workspace_id,sheet_key,logical_key,item_key)
);
create table public.v2_collection_autofill_write_tokens (
 transaction_id bigint primary key,
 run_id uuid not null references public.v2_collection_autofill_runs(id) on delete cascade
);
alter table public.v2_collection_autofill_state enable row level security;
alter table public.v2_collection_autofill_runs enable row level security;
alter table public.v2_collection_autofill_items enable row level security;
alter table public.v2_collection_autofill_write_tokens enable row level security;
revoke all on public.v2_collection_autofill_state,public.v2_collection_autofill_runs,public.v2_collection_autofill_items,
 public.v2_collection_autofill_write_tokens from public,anon,authenticated;
grant select on public.v2_collection_autofill_runs to authenticated;
create policy v2_autofill_run_read on public.v2_collection_autofill_runs for select to authenticated
 using(public.v2_is_workspace_member(workspace_id));

create function public.v2_collection_write_origin()
returns trigger language plpgsql security definer set search_path=public,pg_temp as $$
declare robot boolean;
begin
 select exists(select 1 from public.v2_collection_autofill_write_tokens where transaction_id=txid_current()) into robot;
 new.updated_origin:=case when robot then 'chatgpt' else 'user' end;
 if robot then new.updated_by:=null;
 elsif auth.uid() is not null then new.updated_by:=auth.uid(); end if;
 if tg_table_name='v2_collection_values' then new.value:=new.value-'_updated_origin';
 elsif not robot and tg_op='UPDATE' and new.importance is distinct from old.importance then new.rating_pending:=false; end if;
 return new;
end $$;
revoke all on function public.v2_collection_write_origin() from public,anon,authenticated;
create trigger v2_collection_values_origin before insert or update on public.v2_collection_values
 for each row execute function public.v2_collection_write_origin();
create trigger v2_collection_news_origin before insert or update on public.v2_news_items
 for each row execute function public.v2_collection_write_origin();

create function public.v2_mirror_collection_data()
returns trigger language plpgsql security definer set search_path=public,pg_temp as $$
declare ws uuid; logical text; contents jsonb; origin text; editor uuid; stamp timestamptz;
begin
 select s.workspace_id,public.v2_collection_field_logical_key(to_jsonb(f)) into ws,logical
 from public.v2_collection_sessions s join public.v2_template_fields f on f.id=new.field_id
 join public.v2_template_sections sec on sec.id=f.section_id
 where s.id=new.session_id and public.v2_collection_sheet_key(to_jsonb(sec))='data';
 if ws is null then return null; end if;
 contents:=new.value;origin:=new.updated_origin;editor:=new.updated_by;stamp:=new.updated_at;
 insert into public.v2_collection_live_values(workspace_id,logical_key,value,updated_by,updated_origin,updated_at)
 values(ws,logical,contents,editor,origin,stamp)
 on conflict(workspace_id,logical_key) do update set value=excluded.value,updated_by=excluded.updated_by,
 updated_origin=excluded.updated_origin,updated_at=excluded.updated_at;
 return null;
end $$;
revoke all on function public.v2_mirror_collection_data() from public,anon,authenticated;
create trigger v2_collection_values_mirror after insert or update on public.v2_collection_values
 for each row execute function public.v2_mirror_collection_data();

-- Native news boxes placed on the permanent sheet also become enduring rows.
-- Purging a historical news session must not erase the permanent worksheet.
create function public.v2_mirror_collection_data_news()
returns trigger language plpgsql security definer set search_path=public,pg_temp as $$
declare entry public.v2_news_items;ws uuid;logical text;contents jsonb;rows_json jsonb;row_json jsonb;
begin
 entry:=case when tg_op='DELETE' then old else new end;
 select s.workspace_id,public.v2_collection_field_logical_key(to_jsonb(f)) into ws,logical
 from public.v2_collection_sessions s join public.v2_template_fields f on f.id=entry.field_id
 join public.v2_template_sections sec on sec.id=f.section_id where s.id=entry.collection_session_id
 and public.v2_collection_sheet_key(to_jsonb(sec))='data' and (tg_op<>'DELETE' or s.deleted_at is null);
 if ws is null then return null;end if;
 select value into contents from public.v2_collection_live_values where workspace_id=ws and logical_key=logical for update;
 contents:=coalesce(contents,'{}');
 select coalesce(jsonb_agg(r),'[]') into rows_json from jsonb_array_elements(coalesce(contents->'rows','[]')) r where r->>'id'<>entry.id::text;
 if tg_op<>'DELETE' then
 row_json:=to_jsonb(entry)||jsonb_build_object('source_name',coalesce(entry.source_name,(select name from public.v2_news_sources where id=entry.source_id)));
 rows_json:=rows_json||jsonb_build_array(row_json);
 end if;
 insert into public.v2_collection_live_values(workspace_id,logical_key,value,updated_by,updated_origin,updated_at)
 values(ws,logical,contents||jsonb_build_object('rows',rows_json),entry.updated_by,entry.updated_origin,now())
 on conflict(workspace_id,logical_key) do update set value=excluded.value,updated_by=excluded.updated_by,updated_origin=excluded.updated_origin,updated_at=excluded.updated_at;
 return null;
end $$;
revoke all on function public.v2_mirror_collection_data_news() from public,anon,authenticated;
create trigger v2_collection_news_mirror after insert or update or delete on public.v2_news_items for each row execute function public.v2_mirror_collection_data_news();
insert into public.v2_collection_live_values(workspace_id,logical_key,value,updated_by,updated_at)
select s.workspace_id,public.v2_collection_field_logical_key(to_jsonb(f)),jsonb_build_object('rows',jsonb_agg(to_jsonb(n)||jsonb_build_object('source_name',src.name) order by n.occurred_at,n.id)),
 (array_agg(n.updated_by order by n.updated_at desc))[1],max(n.updated_at)
from public.v2_news_items n join public.v2_collection_sessions s on s.id=n.collection_session_id
join public.v2_template_fields f on f.id=n.field_id join public.v2_template_sections sec on sec.id=f.section_id
left join public.v2_news_sources src on src.id=n.source_id
where public.v2_collection_sheet_key(to_jsonb(sec))='data' and s.deleted_at is null and s.status<>'archived'
group by s.workspace_id,public.v2_collection_field_logical_key(to_jsonb(f))
on conflict(workspace_id,logical_key) do update set value=v2_collection_live_values.value||jsonb_build_object('rows',coalesce(v2_collection_live_values.value->'rows','[]')||excluded.value->'rows'),
updated_at=greatest(v2_collection_live_values.updated_at,excluded.updated_at);

-- The same RPC serves a new, old, and empty session. A deliberate blank live
-- answer overlays older filled answers instead of resurrecting stale content.
create function public.v2_collection_current_values(p_session_id uuid)
returns setof public.v2_collection_values language plpgsql stable security definer set search_path=public,pg_temp as $$
declare s public.v2_collection_sessions;
begin
 select * into s from public.v2_collection_sessions where id=p_session_id;
 if s.id is null or auth.uid() is null or not coalesce(public.v2_is_workspace_member(s.workspace_id),false) then
 raise exception 'Collection access required' using errcode='42501';end if;
 return query select coalesce(l.id,v.id),s.id,f.id,
 coalesce(l.value,v.value,'{}')||jsonb_build_object('_updated_origin',coalesce(l.updated_origin,v.updated_origin,'user')),
 coalesce(l.updated_by,v.updated_by),coalesce(l.created_at,v.created_at),coalesce(l.updated_at,v.updated_at),
 coalesce(l.updated_origin,v.updated_origin,'user')
 from public.v2_template_sections sec join public.v2_template_fields f on f.section_id=sec.id
 left join public.v2_collection_values v on v.session_id=s.id and v.field_id=f.id
 left join public.v2_collection_live_values l on l.workspace_id=s.workspace_id
 and l.logical_key=public.v2_collection_field_logical_key(to_jsonb(f)) and public.v2_collection_sheet_key(to_jsonb(sec))='data'
 where sec.template_id=s.template_id and sec.active and f.active and (l.id is not null or v.id is not null);
end $$;
revoke all on function public.v2_collection_current_values(uuid) from public,anon;
grant execute on function public.v2_collection_current_values(uuid) to authenticated;

create function public.v2_save_collection_value(p_session_id uuid,p_field_id uuid,p_value jsonb)
returns void language plpgsql security definer set search_path=public,pg_temp as $$
declare ws uuid;
begin
 select workspace_id into ws from public.v2_collection_sessions where id=p_session_id;
 if auth.uid() is null or ws is null or not coalesce(public.v2_can_edit_workspace(ws),false) then
 raise exception 'Collection edit permission required' using errcode='42501';end if;
 if jsonb_typeof(p_value) is distinct from 'object' then raise exception 'Invalid collection value';end if;
 perform 1 from public.v2_workspaces where id=ws for update;
 perform 1 from public.v2_collection_sessions where id=p_session_id for update;
 insert into public.v2_collection_values(session_id,field_id,value,updated_by)
 values(p_session_id,p_field_id,p_value,auth.uid()) on conflict(session_id,field_id)
 do update set value=excluded.value,updated_by=auth.uid();
end $$;
revoke all on function public.v2_save_collection_value(uuid,uuid,jsonb) from public,anon;
grant execute on function public.v2_save_collection_value(uuid,uuid,jsonb) to authenticated;

create function public.v2_lock_collection_workspace(p_session_id uuid)
returns void language plpgsql security definer set search_path=public,pg_temp as $$
declare ws uuid;
begin
 select workspace_id into ws from public.v2_collection_sessions where id=p_session_id;
 if auth.uid() is null or ws is null or not coalesce(public.v2_can_edit_workspace(ws),false) then
 raise exception 'Collection edit permission required' using errcode='42501';end if;
 perform 1 from public.v2_workspaces where id=ws for update;
end $$;
create function public.v2_collection_locked_live_value(p_session_id uuid,p_field_id uuid)
returns jsonb language plpgsql security definer set search_path=public,pg_temp as $$
declare contents jsonb;
begin
 perform public.v2_lock_collection_workspace(p_session_id);
 select live.value into contents from public.v2_collection_live_values live
 join public.v2_collection_sessions s on s.id=p_session_id and s.workspace_id=live.workspace_id
 join public.v2_template_sections sec on sec.template_id=s.template_id
 join public.v2_template_fields f on f.section_id=sec.id and f.id=p_field_id
 where live.logical_key=public.v2_collection_field_logical_key(to_jsonb(f))
 and public.v2_collection_sheet_key(to_jsonb(sec))='data' for update of live;
 return contents;
end $$;
revoke all on function public.v2_lock_collection_workspace(uuid),public.v2_collection_locked_live_value(uuid,uuid) from public,anon;
grant execute on function public.v2_lock_collection_workspace(uuid),public.v2_collection_locked_live_value(uuid,uuid) to authenticated;

alter function public.v2_collection_snapshot(uuid,text,uuid) rename to v2_collection_historical_snapshot;
create function public.v2_collection_snapshot(p_workspace_id uuid,p_mode text default 'latest_filled',p_session_id uuid default null)
returns table(sheet_key text,logical_key text,field_id uuid,field jsonb,section jsonb,session_id uuid,session_created_at timestamptz,
 value jsonb,news_items jsonb,updated_at timestamptz,updated_by uuid,snapshot_key text)
language plpgsql stable security definer set search_path=public,pg_temp as $$
declare sid uuid; tid uuid; created timestamptz;
begin
 if auth.uid() is null or not coalesce(public.v2_is_workspace_member(p_workspace_id),false) then
 raise exception 'Workspace membership required' using errcode='42501';end if;
 -- The historical RPC also validates modes and an explicitly requested session.
 return query select h.sheet_key,h.logical_key,h.field_id,h.field,h.section,h.session_id,h.session_created_at,
 h.value||jsonb_build_object('_updated_origin',coalesce(v.updated_origin,'user')),h.news_items,h.updated_at,h.updated_by,h.snapshot_key
 from public.v2_collection_historical_snapshot(p_workspace_id,p_mode,p_session_id) h
 left join public.v2_collection_values v on v.session_id=h.session_id and v.field_id=h.field_id
 where h.sheet_key<>'data';
 select s.id,s.template_id,s.created_at into sid,tid,created from public.v2_collection_sessions s
 where s.workspace_id=p_workspace_id and s.deleted_at is null and s.status<>'archived'
 and (p_mode<>'session' or s.id=p_session_id) order by s.created_at desc,s.id desc limit 1;
 if tid is null then return;end if;
 return query select 'data'::text,l.logical_key,f.id,to_jsonb(f),to_jsonb(sec),sid,created,
 l.value||jsonb_build_object('_updated_origin',l.updated_origin),'[]'::jsonb,l.updated_at,l.updated_by,'data:'||l.logical_key
 from public.v2_template_sections sec join public.v2_template_fields f on f.section_id=sec.id and f.active
 join public.v2_collection_live_values l on l.workspace_id=p_workspace_id
 and l.logical_key=public.v2_collection_field_logical_key(to_jsonb(f))
 where sec.template_id=tid and sec.active and public.v2_collection_sheet_key(to_jsonb(sec))='data';
end $$;
revoke all on function public.v2_collection_snapshot(uuid,text,uuid) from public,anon;
grant execute on function public.v2_collection_snapshot(uuid,text,uuid) to authenticated;

-- Extend the existing no-op/cascade protections rather than replace them.
do $$ declare definition text;begin
 definition:=pg_get_functiondef('public.v2_validate_news_item_links()'::regprocedure);
 definition:=replace(definition,'''news_table'', ''news_source_table''','''news_table'', ''news_source_table'', ''unbiased_news''');
 if strpos(definition,'unbiased_news')=0 then raise exception 'Unexpected news reference guard';end if;
 execute definition;
 definition:=pg_get_functiondef('public.v2_record_collection_field_activity()'::regprocedure);
 definition:=replace(definition,'insert into public.v2_collection_field_activity(session_id,field_id,actor_id)'
 ||E'\n    values(sid,fid,actor) returning version into event_version;',
 'if exists(select 1 from public.v2_collection_autofill_write_tokens where transaction_id=txid_current()) then actor:=null; end if;'
 ||E'\n  insert into public.v2_collection_field_activity(session_id,field_id,actor_id,actor_origin,workspace_id,logical_key,sheet_key)'
 ||E'\n  select sid,fid,actor,case when exists(select 1 from public.v2_collection_autofill_write_tokens where transaction_id=txid_current()) then ''chatgpt'' else ''user'' end,'
 ||'s.workspace_id,public.v2_collection_field_logical_key(to_jsonb(f)),public.v2_collection_sheet_key(to_jsonb(sec))'
 ||' from public.v2_collection_sessions s join public.v2_template_fields f on f.id=fid join public.v2_template_sections sec on sec.id=f.section_id'
 ||' where s.id=sid returning version into event_version;');
 if strpos(definition,'actor_origin')=0 then raise exception 'Unexpected activity function';end if;
 definition:=replace(definition,'if actor is not null then',
 'if actor is not null then insert into public.v2_collection_live_seen(workspace_id,logical_key,user_id,seen_version)'
 ||' select workspace_id,logical_key,actor,event_version from public.v2_collection_field_activity where version=event_version and sheet_key=''data'''
 ||' on conflict(workspace_id,logical_key,user_id) do update set seen_version=greatest(v2_collection_live_seen.seen_version,excluded.seen_version);');
 execute definition;
end $$;

-- Extend completion and history to the new structured values and enduring data.
do $$ declare definition text; needle text;begin
 definition:=pg_get_functiondef('public.patch_v2_entity_rows(text,uuid,uuid,text,text,jsonb,text)'::regprocedure);
 definition:=replace(definition,'''liquidity_table'',''news_table'',''news_source_table''','''liquidity_table'',''news_table'',''news_source_table'',''unbiased_news''');
 needle:='-- Lock the parent before reading: two first writes cannot both overwrite an absent value row.';
 definition:=replace(definition,needle,
 'if p_scope=''collection'' then perform public.v2_lock_collection_workspace(p_entity_id);end if;'||E'\n '||needle);
 needle:='v:=coalesce(v,''{}'');';
 definition:=replace(definition,needle,
 'if p_scope=''collection'' then r:=public.v2_collection_locked_live_value(p_entity_id,p_field_id);'
 ||' if r is not null then v:=r;end if;end if;'||E'\n '||needle);
 if strpos(definition,'v2_collection_locked_live_value')=0 then raise exception 'Unexpected structured patch function';end if;
 execute definition;
 definition:=pg_get_functiondef('public.v2_field_complete(text,jsonb)'::regprocedure);
 definition:=replace(definition,'if p_type in (''info'',''button'') then return true; end if;',
 'if p_type=''correlation'' then return coalesce(v->>''qqq'','''')~''^[+-]?[0-9]+([.][0-9]+)?$'' and coalesce(v->>''spy'','''')~''^[+-]?[0-9]+([.][0-9]+)?$'';end if;'
 ||' if p_type=''options_expiry'' then return exists(select 1 from jsonb_array_elements(case when jsonb_typeof(v->''dates'')=''array'' then v->''dates'' else ''[]''::jsonb end) d'
 ||' where nullif(d->>''date'','''') is not null and exists(select 1 from jsonb_array_elements(case when jsonb_typeof(d->''exchanges'')=''array'' then d->''exchanges'' else ''[]''::jsonb end) e'
 ||' where nullif(e->>''price'','''') is not null or nullif(e->>''amount'','''') is not null));end if;'
 ||' if p_type in (''info'',''button'') then return true;end if;');
 definition:=replace(definition,'''news_table'',''news_source_table''','''news_table'',''news_source_table'',''unbiased_news''');
 if strpos(definition,'options_expiry')=0 then raise exception 'Unexpected completion function';end if;
 execute definition;
 definition:=pg_get_functiondef('public.v2_collection_history(uuid,boolean)'::regprocedure);
 needle:='left join public.v2_collection_values v on v.session_id=s.id and v.field_id=f.id';
 definition:=replace(definition,needle,
 'left join lateral(select live.value,live.updated_at,live.updated_by from public.v2_collection_live_values live'
 ||' where live.workspace_id=s.workspace_id and live.logical_key=public.v2_collection_field_logical_key(to_jsonb(f))'
 ||' and public.v2_collection_sheet_key(to_jsonb(sec))=''data'' union all select historical.value,historical.updated_at,historical.updated_by'
 ||' from public.v2_collection_values historical where historical.session_id=s.id and historical.field_id=f.id and not exists('
 ||'select 1 from public.v2_collection_live_values live where live.workspace_id=s.workspace_id and live.logical_key=public.v2_collection_field_logical_key(to_jsonb(f))'
 ||' and public.v2_collection_sheet_key(to_jsonb(sec))=''data'')) v on true');
 if strpos(definition,'v2_collection_live_values')=0 then raise exception 'Unexpected history function';end if;
 execute definition;
end $$;

create or replace function public.v2_collection_activity(p_workspace_id uuid)
returns table(session_id uuid,is_new boolean,unread jsonb) language plpgsql stable security definer set search_path=public,pg_temp as $$
begin
 if auth.uid() is null or not coalesce(public.v2_is_workspace_member(p_workspace_id),false) then
 raise exception 'Workspace membership required' using errcode='42501';end if;
 return query select s.id,st.user_id is null and (s.created_by<>auth.uid() or s.creation_origin='chatgpt'),
 case when st.user_id is null then '[]'::jsonb else coalesce(a.items,'[]'::jsonb) end
 from public.v2_collection_sessions s
 left join public.v2_collection_user_state st on st.session_id=s.id and st.user_id=auth.uid()
 left join lateral (
 select jsonb_agg(jsonb_build_object('field_id',e.visible_id,'title',e.title,'actor_id',e.actor_id,'actor_origin',e.actor_origin,
 'actor_label',case when e.actor_origin='chatgpt' then 'ChatGPT' else public.v2_collection_actor_label(e.actor_id) end,
 'updated_at',e.changed_at,'version',e.version::text) order by e.version desc) items
 from (
 select distinct on(f.id) ev.*,f.id visible_id,f.title from public.v2_template_sections sec
 join public.v2_template_fields f on f.section_id=sec.id and f.active
 join public.v2_collection_field_activity ev on ev.workspace_id=s.workspace_id and
 ((ev.sheet_key='data' and public.v2_collection_sheet_key(to_jsonb(sec))='data'
 and ev.logical_key=public.v2_collection_field_logical_key(to_jsonb(f))) or (ev.session_id=s.id and ev.field_id=f.id and ev.sheet_key<>'data'))
 left join public.v2_collection_field_seen seen on seen.session_id=s.id and seen.field_id=f.id and seen.user_id=auth.uid()
 left join public.v2_collection_live_seen live on live.workspace_id=s.workspace_id and live.logical_key=ev.logical_key and live.user_id=auth.uid()
 where sec.template_id=s.template_id and sec.active and ev.actor_id is distinct from auth.uid()
 and ev.version>case when ev.sheet_key='data' then coalesce(live.seen_version,st.baseline_version,0)
 else greatest(coalesce(st.baseline_version,0),coalesce(seen.seen_version,0)) end order by f.id,ev.version desc
 ) e) a on st.user_id is not null
 where s.workspace_id=p_workspace_id and s.deleted_at is null and s.status<>'archived';
end $$;

create or replace function public.v2_ack_collection_field(p_session_id uuid,p_field_id uuid,p_observed_version bigint)
returns void language plpgsql security definer set search_path=public,pg_temp as $$
declare ws uuid; logical text; sheet text;
begin
 select s.workspace_id,public.v2_collection_field_logical_key(to_jsonb(f)),public.v2_collection_sheet_key(to_jsonb(sec)) into ws,logical,sheet
 from public.v2_collection_sessions s join public.v2_template_sections sec on sec.template_id=s.template_id
 join public.v2_template_fields f on f.section_id=sec.id and f.id=p_field_id
 where s.id=p_session_id and s.deleted_at is null and public.v2_is_workspace_member(s.workspace_id) for update of s;
 if auth.uid() is null or ws is null then raise exception 'Collection access required' using errcode='42501';end if;
 if not exists(select 1 from public.v2_collection_user_state where session_id=p_session_id and user_id=auth.uid()) then
 raise exception 'Open this collection first' using errcode='23514';end if;
 if not exists(select 1 from public.v2_collection_field_activity e where e.version=p_observed_version and e.workspace_id=ws
 and ((sheet='data' and e.sheet_key='data' and e.logical_key=logical) or (e.session_id=p_session_id and e.field_id=p_field_id))) then
 raise exception 'Unknown observed field version' using errcode='22023';end if;
 if sheet='data' then
 insert into public.v2_collection_live_seen(workspace_id,logical_key,user_id,seen_version) values(ws,logical,auth.uid(),p_observed_version)
 on conflict(workspace_id,logical_key,user_id) do update set seen_version=greatest(v2_collection_live_seen.seen_version,excluded.seen_version);
 else
 insert into public.v2_collection_field_seen(session_id,field_id,user_id,seen_version) values(p_session_id,p_field_id,auth.uid(),p_observed_version)
 on conflict(session_id,field_id,user_id) do update set seen_version=greatest(v2_collection_field_seen.seen_version,excluded.seen_version);
 end if;
end $$;

-- Service-only functions are intentionally not available to the browser.
create function public.v2_collection_autofill_known_items(p_run_id uuid,p_keys text[])
returns setof text language sql stable security definer set search_path=public,pg_temp as $$
 select i.item_key from public.v2_collection_autofill_runs r join public.v2_collection_autofill_items i
 on i.workspace_id=r.workspace_id and i.sheet_key=r.sheet_key and i.logical_key=r.logical_key
 where r.id=p_run_id and i.item_key=any(p_keys);
$$;
revoke all on function public.v2_collection_autofill_known_items(uuid,text[]) from public,anon,authenticated;
grant execute on function public.v2_collection_autofill_known_items(uuid,text[]) to service_role;

create function public.v2_collection_autofill_due(p_limit integer default 250)
returns table(field_id uuid,session_id uuid) language sql stable security definer set search_path=public,pg_temp as $$
 select f.id,s.id from public.v2_workspaces w
 join lateral (select c.* from public.v2_collection_sessions c where c.workspace_id=w.id and c.deleted_at is null and c.status<>'archived'
 order by c.created_at desc,c.id desc limit 1) s on true
 join public.v2_template_sections sec on sec.template_id=s.template_id and sec.active
 join public.v2_template_fields f on f.section_id=sec.id and f.active
 left join public.v2_collection_autofill_state state on state.workspace_id=w.id and state.sheet_key=public.v2_collection_sheet_key(to_jsonb(sec))
 and state.logical_key=public.v2_collection_field_logical_key(to_jsonb(f))
 where f.settings#>>'{autofill,enabled}'='true' and coalesce(state.next_due_at,now())<=now()
 and coalesce(state.lease_until,'-infinity')<=now() order by coalesce(state.next_due_at,'-infinity'),f.id limit least(greatest(p_limit,1),500);
$$;

create function public.v2_collection_autofill_claim(p_field_id uuid,p_session_id uuid,p_manual boolean default false)
returns jsonb language plpgsql security definer set search_path=public,pg_temp as $$
 declare f public.v2_template_fields; sec public.v2_template_sections; s public.v2_collection_sessions;
 state public.v2_collection_autofill_state; rid uuid; logical text; sheet text; hours integer; baseline jsonb; rev bigint;
begin
 select * into f from public.v2_template_fields where id=p_field_id and active;
 select * into sec from public.v2_template_sections where id=f.section_id and active;
 select * into s from public.v2_collection_sessions where id=p_session_id and deleted_at is null and status<>'archived';
 if s.id is null or sec.template_id is distinct from s.template_id or f.settings#>>'{autofill,enabled}' is distinct from 'true' then
 raise exception 'Active automation and a matching collection are required' using errcode='23514';end if;
 logical:=public.v2_collection_field_logical_key(to_jsonb(f));sheet:=public.v2_collection_sheet_key(to_jsonb(sec));
 perform 1 from public.v2_workspaces where id=s.workspace_id for update;
 -- A manual button on an archived-in-history sheet still scans the current
 -- news field and takes its baseline, instructions and template revision.
 if sheet='news_macro' then
 select * into s from public.v2_collection_sessions where id=public.v2_collection_autofill_target(s.workspace_id,s.template_id);
 select target.* into f from public.v2_template_sections target_sec join public.v2_template_fields target on target.section_id=target_sec.id
 where target_sec.template_id=s.template_id and target_sec.active and target.active
 and public.v2_collection_sheet_key(to_jsonb(target_sec))=sheet and public.v2_collection_field_logical_key(to_jsonb(target))=logical limit 1;
 select * into sec from public.v2_template_sections where id=f.section_id;
 if f.id is null or f.settings#>>'{autofill,enabled}' is distinct from 'true' then
 raise exception 'The latest news field must have active automation' using errcode='23514';end if;
 end if;
 select editor_revision into rev from public.v2_templates where id=s.template_id;
 insert into public.v2_collection_autofill_state(workspace_id,sheet_key,logical_key) values(s.workspace_id,sheet,logical) on conflict do nothing;
 select * into state from public.v2_collection_autofill_state where workspace_id=s.workspace_id and sheet_key=sheet and logical_key=logical for update;
 if state.lease_until>now() then return jsonb_build_object('run_id',state.lease_id,'status','running');end if;
 if state.lease_id is not null then update public.v2_collection_autofill_runs set status='failed',message='הסריקה הקודמת חרגה מזמן ההמתנה.',finished_at=now() where id=state.lease_id and status='running';end if;
 if not p_manual and state.next_due_at>now() then return null;end if;
 if sheet='data' then select l.value into baseline from public.v2_collection_live_values l where l.workspace_id=s.workspace_id and l.logical_key=logical;
 else select v.value into baseline from public.v2_collection_values v where v.session_id=s.id and v.field_id=f.id;end if;
 baseline:=coalesce(baseline,'{}');
 insert into public.v2_collection_autofill_runs(workspace_id,field_id,sheet_key,logical_key,manual,baseline,template_revision,session_id,baseline_version)
 values(s.workspace_id,f.id,sheet,logical,p_manual,baseline,rev,s.id,coalesce((select max(version) from public.v2_collection_field_activity
 where workspace_id=s.workspace_id and sheet_key=sheet and logical_key=logical and (sheet='data' or session_id=s.id)),0)) returning id into rid;
 hours:=case when coalesce(f.settings#>>'{autofill,interval_hours}','')~'^(?:[1-9]|1[0-9]|2[0-4])$'
 then (f.settings#>>'{autofill,interval_hours}')::integer else 24 end;
 update public.v2_collection_autofill_state set lease_id=rid,lease_until=now()+interval '20 minutes',
 next_due_at=case when p_manual then next_due_at else state.next_due_at+(floor(extract(epoch from(now()-state.next_due_at))/(hours*3600))+1)*hours*interval '1 hour' end
 where workspace_id=s.workspace_id and sheet_key=sheet and logical_key=logical;
 return jsonb_build_object('run_id',rid,'status','claimed','field',to_jsonb(f),'section',to_jsonb(sec),'session',to_jsonb(s),
 'baseline',baseline,'source_cursors',state.source_cursors,'last_scanned_at',state.last_scanned_at);
end $$;

create function public.v2_collection_autofill_target(p_workspace_id uuid,p_template_id uuid)
returns uuid language plpgsql security definer set search_path=public,pg_temp as $$
declare s public.v2_collection_sessions; policy jsonb; mode text; days integer; threshold integer; ready boolean; owner uuid;
begin
 perform 1 from public.v2_workspaces where id=p_workspace_id for update;
 select * into s from public.v2_collection_sessions where workspace_id=p_workspace_id and deleted_at is null and status<>'archived'
 order by created_at desc,id desc limit 1 for update;
 if s.id is not null then p_template_id:=s.template_id;end if;
 select sec.settings->'autofill_rollover' into policy from public.v2_template_sections sec
 where sec.template_id=p_template_id and sec.active and public.v2_collection_sheet_key(to_jsonb(sec))='news_macro' order by sec.position limit 1;
 mode:=coalesce(policy->>'mode','days');days:=case when coalesce(policy->>'days','')~'^[1-7]$' then (policy->>'days')::integer else 1 end;
 ready:=s.id is null or (mode='days' and s.created_at<=now()-days*interval '24 hours');
 if s.id is not null and mode in ('light','heavy') then
 threshold:=case when mode='light' then 5 else 10 end;
 select count(*)>0 and bool_and(public.v2_collection_item_count(f.id,s.id)>=threshold) into ready
 from public.v2_template_sections sec join public.v2_template_fields f on f.section_id=sec.id and f.active
 where sec.template_id=s.template_id and sec.active and public.v2_collection_sheet_key(to_jsonb(sec))='news_macro'
 and f.field_type not in ('info','button','checkbox','yes_no_na','economic_calendar');
 end if;
 if ready then
 select owner_id into owner from public.v2_workspaces where id=p_workspace_id;
 insert into public.v2_collection_sessions(workspace_id,template_id,title,created_by,creation_origin)
 values(p_workspace_id,p_template_id,'חדשות · '||to_char(now() at time zone 'Asia/Jerusalem','DD.MM.YYYY HH24:MI'),owner,'chatgpt') returning * into s;
 end if;
 return s.id;
end $$;

create function public.v2_collection_item_count(p_field_id uuid,p_session_id uuid)
returns integer language plpgsql stable security definer set search_path=public,pg_temp as $$
declare v jsonb; rows jsonb; body text; automatic integer; manual integer; native integer;
pattern text:='(?is)<div[^>]*?\sdata-autofill-id="[^"]*?"[^>]*?>.*?</div>';
begin
 select count(*)::integer into native from public.v2_news_items n where n.field_id=p_field_id and n.collection_session_id=p_session_id and nullif(btrim(n.body),'') is not null;
 select value into v from public.v2_collection_values where session_id=p_session_id and field_id=p_field_id;
 rows:=case when jsonb_typeof(v->'rows')='array' then v->'rows' when jsonb_typeof(v#>'{structured,rows}')='array' then v#>'{structured,rows}' end;
 if rows is not null then
 select count(*)::integer into manual from jsonb_array_elements(rows) r where nullif(btrim(regexp_replace(coalesce(r->>'body',''),'<[^>]*>','','g')),'') is not null;
 return native+manual;
 end if;
 body:=coalesce(v->>'text','');
 -- One AI item has both summary and attribution paragraphs. Count its outer
 -- container once, then count only the remaining human-written paragraphs.
 select count(*)::integer into automatic from regexp_matches(body,pattern,'g');
 body:=regexp_replace(body,pattern,'','g');
 select count(*)::integer into manual from regexp_split_to_table(body,'(?i)</p>\s*<p[^>]*>|</div>\s*<div[^>]*>|\n\s*\n') p
 where nullif(btrim(replace(regexp_replace(p,'<[^>]*>','','g'),'&nbsp;','')),'') is not null;
 return native+automatic+manual;
end;
$$;

create function public.v2_collection_autofill_apply(p_run_id uuid,p_payload jsonb,p_cursors jsonb,p_scan_started_at timestamptz)
returns jsonb language plpgsql security definer set search_path=public,pg_temp as $$
declare run public.v2_collection_autofill_runs; state public.v2_collection_autofill_state; f public.v2_template_fields;
 sid uuid; fid uuid; owner uuid; current_value jsonb; next_value jsonb; accepted jsonb:='[]'; item jsonb; src uuid; inserted integer; amount integer:=0;
 replace_value boolean; rev bigint; final_status text;
begin
 select * into run from public.v2_collection_autofill_runs where id=p_run_id;
 if run.id is null then raise exception 'Unknown run';end if;
 perform 1 from public.v2_workspaces where id=run.workspace_id for update;
 select * into state from public.v2_collection_autofill_state where workspace_id=run.workspace_id and sheet_key=run.sheet_key and logical_key=run.logical_key for update;
 if run.status<>'running' then return jsonb_build_object('run_id',run.id,'status',run.status,'session_id',run.session_id,'count',run.count,'message',run.message);end if;
 if state.lease_id is distinct from run.id or state.lease_until<now() then raise exception 'Expired automation lease';end if;
 select * into f from public.v2_template_fields where id=run.field_id and active;
 select t.editor_revision into rev from public.v2_templates t join public.v2_template_sections sec on sec.template_id=t.id where sec.id=f.section_id;
 if rev is distinct from run.template_revision or f.settings#>>'{autofill,enabled}' is distinct from 'true' then
 final_status:='conflict';
 else
 sid:=run.session_id;fid:=run.field_id;
 if run.sheet_key='data' then select value into current_value from public.v2_collection_live_values where workspace_id=run.workspace_id and logical_key=run.logical_key for update;
 else select value into current_value from public.v2_collection_values where session_id=sid and field_id=fid for update;end if;
 current_value:=coalesce(current_value,'{}');
 replace_value:=p_payload->>'write_mode'='replace';
 if run.sheet_key='news_macro' and (jsonb_array_length(coalesce(p_payload->'items','[]'))>0 or p_payload ? 'value') then
 sid:=public.v2_collection_autofill_target(run.workspace_id,(select template_id from public.v2_collection_sessions where id=run.session_id));
 select target.id into fid from public.v2_collection_sessions s join public.v2_template_sections sec on sec.template_id=s.template_id
 join public.v2_template_fields target on target.section_id=sec.id and target.active
 where s.id=sid and public.v2_collection_field_logical_key(to_jsonb(target))=run.logical_key
 and public.v2_collection_sheet_key(to_jsonb(sec))=run.sheet_key and target.field_type=f.field_type limit 1;
 if fid is null or fid is distinct from run.field_id then raise exception 'The target template changed during the scan';end if;
 select value into current_value from public.v2_collection_values where session_id=sid and field_id=fid for update;
 current_value:=coalesce(current_value,'{}');
 end if;
 if replace_value and (current_value is distinct from run.baseline or exists(select 1 from public.v2_collection_field_activity
 where workspace_id=run.workspace_id and sheet_key=run.sheet_key and logical_key=run.logical_key and version>run.baseline_version
 and (run.sheet_key='data' or session_id=sid)))
 and not (run.sheet_key='news_macro' and sid is distinct from run.session_id and current_value='{}'::jsonb
 and not exists(select 1 from public.v2_news_items where collection_session_id=sid and field_id=fid)) then final_status:='conflict';
 else
 -- Dedup is part of this transaction, including item registration and writes.
 for item in select * from jsonb_array_elements(coalesce(p_payload->'items','[]')) loop
 insert into public.v2_collection_autofill_items(workspace_id,sheet_key,logical_key,item_key,source_url,published_at)
 values(run.workspace_id,run.sheet_key,run.logical_key,item->>'key',item->>'url',(item->>'published_at')::timestamptz) on conflict do nothing;
 get diagnostics inserted=row_count;
 if inserted=1 then accepted:=accepted||jsonb_build_array(item);end if;
 end loop;
 amount:=jsonb_array_length(accepted);
 if amount>0 or p_payload ? 'value' then
 insert into public.v2_collection_autofill_write_tokens(transaction_id,run_id) values(txid_current(),run.id);
 select owner_id into owner from public.v2_workspaces where id=run.workspace_id;
 if f.field_type in ('news_table','news_source_table','unbiased_news') and run.sheet_key<>'data' then
 if replace_value and amount>0 then
 delete from public.v2_news_items where collection_session_id=sid and field_id=fid;
 -- Converted templates may still contain embedded legacy news rows.
 update public.v2_collection_values set value=value-'rows'-'structured'-'text',updated_at=now() where session_id=sid and field_id=fid;
 end if;
 for item in select * from jsonb_array_elements(accepted) loop
 select id into src from public.v2_news_sources where workspace_id=run.workspace_id and lower(name)=lower(item->>'source_name') limit 1;
 if src is null then insert into public.v2_news_sources(workspace_id,name,status_defined)
 values(run.workspace_id,item->>'source_name',false) on conflict do nothing;
 select id into src from public.v2_news_sources where workspace_id=run.workspace_id and lower(name)=lower(item->>'source_name') limit 1;end if;
 insert into public.v2_news_items(workspace_id,collection_session_id,field_id,source_id,source_url,source_name,body,importance,category,rating_pending,occurred_at,created_by)
 values(run.workspace_id,sid,fid,src,item->>'url',item->>'source_name',item->>'html',coalesce(item->>'importance','neutral'),
 nullif(item->>'category',''),coalesce((item->>'rating_pending')::boolean,false),(item->>'published_at')::timestamptz,owner);
 end loop;
 else
 if f.field_type in ('news_table','news_source_table','unbiased_news') then
 select coalesce(jsonb_agg(jsonb_build_object('id',gen_random_uuid(),'body',i->>'html','importance',i->>'importance',
 'source_name',i->>'source_name','source_url',i->>'url','category',i->'category','rating_pending',i->'rating_pending',
 'occurred_at',i->'published_at','updated_at',now(),'updated_origin','chatgpt')),'[]') into next_value from jsonb_array_elements(accepted) i;
 next_value:=current_value||jsonb_build_object('rows',case when replace_value then '[]'::jsonb else coalesce(current_value->'rows','[]') end||next_value);
 elsif p_payload ? 'value' then
 next_value:=current_value||p_payload->'value';
 if not replace_value and p_payload#>>'{value,text}' is not null then next_value:=next_value||jsonb_build_object('text',coalesce(current_value->>'text','')||p_payload#>>'{value,text}');end if;
 if not replace_value and jsonb_typeof(p_payload#>'{value,dates}')='array' then next_value:=next_value||jsonb_build_object('dates',coalesce(current_value->'dates','[]')||p_payload#>'{value,dates}');end if;
 if not replace_value and jsonb_typeof(p_payload#>'{value,rows}')='array' then next_value:=next_value||jsonb_build_object('rows',coalesce(current_value->'rows','[]')||p_payload#>'{value,rows}');end if;
 amount:=1;
 else
 next_value:=current_value||jsonb_build_object('text',case when replace_value then '' else coalesce(current_value->>'text','') end||
 coalesce((select string_agg(entry->>'html',E'\n') from jsonb_array_elements(accepted) entry),''));
 end if;
 -- Preserve images, evidence and unrelated metadata. Newly found relevant
 -- content supersedes a previous "nothing new" marker.
 next_value:=next_value||jsonb_build_object('not_relevant',false,'completion',null);
 insert into public.v2_collection_values(session_id,field_id,value,updated_at) values(sid,fid,next_value,now())
 on conflict(session_id,field_id) do update set value=excluded.value,updated_at=excluded.updated_at;
 end if;
 delete from public.v2_collection_autofill_write_tokens where transaction_id=txid_current();
 end if;
 final_status:=case when amount>0 then 'completed' else 'unchanged' end;
 end if;end if;
 update public.v2_collection_autofill_runs set status=final_status,session_id=coalesce(sid,session_id),count=amount,finished_at=now(),
 message=case when final_status='conflict' then 'התיבה או התבנית השתנו בזמן הסריקה; התוכן נשמר ללא שינוי.' end where id=run.id;
 update public.v2_collection_autofill_state set lease_id=null,lease_until=null,
 last_scanned_at=case when final_status='conflict' then last_scanned_at else p_scan_started_at end,
 source_cursors=case when final_status='conflict' then source_cursors else p_cursors end
 where workspace_id=run.workspace_id and sheet_key=run.sheet_key and logical_key=run.logical_key;
 return jsonb_build_object('run_id',run.id,'status',final_status,'session_id',sid,'count',amount);
end $$;

create function public.v2_collection_autofill_fail(p_run_id uuid,p_message text)
returns void language plpgsql security definer set search_path=public,pg_temp as $$
begin
 update public.v2_collection_autofill_runs set status='failed',message=left(p_message,1000),finished_at=now() where id=p_run_id and status='running';
 update public.v2_collection_autofill_state set lease_id=null,lease_until=null where lease_id=p_run_id;
end $$;

create function public.v2_collection_autofill_rollover()
returns integer language plpgsql security definer set search_path=public,pg_temp as $$
declare row record; before_id uuid; after_id uuid; amount integer:=0;
begin
 for row in select s.* from public.v2_workspaces w join lateral(select c.* from public.v2_collection_sessions c
 where c.workspace_id=w.id and c.deleted_at is null and c.status<>'archived' order by c.created_at desc,c.id desc limit 1) s on true
 where exists(select 1 from public.v2_template_sections sec join public.v2_template_fields f on f.section_id=sec.id
 where sec.template_id=s.template_id and sec.active and f.active and f.settings#>>'{autofill,enabled}'='true'
 and public.v2_collection_sheet_key(to_jsonb(sec))='news_macro') loop
 before_id:=row.id;after_id:=public.v2_collection_autofill_target(row.workspace_id,row.template_id);
 if after_id<>before_id then amount:=amount+1;end if;
 end loop;return amount;
end $$;

revoke all on function public.v2_collection_autofill_due(integer),public.v2_collection_autofill_claim(uuid,uuid,boolean),
 public.v2_collection_autofill_target(uuid,uuid),public.v2_collection_item_count(uuid,uuid),
 public.v2_collection_autofill_apply(uuid,jsonb,jsonb,timestamptz),public.v2_collection_autofill_fail(uuid,text),public.v2_collection_autofill_rollover()
 from public,anon,authenticated;
grant execute on function public.v2_collection_autofill_due(integer),public.v2_collection_autofill_claim(uuid,uuid,boolean),
 public.v2_collection_autofill_apply(uuid,jsonb,jsonb,timestamptz),public.v2_collection_autofill_fail(uuid,text),public.v2_collection_autofill_rollover()
 to service_role;
grant all on public.v2_collection_autofill_state,public.v2_collection_autofill_runs,public.v2_collection_autofill_items to service_role;
notify pgrst,'reload schema';
commit;
