-- Record partial news scans in the same transaction as successful items/cursors.
-- Reuse existing statuses and message column; no new tables or retry mechanism.
do $migration$
declare definition text;
begin
 select pg_get_functiondef('public.v2_collection_autofill_apply(uuid,jsonb,jsonb,timestamptz)'::regprocedure) into definition;
 if position('v2_independent_news_sources' in definition)>0 then return;end if;
 if position('final_status:=case when amount>0 then ''completed'' else ''unchanged'' end;' in definition)=0
 or position('message=case when final_status=''conflict''' in definition)=0 then
  raise exception 'Unexpected collection apply definition; review before patching';end if;
 definition:=replace(definition,
  'final_status:=case when amount>0 then ''completed'' else ''unchanged'' end;',
  '/* v2_independent_news_sources */ final_status:=case when nullif(p_payload->>''source_failure_message'','''') is not null then ''failed'' when amount>0 then ''completed'' else ''unchanged'' end;');
 definition:=replace(definition,
  'message=case when final_status=''conflict''',
  'message=case when final_status=''failed'' then left(p_payload->>''source_failure_message'',1000) when final_status=''conflict''');
 definition:=replace(definition,
  '''session_id'',sid,''count'',amount);',
  '''session_id'',sid,''count'',amount,''message'',case when final_status=''failed'' then left(p_payload->>''source_failure_message'',1000) end);');
 execute definition;
end $migration$;
