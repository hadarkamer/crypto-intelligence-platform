-- Keep pending YouTube work distinct from failure, in the existing atomic save.
-- No new state/status, permissions, scheduling, or retry mechanism.
do $migration$
declare definition text;
begin
 select pg_get_functiondef('public.v2_collection_autofill_apply(uuid,jsonb,jsonb,timestamptz)'::regprocedure) into definition;
 if position('youtube_pending_message_v1' in definition)>0 then return;end if;
 if position('message=case when final_status=''failed''' in definition)=0
 or position('''message'',case when final_status=''failed''' in definition)=0 then
  raise exception 'Unexpected collection apply message definition; review before patching';end if;
 definition:=replace(definition,
  'message=case when final_status=''failed''',
  'message=/* youtube_pending_message_v1 */case when final_status in (''completed'',''unchanged'') then left(nullif(p_payload->>''source_pending_message'',''''),1000) when final_status=''failed''');
 definition:=replace(definition,
  '''message'',case when final_status=''failed''',
  '''message'',case when final_status in (''completed'',''unchanged'') then left(nullif(p_payload->>''source_pending_message'',''''),1000) when final_status=''failed''');
 execute definition;
end $migration$;
