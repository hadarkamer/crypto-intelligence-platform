begin;

-- Reuse the existing claim transaction and durable field leases. A competing
-- YouTube field stays due; no new queue, table, worker or provider retry.
do $patch$
declare definition text;
 needle text := E' if sheet=''data'' then select l.value into baseline';
 guard text := $guard$
 -- youtube_shared_lease: all callers, including independent manual requests,
 -- must check the provider lane atomically before creating a field lease.
 if exists(select 1 from unnest(app_private.collection_autofill_source_urls(f.settings)) u
   where u ~* '^https://((www[.]|m[.])?youtube[.]com|(www[.])?youtu[.]be)(:443)?/') then
  perform pg_advisory_xact_lock(hashtextextended('collection-autofill:youtube',0));
  if exists(
   select 1 from public.v2_collection_autofill_state busy
   join public.v2_collection_autofill_runs running on running.id=busy.lease_id
   join public.v2_template_fields active_field on active_field.id=running.field_id
   where busy.lease_until>clock_timestamp() and running.status='running'
    and exists(select 1 from unnest(app_private.collection_autofill_source_urls(active_field.settings)) u
     where u ~* '^https://((www[.]|m[.])?youtube[.]com|(www[.])?youtu[.]be)(:443)?/')
  ) then
   if p_manual then
    raise exception 'סריקת YouTube אחרת עדיין מתבצעת. התיבה לא נסרקה; ניתן להפעילה לאחר סיום הסריקה הקיימת.' using errcode='55P03';
   end if;
   return null;
  end if;
 end if;
$guard$;
begin
 definition:=pg_get_functiondef('public.v2_collection_autofill_claim(uuid,uuid,boolean)'::regprocedure);
 if strpos(definition,'youtube_shared_lease')=0 then
  if (length(definition)-length(replace(definition,needle,'')))/length(needle)<>1 then
   raise exception 'Unexpected autofill claim definition; YouTube lease guard not installed';
  end if;
  execute replace(definition,needle,guard||needle);
 end if;
end $patch$;

commit;
