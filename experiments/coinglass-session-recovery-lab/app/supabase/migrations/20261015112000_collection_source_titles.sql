-- Correct labels only for the reviewed fields and verified source selection.
do $labels$
declare guard_state "char"; command text;
begin
 lock table public.v2_template_fields in access exclusive mode;
 select tgenabled into guard_state from pg_trigger where tgrelid='public.v2_template_fields'::regclass and tgname='v2_used_template_fields_guard' and not tgisinternal;
 if guard_state is not null then alter table public.v2_template_fields disable trigger v2_used_template_fields_guard;end if;
 update public.v2_template_fields set title='דור אופיר (X)'
 where id='7fdf4db1-768d-472c-964d-70d4e53f155b' and title='דור אופיר (וואצאפ)'
 and settings->'autofill_source_urls'='["https://x.com/Dor_Ophir?lang=he"]'::jsonb;
 update public.v2_template_fields set title='רז איטח (YouTube)'
 where id='91808243-660f-4bab-97cd-0120e0588f61' and title='רז איטח? (וואצאפ)'
 and not (settings ? 'autofill_source_urls')
 and jsonb_array_length(settings->'links')=1
 and settings#>>'{links,0,url}'='https://www.youtube.com/@RazIttah/videos';
 if guard_state is not null then
  command:=case guard_state when 'O' then 'enable' when 'D' then 'disable' when 'R' then 'enable replica' when 'A' then 'enable always' end;
  if command is null then raise exception 'Unexpected template guard state';end if;
  execute 'alter table public.v2_template_fields '||command||' trigger v2_used_template_fields_guard';
 end if;
end $labels$;
