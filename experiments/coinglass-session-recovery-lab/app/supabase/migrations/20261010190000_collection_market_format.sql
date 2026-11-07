-- Apply the requested format to the two known source fields in all template
-- versions. Existing observations, manual notes and source URLs are untouched.
do $format$
declare guard_state "char"; command text;
begin
  -- Maintenance changes an existing version in one locked transaction. Keep all
  -- other validation/revision triggers active and restore this guard exactly.
  lock table public.v2_template_fields in access exclusive mode;
  select tgenabled into guard_state from pg_trigger
    where tgrelid='public.v2_template_fields'::regclass
      and tgname='v2_used_template_fields_guard' and not tgisinternal;
  if guard_state is not null then
    alter table public.v2_template_fields disable trigger v2_used_template_fields_guard;
  end if;
update public.v2_template_fields f
set settings = jsonb_set(f.settings, '{autofill}',
  coalesce(f.settings->'autofill','{}'::jsonb) || jsonb_build_object('instructions',
    case f.field_key
      when 'סריקת שוק/טכני/דומיננס:8a807ea8-1cf8-4592-a6b2-b60b4c9f24d0' then
        'כתוב רק את מספר BTC.D הנוכחי באחוזים. הוסף גרף אם הוא זמין, או שורות שינוי אחוזי מאומת לפי המקור: 1D, 1W, 1M. מספרים עם סימן + או −; נתון חסר נשאר חסר, ללא הסבר ארוך או השוואה לסריקה קודמת.'
      else
        'כתוב את סכום הדולר של נר Daily Total Net Inflow האחרון בפורמט K/M/B, חיובי בירוק ושלילי באדום. הוסף צילום עם זום לנרות האחרונים. אם הצילום אינו זמין, הצג את הנרות האחרונים המאומתים כשורות תאריך וסכום באדום וירוק. אין להמציא נתון חסר.'
    end), true)
from public.v2_template_sections s join public.v2_templates t on t.id=s.template_id
where f.section_id=s.id and t.workspace_id='a6bb2bb8-2c40-4404-9142-1304620fcd31'
  and t.kind='collection' and public.v2_collection_sheet_key(to_jsonb(s))='data'
  and f.field_key in (
    'סריקת שוק/טכני/דומיננס:8a807ea8-1cf8-4592-a6b2-b60b4c9f24d0',
    'סריקת שוק/טכני/נתוני etf:d7782bcd-a516-4a8b-9389-528c62bf7675'
  )
  and f.settings#>>'{autofill,instructions}' is distinct from case f.field_key
    when 'סריקת שוק/טכני/דומיננס:8a807ea8-1cf8-4592-a6b2-b60b4c9f24d0' then
      'כתוב רק את מספר BTC.D הנוכחי באחוזים. הוסף גרף אם הוא זמין, או שורות שינוי אחוזי מאומת לפי המקור: 1D, 1W, 1M. מספרים עם סימן + או −; נתון חסר נשאר חסר, ללא הסבר ארוך או השוואה לסריקה קודמת.'
    else
      'כתוב את סכום הדולר של נר Daily Total Net Inflow האחרון בפורמט K/M/B, חיובי בירוק ושלילי באדום. הוסף צילום עם זום לנרות האחרונים. אם הצילום אינו זמין, הצג את הנרות האחרונים המאומתים כשורות תאריך וסכום באדום וירוק. אין להמציא נתון חסר.'
  end;
  if guard_state is not null then
    command:=case guard_state when 'O' then 'enable' when 'D' then 'disable'
      when 'R' then 'enable replica' when 'A' then 'enable always' else null end;
    if command is null then raise exception 'Unexpected template guard state'; end if;
    execute 'alter table public.v2_template_fields '||command||' trigger v2_used_template_fields_guard';
  end if;
end $format$;
