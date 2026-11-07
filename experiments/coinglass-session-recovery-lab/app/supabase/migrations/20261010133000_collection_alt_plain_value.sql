-- This is a presentation-only repair: retain precision, manual notes, provenance
-- and every original edit timestamp/actor. In particular, an older worksheet must
-- never replace the authoritative shared live value through its mirror trigger.
do $repair$
declare
  trigger_state record;
  saved_trigger_states jsonb;
begin
  -- ALTER TRIGGER and both repairs share this transaction. No concurrent writer
  -- can observe disabled triggers or write either value table until it commits.
  lock table public.v2_collection_values, public.v2_collection_live_values
    in access exclusive mode;
  select coalesce(jsonb_agg(jsonb_build_object('name', tgname, 'state', tgenabled)
    order by tgname), '[]'::jsonb)
  into saved_trigger_states
  from pg_catalog.pg_trigger
  where tgrelid = 'public.v2_collection_values'::regclass and not tgisinternal;

  -- Ordinary edit triggers would mirror historical values, change their author
  -- and timestamps, and generate new activity for a purely cosmetic repair.
  -- Internal constraint triggers stay enabled. Preserve O/D/R/A exactly.
  for trigger_state in select * from jsonb_to_recordset(saved_trigger_states)
    as state(name text, state text)
  loop
    if trigger_state.state <> 'D' then
      execute format('alter table public.v2_collection_values disable trigger %I', trigger_state.name);
    end if;
  end loop;

  update public.v2_collection_values
  set value = jsonb_set(value, '{text}', to_jsonb(substring(value->>'text'
    from '^<p class="rt-color-[a-z]+">(-?[0-9]+(?:\.[0-9]+)?)</p>$')))
  where value#>>'{_autofill,kind}' = 'alt_correlation'
    and value->>'text' ~ '^<p class="rt-color-[a-z]+">-?[0-9]+(?:\.[0-9]+)?</p>$';

  -- Repair the live value using its own current contents, never a historical
  -- snapshot. A different observation or any manual note remains authoritative.
  update public.v2_collection_live_values
  set value = jsonb_set(value, '{text}', to_jsonb(substring(value->>'text'
    from '^<p class="rt-color-[a-z]+">(-?[0-9]+(?:\.[0-9]+)?)</p>$')))
  where value#>>'{_autofill,kind}' = 'alt_correlation'
    and value->>'text' ~ '^<p class="rt-color-[a-z]+">-?[0-9]+(?:\.[0-9]+)?</p>$';

  for trigger_state in select * from jsonb_to_recordset(saved_trigger_states)
    as state(name text, state text)
  loop
    case trigger_state.state
      when 'O' then execute format('alter table public.v2_collection_values enable trigger %I', trigger_state.name);
      when 'R' then execute format('alter table public.v2_collection_values enable replica trigger %I', trigger_state.name);
      when 'A' then execute format('alter table public.v2_collection_values enable always trigger %I', trigger_state.name);
      when 'D' then null; -- It was disabled before the repair and stays disabled.
      else raise exception 'Unexpected collection trigger state: %', trigger_state.state;
    end case;
  end loop;
  -- On any failure this DO statement rolls back the data and trigger DDL alike.
end $repair$;
