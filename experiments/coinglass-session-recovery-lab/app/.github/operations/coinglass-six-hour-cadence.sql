-- One-time configuration repair. NOT a schema migration or a new scheduler.
-- The caller supplies coinglass_cadence.workspace_id and template_id and an
-- authenticated editor identity in the same transaction. No production calls
-- are made by tests. Keep this transaction open for inspection before COMMIT.
do $repair$
declare
  workspace uuid := current_setting('coinglass_cadence.workspace_id')::uuid;
  template uuid := current_setting('coinglass_cadence.template_id')::uuid;
  snapshot jsonb; sections jsonb; entry record; field jsonb; settings jsonb;
  reference_schedule jsonb; result jsonb; seen text[] := '{}'; changed integer := 0;
  keys constant text[] := array[
    'legacy:079c525e-9463-4919-86af-e1a598110e76',
    'legacy:8b48ad86-5ed8-49f9-b324-7fedfbb757f3',
    'סריקת שוק/נזילות/נזילות קוינגאלס'
  ];
  logical text; model integer; sources text[]; expected_url text;
begin
  if auth.uid() is null or not coalesce(public.v2_can_edit_workspace(workspace),false) then
    raise exception 'An authenticated workspace editor is required' using errcode='42501';
  end if;
  perform pg_advisory_xact_lock(hashtextextended(workspace::text||'collection',0));
  perform 1 from public.v2_workspaces where id=workspace for update;
  perform 1 from public.v2_templates where id=template and workspace_id=workspace
    and kind='collection' and active and is_default for update;
  if not found then raise exception 'The reviewed active default template is required'; end if;
  if exists(select 1 from public.v2_collection_autofill_state
    where workspace_id=workspace and lease_until>clock_timestamp()) then
    raise exception 'Wait for active collection work to finish; no lease was changed';
  end if;
  snapshot := public.v2_collection_editor_snapshot(template);
  sections := snapshot->'sections';
  for entry in
    select s.ordinality-1 as si, f.ordinality-1 as fi, s.value as section, f.value as field
    from jsonb_array_elements(sections) with ordinality s
    cross join lateral jsonb_array_elements(s.value->'fields') with ordinality f
  loop
    field := entry.field;
    logical := public.v2_collection_field_logical_key(field);
    if not logical=any(keys) then continue; end if;
    if logical=any(seen) then raise exception 'Ambiguous CoinGlass field identity'; end if;
    seen := array_append(seen,logical);
    settings := field->'settings';
    model := array_position(keys,logical);
    sources := app_private.collection_autofill_source_urls(settings);
    expected_url := lower('https://www.coinglass.com/pro/futures/'||
      (case model when 1 then 'LiquidationHeatMap' when 2 then 'LiquidationHeatMapNew'
       else 'LiquidationHeatMapModel3' end)||'?coin=BTC&type=symbol');
    if cardinality(sources)<>1 or not (
      lower(sources[1]) in (expected_url,replace(expected_url,'www.',''))
      or (model=3 and lower(sources[1]) in (
        'https://www.coinglass.com/liquidation-heatmap-model3?coin=btc&type=symbol',
        'https://coinglass.com/liquidation-heatmap-model3?coin=btc&type=symbol'))
    ) then raise exception 'The reviewed BTC/model source changed'; end if;
    if public.v2_collection_sheet_key(entry.section)<>'data'
      or field->>'field_type'<>'liquidity_table'
      or not coalesce((field->>'active')::boolean,false)
      or not coalesce((entry.section->>'active')::boolean,false)
      or settings#>>'{autofill,enabled}' is distinct from 'true'
      or coalesce(settings#>>'{autofill,interval_hours}','') not in ('1','6') then
      raise exception 'CoinGlass configuration changed since review';
    end if;
    if array_length(seen,1)=1 then reference_schedule:=settings#>'{autofill,schedule}';
    elsif settings#>'{autofill,schedule}' is distinct from reference_schedule then
      raise exception 'The maps no longer share the same active-hour policy';
    end if;
    if settings#>>'{autofill,interval_hours}'<>'6' then
      sections:=jsonb_set(sections,
        array[entry.si::text,'fields',entry.fi::text,'settings','autofill','interval_hours'],'6'::jsonb);
      changed:=changed+1;
    end if;
  end loop;
  if cardinality(seen)<>3 then raise exception 'Expected exactly the three reviewed CoinGlass fields'; end if;
  if changed=0 then
    raise notice 'CoinGlass cadence already equals six hours; nothing written';
    return;
  end if;
  result:=public.v2_save_collection_template_live(workspace,template,template,
    (snapshot->>'revision')::bigint,snapshot#>>'{template,name}',true,true,sections);
  if result->>'forked'='true' or (result->>'template_id')::uuid is distinct from template then
    raise exception 'Unexpected template replacement; configuration repair rolled back';
  end if;
  raise notice 'Changed % cadence values to six hours using the existing template editor',changed;
end $repair$;
