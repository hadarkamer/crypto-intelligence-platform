-- Preserve bounded hover measurements in the existing atomic save.
-- No tables, schedules, roles or permission changes.
CREATE OR REPLACE FUNCTION public.v2_ai_liquidity_levels(p_row_id text, p_levels jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 IMMUTABLE
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  lev jsonb; out_levels jsonb := '[]'::jsonb; n integer := 0;
  side text; amount text; price text; shape text; precision text; basis text;
  low numeric; high numeric; uncertainty numeric; score numeric; item jsonb;
  discrete jsonb; point numeric; previous_point numeric; count_points integer;
  observation jsonb; modern boolean; metadata jsonb; evidence text;
begin
  if jsonb_typeof(p_levels) is distinct from 'array' then
    raise exception 'AI levels must be an array' using errcode = '22023';
  end if;
  if jsonb_array_length(p_levels) > 40 then
    raise exception 'Too many AI levels' using errcode = '22023';
  end if;
  for lev in select value from jsonb_array_elements(p_levels) loop
    if jsonb_typeof(lev) is distinct from 'object' then
      raise exception 'AI level must be an object' using errcode = '22023';
    end if;
    side := lev ->> 'side'; amount := lev ->> 'amount'; price := lev ->> 'price_text';
    if side is null or side not in ('above', 'below') or amount is null
       or amount not in ('many', 'normal', 'few') or nullif(btrim(price), '') is null then
      raise exception 'Invalid AI level' using errcode = '22023';
    end if;
    metadata := '{}'::jsonb;
    shape := lev ->> 'shape'; precision := lev ->> 'price_precision';
    basis := lev ->> 'evidence_basis'; observation := lev -> 'source_observation';
    modern := coalesce(observation ->> 'level_contract_version', '') = 'prominent-rows.v1';
    if shape is not null or precision is not null or basis is not null or modern then
      if shape is null or shape not in ('point','adjacent_prices','range')
         or precision is null or precision not in ('verified_label','axis_estimate')
         or jsonb_typeof(lev -> 'price_low') is distinct from 'number'
         or jsonb_typeof(lev -> 'price_high') is distinct from 'number' then
        raise exception 'Invalid AI level precision' using errcode = '22023';
      end if;
      low := (lev ->> 'price_low')::numeric; high := (lev ->> 'price_high')::numeric;
      if low <= 0 or high < low or (shape = 'range' and high = low) then
        raise exception 'Invalid AI level bounds' using errcode = '22023';
      end if;
      discrete := lev -> 'prices'; previous_point := null; count_points := 0;
      if discrete is not null and jsonb_typeof(discrete) is distinct from 'null' then
        if jsonb_typeof(discrete) is distinct from 'array' then
          raise exception 'Invalid discrete prices' using errcode = '22023';
        end if;
        for item in select value from jsonb_array_elements(discrete) loop
          if jsonb_typeof(item) is distinct from 'number' then
            raise exception 'Invalid discrete price' using errcode = '22023';
          end if;
          point := item::text::numeric;
          if point <= 0 or point < low or point > high or (previous_point is not null and point <= previous_point) then
            raise exception 'Discrete price outside level' using errcode = '22023';
          end if;
          previous_point := point; count_points := count_points + 1;
        end loop;
        if (shape = 'point' and count_points <> 1) or (shape = 'adjacent_prices' and count_points <> 2)
           or (shape = 'range' and count_points not in (0,2)) then
          raise exception 'Discrete shape mismatch' using errcode = '22023';
        end if;
      elsif shape = 'adjacent_prices' or (shape = 'point' and modern) then
        raise exception 'Missing discrete price' using errcode = '22023';
      end if;
      evidence := lev ->> 'price_evidence'; uncertainty := null;
      if lev ? 'uncertainty_usd' and jsonb_typeof(lev -> 'uncertainty_usd') is distinct from 'null' then
        if jsonb_typeof(lev -> 'uncertainty_usd') is distinct from 'number' then
          raise exception 'Invalid uncertainty' using errcode = '22023';
        end if;
        uncertainty := (lev ->> 'uncertainty_usd')::numeric;
      end if;
      if evidence is not null and (jsonb_typeof(lev -> 'price_evidence') <> 'string' or length(evidence) > 1000) then
        raise exception 'Invalid price evidence' using errcode = '22023';
      end if;
      if precision = 'verified_label' then
        if basis is null or basis not in ('numeric_row_label','tooltip') or nullif(btrim(evidence),'') is null
           or (shape <> 'range' and count_points = 0) or (uncertainty is not null and uncertainty <> 0)
           or (shape <> 'range' and ((discrete ->> 0)::numeric <> low or (discrete ->> (count_points - 1))::numeric <> high)) then
          raise exception 'Exact price lacks matching source evidence' using errcode = '22023';
        end if;
        for item in select value from jsonb_array_elements(case when shape = 'range' then jsonb_build_array(low,high) else discrete end) loop
          point := item::text::numeric;
          if not exists(select 1 from regexp_matches(evidence, '([0-9]+(?:,[0-9]{3})*(?:\.[0-9]+)?)', 'g') as labels(label)
                        where replace(label[1], ',', '')::numeric = point) then
            raise exception 'Exact price missing from evidence' using errcode = '22023';
          end if;
        end loop;
      elsif basis is distinct from 'axis_interpolation' or (modern and (uncertainty is null or uncertainty <= 0))
            or (uncertainty is not null and uncertainty <= 0) then
        raise exception 'Axis estimate lacks uncertainty' using errcode = '22023';
      end if;
      if precision = 'axis_estimate' and shape <> 'range' and count_points > 0 and uncertainty is not null then
        for item in select value from jsonb_array_elements(discrete) loop
          point := item::text::numeric;
          if uncertainty < greatest(point - low, high - point) then
            raise exception 'Uncertainty does not cover estimate bounds' using errcode = '22023';
          end if;
        end loop;
      end if;
      if modern and ((amount = 'many' and shape = 'range' and
          (lev->>'row_colour' is distinct from 'yellow' or lev->'continuous' is distinct from 'true'::jsonb
           or lev->'weak_gap_present' is distinct from 'false'::jsonb))
          or (amount = 'normal' and shape <> 'range')) then
        raise exception 'Prominent shape/intensity mismatch' using errcode = '22023';
      end if;
      score := null;
      if lev ? 'strength_score' and jsonb_typeof(lev -> 'strength_score') is distinct from 'null' then
        if jsonb_typeof(lev -> 'strength_score') is distinct from 'number' then
          raise exception 'Invalid strength score' using errcode = '22023';
        end if;
        score := (lev ->> 'strength_score')::numeric;
        if score < 0 or score > 1 then raise exception 'Invalid strength score' using errcode = '22023'; end if;
      end if;
      metadata := jsonb_strip_nulls(jsonb_build_object(
        'shape', shape, 'price_precision', precision, 'prices', discrete,
        'price_low', low, 'price_high', high, 'evidence_basis', basis,
        'price_evidence', evidence, 'uncertainty_usd', uncertainty, 'strength_score', score));
    end if;
    if amount='many' and shape='range' then
      metadata := metadata || jsonb_build_object('row_colour','yellow','continuous',true,'weak_gap_present',false);
    end if;
    if observation is not null and jsonb_typeof(observation) is distinct from 'null' then
      if jsonb_typeof(observation) is distinct from 'object'
         or coalesce(observation ->> 'sha256','') !~ '^[0-9a-f]{64}$'
         or coalesce(observation ->> 'artifact_id','') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
         or observation ->> 'quality' is distinct from 'visual_estimate'
         or jsonb_typeof(observation -> 'heatmap_model') is distinct from 'number'
         or observation ->> 'heatmap_model' not in ('1','2','3')
         or coalesce(observation ->> 'timeframe','') not in ('12H','24H','48H')
         or coalesce(observation ->> 'captured_at','') !~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?(Z|[+-]\d{2}:\d{2})$'
         or observation ->> 'source_url' is distinct from (case observation ->> 'heatmap_model'
           when '1' then 'https://www.coinglass.com/pro/futures/LiquidationHeatMap?coin=BTC&type=symbol'
           when '2' then 'https://www.coinglass.com/pro/futures/LiquidationHeatMapNew?coin=BTC&type=symbol'
           when '3' then 'https://www.coinglass.com/pro/futures/LiquidationHeatMapModel3?coin=BTC&type=symbol' end) then
        raise exception 'Invalid source observation' using errcode = '22023';
      end if;
      metadata := metadata || jsonb_build_object('source_observation', observation);
    end if;
    if lev ? 'source_row_ids' then
      if jsonb_typeof(lev->'source_row_ids') is distinct from 'array'
         or jsonb_array_length(lev->'source_row_ids') not between 1 and 1600
         or jsonb_typeof(lev->'source_value') is distinct from 'number'
         or (lev->>'source_value')::numeric < 0 or (lev->>'source_value')::numeric >= 1e15
         or precision is distinct from 'verified_label' or basis is distinct from 'tooltip'
         or observation is null then
        raise exception 'Invalid hover level evidence' using errcode='22023';
      end if;
      if exists(select 1 from jsonb_array_elements(lev->'source_row_ids') x
                where jsonb_typeof(x) <> 'string' or (x#>>'{}') !~ '^r[0-9]{1,4}$')
         or (select count(*) <> count(distinct x) from jsonb_array_elements(lev->'source_row_ids') x) then
        raise exception 'Invalid hover row references' using errcode='22023';
      end if;
      metadata := metadata || jsonb_build_object('source_row_ids',lev->'source_row_ids','source_value',lev->'source_value');
    end if;
    out_levels := out_levels || jsonb_build_array(metadata || jsonb_build_object(
      'id', p_row_id || ':ai:' || n::text, 'row_id', p_row_id, 'side', side,
      'price_text', price, 'amount', amount, 'taken', false, 'position', n, 'source', 'ai'));
    n := n + 1;
  end loop;
  return out_levels;
end;
$function$;

CREATE OR REPLACE FUNCTION public.v2_commit_liquidity_ai_run(p_session_id uuid, p_field_id uuid, p_baseline_updated_at timestamp with time zone, p_results jsonb, p_audit jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  wid uuid; tid uuid; sstatus text; sdel timestamptz;
  ftype text; factive boolean;
  v jsonb; prev jsonb; cur timestamptz; newts timestamptz;
  normalized jsonb; result_rows jsonb := '[]'::jsonb;
  r jsonb; res jsonb; tf text; rid text; seen text[] := '{}';
begin
  if auth.uid() is null then
    raise exception 'Authentication required' using errcode = '42501';
  end if;
  if jsonb_typeof(p_results) <> 'array'
     or jsonb_array_length(p_results) = 0
     or jsonb_array_length(p_results) > 3 then
    raise exception 'Invalid results payload' using errcode = '22023';
  end if;
  if jsonb_typeof(p_audit) <> 'array'
     or jsonb_array_length(p_audit) <> jsonb_array_length(p_results) then
    raise exception 'Invalid audit payload' using errcode = '22023';
  end if;

  select workspace_id, template_id, status, deleted_at
    into wid, tid, sstatus, sdel
    from public.v2_collection_sessions
   where id = p_session_id
   for update;
  if wid is null then
    raise exception 'Collection not found' using errcode = '42704';
  end if;
  if sdel is not null then
    raise exception 'Archived collection is read only' using errcode = '42501';
  end if;
  if sstatus is distinct from 'draft' then
    raise exception 'Collection is not open for editing' using errcode = '42501';
  end if;
  if not coalesce(public.v2_can_edit_workspace(wid), false) then
    raise exception 'Entity edit permission required' using errcode = '42501';
  end if;

  select f.field_type, f.active
    into ftype, factive
    from public.v2_template_fields f
    join public.v2_template_sections sec on sec.id = f.section_id
    join public.v2_templates t on t.id = sec.template_id
   where f.id = p_field_id
     and sec.template_id = tid
     and t.workspace_id = wid;
  if ftype is distinct from 'liquidity_table' or coalesce(factive, false) is false then
    raise exception 'Field is not an active liquidity field of this collection' using errcode = '23514';
  end if;

  select value, updated_at into v, cur
    from public.v2_collection_values
   where session_id = p_session_id and field_id = p_field_id
   for update;

  if p_baseline_updated_at is distinct from cur then
    return jsonb_build_object('status', 'conflict', 'updated_at', cur);
  end if;

  v := coalesce(v, '{}'::jsonb);
  if jsonb_typeof(v) <> 'object' then
    raise exception 'Existing structured value has invalid shape' using errcode = '23514';
  end if;
  prev := v;
  normalized := public.v2_normalize_structured_rows(p_field_id, 'liquidity_table', v);

  -- Bound the extra evidence. Numeric semantics and source binding are checked
  -- by the shared application validator before saving and again before display.
  for res in select value from jsonb_array_elements(p_results) loop
    if res->'hover_snapshot' is not null and res->'hover_snapshot' <> 'null'::jsonb then
      if jsonb_typeof(res->'hover_snapshot') is distinct from 'object'
         or res#>>'{hover_snapshot,version}' is distinct from 'displayed-hover.v1'
         or res#>>'{hover_snapshot,scope}' is distinct from 'displayed_latest_column'
         or res#>>'{hover_snapshot,timeframe}' is distinct from res->>'timeframe'
         or res#>'{hover_snapshot,complete}' is distinct from 'true'::jsonb
         or jsonb_typeof(res#>'{hover_snapshot,rows}') is distinct from 'array'
         or jsonb_array_length(res#>'{hover_snapshot,rows}') not between 3 and 1600
         or octet_length((res->'hover_snapshot')::text)>262144 then
        raise exception 'Invalid hover snapshot payload' using errcode='22023';
      end if;
    end if;
  end loop;

  -- Replace only the timeframes we actually captured; reuse the existing row id.
  for r in select value from jsonb_array_elements(normalized) loop
    tf := r ->> 'timeframe';
    res := null;
    if tf is not null and not (tf = any(seen)) then
      select value into res from jsonb_array_elements(p_results) where value ->> 'timeframe' = tf limit 1;
    end if;
    if res is not null then
      seen := seen || tf;
      r := r || jsonb_build_object('levels', public.v2_ai_liquidity_levels(r ->> 'id', res -> 'levels'), 'hover_snapshot', res->'hover_snapshot');
    end if;
    result_rows := result_rows || jsonb_build_array(r);
  end loop;

  for res in select value from jsonb_array_elements(p_results) loop
    tf := res ->> 'timeframe';
    if tf is null or tf = any(seen) then
      continue;
    end if;
    seen := seen || tf;
    rid := p_field_id::text || ':' || tf;
    result_rows := result_rows || jsonb_build_array(jsonb_build_object(
      'id', rid,
      'timeframe', tf,
      'position', jsonb_array_length(result_rows),
      'levels', public.v2_ai_liquidity_levels(rid, res -> 'levels'),
      'hover_snapshot', res->'hover_snapshot'
    ));
  end loop;

  v := v || jsonb_build_object('rows', result_rows);
  if jsonb_typeof(v #> '{structured,rows}') = 'array' then
    v := jsonb_set(v, '{structured,rows}', result_rows);
  end if;

  insert into public.v2_collection_values(session_id, field_id, value, updated_by, updated_at)
  values (p_session_id, p_field_id, v, auth.uid(), now())
  on conflict (session_id, field_id)
    do update set value = excluded.value, updated_by = auth.uid(), updated_at = now()
  returning updated_at into newts;

  -- Audit in the SAME transaction: an audit failure rolls the write back.
  insert into public.v2_liquidity_ai_runs(
    workspace_id, session_id, field_id, timeframe, source_url, provider, model,
    captured_at, observed_price, screenshot, analysis, previous_value, usage, created_by)
  select
    wid, p_session_id, p_field_id,
    a ->> 'timeframe', a ->> 'source_url', a ->> 'provider', a ->> 'model',
    (a ->> 'captured_at')::timestamptz, (a ->> 'observed_price')::numeric,
    a ->> 'screenshot', coalesce(a -> 'analysis', '{}'::jsonb), prev, a -> 'usage', auth.uid()
  from jsonb_array_elements(p_audit) a;

  return jsonb_build_object('status', 'ok', 'updated_at', newts, 'rows', jsonb_array_length(result_rows));
end
$function$;

