-- Preserve source precision and evidence in dedicated heatmap saves.
-- This helper is pure; permissions, target guards and the atomic audit remain
-- enforced by the unchanged v2_commit_liquidity_ai_run transaction.
create or replace function public.v2_ai_liquidity_levels(p_row_id text, p_levels jsonb)
returns jsonb
language plpgsql
immutable
set search_path = public, pg_temp
as $function$
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
      if modern and ((amount = 'many' and shape = 'range') or (amount = 'normal' and shape <> 'range')) then
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
    out_levels := out_levels || jsonb_build_array(metadata || jsonb_build_object(
      'id', p_row_id || ':ai:' || n::text, 'row_id', p_row_id, 'side', side,
      'price_text', price, 'amount', amount, 'taken', false, 'position', n, 'source', 'ai'));
    n := n + 1;
  end loop;
  return out_levels;
end;
$function$;
