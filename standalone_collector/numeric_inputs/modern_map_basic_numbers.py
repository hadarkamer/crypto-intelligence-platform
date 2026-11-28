"""Verified BASIC numbers for one displayed map. Values from different map epochs never combine."""
from datetime import datetime
from decimal import Decimal
import re

from modern_map_cells import _agree, _numeric, _percent, _rounded, _verify_percent
from modern_map_evidence import equivalent, number
from model1_execution import StageFailure

SCHEMA = 'coinglass.basic.v1'
BASIC_SYMBOLS = ('BTC', 'ETH', 'SOL', 'XRP', 'HYPE', 'BNB', 'DOGE', 'ZEC')
BASIC_MODELS = (1, 2, 3)
BASIC_RANGES = ('12H', '24H', '48H', '1W')
HORIZON_SECONDS = {'12H': 43200, '24H': 86400, '48H': 172800, '1W': 604800}
RANGE_PATTERNS = {
    key: re.compile(rf'^{int(key[:-1])}\s*(?:h|hours?)$', re.I)
    for key in BASIC_RANGES if key.endswith('H')
}
RANGE_PATTERNS['1W'] = re.compile(r'^(?:1\s*(?:w|week)|7\s*(?:d|days?))$', re.I)


def basic_target(symbol, model, timeframe):
    return symbol in BASIC_SYMBOLS and type(model) is int and model in BASIC_MODELS and timeframe in BASIC_RANGES


def coin_map_url(symbol, model=1):
    if not isinstance(symbol, str) or not re.fullmatch('[A-Z0-9]{1,20}', symbol):
        raise ValueError('Invalid configured symbol')
    if type(model) is not int or model not in BASIC_MODELS:
        raise ValueError('Invalid heatmap model')
    url = f'https://www.coinglass.com/LiquidationHeatMap/{symbol}?mode=coin'
    return url if model == 1 else url + f'&model={model}'


def basic_source_url(symbol, model=1):
    if symbol not in BASIC_SYMBOLS:
        raise ValueError('Unsupported basic Coin symbol')
    return coin_map_url(symbol, model)


def range_matches(timeframe, label):
    return timeframe in BASIC_RANGES and isinstance(label, str) and bool(RANGE_PATTERNS[timeframe].fullmatch(label.strip()))


def valid_range_selection(timeframe, selection):
    if not isinstance(selection, dict) or set(selection) != {'label', 'value'}:
        return False
    if not range_matches(timeframe, selection['label']):
        return False
    value = selection['value']
    if value is None:
        return True  # The official trigger can omit its internal value.
    if not isinstance(value, str) or not re.fullmatch(r'[a-zA-Z0-9 ._-]{1,32}', value):
        return False
    # Some providers use opaque option IDs. An explicit duration must agree
    # with the selected label and the target using the same unit vocabulary.
    if re.fullmatch(r'\d+\s*(?:h|hours?|d|days?|w|weeks?)', value.strip(), re.I):
        return range_matches(timeframe, value)
    return True


# Retained receipts and offline BTC examples import this historic URL.
SOURCE_URL = basic_source_url('BTC')
AMOUNTS = ('raw_total', 'raw_above_total', 'raw_below_total', 'raw_realized_total',
           'raw_realized_longs', 'raw_realized_shorts', 'raw_largest_value')
PERCENTAGES = ('raw_above_pct','raw_below_pct')
REFERENCE = ('raw_price_step','raw_column_period','raw_largest_price')
SAME_MAP = ('raw_total','raw_above_total','raw_below_total','raw_price_step','raw_column_period',
            'raw_largest_value','raw_realized_total','raw_realized_longs','raw_realized_shorts',
            'raw_above_pct','raw_below_pct','raw_largest_price')
CORE_AMOUNTS = ('raw_total','raw_above_total','raw_below_total')


def require(condition, reason='evidence_number_mismatch'):
    if not condition:
        raise StageFailure('source_not_readable', reason)


def reject_observation(detail, reason='evidence_number_mismatch'):
    error=StageFailure('source_not_readable',reason)
    error.basic_reason=detail
    raise error


def stamp(raw):
    require(isinstance(raw,str) and re.fullmatch(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,6})?Z',raw), 'hover_time_invalid')
    try:
        return datetime.fromisoformat(raw.replace('Z','+00:00'))
    except ValueError:
        raise StageFailure('source_not_readable','hover_time_invalid') from None


def period_seconds(raw):
    match=re.fullmatch(r'([1-9]\d{0,3})(s|sec|seconds?|m|min|minutes?|h|hr|hours?|d|days?)',raw) if isinstance(raw,str) else None
    if match is None:
        reject_observation('column_period_missing_or_invalid','hover_time_invalid')
    return int(match[1])*{'s':1,'m':60,'h':3600,'d':86400}[match[2][0]]


def market_amounts(overview):
    def read(name):
        if name=='raw_largest_value' and overview.get(name) is None:
            return None
        if overview.get(name) in (None,''):
            reject_observation(name.removeprefix('raw_')+'_missing')
        return verified_number(overview[name],True)
    values={name:read(name) for name in CORE_AMOUNTS}
    if values['raw_total'][0]==0:
        upper=values['raw_total'][1]
        lower=lambda item:max(Decimal(0),item[0]-item[1])
        if lower(values['raw_above_total'])+lower(values['raw_below_total'])>upper:
            reject_observation('printed_zero_total_conflicts_with_sides')
        values_to_check=[overview.get('raw_largest_value')]
        rows=overview.get('sidebar_levels')
        if isinstance(rows,list):
            values_to_check.extend(row.get('raw_value') for row in rows if isinstance(row,dict))
        if any(lower(verified_number(text,True))>upper for text in values_to_check if text is not None):
            reject_observation('printed_zero_total_conflicts_with_levels')
        # Even a consistent printed zero cannot provide the denominator that
        # this result shape requires. Retain the source without inventing 0%.
        reject_observation('printed_zero_total_percent_undefined')
    return {name:values[name] if name in values else read(name) for name in AMOUNTS}


def verified_number(raw, amount=False, *, positive=False):
    value = number(raw, amount)
    require(value[0]>0 if positive else value[0]>=0)
    return value


def ratio(value, total):
    return _rounded(100*value/total)


def safe_price(value):
    require(type(value) in (int,float,Decimal))
    result=Decimal(str(value))
    require(result.is_finite() and result>0 and result<Decimal('1e18'))
    return result


def fact_rows(overview):
    require(not set(overview.get('ambiguous_fields',[])).intersection((*SAME_MAP,'source_time','sidebar_shape','sidebar_levels')))
    rows=overview.get('sidebar_levels')
    require(isinstance(rows,list) and 0<len(rows)<=1600, 'level_list_shape')
    require(not overview.get('unreadable_sidebar_containers'), 'level_list_shape')
    out=[]
    for row in rows:
        require(isinstance(row,dict) and all(isinstance(row.get(k),str) for k in
                                            ('raw_price','raw_value','raw_distance_pct')), 'level_list_shape')
        out.append({k:row[k] for k in ('raw_price','raw_value','raw_distance_pct')})
    # The DOM reader returns unique rows as well as the raw, repeated triples.
    # Preserve observed multiplicity for the existing source receipt and counter.
    duplicates=overview.get('duplicate_sidebar_levels',0)
    if not duplicates:
        return out
    original=overview.get('raw_sidebar_rows')
    require(type(duplicates) is int and duplicates>0 and isinstance(original,list) and
            len(original)==len(out)+duplicates and len(original)<=1600,'level_list_shape')
    by_price={number(item['raw_price'])[0]:item for item in out}
    expanded=[]
    for triple in original:
        require(isinstance(triple,list) and len(triple)==3 and all(isinstance(v,str) for v in triple),'level_list_shape')
        price,amount,distance=(v.strip().replace(' ','').removeprefix('$') for v in triple)
        item=by_price.get(number(price)[0])
        require(item and number(amount,True)[0]==number(item['raw_value'],True)[0] and
                _percent(distance,True)[0]==_percent(item['raw_distance_pct'],True)[0],'hover_evidence_mismatch')
        expanded.append(dict(raw_price=price,raw_value=amount,raw_distance_pct=distance))
    require({number(row['raw_price'])[0] for row in expanded}==by_price.keys(),'level_list_shape')
    return expanded


def raw_display(overview):
    """Only original numeric market labels. No unrestricted page text or markup."""
    source=overview.get('display_values',{})
    result={}
    for key in (*AMOUNTS,*PERCENTAGES,*REFERENCE,'raw_last_price'):
        tokens=source.get(key)
        if not tokens:
            continue
        require(isinstance(tokens,list) and len(tokens)<=8)
        values=[]
        for token in tokens:
            require(isinstance(token,str) and 0<len(token)<=45)
            norm=token.strip().replace('$','').replace(' ','').upper()
            if key=='raw_column_period':
                expect=(overview.get(key) or '').replace(' ','').upper()
                require(norm==expect)
            elif key in PERCENTAGES:
                require(norm==overview.get(key))
            else:
                require(re.fullmatch(r'\s*\$?\s*[\d,.]+\s*[KMBT]?\s*',token,re.I))
                require(number(norm,key in AMOUNTS)[0]==number(overview[key],key in AMOUNTS)[0])
            values.append(token)
        result[key]=values
    return result


def normalize_enrichment(entries, column, epoch, step, symbol, model, timeframe):
    result=[]
    require(isinstance(entries,(list,tuple)) and len(entries)<=1600)
    for entry in entries:
        require(isinstance(entry,dict) and entry.get('source') in ('hover','measure'))
        require(entry.get('source_time')==epoch and entry.get('column_time')==column, 'hover_snapshot_changed')
        require(number(entry['raw_price_step'])[0]==step[0], 'hover_resolution_insufficient')
        require(all(entry.get(k,v)==v for k,v in [('symbol',symbol),('heatmap_model',model),('timeframe',timeframe),('mode','Coin')]))
        verified_number(entry.get('raw_price'),positive=True)
        verified_number(entry.get('raw_value'),True)
        result.append({k:entry[k] for k in ('raw_price','raw_value','raw_price_step','column_time','source')} |
                      {'source_time':epoch})
    return result


def collect_levels(sidebar, enrichment, ref, total):
    rows={}
    duplicates=0
    raw_rows=[]
    for entry in sidebar:
        _verify_percent(entry['raw_distance_pct'],number(entry['raw_price']),ref,offset=100)
        raw_rows.append((entry,'sidebar'))
    raw_rows.extend((r,r['source']) for r in enrichment)
    for row,source in raw_rows:
        price=verified_number(row['raw_price'],positive=True)[0]
        value=verified_number(row['raw_value'],True)
        require(price!=ref[0], 'side_conflicts_with_price')
        if price in rows:
            old=rows[price]
            prior=number(old['raw_value'],True)
            require(_agree(prior,value),'hover_evidence_mismatch')
            if source=='sidebar' and old['raw_distance_pct']!=row['raw_distance_pct']:
                require(_percent_equivalent(old['raw_distance_pct'], row['raw_distance_pct']), 'hover_evidence_mismatch')
            if source not in old['sources']:
                old['sources'].append(source)
            duplicates+=1
            if value[1]>=prior[1]:
                continue
            old.update(raw_value=row['raw_value'],amount=_numeric(value[0]),share_pct=ratio(value[0],total),source=source)
        else:
            rows[price]=dict(raw_price=row['raw_price'],raw_value=row['raw_value'],
                             raw_distance_pct=row.get('raw_distance_pct'),price=_numeric(price),amount=_numeric(value[0]),
                             side='above' if price>ref[0] else 'below',distance_pct=ratio(price-ref[0],ref[0]),
                             share_pct=ratio(value[0],total),source=source,sources=[source])
    return list(rows.values()),duplicates


def _percent_equivalent(first,second):
    return _agree(_percent(first,True),_percent(second,True))


def chart_groups(regions, levels, step, ref, total, anchor):
    if regions is None:
        return None
    require(isinstance(regions,(tuple,list)) and len(regions)<=1600)
    rows=[]
    occupied={}
    unique_matches=set()
    for g in regions:
        require(isinstance(g,dict) and g.get('side') in ('above','below') and g.get('colour') in ('yellow','green','blue'))
        require(isinstance(g.get('prices'),list) and 0<len(g['prices'])<=1600)
        prices=[safe_price(p) for p in g['prices']]
        require(all((p>ref if g['side']=='above' else p<ref) for p in prices), 'side_conflicts_with_price')
        require(all(abs(b-a-step)<=step*Decimal('.05') for a,b in zip(prices,prices[1:])), 'hover_price_grid_gap')
        indices=[((p-anchor)/step).to_integral_value() for p in prices]
        require(all(abs(p-anchor-index*step)<=step*Decimal('.35') for p,index in zip(prices,indices)),
                'hover_resolution_insufficient')
        require(not set(occupied).intersection(indices) and len(set(indices))==len(indices),'hover_pool_overlap_or_gap')
        require(g['colour']!='green' or len(prices)>=2, 'hover_pool_rows_invalid')
        group_key=(g['side'],g['colour'])
        require(not (occupied.get(indices[0]-1)==group_key or occupied.get(indices[-1]+1)==group_key),
                'hover_pool_overlap_or_gap')
        occupied.update(dict.fromkeys(indices,group_key))
        matches=[]
        for p in prices:
            near=[r for r in levels if abs(Decimal(str(r['price']))-p)<=step*Decimal('.35') and r['side']==g['side']]
            require(len(near)<=1,'hover_candidate_disagreement')
            if near:
                item=near[0]
                require(item['price'] not in unique_matches,'hover_pool_overlap_or_gap')
                matches.append(item)
                unique_matches.add(item['price'])
        exact=len(matches)==len(prices)
        known=sum((Decimal(str(r['amount'])) for r in matches),Decimal(0))
        center=(prices[0]+prices[-1])/2
        rows.append(dict(side=g['side'],colour=g['colour'],kind='verified_amount' if exact else 'visual_estimate',
                         coverage='verified' if exact else 'partial' if matches else 'visual',
                         price_low_estimate=_numeric(prices[0]),price_high_estimate=_numeric(prices[-1]),
                         native_bins=len(prices),priced_bins=len(matches),known_amount=_numeric(known) if matches else None,
                         amount=_numeric(known) if exact else None,share_pct=ratio(known,total) if exact else None,
                         distance_pct=ratio(center-ref,ref),matched_sidebar_prices=[r['raw_price'] for r in matches if 'sidebar' in r['sources']],
                         verified_prices=[r['raw_price'] for r in matches],prices=[_numeric(v) for v in prices]))
    require(len(occupied)<=1600)
    require(not any(row['colour']=='blue' for row in rows) or all(row['colour']=='blue' for row in rows),
            'hover_candidate_disagreement')
    return rows


def build_basic_snapshot(overview, *, symbol, model, timeframe, source_url, source_time_after,
                         column_time, column_time_after, raw_last_price, source_sha256, captured_at,
                         chart_regions=None, provenance='live', native_bins_inspected=None,
                         enrichment=(), end_observation=None, raw_last_price_after=None, cursor_proof=None, require_fresh_evidence=False):
    """Build from visible market observations and explicit live or retained closing evidence."""
    require(basic_target(symbol, model, timeframe) and source_url == basic_source_url(symbol, model))
    require(provenance in ('live','retained_reconstruction'))
    require(isinstance(source_sha256,str) and re.fullmatch(r'[0-9a-f]{64}',source_sha256))
    source_time=overview.get('source_time')
    source=stamp(source_time)
    col=stamp(column_time)
    captured=stamp(captured_at)
    require(source_time_after==source_time and -120<=(captured-source).total_seconds()<=1200,'hover_snapshot_changed')
    if not 0<=(source-col).total_seconds()<=period_seconds(overview.get('raw_column_period')):
        reject_observation('column_outside_displayed_period','hover_source_stale')
    nums=market_amounts(overview)
    require(end_observation is not None and isinstance(end_observation,dict),'hover_snapshot_changed')
    require(end_observation.get('source_time')==source_time_after,'hover_snapshot_changed')
    required_close=CORE_AMOUNTS
    for k in required_close:
        require(end_observation.get(k)==overview.get(k),'hover_snapshot_changed')
    require(column_time_after is None or column_time_after==column_time,'hover_snapshot_changed')
    ref=verified_number(raw_last_price,positive=True)
    if overview.get('raw_last_price') is not None:
        require(_agree(ref,number(overview['raw_last_price'])),'hover_evidence_mismatch')
    if raw_last_price_after is None:
        raw_last_price_after=end_observation.get('raw_last_price')
    end_ref=number(raw_last_price_after) if raw_last_price_after is not None else None
    if end_ref is not None:
        require(end_ref[0]==ref[0], 'hover_snapshot_changed')
    if provenance=='live':
        require(column_time_after==column_time and end_ref is not None,'hover_snapshot_changed')
        expected=dict(symbol=symbol,heatmap_model=model,timeframe=timeframe,mode='Coin',column_time=column_time_after,
                      same_document=True,settings_verified=True,same_market_labels=True)
        require(all(_same_json(v,end_observation.get(k)) for k,v in expected.items()),'hover_snapshot_changed')
        if 'sidebar_levels' in end_observation:
            require(all(end_observation.get(k)==overview.get(k) for k in SAME_MAP) and
                    fact_rows(end_observation)==fact_rows(overview),'hover_snapshot_changed')
        require(end_observation.get('sidebar_rows')==len({number(r['raw_price'])[0] for r in fact_rows(overview)}),
                'hover_snapshot_changed')
        require(end_observation.get('chart_regions')==(len(chart_regions) if chart_regions is not None else None) and
                end_observation.get('native_bins_inspected')==native_bins_inspected,'hover_snapshot_changed')
    raw={k:overview.get(k) for k in (*AMOUNTS,*PERCENTAGES,*REFERENCE)}
    step=verified_number(raw['raw_price_step'],positive=True)
    if raw['raw_largest_price'] is not None:
        verified_number(raw['raw_largest_price'],positive=True)
    equivalent(raw['raw_total'],[raw['raw_above_total'],raw['raw_below_total']])
    equivalent(raw['raw_realized_total'],[raw['raw_realized_longs'],raw['raw_realized_shorts']])
    sides={}
    for side in ('above','below'):
        val=nums['raw_'+side+'_total']
        _verify_percent(raw['raw_'+side+'_pct'],val,nums['raw_total'])
        sides[side]=ratio(val[0],nums['raw_total'][0])
    sidebar=fact_rows(overview)
    extra=normalize_enrichment(enrichment,column_time,source_time,step,symbol,model,timeframe)
    total=nums['raw_total'][0]
    levels,duplicates=collect_levels(sidebar,extra,ref,total)
    sidebar_unique={}
    for row in sidebar:
        key=number(row['raw_price'])[0]
        original=number(row['raw_value'],True)
        if key not in sidebar_unique or original[1]<sidebar_unique[key][1]:
            sidebar_unique[key]=original
    added=[row['raw_price'] for row in levels if 'sidebar' not in row['sources']]
    enrichment_report=dict(observations=len(extra),additional_verified_count=len(added),
                           additional_verified_prices=added,overlapping_observations=len(extra)-len(added))
    side_sum=sum(v[0] for v in sidebar_unique.values())
    verified_sum=sum(Decimal(str(row['amount'])) for row in levels)
    for side in ('above','below'):
        values=[number(row['raw_value'],True) for row in levels if row['side']==side]
        maxval,delta=nums['raw_'+side+'_total']
        require(sum(v[0]-v[1] for v in values)<=maxval+delta, 'hover_evidence_mismatch')
    maximum=nums['raw_largest_value']
    if maximum is not None:
        maxval,maxerr=maximum
        require(all(number(row['raw_value'],True)[0]-number(row['raw_value'],True)[1]<=maxval+maxerr for row in levels),'hover_evidence_mismatch')
        if raw['raw_largest_price'] is not None:
            named=number(raw['raw_largest_price'])[0]
            require(all(_agree(number(row['raw_value'],True),maximum) for row in levels
                        if number(row['raw_price'])[0]==named),'hover_evidence_mismatch')
    regions=chart_groups(chart_regions,levels,step[0],ref[0],total,number(sidebar[0]['raw_price'])[0])
    if native_bins_inspected is not None:
        require(type(native_bins_inspected) is int and 0<native_bins_inspected<=1601)
    bins=sum(r['native_bins'] for r in regions) if regions is not None else None
    priced=sum(r['priced_bins'] for r in regions) if regions is not None else None
    if bins is not None and native_bins_inspected is not None:
        require(bins<=native_bins_inspected,'hover_resolution_insufficient')
    coverage=dict(sidebar_count=len(sidebar_unique),sidebar_amount=_numeric(side_sum),sidebar_share_pct=ratio(side_sum,total),
                  verified_count=len(levels),verified_amount=_numeric(verified_sum),verified_share_pct=ratio(verified_sum,total),
                  significant_bins=bins,priced_bins=priced,unpriced_bins=bins-priced if bins is not None else None,
                  regions_total=len(regions) if regions is not None else None,
                  regions_with_amount=sum(r['amount'] is not None for r in regions) if regions is not None else None,
                  native_bins_inspected=native_bins_inspected,full_map_amounts_verified=False,duplicates_removed=duplicates)
    fields=raw_display(overview)
    source_rows={k:end_observation.get(k) for k in required_close}
    source_rows.update(source_time=source_time_after,raw_last_price=raw_last_price_after)
    if provenance=='live':
        source_rows.update({k:end_observation[k] for k in ('symbol','heatmap_model','timeframe','mode','column_time',
                          'same_document','settings_verified','same_market_labels','sidebar_rows','chart_regions','native_bins_inspected')})
    if 'range_control' in end_observation:
        selection = end_observation['range_control']
        if not valid_range_selection(timeframe, selection):
            reject_observation('closing_range_control_mismatch', 'hover_snapshot_changed')
        source_rows['range_control'] = dict(selection)
    elif require_fresh_evidence or cursor_proof is not None:
        reject_observation('closing_range_control_missing', 'hover_snapshot_changed')
    if require_fresh_evidence and cursor_proof is None:
        reject_observation('cursor_proof_missing', 'hover_snapshot_changed')
    cursor={}
    if cursor_proof is not None:
        from modern_map_cursor import validate_receipt
        validate_receipt(cursor_proof,overview.get('raw_column_period'),timeframe,column_time)
        cursor['cursor_proof']=cursor_proof
    strongest={}
    for side in ('above','below'):
        members=[row for row in levels if row['side']==side]
        peak=max((number(row['raw_value'],True)[0]-number(row['raw_value'],True)[1] for row in members),default=Decimal(0))
        strongest[side]=[row['raw_price'] for row in members if sum(number(row['raw_value'],True))>=peak]
    return dict(schema=SCHEMA,result_type='verified_basic',collection_tier='enriched' if added else 'basic',
                enrichment=enrichment_report,symbol=symbol,heatmap_model=model,timeframe=timeframe,
                mode='Coin',layout='Single chart',lens='Heat',palette='Viridis',theme='dark',intensity=[0,100],
                same_document=source_rows.get('same_document') is True,source_url=source_url,source_time=source_time,source_time_after=source_time_after,
                column_time=column_time,column_time_after=column_time_after,captured_at=captured_at,
                raw_last_price=raw_last_price,raw_last_price_after=raw_last_price_after,reference_price=_numeric(ref[0]),
                source_sha256=source_sha256,provenance=provenance,end_reference_verified=end_ref is not None,
                end_column_verified=column_time_after==column_time,metric_unit='source_metric',**raw,
                parsed_amounts={k.removeprefix('raw_'):_numeric(v[0]) if v is not None else None for k,v in nums.items()},
                side_percent=sides,difference_above_minus_below=_numeric(nums['raw_above_total'][0]-nums['raw_below_total'][0]),
                difference_percentage_points=ratio(nums['raw_above_total'][0]-nums['raw_below_total'][0],total),
                strongest_observed_prices=strongest,levels=levels,chart_regions=regions,coverage=coverage,
                source_evidence=dict(sidebar_rows=sidebar,sidebar_reference=overview.get('raw_last_price'),enrichment=extra,
                                     closing=source_rows,display_values=fields,**cursor),
                printed_precision=dict(source_time='minute',column_time='minute',amounts='original_tokens',
                                       captured_at='end_observation' if provenance=='retained_reconstruction' else 'capture_clock'))


def _same_json(first, second):
    """JSONB can lose insignificant trailing zeros; keep booleans distinct from numbers."""
    if type(first) in (int,float):
        if type(second) not in (int,float):
            return False
        a,b=Decimal(str(first)),Decimal(str(second))
        return a.is_finite() and b.is_finite() and a==b
    if type(first) is not type(second):
        return False
    if isinstance(first,dict):
        return first.keys()==second.keys() and all(_same_json(v,second[k]) for k,v in first.items())
    if isinstance(first,list):
        return len(first)==len(second) and all(_same_json(a,b) for a,b in zip(first,second))
    return first==second


def validate_basic_snapshot(snapshot, *, restored=False, require_fresh_evidence=False):
    """Replay the same derivation; reject any changed counts, prices, nulls or coverage claims."""
    require(isinstance(snapshot,dict) and snapshot.get('schema')==SCHEMA)
    try:
        e=snapshot['source_evidence']
        r=snapshot['chart_regions']
        raw={k:snapshot[k] for k in (*AMOUNTS,*PERCENTAGES,*REFERENCE,'source_time')}
        raw.update(sidebar_levels=e['sidebar_rows'],display_values=e['display_values'])
        # Retain the DOM's own printed precision independently of the chart reference.
        raw['raw_last_price']=e.get('sidebar_reference')
        base=build_basic_snapshot(raw, symbol=snapshot['symbol'],model=snapshot['heatmap_model'],
                timeframe=snapshot['timeframe'],source_url=snapshot['source_url'],captured_at=snapshot['captured_at'],
                source_time_after=snapshot['source_time_after'],column_time=snapshot['column_time'],
                column_time_after=snapshot['column_time_after'],raw_last_price=snapshot['raw_last_price'],
                raw_last_price_after=snapshot['raw_last_price_after'],source_sha256=snapshot['source_sha256'],
                cursor_proof=e.get('cursor_proof'),require_fresh_evidence=require_fresh_evidence,
                chart_regions=None if r is None else [{k:g[k] for k in ('side','colour','prices')} for g in r],
                native_bins_inspected=snapshot['coverage']['native_bins_inspected'],provenance=snapshot['provenance'],
                enrichment=e['enrichment'],end_observation=e['closing'])
        if _same_json(base,snapshot):
            return snapshot
        # Previous checkpoints used the matched *numeric labels* to calculate
        # distance on fully matched regions, whilst the UI shows the plotted
        # endpoints. Only replay exactly that historical derivation. The
        # original receipt stays immutable and the result uses the shown midpoint.
        if restored:
            reference=number(base['raw_last_price'])[0]
            legacy_groups=[]
            for group in base['chart_regions'] or []:
                previous=group
                if group['coverage']=='verified':
                    labels=group['verified_prices']
                    midpoint=(number(labels[0])[0]+number(labels[-1])[0])/2
                    previous=dict(group,distance_pct=ratio(midpoint-reference,reference))
                legacy_groups.append(previous)
            if _same_json(dict(base,chart_regions=legacy_groups),snapshot):
                return base
        require(False,'hover_evidence_mismatch')
    except (KeyError,TypeError,ValueError,ArithmeticError):
        raise StageFailure('source_not_readable','hover_evidence_mismatch') from None
