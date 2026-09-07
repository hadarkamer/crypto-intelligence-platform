"""Isolated HYPE archive supplement using official Binance Futures MARK 1m.

The original Spot run is immutable. Source features, source timestamps and BTC
parents are retained; the next-full-minute entry has its own MARK contract.
This module never changes canonical Spot validation or promotes a formula.
"""
from __future__ import annotations
import argparse
from contextlib import closing
from datetime import datetime,timedelta
import hashlib
import json
from pathlib import Path
import sqlite3
from typing import Any,Mapping

import binance_futures_mark_price_path as provider
from research_ordered_first_touch import METHOD_VERSION,calculate_all_ordered_first_touch_outcomes,_excursion_metrics
from research_telegram_archive_backfill import WINDOWS,canonical,initialize,parent_policy
from research_telegram_archive_features import FEATURE_VERSION,DIRECTION_VERSION,TIME_VERSION,utc
from research_telegram_html_archive import _hash

BACKFILL_VERSION='archive-hype-futures-mark-supplement-v1'
ENTRY_VERSION='archive-next-full-minute-binance-futures-mark-open-v1'
METRIC_VERSION='archive-common-window-futures-mark-1m-v1'
VERIFIED_QUALITY='VERIFIED_BINANCE_FUTURES_MARK_1M_CLOSED_CANDLES'
PARTIAL_QUALITY='INCOMPLETE_BINANCE_FUTURES_MARK_1M_PATH'
MINUTE=timedelta(minutes=1)
SOURCE={'symbol':'HYPE','pair':'HYPEUSDT','exchange':'binance','market':'futures',
    'price_kind':'MARK','interval':'1m','interval_seconds':60,
    'source_url':provider.SOURCE_URL,'method_version':provider.METHOD_VERSION,
    'provenance':provider.PROVENANCE}
CONTRACT_KEYS={'backfill_version','prepared_stage_digest','entry_policy_version','feature_version',
    'direction_version','time_version','parent_policy_version','source_scope','threshold_bps',
    'window_minutes','variants','cache_sha256','live_union_eligible','phase','formula_relevance',
    'base_run_key','base_artifact_sha256','price_source','observed_at_utc'}


def run_identity(contract:Mapping[str,Any])->str:
    return _hash(BACKFILL_VERSION,canonical(contract))


def validate_contract(contract:Mapping[str,Any],run_key:str)->None:
    expected={'backfill_version':BACKFILL_VERSION,'entry_policy_version':ENTRY_VERSION,
        'feature_version':FEATURE_VERSION,'direction_version':DIRECTION_VERSION,'time_version':TIME_VERSION,
        'parent_policy_version':parent_policy.POLICY_VERSION,'source_scope':'ARCHIVE_ONLY',
        'threshold_bps':list(range(25,201,25)),'window_minutes':list(WINDOWS),
        'variants':['NORMAL','INVERSE'],'live_union_eligible':False,'phase':'DISCOVERY',
        'formula_relevance':'NOT_EVALUATED','price_source':SOURCE}
    if set(contract)!=CONTRACT_KEYS or any(contract.get(k)!=v for k,v in expected.items()) or run_identity(contract)!=run_key:
        raise ValueError('Unknown or changed HYPE Futures supplement contract')
    for key in ('prepared_stage_digest','cache_sha256','base_run_key','base_artifact_sha256'):
        value=contract.get(key)
        if not isinstance(value,str) or len(value)!=64 or any(c not in '0123456789abcdef' for c in value):
            raise ValueError('HYPE supplement provenance digest is missing')
    if utc(contract['observed_at_utc']).isoformat()!=contract['observed_at_utc']:
        raise ValueError('HYPE supplement observation cutoff must be frozen UTC')


def event_identity(base_event_key:str)->str:
    return _hash(BACKFILL_VERSION,base_event_key,ENTRY_VERSION,provider.METHOD_VERSION)


def derive_event(original:Mapping[str,Any],*,base_run_key:str)->dict[str,Any]:
    if original.get('symbol')!='HYPE' or original.get('calculation_status')!='UNSUPPORTED_SPOT_SYMBOL' or original.get('reconstruction_status')!='READY_FOR_SPOT_ENTRY_PATH':
        raise ValueError('Supplement accepts only previously unsupported reconstructed HYPE')
    if original.get('analysis_direction') not in ('LONG','SHORT') or original.get('conflicts') or original.get('missing_evidence'):
        raise ValueError('Supplement cannot repair missing or conflicting source evidence')
    source_time=utc(original['source_message_time_utc'])
    entry=utc(original['entry_time_utc'])
    if entry!=source_time.replace(second=0,microsecond=0)+MINUTE:
        raise ValueError('Original next-full-minute entry is inconsistent')
    return {**original,'base_archive_run_key':base_run_key,
        'base_archive_event_key':original['archive_event_key'],
        'base_entry_policy_version':original['entry_policy_version'],
        'archive_event_key':event_identity(original['archive_event_key']),
        'entry_policy_version':ENTRY_VERSION,'entry_price':None,'entry_price_source':None,
        'price_source_contract':dict(SOURCE),'source_scope':'ARCHIVE_ONLY','record_mode':'ARCHIVE',
        'statistical_phase':'DISCOVERY','live_union_eligible':False,
        'reconstruction_status':'READY_FOR_FUTURES_MARK_ENTRY_PATH','calculation_status':'PENDING'}


def _validated_bar(raw:Mapping[str,Any])->dict[str,Any]:
    # Shared exact finite OHLC/closed-minute validation; no Spot provenance is
    # inferred by using this pure candle validator.
    for field in ('open_time_utc','close_time_utc'):utc(raw[field])
    return parent_policy.validate_candle(raw)


def load_cache(path:Path)->dict[datetime,dict[str,Any]]:
    bars={}
    if not path.exists():return bars
    with path.open(encoding='utf-8') as handle:
        if json.loads(next(handle,'null'))!={'price_source_contract':SOURCE}:
            raise ValueError('HYPE cache source is not official Futures MARK')
        for line in handle:
            bar=_validated_bar(json.loads(line));opened=bar['open_time_utc']
            if opened in bars and bars[opened]!=bar:raise ValueError('Conflicting HYPE MARK cache candle')
            bars[opened]=bar
    return bars


def fill_cache(path:Path,*,start:datetime,end:datetime,allow_network:bool,max_fetches:int)->tuple[dict[datetime,dict[str,Any]],int,list[dict[str,str]]]:
    bars=load_cache(path);fetches=0;failures=[]
    if not path.exists():path.write_text(canonical({'price_source_contract':SOURCE})+'\n',encoding='utf-8')
    cursor=start
    while cursor<end and allow_network and fetches<max_fetches:
        if cursor in bars:cursor+=MINUTE;continue
        stop=min(end,cursor+timedelta(minutes=1440))
        fetches+=1
        try:
            result=provider.fetch_closed_candles('HYPE',cursor,stop)
            if any(result.get(key)!=value for key,value in SOURCE.items()):
                raise ValueError('HYPE fetch returned a different source contract')
            new=[]
            for raw in result['candles']:
                bar=_validated_bar(raw);opened=bar['open_time_utc']
                if not cursor<=opened<stop:raise ValueError('HYPE cache fetch returned an out-of-window candle')
                if opened in bars and bars[opened]!=bar:raise ValueError('Fresh HYPE MARK candle conflicts with cache')
                if opened not in bars:new.append(bar)
            with path.open('a',encoding='utf-8') as handle:
                for bar in new:handle.write(canonical(bar)+'\n');bars[bar['open_time_utc']]=bar
        except Exception as exc:
            failures.append({'start':cursor.isoformat(),'error':str(exc)[:240]})
            break
        cursor=stop
    return bars,fetches,failures


def calculate_event(event:dict[str,Any],bars:Mapping[datetime,Mapping[str,Any]],*,observed_at:datetime)->tuple[dict[str,Any],list[dict[str,Any]],list[dict[str,Any]]]:
    if event.get('symbol')!='HYPE' or event.get('entry_policy_version')!=ENTRY_VERSION or event.get('price_source_contract')!=SOURCE:
        raise ValueError('HYPE measurement requires its isolated Futures MARK entry contract')
    entry=utc(event['entry_time_utc']);now=utc(observed_at)
    if entry>now:return {**event,'calculation_status':'NOT_YET_ENTRY'},[],[]
    if entry not in bars:return {**event,'calculation_status':'DATA_MISSING_ENTRY_CANDLE'},[],[]
    entry_bar=_validated_bar(bars[entry])
    if entry_bar['open_time_utc']!=entry:raise ValueError('HYPE entry candle time does not match its cache key')
    reference=entry_bar['open']
    event={**event,'entry_price':reference,'entry_price_source':'BINANCE_FUTURES_MARK_1M_OPEN'}
    labels=[];metrics=[]
    for variant in ('NORMAL','INVERSE'):
        direction=event['analysis_direction'] if variant=='NORMAL' else {'LONG':'SHORT','SHORT':'LONG'}[event['analysis_direction']]
        event_id=_hash(event['archive_event_key'],variant)
        for horizon in WINDOWS:
            end=entry+timedelta(minutes=horizon);cutoff=min(end,now.replace(second=0,microsecond=0))
            expected=[entry+index*MINUTE for index in range(max(0,int((cutoff-entry).total_seconds()//60)))]
            path=[_validated_bar(bars[opened]) for opened in expected if opened in bars]
            prefix_complete=[bar['open_time_utc'] for bar in path]==expected
            complete=bool(prefix_complete and now>=end)
            path_sha=hashlib.sha256(canonical({'source':SOURCE,'candles':path}).encode()).hexdigest()
            full={'method_version':METRIC_VERSION,'measurement_kind':'FIXED_WINDOW','event_id':event_id,
                'symbol':'HYPE','direction':direction,'signal_variant':variant,'window_minutes':horizon,
                'entry_policy_version':ENTRY_VERSION,'source_scope':'ARCHIVE_ONLY','reference_price':reference,
                'measurement_start_utc':entry,'window_end_utc':end,'observed_at_utc':now,
                'observed_from_utc':path[0]['open_time_utc'] if path else None,
                'observed_through_utc':path[-1]['close_time_utc'] if path else None,
                'status':'READY' if complete else 'OPEN' if prefix_complete else 'DATA_MISSING',
                'observation_closed':now>=end,'path_complete':complete,'observed_prefix_complete':prefix_complete,
                'expected_candles':len(expected),'path_samples':len(path),'initial_gap_seconds':0,
                'trailing_partial_minute_seconds':0,'candle_interval_seconds':60,'source':dict(SOURCE),
                'data_quality_status':VERIFIED_QUALITY if complete else PARTIAL_QUALITY,'path_sha256':path_sha,
                'boundary_policy':'NEXT_FULL_MINUTE_MARK_OPEN_CLOSED_1M_ONLY',
                'mfe_pct':None,'mae_pct':None,'asymmetry_ratio':None,
                'asymmetry_method':'sum_mfe_pct_over_sum_mae_pct_same_full_window_v1','asymmetry_status':'WINDOW_NOT_READY'}
            if complete:
                full.update(_excursion_metrics(reference_price=reference,direction=direction,
                    maximum=max([reference]+[bar['high'] for bar in path]),minimum=min([reference]+[bar['low'] for bar in path])))
                full['asymmetry_ratio']=full['mfe_pct']/full['mae_pct'] if full['mae_pct']>0 else None
                full['asymmetry_status']='DEFINED' if full['mae_pct']>0 else 'UNDEFINED_ZERO_MAE'
            metrics.append(full)
            for label in calculate_all_ordered_first_touch_outcomes(reference_price=reference,direction=direction,event_time=entry,
                    candles=path,observation_closed=now>=end,path_complete=prefix_complete):
                label.update({'event_id':event_id,'outcome_id':_hash(event_id,METHOD_VERSION,horizon,label['threshold_bps']),
                    'window_minutes':horizon,'outcome_method_version':METHOD_VERSION,'signal_variant':variant,
                    'source_scope':'ARCHIVE_ONLY','record_mode':'ARCHIVE','entry_policy_version':ENTRY_VERSION,
                    'data_quality_status':VERIFIED_QUALITY if prefix_complete else PARTIAL_QUALITY,
                    'source_price_exchange':'binance','source_price_market':'futures','source_price_pair':'HYPEUSDT',
                    'source_price_kind':'MARK','price_source_contract':dict(SOURCE),'path_sha256':path_sha})
                labels.append(label)
    event['calculation_status']='COMPLETE_64_LABELS' if all(metric['status']=='READY' for metric in metrics) else 'RETRY_INCOMPLETE_PATH'
    return event,labels,metrics


def run(*,source_artifact:Path,expected_source_sha256:str,base_run_key:str,output_dir:Path,
        observed_at:datetime,event_limit:int=1500,allow_network:bool=False,max_fetches:int=40)->dict[str,Any]:
    import research_telegram_archive_runtime_importer as importer
    if type(event_limit) is not int or not 1<=event_limit<=15000 or type(max_fetches) is not int or not 0<=max_fetches<=100:
        raise ValueError('Bounded event and fetch limits required')
    now=utc(observed_at)
    output_dir.mkdir(parents=True,exist_ok=True)
    target=output_dir/'archive_reconstructed_research.sqlite'
    if target.resolve()==source_artifact.resolve():raise ValueError('Supplement must not overwrite the source artifact')
    with closing(importer.open_reviewed_artifact(source_artifact,expected_source_sha256)) as source:
        original=source.execute('SELECT contract_json FROM archive_reconstruction_runs WHERE run_key=?',(base_run_key,)).fetchone()
        if original is None:raise ValueError('Reviewed base archive run is missing')
        base_contract=json.loads(original[0]);importer.validate_contract(base_contract,base_run_key)
        if base_contract.get('backfill_version')!=importer.BACKFILL_VERSION:
            raise ValueError('Supplement requires the original Spot archive run')
        rows=source.execute("SELECT * FROM archive_reconstructed_events WHERE run_key=? AND symbol='HYPE' AND calculation_status='UNSUPPORTED_SPOT_SYMBOL' ORDER BY source_time_utc,event_key LIMIT 15001",(base_run_key,)).fetchall()
        if not rows or len(rows)>15000:raise ValueError('No bounded unsupported HYPE population found')
        events=[derive_event(importer._event_record(row)['event_payload']|{'calculation_status':row['calculation_status']},base_run_key=base_run_key) for row in rows]
        parent_rows=source.execute('SELECT * FROM archive_btc_parents WHERE run_key=? ORDER BY btc_parent_movement_id',(base_run_key,)).fetchall()
        cache_path=output_dir/'hype_futures_mark_1m.jsonl'
        start=min(utc(event['entry_time_utc']) for event in events)
        end=min(max(utc(event['entry_time_utc']) for event in events)+timedelta(days=1),now.replace(second=0,microsecond=0))
        bars,fetches,failures=fill_cache(cache_path,start=start,end=end,allow_network=allow_network,max_fetches=max_fetches)
        contract={'backfill_version':BACKFILL_VERSION,'prepared_stage_digest':base_contract['prepared_stage_digest'],
            'entry_policy_version':ENTRY_VERSION,'feature_version':FEATURE_VERSION,'direction_version':DIRECTION_VERSION,
            'time_version':TIME_VERSION,'parent_policy_version':parent_policy.POLICY_VERSION,'source_scope':'ARCHIVE_ONLY',
            'threshold_bps':list(range(25,201,25)),'window_minutes':list(WINDOWS),'variants':['NORMAL','INVERSE'],
            'cache_sha256':importer.file_sha256(cache_path),'live_union_eligible':False,'phase':'DISCOVERY',
            'formula_relevance':'NOT_EVALUATED','base_run_key':base_run_key,'base_artifact_sha256':expected_source_sha256,
            'price_source':SOURCE,'observed_at_utc':now.isoformat()}
        run_key=run_identity(contract);validate_contract(contract,run_key)
        processed=0
        with sqlite3.connect(target) as conn:
            initialize(conn)
            conn.execute('INSERT OR IGNORE INTO archive_reconstruction_runs VALUES (?,?)',(run_key,canonical(contract)))
            conn.executemany('INSERT OR IGNORE INTO archive_reconstructed_events VALUES (?,?,?,?,?,?,?)',[(run_key,event['archive_event_key'],event['source_message_time_utc'],'HYPE',event['reconstruction_status'],'PENDING',canonical(event)) for event in events])
            conn.executemany('INSERT OR IGNORE INTO archive_btc_parents VALUES (?,?,?)',[(run_key,row['btc_parent_movement_id'],row['parent_json']) for row in parent_rows])
            conn.commit()
            pending=conn.execute("SELECT event_key,event_json FROM archive_reconstructed_events WHERE run_key=? AND calculation_status<>'COMPLETE_64_LABELS' ORDER BY source_time_utc,event_key LIMIT ?",(run_key,event_limit)).fetchall()
            for key,encoded in pending:
                event,labels,metrics=calculate_event(json.loads(encoded),bars,observed_at=now)
                conn.executemany('INSERT OR REPLACE INTO archive_delayed_entry_outcomes VALUES (?,?,?,?,?,?,?,?)',[(run_key,key,row['signal_variant'],row['window_minutes'],row['threshold_bps'],row['outcome_id'],row['status'],canonical(row)) for row in labels])
                conn.executemany('INSERT OR REPLACE INTO archive_common_window_metrics VALUES (?,?,?,?,?,?)',[(run_key,key,row['signal_variant'],row['window_minutes'],row['status'],canonical(row)) for row in metrics])
                conn.execute('UPDATE archive_reconstructed_events SET calculation_status=?,event_json=? WHERE run_key=? AND event_key=?',(event['calculation_status'],canonical(event),run_key,key));conn.commit();processed+=1
            counts=dict(conn.execute('SELECT calculation_status,COUNT(*) FROM archive_reconstructed_events WHERE run_key=? GROUP BY calculation_status',(run_key,)))
        report={**contract,'run_key':run_key,'source_events':len(events),'processed_this_invocation':processed,
            'calculation_status_counts':counts,'mark_cache_candles':len(bars),'fetches_this_invocation':fetches,
            'fetch_failures':failures,'observed_at_utc':now.isoformat(),'production_rows_written':0,
            'sqlite_artifact':str(target),'sqlite_sha256':importer.file_sha256(target)}
        (output_dir/'hype_supplement_report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
        return report


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-artifact',type=Path,required=True)
    parser.add_argument('--expected-source-sha256',required=True)
    parser.add_argument('--base-run-key',required=True)
    parser.add_argument('--output-dir',type=Path,required=True)
    parser.add_argument('--observed-at',required=True)
    parser.add_argument('--event-limit',type=int,default=1500)
    parser.add_argument('--allow-network',action='store_true')
    parser.add_argument('--max-fetches',type=int,default=40)
    args=vars(parser.parse_args());args['observed_at']=utc(args['observed_at'])
    print(json.dumps(run(**args),ensure_ascii=False,indent=2))
