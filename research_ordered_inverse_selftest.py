"""Research inverse identity, canonical recomputation and queue regression tests."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import research_ordered_inverse_store as store
import research_ordered_inverse_worker as worker

START = datetime(2026,9,7,1,tzinfo=timezone.utc)


def original():
    return {'event_id':7,'schema_version':'research-event-v1','event_kind':'ALERT',
        'event_type':'MAX_PAIN_ALERT','alert_time_utc':START,'symbol':'BTC','direction':'LONG',
        'source_side':'SHORT','timeframe':'4h','score':80.0,'current_price':100.0,
        'target_price':102.0,'initial_target_distance_pct':2.0,'categories':['MAX_PAIN'],
        'setup_key':'a'*64,'event_fingerprint':'b'*64,'strategy_version':'native-v1',
        'code_version':'original-code','runtime_session_id':'original-run',
        'delivery_status':'DELIVERED','engine_snapshot':{'price_source':'binance_spot',
            'price_pair':'BTCUSDT','sheet_snapshot_id':'snapshot-original','raw_score':80}}


class Result:
    def __init__(self, rows=()): self.rows=list(rows)
    def fetchone(self): return self.rows[0] if self.rows else None
    def fetchall(self): return self.rows


class Database:
    def __init__(self, source=None):
        self.events={7:source or original()}; self.requests={}; self.labels={}
        self.sql=[]; self.open_transactions=0; self.sheet_payloads=[]
    def __enter__(self): self.open_transactions+=1; return self
    def __exit__(self,*args): self.open_transactions-=1
    def execute(self, query, params=()):
        self.sql.append((query,params))
        if 'to_regclass' in query:
            return Result([{'available':'research_ordered_inverse_requests' in query}])
        if query.startswith('SELECT * FROM research_events WHERE event_id='):
            return Result([self.events[params[0]]] if params[0] in self.events else [])
        if 'INSERT INTO research_ordered_inverse_requests' in query:
            eid,version,fp,sha,contract,status,requested,due,error=params
            self.requests.setdefault(eid,{'linked_source_event_id':eid,'inverse_version':version,
                'source_fingerprint':fp,'source_contract_sha256':sha,'source_contract':json.loads(contract),
                'queue_status':status,'outcome_event_id':None,'last_error':error,
                'requested_at_utc':requested,'next_attempt_at_utc':due})
            return Result()
        if 'SELECT outcome_event_id,source_contract_sha256' in query:
            return Result([self.requests[params[0]]])
        if 'INSERT INTO research_events (' in query:
            event=dict(params); event['categories']=json.loads(event['categories'])
            event['engine_snapshot']=json.loads(event['engine_snapshot'])
            existing=next((e for e in self.events.values() if e['event_fingerprint']==event['event_fingerprint']),None)
            if not existing: event['event_id']=9001; self.events[9001]=event
            return Result()
        if query.startswith('SELECT * FROM research_events WHERE event_fingerprint='):
            return Result([e for e in self.events.values() if e['event_fingerprint']==params[0]])
        if 'SET outcome_event_id=' in query:
            self.requests[params[1]]['outcome_event_id']=params[0]; return Result()
        if 'SELECT claim_token' in query:
            row=self.requests[params[0]]
            return Result([row] if row.get('claim_token')==params[2] else [])
        if 'INSERT INTO research_ordered_first_touch_outcomes' in query:
            row=json.loads(params[0]); assert row['direction']==self.events[row['event_id']]['direction']
            key=(row['event_id'],row['window_minutes'],row['threshold_bps'])
            old=self.labels.get(key)
            if old and old['status'] not in ('OPEN','DATA_MISSING'): return Result()
            self.labels[key]=row; return Result([{'event_id':row['event_id']}])
        if 'INSERT INTO research_ordered_first_touch_sync_outbox' in query:
            self.sheet_payloads.append(json.loads(params[5])); return Result()
        if 'SELECT status,COUNT(*)' in query:
            from collections import Counter
            counts=Counter(row['status'] for key,row in self.labels.items() if key[0]==params[0])
            return Result([{'status':key,'count':value} for key,value in counts.items()])
        if 'SET queue_status=%s' in query:
            status,checked,due,error,result,eid,version,token=params
            row=self.requests[eid]
            if row.get('claim_token')==token:
                row.update(queue_status=status,last_checked_at_utc=checked,next_attempt_at_utc=due,
                           last_error=error,result=json.loads(result),claim_token=None)
            return Result()
        if 'SELECT linked_source_event_id,outcome_event_id' in query:
            return Result([row for eid,row in self.requests.items() if eid in params[0]
                and row['outcome_event_id'] and row['queue_status']!='REJECTED'])
        if query.startswith('SELECT * FROM research_ordered_first_touch_outcomes'):
            return Result([row for (eid,window,threshold),row in self.labels.items()
                if eid in params[0] and window==params[1] and threshold==params[2]])
        raise AssertionError('Unexpected SQL: '+query[:140])


def candle(index,high=100.0,low=100.0):
    t=START+timedelta(minutes=index)
    return SimpleNamespace(open_time_utc=t,close_time_utc=t+timedelta(seconds=59,milliseconds=999),
        open=100.0,high=high,low=low,close=100.0,volume=1)


def run_job(db, candles, *, now=START+timedelta(days=1), bad_route=False):
    store.request_inverse(db,{'event_id':7,'current_price':1},now=now)
    pending=[db.requests[7]]
    def claim(conn,*,now,token):
        if not pending: return None
        row=pending.pop(); row.update(claim_token=token,queue_status='IN_FLIGHT')
        return dict(row)
    def fetch(symbol,start,end):
        assert db.open_transactions==0, 'external fetch cannot hold a claim/source transaction'
        assert symbol=='BTC' and start==START
        return {'symbol':'BTC','exchange':'binance','market':'futures' if bad_route else 'spot',
            'pair':'BTCUSDT','interval':'1m','interval_seconds':60,'complete':True,
            'candles':candles,'expected_candles':len(candles),'retrieved_at_utc':now.isoformat()}
    with patch.object(worker,'_claim',claim):
        return worker.run_pending('postgresql://selftest',now=now,event_limit=4,
            fetch_candles=fetch,connect=lambda _:db)


def run():
    source=original(); before=deepcopy(source)
    event=store.derived_event(source)
    assert source==before and event['event_kind']=='DECISION_SAMPLE'
    assert event['delivery_status']=='NOT_APPLICABLE' and event['delivered_at_utc'] is None
    assert event['direction']=='SHORT', 'MaxPain original analysis LONG must flip only once'
    assert event['alert_time_utc']==START and event['current_price']==100
    assert event['event_fingerprint']==store.derived_event(source)['event_fingerprint']
    assert event['engine_snapshot']['price_source']=='binance_spot'
    assert event['engine_snapshot']['inverse_analysis']['linked_source_event_id']==7
    assert event['target_price'] is None and event['engine_snapshot']['inverse_analysis']['original_target_price']==102
    try: store.derived_event({**event,'event_id':9001})
    except ValueError: pass
    else: raise AssertionError('Inverse was inverted twice')
    for changes in ({'direction':'NEUTRAL'},{'delivery_status':'UNKNOWN'},
                    {'current_price':0},{'alert_time_utc':START.replace(tzinfo=None)}):
        assert store.source_error({**source,**changes})
    invalid=deepcopy(source); invalid['engine_snapshot']['price_source']='bybit_futures'
    assert store.source_error(invalid)

    for field, bad in (('current_price',float('nan')),('current_price',float('inf')),
                       ('target_price',float('-inf')),('score',float('nan'))):
        rejected=Database({**source,field:bad})
        assert store.request_inverse(rejected,{'event_id':7},now=START) is None
        saved=deepcopy(rejected.requests[7])
        assert saved['queue_status']=='REJECTED' and saved['outcome_event_id'] is None
        audit=saved['source_contract']
        assert audit['audit_kind']=='REJECTED_IMMUTABLE_SOURCE'
        assert audit['source_event_id']==7 and audit['source_event_fingerprint']==source['event_fingerprint']
        assert audit['source_fields'][field]=={'invalid_nonfinite_number':str(bad)}
        assert audit['rejection_reason'] and len(rejected.events)==1
        json.dumps(audit,allow_nan=False)
        # Invalid source data stays unchanged; no zero or synthetic replacement.
        assert str(rejected.events[7][field])==str(bad)
        # Even if the original later changes, this rejected request cannot adopt it.
        rejected.events[7]=original()
        assert store.request_inverse(rejected,{'event_id':7},now=START) is None
        assert rejected.requests[7]==saved
        # Another valid source in the same pass can still be queued.
        rejected.events[8]={**original(),'event_id':8,'event_fingerprint':'c'*64}
        assert store.request_inverse(rejected,{'event_id':8},now=START) is None
        assert rejected.requests[8]['queue_status']=='PENDING'
    naive=Database({**source,'alert_time_utc':START.replace(tzinfo=None)})
    store.request_inverse(naive,{'event_id':7},now=START)
    assert naive.requests[7]['queue_status']=='REJECTED'
    assert naive.requests[7]['source_contract']['source_fields']['alert_time_utc']=={'source_datetime':'2026-09-07T01:00:00'}

    db=Database(); assert store.request_inverse(db,{'event_id':7},now=START) is None
    assert store.request_inverse(db,{'event_id':7},now=START+timedelta(hours=1)) is None
    assert len(db.requests)==1 and len(db.events)==1, 'requesting is not eager event materialization'
    path=[candle(i,low=99.7 if i==0 else 100.0) for i in range(1440)]
    result=run_job(db,path)
    assert result['inverse_checked']==1 and result['inverse_written']==32
    assert db.events[7]==before and len(db.events)==2
    assert db.requests[7]['queue_status']=='COMPLETE'
    assert db.labels[(9001,60,25)]['status']=='SUCCESS'
    assert db.labels[(9001,60,50)]['status']=='UNRESOLVED'
    assert db.labels[(9001,60,25)]['favorable_touch_price']==99.75
    assert len(db.sheet_payloads)==32 and all(p['row']['event_id']=='9001' for p in db.sheet_payloads)
    assert store.outcome_event_ids(db,[7])=={7:9001}
    found=store.load_inverse_outcomes(db,[7],60,25)
    assert set(found)=={7} and found[7]['outcome_event_id']==9001
    assert found[7]['event_id']==9001 and found[7]['linked_source_event_id']==7
    assert store.request_inverse(db,{'event_id':7},now=START)==9001
    # A later incomplete retry must not erase valid terminal canonical labels.
    result=run_job(db,path[:2])
    assert result['inverse_written']==0 and db.requests[7]['queue_status']=='COMPLETE'
    assert len(db.events)==2 and db.labels[(9001,60,25)]['status']=='SUCCESS'

    ambiguous=Database()
    path=[candle(i,high=100.6 if i==0 else 100,low=99.4 if i==0 else 100) for i in range(1440)]
    run_job(ambiguous,path)
    label=ambiguous.labels[(9001,60,50)]
    assert label['first_touch_side']=='AMBIGUOUS' and label['success'] is None
    incomplete=Database(); run_job(incomplete,path[:2])
    assert set(row['status'] for row in incomplete.labels.values())=={'DATA_MISSING'}
    assert incomplete.requests[7]['queue_status']=='RETRY'
    opened=Database(); run_job(opened,[candle(i) for i in range(30)],now=START+timedelta(minutes=30))
    assert set(row['status'] for row in opened.labels.values())=={'OPEN'}
    assert opened.requests[7]['next_attempt_at_utc']==START+timedelta(minutes=31)
    wrong_route=Database(); result=run_job(wrong_route,path,bad_route=True)
    assert result['inverse_path_failures']==1 and not wrong_route.labels
    assert wrong_route.requests[7]['queue_status']=='RETRY'
    sql=(Path(__file__).parent/'migrations/033_ordered_inverse_analysis_requests.sql').read_text()
    assert 'REFERENCES research_events(event_id)' in sql and 'DROP ' not in sql
    assert all('research_event_btc_movements' not in query for query,_ in db.sql)
    print('canonical inverse analysis adapter selftest: PASS')


if __name__=='__main__': run()
