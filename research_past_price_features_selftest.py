"""Causal-window, partial coverage, retry immutability and bounded HTTP checks."""
from __future__ import annotations
from datetime import datetime,timedelta,timezone
from types import SimpleNamespace
from unittest.mock import patch
import json
import sqlite3

import research_past_price_features as past
import research_past_price_features_store as store
import research_past_price_features_worker as worker

ENTRY=datetime(2026,9,7,12,0,30,tzinfo=timezone.utc)
START=ENTRY.replace(second=0)-timedelta(days=1)
ROUTE={'symbol':'ETH','exchange':'binance','market':'spot','pair':'ETHUSDT','interval':'1m','interval_seconds':60,'complete':True}
BTC={**ROUTE,'symbol':'BTC','pair':'BTCUSDT'}


def candle(i,*,high=101.,low=99.,close=100.):
    opened=START+timedelta(minutes=i)
    return SimpleNamespace(open_time_utc=opened,close_time_utc=opened+timedelta(seconds=59,milliseconds=999),
        open=100.,high=high,low=low,close=close)


def calculate(path,btc=None,**kwargs):
    args=dict(symbol='ETH',event_time=ENTRY,candles=path,path_result=ROUTE,
        btc_candles=btc if btc is not None else path,btc_path_result=BTC,computed_at=ENTRY+timedelta(minutes=1))
    args.update(kwargs)
    return past.calculate_past_price_features(**args)


def check_causality_and_features():
    path=[candle(i) for i in range(1440)]
    path[-1]=candle(1439,high=102,close=102)
    btc=[candle(i) for i in range(1440)]
    result=calculate(path,btc)
    assert result['status']=='READY' and result['paired_windows_ready']==6
    item=result['windows']['15m']['asset']
    assert item['path_samples']==15 and item['gap_to_entry_seconds']==30
    assert item['direction']=='UP' and abs(item['return_pct']-2)<1e-12
    assert item['range_pct']==3 and item['volatility_pct']>0
    assert item['window_end_utc']<ENTRY and item['window_start_utc']==ENTRY.replace(second=0)-timedelta(minutes=15)
    future=candle(1440,high=10000,low=1,close=10000)
    changed=calculate(path+[future],btc+[future],computed_at=ENTRY+timedelta(days=30))
    assert changed['feature_sha256']==result['feature_sha256']
    flat=past.flatten_event_features(result,'LONG')
    assert flat['historical.closed_1m.1h.alignment']=='SUPPORTS'
    assert flat['historical.closed_1m.1h.btc_direction']=='FLAT'
    assert abs(flat['historical.closed_1m.1h.relative_strength_pct']-2)<1e-12
    assert past.flatten_event_features(result,'SHORT')['historical.closed_1m.1h.alignment']=='OPPOSES'
    assert not any('mfe' in key or 'mae' in key or 'regime' in key for key in flat)
    assert result['market_regime_status']=='UNCLASSIFIED_NO_DOCUMENTED_REGIME_RULE'


def check_missing_and_provenance():
    path=[candle(i) for i in range(1440)]
    missing=calculate(path[100:],path[100:])
    assert missing['status']=='PARTIAL'
    f=past.flatten_event_features(missing,'LONG')
    assert 'historical.closed_1m.1h.return_pct' in f and 'historical.closed_1m.24h.return_pct' not in f
    duplicate=calculate(path+[path[-1]])
    assert duplicate['asset_windows_ready']==0
    futures=calculate(path,path_result={**ROUTE,'market':'futures'})
    f=past.flatten_event_features(futures,'LONG')
    assert 'historical.closed_1m.1h.return_pct' not in f
    assert 'historical.closed_1m.1h.btc_return_pct' in f
    # Missing BTC never becomes flat/no-signal or a relative-strength value.
    no_btc=calculate(path,[],btc_path_result={})
    f=past.flatten_event_features(no_btc,'LONG')
    assert 'historical.closed_1m.1h.return_pct' in f
    assert 'historical.closed_1m.1h.relative_strength_pct' not in f
    same=calculate(path,symbol='BTC',path_result=BTC,btc_path_result={})
    assert same['status']=='READY'
    assert all(c['relative_strength_pct']==0 for c in same['windows'].values())


def check_bounded_network():
    worker._CACHE.clear()
    calls=[]
    class Response:
        def raise_for_status(self):pass
        def json(self):return []
    def get(url,**kwargs):
        calls.append(kwargs)
        return Response()
    with patch.object(worker.time,'monotonic',return_value=10.):
        result=worker.fetch_prior_path('ETH',ENTRY,deadline=13.,request_get=get)
    assert calls[0]['timeout']==3. and len(calls)==1
    assert calls[0]['params']['endTime']<int(ENTRY.timestamp()*1000)
    assert result['complete'] is False
    with patch.object(worker.time,'monotonic',return_value=15.):
        try:worker.fetch_prior_path('ETH',ENTRY,deadline=13.,request_get=get)
        except TimeoutError:pass
        else:raise AssertionError('Expired budget fetched price history')
    assert len(calls)==1


def check_store_windows_and_refresh():
    db=sqlite3.connect(':memory:');db.row_factory=sqlite3.Row
    db.execute('''CREATE TABLE research_past_price_features(event_id INTEGER,method_version TEXT,
        event_time_utc TEXT,status TEXT,result TEXT,search_refresh_pending INTEGER,
        feature_sha256 TEXT,next_attempt_at_utc TEXT,updated_at_utc TEXT)''')
    db.execute("INSERT INTO research_past_price_features VALUES(7,?,?,'PENDING','{}',0,NULL,NULL,NULL)",(past.METHOD_VERSION,str(ENTRY)))
    class Result:
        def __init__(self,cursor):self.cursor=cursor
        def fetchone(self):
            row=self.cursor.fetchone()
            if row is None:return None
            item=dict(row)
            if 'result' in item:item['result']=json.loads(item['result'])
            return item
    class Adapter:
        def execute(self,query,params=()):
            sql=query.replace("NOW()+INTERVAL '6 hours'","datetime('now','+6 hours')").replace('NOW()','CURRENT_TIMESTAMP').replace(' FOR UPDATE','').replace('%s','?').replace('::jsonb','')
            return Result(db.execute(sql,params))
    conn=Adapter()
    initial=calculate([candle(i,high=102) for i in range(1425,1440)],symbol='BTC',path_result=BTC)
    assert store.write_result(conn,event_id=7,result=initial)
    first=db.execute('SELECT feature_sha256 FROM research_past_price_features').fetchone()[0]
    store.ack_refresh(conn,7,'stale_hash')
    assert db.execute('SELECT search_refresh_pending FROM research_past_price_features').fetchone()[0]==1
    store.ack_refresh(conn,7,first)
    later=calculate([candle(i,high=105) for i in range(1440)],symbol='BTC',path_result=BTC,computed_at=ENTRY+timedelta(hours=1))
    assert store.write_result(conn,event_id=7,result=later)
    row=db.execute('SELECT * FROM research_past_price_features').fetchone();saved=json.loads(row['result'])
    assert row['status']=='READY' and row['search_refresh_pending']==1
    assert saved['windows']['15m']['asset']['high']==102  # Existing predicate stays frozen.
    assert saved['windows']['1h']['asset']['high']==105  # Newly complete horizon is added.
    assert not store.write_result(conn,event_id=7,result=later)
    db.close()


def check_one_event_worker():
    path=[candle(i) for i in range(1440)]
    calls=[];writes=[]
    class Connection:
        def __enter__(self):return self
        def __exit__(self,*args):return False
    def fetch(symbol,event_time,*,deadline):
        calls.append((symbol,event_time,deadline));return {**BTC,'candles':path}
    event={'event_id':3,'symbol':'BTC','direction':'LONG','alert_time_utc':ENTRY,'event_kind':'ALERT','delivery_status':'DELIVERED'}
    with patch.object(store,'available',return_value=True),patch.object(store,'seed_bounded_history'),patch.object(store,'seed_recent_events'),\
        patch.object(store,'load_due_event',return_value=event),patch.object(store,'write_result',side_effect=lambda conn,**kw:writes.append(kw) or True):
        result=worker.run_pending('test',now=ENTRY+timedelta(minutes=1),connect=lambda _:Connection(),fetch_path=fetch)
    assert len(calls)==1 and len(writes)==1 and result['past_features_checked']==1
    assert writes[0]['result']['status']=='READY'


def main():
    check_causality_and_features();check_missing_and_provenance();check_bounded_network();check_store_windows_and_refresh();check_one_event_worker()
    print('PASS past price causal boundaries, six windows, missing coverage, BTC strength, immutable retry windows, stale refresh and bounded HTTP')


if __name__=='__main__':main()
