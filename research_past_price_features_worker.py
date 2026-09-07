"""One event per ordered pass, bounded official HTTP and durable retry state."""
from __future__ import annotations

from collections import OrderedDict
from datetime import datetime, timezone
import time
from typing import Any
import requests

import binance_spot_price_path
import hyperliquid_spot_price_path
import canonical_price_path
import research_past_price_features as features
import research_past_price_features_store as store

_CACHE: OrderedDict=OrderedDict()
_CACHE_SIZE=8


def _connect(url):
    import psycopg
    from psycopg.rows import dict_row
    return psycopg.connect(url,row_factory=dict_row,connect_timeout=5,
        options='-c statement_timeout=15000 -c lock_timeout=1000')


def fetch_prior_path(symbol,event_time,*,deadline,request_get=None,request_post=None):
    start,end=features.prior_bounds(event_time)
    key=(str(symbol).upper(),start,end)
    if key in _CACHE:
        _CACHE.move_to_end(key)
        return _CACHE[key]
    def budgeted(call):
        def request(*args,**kwargs):
            remaining=deadline-time.monotonic()
            if remaining<=0:raise TimeoutError('PAST_FEATURE_NETWORK_DEADLINE')
            kwargs['timeout']=min(5.,remaining)
            response=call(*args,**kwargs)
            if time.monotonic()>deadline:raise TimeoutError('PAST_FEATURE_NETWORK_DEADLINE')
            return response
        return request
    if canonical_price_path.provider_for_symbol(symbol)=='hyperliquid':
        path=hyperliquid_spot_price_path.fetch_closed_candles(symbol,start,end,request_post=budgeted(request_post or requests.post))
    else:
        path=binance_spot_price_path.fetch_closed_candles(symbol,start,end,request_get=budgeted(request_get or requests.get))
        path={**path,'provenance':'EXCHANGE_API_HISTORICAL_CANDLES_IMPORTED'}
    canonical_price_path.validated_route(symbol,path,require_complete=False)
    if path.get('complete') is True:
        _CACHE[key]=path
        _CACHE.move_to_end(key)
        while len(_CACHE)>_CACHE_SIZE:_CACHE.popitem(last=False)
    return path


def run_pending(database_url,*,now=None,connect=None,fetch_path=None,max_seconds=15) -> dict[str,int]:
    """Called under the existing ordered pass advisory lock; zero Telegram work."""
    now=features.utc(now or datetime.now(timezone.utc))
    connect=connect or _connect
    fetch=fetch_path or fetch_prior_path
    deadline=time.monotonic()+max(.1,min(float(max_seconds),20))
    result={'past_features_checked':0,'past_features_written':0,'past_features_schema_missing':0,'past_features_failures':0}
    with connect(database_url) as conn:
        if not store.available(conn):
            result['past_features_schema_missing']=1
            return result
        store.seed_bounded_history(conn)
        store.seed_recent_events(conn)
        event=store.load_due_event(conn)
    if not event:return result
    result['past_features_checked']=1
    try:
        if event.get('event_kind')!='ALERT' or event.get('delivery_status')!='DELIVERED':
            raise ValueError('PAST_FEATURE_SOURCE_NOT_NATIVE_DELIVERED_ALERT')
        symbol=str(event['symbol']).upper()
        errors={}
        try:
            own=fetch(symbol,event['alert_time_utc'],deadline=deadline)
        except Exception as exc:
            own={}
            errors['asset']=type(exc).__name__
        if symbol=='BTC':btc=own
        else:
            try:
                btc=fetch('BTC',event['alert_time_utc'],deadline=deadline)
            except Exception as exc:
                btc={}
                errors['btc']=type(exc).__name__
        calculated=features.calculate_past_price_features(symbol=symbol,event_time=event['alert_time_utc'],
            candles=own.get('candles') or (),path_result=own,
            btc_candles=btc.get('candles') or (),btc_path_result=btc,computed_at=now)
        calculated['fetch_errors']=errors
        with connect(database_url) as conn:
            if store.write_result(conn,event_id=int(event['event_id']),result=calculated):
                result['past_features_written']=1
        result['past_features_failures']=int(bool(errors))
    except Exception as exc:
        result['past_features_failures']=1
        with connect(database_url) as conn:
            store.defer_event(conn,int(event['event_id']),reason=str(exc))
    return result
