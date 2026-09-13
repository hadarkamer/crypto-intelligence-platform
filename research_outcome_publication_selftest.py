"""Bounded slot publication, source ordering and exact-generation preservation."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from itertools import product
import hashlib
import json
import re
from types import SimpleNamespace
from unittest.mock import patch

import google_sheets_sync
import research_outcome_publication as publication
import research_sheet_outbox as outbox
from research_sheet_fresh_delivery_selftest import Database, _database, _add, NOW


def real_case(*, event_id=7, start=None, direction='LONG', symbol='BTC', window=60, bps=25):
    import research_ordered_first_touch as calculator
    start=start or datetime(2026,9,13,8,0,0,123456,tzinfo=timezone.utc)
    candle_open=(start+timedelta(minutes=1)).replace(second=0,microsecond=0)
    candle=SimpleNamespace(open_time_utc=candle_open,
        close_time_utc=candle_open+timedelta(seconds=59,milliseconds=999),
        open=100.0,high=100.1,low=99.7,close=99.8,volume=1.0)
    event={'event_id':event_id,'event_fingerprint':f'{event_id:064x}',
           'symbol':symbol,'direction':direction,'event_kind':'ALERT',
           'delivery_status':'DELIVERED','alert_time_utc':start}
    outcome=calculator.calculate_ordered_first_touch_outcome(reference_price=100,
        direction=direction,event_time=start,candles=[candle],threshold_pct=bps/100,
        observation_closed=False)
    outcome['window_minutes']=window
    path={'pair':symbol+'USDT','candles':[candle]}
    row=google_sheets_sync.ordered_outcome_row(event=event,reference_source='binance_spot',
        path_result=path,outcome=outcome,quality='VERIFIED_BINANCE_SPOT_1M_CLOSED_CANDLES')
    return event,row,path,outcome


class SlotDatabase(Database):
    def __init__(self,conn):
        super().__init__(conn)
        self.conn.create_function('research_sheet_source_timestamp',1,
            lambda value: publication.source_time(value).isoformat() if publication.source_time(value) else None)

    def execute(self,query,params=()):
        query=re.sub(r"\((EXCLUDED|research_sheet_upsert_outbox)\.payload->'row'->>'event_id'\)::bigint",
                     r"CAST(\1.payload->'row'->>'event_id' AS INTEGER)",query)
        query=query.replace('::jsonb','')
        params=tuple(value.isoformat() if isinstance(value,datetime) else value for value in params)
        return super().execute(query,params)


def _slot(database):
    return database.conn.execute('SELECT * FROM research_sheet_upsert_outbox WHERE sheet_name=?',
                                  (publication.SHEET,)).fetchone()


def run():
    event,row,_,_=real_case()
    projected=publication.project(event,row)
    assert projected and set(row)==set(publication.HEADERS) and len(row)==34
    for field in ('measurement_start_utc','decision_time_utc','observed_from_utc','observed_through_utc'):
        if row[field] not in (None,''):
            assert 'T' in projected['row'][field]
            assert publication.source_time(projected['row'][field])==publication.source_time(row[field])
    assert isinstance(row['measurement_start_utc'],datetime), 'Do not mutate caller values'
    assert len({(symbol,direction,window,bps) for symbol,direction,window,bps in product(
        publication.SYMBOLS,publication.DIRECTIONS,publication.HORIZONS,publication.THRESHOLDS_BPS)})==512
    assert publication.status()['max_grid_cells']==17442
    for changes in ({'event_kind':'DECISION_SAMPLE'},{'delivery_status':'HISTORICAL'},
                    {'symbol':'OTHER'},{'event_id':8},{'direction':'SHORT'},
                    {'alert_time_utc':event['alert_time_utc']+timedelta(seconds=1)}):
        assert publication.project({**event,**changes},row) is None
    for changes in ({'window_minutes':30},{'threshold_bps':65},{'outcome_method_version':'next-v8'},
                    {'outcome_id':'wrong'},{'event_id':'07'},{'event_id':'9223372036854775808'},
                    {'measurement_start_utc':'2026-09-13T08:00:00'}):
        assert publication.project(event,{**row,**changes}) is None
    database=SlotDatabase(_database().conn)
    _add(database,'Outcomes','retained',NOW,sync_status='RETRY',attempts=5,payload='{"retained":true}')
    legacy=tuple(database.conn.execute("SELECT * FROM research_sheet_upsert_outbox WHERE sheet_name='Outcomes'").fetchone())
    assert publication.stage(database,event,row)==1
    first=dict(_slot(database))
    assert first['sync_status']=='PENDING' and first['synced_at_utc'] is None
    assert publication.stage(database,event,row)==0
    assert dict(_slot(database))==first
    # ID ordering is numeric, and alert time takes precedence over event ID.
    same_time_event,same_time_row,_,_=real_case(event_id=100,start=event['alert_time_utc'])
    assert publication.stage(database,same_time_event,same_time_row)==1
    later_event,later_row,_,_=real_case(event_id=6,start=event['alert_time_utc']+timedelta(minutes=30))
    assert publication.stage(database,later_event,later_row)==1
    newer=dict(_slot(database))
    assert publication.stage(database,same_time_event,same_time_row)==0
    assert dict(_slot(database))==newer, 'Historical replay must not regress payload or source-time priority'
    # Simulate a claimed generation; a canonical accepted same-event repair
    # has an earlier observation but invalidates the old delivery token.
    claimed=outbox._claim_lane(database,count=1,token='old-token',sheet=publication.SHEET,recent=False)[0]
    repaired={**later_row,'status':'DATA_MISSING','observed_through_utc':later_row['observed_through_utc']+timedelta(minutes=10)}
    assert publication.stage(database,later_event,repaired)==1
    assert publication.stage(database,later_event,later_row)==1
    updated=dict(_slot(database))
    assert updated['claim_token'] is None and updated['sync_status']=='PENDING'
    assert tuple(database.conn.execute("SELECT * FROM research_sheet_upsert_outbox WHERE sheet_name='Outcomes'").fetchone())==legacy
    # The ordinary public staging path must share the same atomic source guard.
    assert outbox.stage_upserts(database,[publication.project(same_time_event,same_time_row)])==0
    assert dict(_slot(database))==updated
    assert outbox.stage_upserts(database,[{'sheet':'Outcomes','key':'outcome_id','row':row}])==0
    assert claimed['payload_sha256']==newer['payload_sha256']
    for target in ('Outcomes','Episodes','Formula_Results','Unknown',''):
        with patch.object(outbox.google_sheets_sync,'delivery_slot',side_effect=AssertionError('Invalid lane must not reserve transport')):
            try:outbox.drain('unused',target_sheet=target)
            except ValueError:pass
            else:raise AssertionError('Held or unknown target accepted')
    _targeted_lane_test()
    print('Outcomes_Current:512slots,real-calculator ISO timestamps,newer-source guard,same-event repair,held evidence,targeted lane PASS')


def _targeted_lane_test():
    from contextlib import nullcontext
    class Result:
        def __init__(self,value):self.value=value
        def fetchone(self):return self.value
    class Lock:
        def __enter__(self):return self
        def __exit__(self,*args):return False
        def execute(self,query,params=()):
            return Result({'acquired':True,'ready':True})
        def commit(self):pass
    class Connection:
        def __enter__(self):return self
        def __exit__(self,*args):return False
        def execute(self,query,params=()):
            assert 'WHERE sheet_name=%s AND row_key=%s AND claim_token=%s::uuid' in query
            assert 'AND payload_sha256=%s AND claimed_payload_sha256=%s' in query
            return SimpleNamespace(rowcount=1)
    claims=[]
    sends=[]
    connections=iter([Lock(),Connection(),Connection()])
    def claim(conn,*,count,token,sheet,recent):
        assert sheet=='Telegram_Events' and recent is False and count==8
        claims.append(token)
        return [{'sheet_name':sheet,'row_key':str(i),'payload':{'event':i},
                 'payload_sha256':'generation','attempts':1} for i in range(count)]
    with patch.object(outbox,'psycopg',object()),patch.object(outbox,'_connect',lambda url:next(connections)),patch.object(
            outbox,'_claim_lane',claim),patch.object(outbox,'_claim_batch',side_effect=AssertionError('No rotation in targeted pass')),patch.object(
            outbox.google_sheets_sync,'enabled',lambda:True),patch.object(outbox.google_sheets_sync,'delivery_slot',lambda:nullcontext(True)),patch.object(
            outbox.google_sheets_sync,'ordered_outcome_batch_limit',lambda:8),patch.object(outbox.google_sheets_sync,'deliver_now',lambda payload,attempts:sends.append(payload) or True):
        result=outbox.drain('test',max_rows=8,target_sheet='Telegram_Events')
    assert len(claims)==len(sends)==1
    assert result['claimed']==result['synced']==8 and result['failed']==0


def seed_legacy_outbox_fixture(conn,*,event,window_minutes,outcome,path_result):
    """Test-only fixture for retained historical lease/ACK regression gates.

    Production no longer emits this unbounded destination. Explicit fixtures
    keep the old evidence's recovery semantics independently testable.
    """
    row=google_sheets_sync.ordered_outcome_row(event=event,reference_source='binance_spot',
        path_result=path_result,outcome={**outcome,'window_minutes':window_minutes},
        quality='VERIFIED_BINANCE_SPOT_1M_CLOSED_CANDLES')
    payload=publication._json({'sheet':'Outcomes','key':'outcome_id','row':row})
    sha=hashlib.sha256(payload.encode()).hexdigest()
    conn.execute('''INSERT INTO research_ordered_first_touch_sync_outbox
        (event_id,window_minutes,threshold_bps,method_version,remote_row_key,payload,payload_sha256)
        VALUES(%s,%s,%s,%s,%s,%s::jsonb,%s)
        ON CONFLICT(event_id,window_minutes,threshold_bps,method_version,destination) DO UPDATE SET
          payload=EXCLUDED.payload,payload_sha256=EXCLUDED.payload_sha256,
          sync_status='PENDING',attempts=0,next_attempt_at_utc=NOW(),last_attempt_at_utc=NULL,
          claim_token=NULL,claimed_at_utc=NULL,claimed_payload_sha256=NULL,
          lease_expires_at_utc=NULL,synced_at_utc=NULL,last_error=NULL,updated_at_utc=NOW()
        WHERE research_ordered_first_touch_sync_outbox.payload_sha256 IS DISTINCT FROM EXCLUDED.payload_sha256''',
        (int(event['event_id']),window_minutes,int(outcome['threshold_bps']),publication.METHOD,
         row['outcome_id'],payload,sha))


if __name__=='__main__':run()
