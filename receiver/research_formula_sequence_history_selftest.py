"""Sequence parity across source families, bounded projections and source guards."""
from copy import deepcopy
from datetime import datetime,timedelta,timezone
import sqlite3
import unittest

import research_formula_ordered_store as store

NOW=datetime(2026,9,7,12,tzinfo=timezone.utc)


def event(event_id,when,*,symbol='BTC',direction='LONG',family='PRICE_OI'):
    side=({'LONG':'SHORT','SHORT':'LONG'}[direction] if family=='MAX_PAIN' else direction)
    return {'event_id':event_id,'symbol':symbol,'direction':direction,'source_side':side,
        'alert_time_utc':when,'event_kind':'ALERT','delivery_status':'DELIVERED',
        'event_type':family+'_ALERT','score':70,'current_price':100+event_id/1000,
        'target_price':110 if direction=='LONG' else 90,
        'engine_snapshot':{'watch_scan_id':f'scan-{event_id//2}',
            'alert_side':side,'magnet':{'side':'UPPER' if direction=='LONG' else 'LOWER'},
            'market_evidence':{'modules':{name:{'score':65+event_id%20,'direction':direction}
                for name in ('positioning','futures_flow','spot_flow')}}}}


class Fixture:
    def __init__(self,events,*,omit=False,drift=None,after_batch=0):
        self.events={item['event_id']:item for item in events}
        self.db=sqlite3.connect(':memory:')
        self.db.row_factory=sqlite3.Row
        self.db.execute('CREATE TABLE research_events(event_id INTEGER,symbol TEXT,direction TEXT,alert_time_utc TEXT,event_kind TEXT,delivery_status TEXT)')
        self.db.executemany('INSERT INTO research_events VALUES(?,?,?,?,?,?)',[
            (item['event_id'],item['symbol'],item['direction'],item['alert_time_utc'].isoformat(),
             item['event_kind'],item['delivery_status']) for item in events])
        self.batches=[]
        self.calls=0
        self.omit,self.drift=omit,drift
        self.after_batch=after_batch
        self.source_cursors=[]

    def cursor(self,*,name):
        assert name.startswith('ordered_sequence_')
        source=SourceCursor(self)
        self.source_cursors.append(source)
        return source

    def execute(self,sql,args):
        self.calls+=1
        if 'unnest' in sql:
            assert sql.endswith(store._EVENT_PROJECT)
            ids=list(args[0]);self.batches.append(ids)
            assert 0<len(ids)<=64
            self.rows=[deepcopy(self.events[eid]) for eid in ids]
            if self.omit and len(self.batches)>self.after_batch:self.rows.pop()
            if self.drift and len(self.batches)>self.after_batch:
                key,value=self.drift;self.rows[0][key]=value
        else:
            # Execute the production metadata predicate/order/cap relationally;
            # only the PG JSON projection above uses fixture source payloads.
            assert 'engine_snapshot' not in sql
            rows=self.db.execute(sql.replace('%s','?'),[arg.isoformat() for arg in args]).fetchall()
            self.rows=[dict(item)|{'alert_time_utc':datetime.fromisoformat(item['alert_time_utc'])} for item in rows]
        return self

    def fetchall(self):return self.rows


class SourceCursor:
    def __init__(self,conn):
        self.conn=conn
        self.closed=False
        self.sizes=[]

    def __enter__(self):return self
    def __exit__(self,*args):self.closed=True

    def execute(self,sql,args):
        self.conn.calls+=1
        # Run the exact causal union relationally, with no JSON materialized.
        assert 'engine_snapshot' not in sql
        self.source=self.conn.db.execute(sql.replace('%s','?'),[
            arg.isoformat() if isinstance(arg,datetime) else arg for arg in args])

    def fetchmany(self,size):
        assert size==512
        rows=self.source.fetchmany(size)
        self.sizes.append(len(rows))
        return [dict(item)|{'alert_time_utc':datetime.fromisoformat(item['alert_time_utc'])} for item in rows]


def features(item):
    return store.evaluator.extract_event_features(item)|store.questions.extended_features(item)


class Tests(unittest.TestCase):
    def test_original_features_and_candidate_matches_are_preserved(self):
        events=[];changed={}
        for family,symbol in (('PRICE_OI','BTC'),('MAX_PAIN','ETH'),('MAGNET','SOL')):
            for direction in ('LONG','SHORT'):
                current=event(len(events)+1,NOW,symbol=symbol,direction=direction,family=family)
                events.append(current);changed[current['event_id']]=current
                for minutes in [0,1,1,29,30,31,59,60,61,239,240,241]+list(range(2,72)):
                    events.append(event(len(events)+1,NOW-timedelta(minutes=minutes),
                        symbol=symbol,direction=direction,family=family))
        # An older refreshed event adds its own window. Unrelated gap rows
        # are not causal evidence and must not widen the metadata source.
        early=event(len(events)+1,NOW-timedelta(days=1))
        events.append(early);changed[early['event_id']]=early
        for i in range(100):
            events.append(event(len(events)+1,NOW-timedelta(hours=12,minutes=i),symbol='OTHER'))
        events.append(event(len(events)+1,NOW+timedelta(minutes=1)))
        conn=Fixture(events)
        history,stats=store.load_sequence_history(conn,changed)
        lower=min(item['alert_time_utc'] for item in changed.values())-timedelta(hours=4)
        original=[item for item in events if lower<=item['alert_time_utc']<=NOW][:5001]
        old_prior=[(item,features(item)) for item in original]
        new_prior=[(item,features(item)) for item in history]
        candidates=store.evaluator.candidate_catalog(include_extended=True)
        self.assertTrue(any(c.get('research_orientation')=='INVERSE' for c in candidates))
        for current in changed.values():
            before=features(current)|store.questions.sequence_features(current,features(current),old_prior)
            after=features(current)|store.questions.sequence_features(current,features(current),new_prior)
            self.assertEqual(before,after)
            self.assertEqual([store.evaluator.matches(c,before,current['direction']) for c in candidates],
                [store.evaluator.matches(c,after,current['direction']) for c in candidates])
        self.assertEqual(stats['sequence_source_rows'],len(history))
        self.assertLess(stats['sequence_source_rows'],len(original)-100)
        self.assertGreater(len(conn.batches),1)
        self.assertEqual(stats['sequence_projection_batches'],len(conn.batches))
        self.assertEqual([row['event_id'] for row in history],sorted(row['event_id'] for row in history))
        self.assertEqual(len(history),len({row['event_id'] for row in history}))

    def test_disjoint_history_and_live_windows_skip_more_than_5000_gap_rows(self):
        current=event(6000,NOW);early=event(6001,NOW-timedelta(days=2))
        # Even matching symbol/direction in the intervening day is irrelevant.
        events=[event(i,NOW-timedelta(days=1)) for i in range(1,5002)]
        events.extend([event(6002,early['alert_time_utc']-timedelta(minutes=1)),
            event(6003,NOW-timedelta(minutes=1)),current,early])
        conn=Fixture(events)
        history,stats=store.load_sequence_history(conn,{6000:current,6001:early})
        self.assertEqual([item['event_id'] for item in history],[6002,6003])
        self.assertEqual(stats['sequence_source_rows'],2)
        for changed in (current,early):
            self.assertEqual(store.questions.sequence_features(changed,features(changed),[(item,features(item)) for item in events]),
                store.questions.sequence_features(changed,features(changed),[(item,features(item)) for item in history]))

    def test_dense_causal_window_pages_all_evidence_without_truncation(self):
        current=event(50000,NOW)
        events=[event(i*2,NOW-timedelta(minutes=2+i%237)) for i in range(1,5002)]
        for item in events:item['engine_snapshot']['watch_scan_id']=f"distinct-{item['event_id']}"
        last=event(30000,NOW-timedelta(seconds=1),family='MAGNET')
        events.extend([last,current])
        conn=Fixture(events)
        history,stats=store.load_sequence_history(conn,{current['event_id']:current})
        before=features(current)|store.questions.sequence_features(current,features(current),[(item,features(item)) for item in events])
        after=features(current)|store.questions.sequence_features(current,features(current),[(item,features(item)) for item in history])
        self.assertEqual(before,after)
        self.assertEqual(after['sequence.240m.prior_distinct_scans'],5002)
        self.assertEqual(after['sequence.240m.previous_primary_family'],'MAGNET')
        candidates=store.evaluator.candidate_catalog(include_extended=True)
        self.assertEqual([store.evaluator.matches(c,before,current['direction']) for c in candidates],
            [store.evaluator.matches(c,after,current['direction']) for c in candidates])
        self.assertEqual(stats['sequence_source_rows'],5002)
        self.assertEqual(stats['sequence_source_pages'],10)
        self.assertEqual([item['event_id'] for item in history],sorted(item['event_id'] for item in events if item is not current))
        self.assertTrue(conn.source_cursors[0].closed)

    def test_adjacent_and_overlapping_windows_are_unique_and_half_open(self):
        events=[event(i,when,direction=direction) for i,(when,direction) in enumerate([
            (NOW-timedelta(hours=8),'LONG'),(NOW-timedelta(hours=4),'LONG'),
            (NOW-timedelta(hours=3),'LONG'),(NOW-timedelta(hours=2),'LONG'),
            (NOW-timedelta(hours=1),'LONG'),(NOW,'LONG'),
            (NOW-timedelta(hours=2),'SHORT'),(NOW,'SHORT')],1)]
        # Adjacent windows retain the earlier changed event as causal history
        # for the later one; overlapping windows never duplicate evidence.
        changed={item['event_id']:item for item in events if item['event_id'] in (2,4,6,8)}
        history,_=store.load_sequence_history(Fixture(events),changed)
        self.assertEqual([item['event_id'] for item in history],[1,2,3,4,5,7])

    def test_missing_or_changed_selected_source_fails_closed(self):
        current=event(2,NOW);old=event(1,NOW-timedelta(minutes=1))
        for kwargs in ({'omit':True},{'drift':('symbol','ETH')},
                       {'drift':('direction','SHORT')},{'drift':('alert_time_utc',NOW)},
                       {'drift':('delivery_status','UNKNOWN')},{'drift':('event_kind','DECISION_SAMPLE')}):
            with self.assertRaisesRegex(RuntimeError,'lost or changed source rows'):
                store.load_sequence_history(Fixture([old,current],**kwargs),{2:current})
        conn=Fixture([])
        history,stats=store.load_sequence_history(conn,{})
        self.assertEqual((history,conn.calls),([],0))
        self.assertEqual(stats['sequence_projection_batches'],0)

    def test_later_page_failure_closes_cursor_without_partial_result(self):
        current=event(1000,NOW)
        prior=[event(i,NOW-timedelta(minutes=1)) for i in range(1,600)]
        conn=Fixture([*prior,current],omit=True,after_batch=8)
        with self.assertRaisesRegex(RuntimeError,'lost or changed source rows'):
            store.load_sequence_history(conn,{1000:current})
        self.assertEqual(len(conn.batches),9)
        self.assertTrue(conn.source_cursors[0].closed)


if __name__=='__main__':unittest.main()
