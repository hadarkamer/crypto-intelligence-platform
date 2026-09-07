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
    def __init__(self,events,*,omit=False,drift=None):
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

    def execute(self,sql,args):
        self.calls+=1
        if 'unnest' in sql:
            assert sql.endswith(store._EVENT_PROJECT)
            ids=list(args[0]);self.batches.append(ids)
            assert 0<len(ids)<=64
            self.rows=[deepcopy(self.events[eid]) for eid in ids]
            if self.omit:self.rows.pop()
            if self.drift:
                key,value=self.drift;self.rows[0][key]=value
        else:
            # Execute the production metadata predicate/order/cap relationally;
            # only the PG JSON projection above uses fixture source payloads.
            assert 'engine_snapshot' not in sql
            rows=self.db.execute(sql.replace('%s','?'),[arg.isoformat() for arg in args]).fetchall()
            self.rows=[dict(item)|{'alert_time_utc':datetime.fromisoformat(item['alert_time_utc'])} for item in rows]
        return self

    def fetchall(self):return self.rows


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
        # An older refreshed event widens the global source query. Unrelated
        # gap rows must still count toward its cap, but need no JSON projection.
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
        self.assertEqual(stats['sequence_source_rows'],len(original))
        self.assertLess(stats['sequence_projected_rows'],stats['sequence_source_rows']-100)
        self.assertGreater(len(conn.batches),1)
        self.assertEqual(stats['sequence_projection_batches'],len(conn.batches))
        self.assertEqual([row['event_id'] for row in history],sorted(row['event_id'] for row in history))
        self.assertEqual(len(history),len({row['event_id'] for row in history}))

    def test_original_global_overflow_blocks_before_projection(self):
        current=event(6000,NOW)
        events=[event(i,NOW-timedelta(hours=1),symbol='UNRELATED') for i in range(1,5002)]
        conn=Fixture(events)
        with self.assertRaisesRegex(RuntimeError,'bounded causal sequence source exceeded'):
            store.load_sequence_history(conn,{6000:current})
        self.assertEqual(conn.calls,1)
        self.assertEqual(conn.batches,[])

    def test_missing_or_changed_selected_source_fails_closed(self):
        current=event(2,NOW);old=event(1,NOW-timedelta(minutes=1))
        for kwargs in ({'omit':True},{'drift':('symbol','ETH')},
                       {'drift':('direction','SHORT')},{'drift':('alert_time_utc',NOW)}):
            with self.assertRaisesRegex(RuntimeError,'lost or changed source rows'):
                store.load_sequence_history(Fixture([old,current],**kwargs),{2:current})
        conn=Fixture([])
        history,stats=store.load_sequence_history(conn,{})
        self.assertEqual((history,conn.calls),([],0))
        self.assertEqual(stats['sequence_projection_batches'],0)


if __name__=='__main__':unittest.main()
