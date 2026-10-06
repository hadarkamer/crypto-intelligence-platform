"""Additional malformed-response and real-storage transition checks; no exchange."""
from copy import deepcopy
import unittest
from unittest.mock import patch
from . import card_sync_evidence as e, card_sync as worker
from .test_card_sync import evidence,Reader,CI,T,A
from .card_lifecycle_store import LifecycleStore,SCHEMA
from .card_sync_store import SyncStore
from .postgres_journal import PostgresJournal


class MalformedEvidenceTests(unittest.TestCase):
    def run_bad(self,change,code):
        ev=evidence(is_open=True);reader=Reader(evidence());change(reader.data)
        with self.assertRaisesRegex(e.SyncError,code):e.collect(ev,reader,clock=lambda:T+10000)

    def test_malformed_position_is_not_an_empty_position(self):
        self.run_bad(lambda d:d.update(position={'assetPositions':[{}]}),'INVALID_POSITION')

    def test_position_missing_symbol_is_not_flat(self):
        self.run_bad(lambda d:d.update(position={'assetPositions':[{'position':{'szi':'0'}}]}),'INVALID_POSITION')

    def test_duplicate_position_rejected(self):
        self.run_bad(lambda d:d['position']['assetPositions'].extend(deepcopy(d['position']['assetPositions'])),'DUPLICATE_POSITION')

    def test_missing_order_symbol_not_ignored(self):
        self.run_bad(lambda d:d.update(inventory=[{'oid':999}]),'INVALID_ORDER_SYMBOL')

    def test_missing_fill_symbol_not_ignored(self):
        self.run_bad(lambda d:d['fills'].append({'time':T+1}),'INVALID_FILL_SYMBOL')

    def test_fill_after_terminal_time_rejected(self):
        self.run_bad(lambda d:d['fills'][-1].update(time=T+1),'FILL_AFTER_TERMINAL')


@unittest.skipUnless(CI,'Disposable PostgreSQL required')
class TransitionStorageTests(unittest.TestCase):
    def setUp(self):
        self.j=PostgresJournal.for_ci(CI)
        with self.j._transaction() as conn:conn.execute(f'DROP SCHEMA IF EXISTS {SCHEMA} CASCADE')
        self.j.bootstrap();self.life=LifecycleStore(self.j);self.life.initialize()
        self.store=SyncStore(self.j);self.store.initialize()
        p=patch('http.client.HTTPSConnection',side_effect=AssertionError('EXCHANGE_FORBIDDEN'))
        p.start();self.addCleanup(p.stop)

    def test_actual_change_is_one_new_durable_revision(self):
        ev=evidence(is_open=True)
        self.life.save(ev['bindings'],ev['snapshot'],expected_revision=0,now_ms=T)
        claim=self.store.claim();o=e.collect(ev,Reader(evidence()),clock=lambda:T+10000)
        saved=self.store.save(claim,o,now_ms=T+10000)
        self.assertTrue(saved['changed']);self.assertEqual(saved['revision'],2)
        stored=self.life.load(A,'DOGE',now_ms=T+10000)
        self.assertTrue(stored['report']['cards'][0]['closure_verified'])
        with self.j._transaction() as conn:
            self.assertEqual(conn.execute(f'SELECT count(*) FROM {SCHEMA}.history').fetchone()[0],2)

    def test_no_registered_executions_means_no_public_reads(self):
        r=worker.tick(self.store,{},reader_factory=lambda: (_ for _ in ()).throw(AssertionError('NO_READS')))
        self.assertEqual(r['status'],'WAITING_FOR_REGISTERED_EXECUTIONS')
        self.assertEqual(r['registered_buckets'],0)
