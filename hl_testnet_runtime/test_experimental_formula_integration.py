"""Actual source contract -> inbox -> persisted stop lifecycle, software only.

Transactions use the explicit storage double, not PostgreSQL. No transport or
production registrar is installed. These checks pin the required commit boundary
for the future orchestrator instead of treating a returned action as a send.
"""
from copy import deepcopy
import unittest
from unittest.mock import patch

import experimental_execution_contract as contract
from .experimental_plan_store import PlanStore, PlanStoreError
from . import r2732_stop_dispatch as stop
from .test_experimental_plan_store import FakeJournal, source_update
from .test_r2732_stop_dispatch import due, sample, public_result, META, NOW, T


class FormulaPersistenceTests(unittest.TestCase):
    def setUp(self):
        guard = patch('socket.socket.connect', side_effect=AssertionError('OFFLINE_ONLY'))
        guard.start(); self.addCleanup(guard.stop)
        self.journal = FakeJournal()
        self.store = PlanStore(self.journal)
        self.condition = due()
        self.source = self.condition['contract']
        self.cid = self.source['occurrence_id']
        self.store.ingest(self.source, now=contract.iso_ms(NOW), not_before=contract.iso_ms(T-60000))
        current = self.store.load(self.cid)
        self.store.change_strategy(self.cid, current['revision'], self.condition, now=contract.iso_ms(NOW))

    def commit(self, prior, value, at=NOW):
        return self.store.change_strategy(self.cid, prior['revision'], value, now=contract.iso_ms(at))

    def reserve(self):
        state = self.store.load(self.cid)
        return self.commit(state, stop.reserve(state['strategy'], META, sample(), now_ms=NOW))

    def test_lost_attempt_commit_ack_does_not_permit_second_stop_attempt(self):
        state = self.reserve()
        begun = stop.begin(state['strategy'], state['strategy']['pending'], META, sample(), now_ms=NOW)
        self.journal.lose_commit_ack_once = True
        with self.assertRaisesRegex(RuntimeError, 'COMMIT_ACK_UNKNOWN'):
            self.commit(state, begun)
        reopened = PlanStore(self.journal).load(self.cid)
        self.assertEqual(reopened['strategy']['requests'][-1]['phase'], 'ATTEMPTED')
        with self.assertRaisesRegex(stop.StopDispatchError, 'ALREADY_CONSUMED'):
            stop.begin(reopened['strategy'], reopened['strategy']['pending'], META, sample(), now_ms=NOW)
        snapshot, lookup = public_result(reopened['strategy'])
        reconciled = stop.observe(reopened['strategy'], snapshot, now_ms=NOW+2, lookup=lookup)
        saved = self.commit(reopened, reconciled, NOW+2)
        self.assertIsNone(saved['strategy']['pending'])
        self.assertEqual(saved['strategy']['requests'][-1]['phase'], 'OBSERVED')
        self.assertEqual(saved['strategy']['original_binding'], self.condition['original_binding'])

    def test_failed_atomic_write_does_not_consume_attempt_and_cannot_be_sent(self):
        state = self.reserve()
        begun = stop.begin(state['strategy'], state['strategy']['pending'], META, sample(), now_ms=NOW)
        self.journal.fail_event_once = True
        with self.assertRaisesRegex(RuntimeError, 'EVENT_WRITE_FAILURE'):
            self.commit(state, begun)
        restored = self.store.load(self.cid)
        self.assertEqual(restored, state)
        self.assertEqual(restored['strategy']['requests'][-1]['phase'], 'RESERVED')

    def test_source_cancel_preserves_owned_fill_and_stop_maintenance(self):
        state = self.reserve()
        before = deepcopy(state['strategy'])
        cancel = source_update(self.source, contract.iso_ms(NOW+1),
                               kind='CANCEL', reason='SOURCE_OBSERVATION_ENDED')
        self.store.ingest(cancel, now=contract.iso_ms(NOW+1), not_before=contract.iso_ms(T-60000))
        with self.assertRaisesRegex(PlanStoreError, 'CONCURRENT_RELOAD_REQUIRED'):
            self.commit(state, before, NOW+1)
        reloaded = self.store.load(self.cid)
        self.assertEqual(reloaded['entry_permission'], 'RETIRED')
        self.assertEqual(reloaded['strategy'], before)
        # Cancellation retires entry, never erases protection of real owned
        # quantity; the amendment protocol still has its original stop proof.
        self.assertEqual(stop.validate(reloaded['strategy'])['original_binding'],
                         self.condition['original_binding'])


if __name__ == '__main__':
    unittest.main()
