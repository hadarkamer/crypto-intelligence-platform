"""Filtered failed-cycle evidence and honest terminal outcomes; no live I/O.

The PostgreSQL rollback/commit acknowledgement cases are covered separately by
the full connection acceptance suite. These fast checks exercise the same
service boundary and archive transition against disposable product stores.
"""
from copy import deepcopy
import unittest
from unittest.mock import patch

from experimental_execution_fixtures import r2732_message
from approved_alert_fixtures import selected_formula_alert
from . import experimental_live_service as service
from . import experimental_execution_archive as history
from .test_experimental_execution_runtime import T
from . import test_experimental_live_service as connection_fixture
from . import test_execution_history_store as history_fixture
from .test_execution_history_store import BASE


class CycleDecisionTests(unittest.TestCase):
    setUp = connection_fixture.FullConnectionTests.setUp
    build = connection_fixture.FullConnectionTests.build
    reconnect = connection_fixture.FullConnectionTests.reconnect
    ingest = connection_fixture.FullConnectionTests.ingest
    seed = connection_fixture.FullConnectionTests.seed

    def test_journal_masks_private_details_but_retains_allowlisted_domain_cause(self):
        for raw, expected in (
                ('EXACT_SHARED_REDUCING_CAPACITY_REQUIRED', 'EXACT_SHARED_REDUCING_CAPACITY_REQUIRED'),
                ('postgresql://private:secret@host/db', 'PERSISTENCE_UNAVAILABLE_NO_SEND')):
            try:
                try:
                    raise ValueError(raw)
                except ValueError:
                    raise service.JournalError('PERSISTENCE_UNAVAILABLE_NO_SEND') from None
            except service.JournalError as exc:
                self.assertEqual(service._cycle_error(exc), expected)

    def test_cycle_failure_survives_reconnect_and_filters_exception_details(self):
        msg = r2732_message(entry=2.3, decision_ms=T-60000)
        self.seed(msg)
        before = deepcopy(self.store.load())
        with patch.object(self.worker, 'run_once', side_effect=ValueError('private URL key=secret')):
            with self.assertLogs(service.__name__, level='ERROR') as logs:
                with self.assertRaisesRegex(ValueError, 'private URL'):
                    self.service.tick()
        self.reconnect()
        state = self.store.load(); cid = msg['occurrence_id']
        self.assertEqual(state['entry_decisions'][cid]['reason'], 'CONNECTION_CYCLE_RECONCILIATION_REQUIRED')
        self.assertEqual(state['entry_decisions'][cid]['stage'], 'CONNECTION_CYCLE')
        self.assertEqual(state['requests'], before['requests'])
        self.assertEqual(state['trades'], before['trades'])
        self.assertNotIn('private URL', repr(state))
        self.assertNotIn('secret', repr(logs.output))
        self.assertFalse(self.http)

    def test_closed_entry_gate_has_durable_wait_without_changing_original_window(self):
        msg = selected_formula_alert('sol_sff1361', reference_ms=T, approved_ms=T+1000)
        self.seed(msg); self.release['entries_enabled'] = False
        for _ in range(3):
            self.service.tick(); self.oracle.t += 1000
        state = self.store.load(); cid = msg['occurrence_id']
        decision = state['entry_decisions'][cid]
        self.assertEqual((decision['stage'], decision['reason']),
            ('ENTRY_SCHEDULING', 'ENTRY_GATE_CLOSED'))
        self.assertEqual(decision['deadline'], msg['expires_at'])
        self.assertEqual(sum(e['kind'] == 'ENTRY_DEFERRED' for e in state['events']), 1)
        self.assertNotIn(cid, state.get('position_recovery_entry_notices', {}))
        self.assertFalse(self.http)
        self.release['entries_enabled'] = True
        self.assertEqual(self.service.tick().get('operation'), 'ENTRY')

    def _two_independent_sources(self):
        self.oracle.t = T+70000
        sol = selected_formula_alert('sol_sff1361', reference_ms=T, approved_ms=T+1000)
        xrp = selected_formula_alert('xrp_liquidity_inverse', reference_ms=T+60000,
            approved_ms=T+61000)
        self.seed(sol); self.seed(xrp)
        return sol, xrp

    def test_earlier_entry_and_protection_priority_are_explained_without_reordering(self):
        sol, xrp = self._two_independent_sources(); cid = xrp['occurrence_id']
        self.assertEqual(self.service.tick().get('operation'), 'ENTRY')
        self.assertEqual(self.oracle.requests[-1]['proposal']['symbol'], 'SOL')
        self.assertEqual(self.store.load()['entry_decisions'][cid]['reason'],
            'ENTRY_WAITING_FOR_EARLIER_CANDIDATE')
        oid = self.oracle.oid('ENTRY')
        self.oracle.fill(oid, self.oracle.orders[oid]['view']['wire_order']['s'])
        for leg in ('STOP', 'TAKE_PROFIT'):
            self.oracle.t += 1000
            self.assertEqual(self.service.tick().get('operation'), 'CREATE_EXIT')
            self.assertEqual(self.oracle.requests[-1]['proposal']['leg'], leg)
            self.assertEqual(self.store.load()['entry_decisions'][cid]['reason'],
                'ENTRY_WAITING_FOR_PROTECTION_OR_MAINTENANCE')
        state = self.store.load()
        self.assertNotIn(sol['occurrence_id'], state.get('entry_decisions', {}))
        self.assertNotIn(cid, state.get('position_recovery_entry_notices', {}))
        self.oracle.t += 1000
        self.assertEqual(self.service.tick().get('operation'), 'ENTRY')
        self.assertEqual(self.oracle.requests[-1]['proposal']['symbol'], 'XRP')
        self.assertEqual([r['proposal']['leg'] for r in self.oracle.requests],
            ['ENTRY', 'STOP', 'TAKE_PROFIT', 'ENTRY'])

    def test_evaluated_specific_refusal_is_not_replaced_by_scheduling_diagnostic(self):
        sol, xrp = self._two_independent_sources()
        account = self.store.load()['routes']['short_account']
        self.oracle.mark[self.oracle._key(account, 'SOL')] = '120'
        self.assertEqual(self.service.tick().get('operation'), 'ENTRY')
        self.assertEqual(self.oracle.requests[-1]['proposal']['symbol'], 'XRP')
        decision = self.store.load()['entry_decisions'][sol['occurrence_id']]
        self.assertEqual(decision['stage'], 'ENTRY_ADMISSION')
        self.assertEqual(decision['reason'], 'TESTNET_MARK_OUTSIDE_ORIGINAL_EXITS')

    def test_repeated_identical_failure_does_not_grow_source_events(self):
        msg = r2732_message(entry=2.3, decision_ms=T-60000); self.seed(msg)
        for _ in range(4):
            service._persist_cycle_failure(self.store, started_ms=self.oracle.t,
                at_ms=self.oracle.t+100, reason='EXACT_SHARED_REDUCING_CAPACITY_REQUIRED')
            self.oracle.t += 1000
        state = self.store.load()
        events = [e for e in state['events'] if e['kind'] == 'ENTRY_DEFERRED']
        self.assertEqual(len(events), 1)
        self.assertEqual(state['entry_decisions'][msg['occurrence_id']]['reason'],
            'EXACT_SHARED_REDUCING_CAPACITY_REQUIRED')
        self.assertFalse(self.http)

    def test_concurrent_later_arrival_is_not_assigned_prior_cycle_failure(self):
        began = self.oracle.t
        self.oracle.t += 1000
        msg = r2732_message(entry=2.3, decision_ms=T-60000); self.seed(msg)
        self.assertEqual(service._persist_cycle_failure(self.store, started_ms=began,
            at_ms=self.oracle.t, reason='PUBLIC_READ_UNAVAILABLE'), 0)
        self.assertNotIn(msg['occurrence_id'], self.store.load().get('entry_decisions', {}))

    def test_ambiguous_committed_attempt_is_not_described_as_unsubmitted(self):
        msg = r2732_message(entry=2.3, decision_ms=T-60000); self.seed(msg)
        self.assertEqual(self.service.tick().get('operation'), 'ENTRY')
        before = self.store.load(); self.oracle.t += 1000
        self.assertEqual(service._persist_cycle_failure(self.store, started_ms=self.oracle.t,
            at_ms=self.oracle.t+500, reason='PERSISTENCE_UNAVAILABLE_NO_SEND'), 0)
        after = self.store.load()
        self.assertEqual(after['requests'], before['requests'])
        self.assertEqual(after['trades'], before['trades'])
        self.assertNotIn(msg['occurrence_id'], after.get('entry_decisions', {}))
        self.assertEqual(len(self.http), 1)

    def test_failed_diagnostic_write_logs_fixed_code_without_retry_or_send(self):
        msg = r2732_message(entry=2.3, decision_ms=T-60000); self.seed(msg)
        with patch.object(self.worker, 'run_once', side_effect=ValueError('PUBLIC_READ_UNAVAILABLE')):
            with patch.object(self.store, 'mutate', side_effect=OSError('secret DSN')) as mutate:
                with self.assertLogs(service.__name__, level='ERROR') as logs:
                    with self.assertRaisesRegex(ValueError, '^PUBLIC_READ_UNAVAILABLE$'):
                        self.service.tick()
        self.assertEqual(mutate.call_count, 1)
        self.assertNotIn('secret', repr(logs.output))
        self.assertEqual(self.service.last_cycle_diagnostic_error_code,
            'CONNECTION_CYCLE_RECONCILIATION_REQUIRED')
        self.assertFalse(self.http)


class TerminalDecisionTests(unittest.TestCase):
    setUp = history_fixture.SQLiteHistoryTests.setUp
    reopen = history_fixture.SQLiteHistoryTests.reopen
    seed = history_fixture.HistoryCases.seed
    receive = history_fixture.HistoryCases.receive
    compact = history_fixture.HistoryCases.compact

    def test_unknown_previous_reason_is_explicit_and_duplicate_cannot_revive(self):
        msg = self.seed()[0]; cid = msg['occurrence_id']
        self.compact(); record = self.reopen().archive_record(cid)
        result = record['source']['entry_result']
        self.assertEqual(result['outcome'], 'NEVER_SUBMITTED')
        self.assertEqual(result['preceding_reason'], 'PRIOR_DECISION_REASON_UNAVAILABLE')
        self.assertFalse(result['decision_evidence_available'])
        self.assertEqual(sum(e['kind'] == 'ENTRY_FINISHED' for e in record['events']), 1)
        self.assertEqual(self.receive(msg, now=BASE+100001)['status'], 'DUPLICATE')
        self.assertNotIn(cid, self.store.load()['sources'])
        self.assertEqual(self.reopen().archive_record(cid)['source']['entry_result'], result)

    def test_known_defer_reason_survives_terminal_archive(self):
        msg = self.seed()[0]; cid = msg['occurrence_id']
        self.store.mutate(lambda state: state.setdefault('entry_decisions', {}).update({cid:
            dict(stage='CONNECTION_CYCLE', reason='PUBLIC_READ_UNAVAILABLE', at_ms=BASE+5000)}))
        self.compact(); record = self.reopen().archive_record(cid)
        result = record['source']['entry_result']
        self.assertEqual(result['preceding_reason'], 'PUBLIC_READ_UNAVAILABLE')
        self.assertEqual(result['preceding_stage'], 'CONNECTION_CYCLE')
        self.assertTrue(result['decision_evidence_available'])
        self.assertEqual(record['auxiliary']['entry_decisions']['reason'], 'PUBLIC_READ_UNAVAILABLE')

    def test_unknown_sent_attempt_cannot_gain_never_submitted_result(self):
        msg = self.seed()[0]; cid = msg['occurrence_id']
        state = self.store.load()
        state['requests']['a'*64] = dict(phase='OUTCOME_UNKNOWN', proposal=dict(card_id=cid, operation='ENTRY'))
        history._finish_unsubmitted_source(state, cid, BASE+100000)
        self.assertNotIn('entry_result', state['sources'][cid])

    def test_positive_unsent_certificate_required_even_after_attempt_aborted(self):
        msg = self.seed()[0]; cid = msg['occurrence_id']; state = self.store.load()
        request = dict(phase='ABORTED_UNSENT', proposal=dict(card_id=cid, operation='ENTRY'),
            observed_oid=None, reply=None, unsent_reason='FINAL_EVIDENCE_EXPIRED')
        state['requests']['a'*64] = request
        history._finish_unsubmitted_source(state, cid, BASE+100000)
        self.assertNotIn('entry_result', state['sources'][cid])
        request['certified_unsent'] = True
        history._finish_unsubmitted_source(state, cid, BASE+100000)
        self.assertEqual(state['sources'][cid]['entry_result']['preceding_reason'], 'FINAL_EVIDENCE_EXPIRED')


if __name__ == '__main__':
    unittest.main()
