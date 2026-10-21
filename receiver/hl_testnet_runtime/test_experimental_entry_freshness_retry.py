"""Aging cached admission facts cannot consume a positively never-sent alert.

Uses the real collector, planner, signing boundary and worker with synthetic
HTTP/signatures and transactional memory. No live network or account keys.
"""
from copy import deepcopy
import unittest
from unittest.mock import patch

import approved_alert_contract as contract
from approved_alert_fixtures import formula_alert
from . import experimental_live_runtime as runtime
from . import test_experimental_live_service as support


class EntryFreshnessRetryTests(unittest.TestCase):
    def setUp(self):
        self.h = support.FullConnectionTests()
        self.h.setUp()
        self.addCleanup(self.h.doCleanups)

    def prepare(self, *, family='r2732', field='allowance_at_ms'):
        h = self.h
        approved = (support.T if family == 'r2732' else
            (support.T//1800000+1)*1800000+(120000 if family == 'sol_g65' else 60000))
        h.oracle.t = approved+10000
        message = formula_alert(family, approved_ms=approved)
        role = 'long_account' if message['side'] == 'LONG' else 'short_account'
        account = h.release['routes'][role]
        now = h.oracle.t
        lifetime = 300000 if field == 'signer_at_ms' else 60000
        h.oracle.t = now-lifetime+1
        h.provider._entry_account(account, role, state=h.store.load(),
            legacy_revision=h.provider._revision({account: []}))
        h.oracle.t = now
        h.seed(message)
        return message

    def expire_after_prepare(self, *, family='r2732', field='allowance_at_ms', boundary=False):
        h = self.h
        message = self.prepare(family=family, field=field)
        if boundary:
            original = h.port.context_loader
            def advance(request):
                value = original(request)
                h.oracle.t += 1
                return value
            target, attribute = h.port, 'context_loader'
        else:
            original = h.store.commit_attempt
            def advance(*args, **kwargs):
                value = original(*args, **kwargs)
                h.oracle.t += 1
                return value
            target, attribute = h.store, 'commit_attempt'
        with patch.object(target, attribute, side_effect=advance):
            result = h.service.tick()
        self.assertEqual(result['status'], 'DEFINITELY_NOT_SUBMITTED_REOBSERVE')
        state = h.store.load()
        request = next(iter(state['requests'].values()))
        self.assertEqual(request['phase'], 'ABORTED_UNSENT')
        self.assertEqual(request['unsent_reason'], 'ENTRY_ACCOUNT_SAMPLE_EXPIRED')
        self.assertTrue(request['certified_unsent'])
        self.assertIsNone(request['reply'])
        self.assertIsNone(request['observed_oid'])
        self.assertEqual(state['trades'][message['occurrence_id']]['phase'], 'RETRY_WAIT_UNSENT')
        self.assertEqual(h.http, [])
        self.assertEqual(h.oracle.requests, [])
        return message, deepcopy(request)

    def refreshed_retry(self, message, previous):
        h = self.h
        cid = message['occurrence_id']
        h.service.tick()  # The unchanged one-second retry delay is still enforced.
        self.assertEqual(h.http, [])
        self.assertEqual(len(h.store.load()['requests']), 1)
        h.oracle.t += runtime.ENTRY_RETRY_DELAY_MS
        h.reconnect()
        self.assertEqual(h.service.tick().get('operation'), 'ENTRY')
        state = h.store.load()
        self.assertEqual(state['requests'][previous['request_id']], previous)
        self.assertEqual(state['sources'][cid]['source'], message)
        current = [r for r in state['requests'].values() if r['request_id'] != previous['request_id']]
        self.assertEqual(len(current), 1)
        self.assertNotEqual(current[0]['proposal']['action']['orders'][0]['c'],
                            previous['proposal']['action']['orders'][0]['c'])
        self.assertEqual(current[0]['proposal']['source_expires_at'], message['expires_at'])
        self.assertEqual(len(h.http), 1)
        self.assertEqual(len(h.oracle.requests), 1)
        h.reconnect()
        h.service.tick()
        self.assertEqual(sum(r['proposal']['operation'] == 'ENTRY'
                             for r in h.oracle.requests), 1)

    def test_xrp_allowance_expiry_in_final_planner_refreshes_and_retries_once(self):
        self.refreshed_retry(*self.expire_after_prepare())

    def test_hype_allowance_expiry_in_final_planner_refreshes_and_retries_once(self):
        self.refreshed_retry(*self.expire_after_prepare(family='hype_row71205'))

    def test_sol_allowance_expiry_in_final_planner_refreshes_and_retries_once(self):
        self.refreshed_retry(*self.expire_after_prepare(family='sol_g65'))

    def test_signer_expiry_in_final_planner_refreshes_and_retries_once(self):
        self.refreshed_retry(*self.expire_after_prepare(field='signer_at_ms'))

    def test_expiry_after_context_validation_is_also_certified_unsent_and_retryable(self):
        self.refreshed_retry(*self.expire_after_prepare(boundary=True))

    def test_expired_original_source_never_retries_after_refresh_or_restart(self):
        message, _ = self.expire_after_prepare()
        h = self.h
        h.oracle.t = contract.moment_ms(message['expires_at'])
        h.reconnect()
        h.service.tick()
        state = h.store.load()
        self.assertEqual(state['trades'][message['occurrence_id']]['phase'], 'CANCELED_WITHOUT_FILL')
        self.assertFalse(runtime.entry_retry_ready(state, message['occurrence_id'], h.oracle.t))
        self.assertEqual(len(state['requests']), 1)
        self.assertEqual(h.http, [])

    def test_canceled_original_source_never_retries_after_refresh_or_restart(self):
        message, _ = self.expire_after_prepare()
        h = self.h
        h.oracle.t += runtime.ENTRY_RETRY_DELAY_MS
        canceled = deepcopy(message)
        canceled.update(kind='CANCEL', source_state='CANCELED',
            cancel_reason='SOURCE_ENTRY_OBSERVED', source_as_of=contract.iso_ms(h.oracle.t),
            source_sequence=h.oracle.t*10+contract.RANK['CANCEL'])
        h.ingest(contract.validate(canceled))
        h.reconnect()
        h.service.tick()
        state = h.store.load()
        self.assertEqual(state['trades'][message['occurrence_id']]['phase'], 'CANCELED_WITHOUT_FILL')
        self.assertEqual(len(state['requests']), 1)
        self.assertEqual(h.http, [])

    def test_fresh_invalid_account_fact_remains_final_refusal(self):
        h = self.h
        message = self.prepare()
        original = h.store.commit_attempt
        def change(*args, **kwargs):
            value = original(*args, **kwargs)
            h.provider._last['context']['entry_accounts'][message['occurrence_id']]['action_headroom'] = 0
            return value
        with patch.object(h.store, 'commit_attempt', side_effect=change):
            self.assertEqual(h.service.tick()['status'], 'DEFINITELY_NOT_SUBMITTED_REOBSERVE')
        state = h.store.load()
        request = next(iter(state['requests'].values()))
        self.assertEqual(request['unsent_reason'], 'FINAL_SOURCE_ENTRY_ADMISSION_RETIRED')
        self.assertEqual(state['trades'][message['occurrence_id']]['phase'], 'CANCELED_WITHOUT_FILL')
        self.assertEqual(h.http, [])


if __name__ == '__main__':
    unittest.main()
