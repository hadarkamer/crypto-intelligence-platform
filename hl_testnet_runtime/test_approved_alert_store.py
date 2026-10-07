"""Approved-alert idempotency, terminality and durable offline restart tests."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import approved_alert_contract as contract
from approved_alert_fixtures import BASE, maxpain_alert
from experimental_execution_fixtures import maxpain_message
from . import experimental_plan_store as store
from .experimental_plan_sqlite_test_support import open_software_store
from .test_experimental_plan_store import FakeJournal

BEFORE = contract.iso_ms(BASE - 60_000)
NOW = contract.iso_ms(BASE + 1000)
LATER = contract.iso_ms(BASE + 30 * 86400_000)


def reduce(previous, message, *, now=NOW, not_before=BEFORE):
    return store.reduce_source(previous, message, now=now,
                               not_before=not_before, domain='software')


class ApprovedAlertStoreTests(unittest.TestCase):
    def setUp(self):
        for target in ('socket.socket.connect', 'socket.socket.connect_ex',
                       'http.client.HTTPSConnection', 'hyperliquid_testnet_executor._wallet',
                       'hyperliquid_testnet_executor._signed_body'):
            guard = patch(target, side_effect=AssertionError('OFFLINE_ONLY'))
            guard.start(); self.addCleanup(guard.stop)

    def test_after_closed_bar_acceptance_needs_no_pretouch_plan_or_heartbeat(self):
        source = maxpain_alert()
        state, changed = reduce(None, source)
        self.assertTrue(changed)
        self.assertEqual(state['entry_permission'], 'WAITING')
        self.assertEqual(state['source'], source)
        self.assertIsNone(state['source']['valid_until'])

    def test_late_duplicate_does_not_cancel_working_or_filled_trade(self):
        source = maxpain_alert()
        state, _ = reduce(None, source)
        state['strategy'] = dict(entry_oid='11', filled_quantity='2', needs_protection=True)
        again, changed = reduce(state, source, now=LATER)
        self.assertFalse(changed)
        self.assertEqual(again, state)
        self.assertEqual(again['entry_permission'], 'WAITING')

    def test_first_late_or_prerelease_alert_creates_permanent_tombstone(self):
        source = maxpain_alert()
        for now, before, reason in (
            (source['expires_at'], BEFORE, 'APPROVED_ALERT_EXPIRED_BEFORE_ACCEPTANCE'),
            (NOW, NOW, 'APPROVED_ALERT_PRECEDES_RELEASE')):
            with self.subTest(reason=reason):
                state, changed = reduce(None, source, now=now, not_before=before)
                self.assertTrue(changed)
                self.assertEqual((state['entry_permission'], state['terminal_reason']),
                                 ('RETIRED', reason))
                later, changed = reduce(state, source, now=LATER)
                self.assertFalse(changed)
                self.assertEqual(later, state)

    def test_recipient_local_time_and_redelivery_do_not_create_second_revision(self):
        state, _ = reduce(None, maxpain_alert())
        peer = maxpain_alert(as_of_ms=BASE + 10_000)
        peer['created_at'] = contract.iso_ms(BASE - 160_000)
        second, changed = reduce(state, peer, now=contract.iso_ms(BASE + 11_000))
        self.assertFalse(changed)
        self.assertEqual(second, state)

    def test_first_cancellation_and_late_alert_cannot_restore_entry_permission(self):
        canceled = maxpain_alert(kind='CANCEL', as_of_ms=BASE + 2000,
                                 cancel_reason='SOURCE_TAKE_TOUCHED')
        state, _ = reduce(None, canceled, now=canceled['source_as_of'])
        again, changed = reduce(state, maxpain_alert(), now=LATER)
        self.assertFalse(changed)
        self.assertEqual(again, state)
        self.assertEqual(state['entry_permission'], 'RETIRED')

    def test_cancel_after_deadline_keeps_filled_strategy_and_is_idempotent(self):
        state, _ = reduce(None, maxpain_alert())
        state['strategy'] = dict(filled_quantity='2', stop_oid='17', take_oid='18')
        canceled = maxpain_alert(kind='CANCEL', as_of_ms=BASE + 300_000,
                                 cancel_reason='SOURCE_STOP_TOUCHED')
        result, changed = reduce(state, canceled, now=canceled['source_as_of'])
        self.assertTrue(changed)
        self.assertEqual(result['strategy'], state['strategy'])
        self.assertEqual(result['entry_permission'], 'RETIRED')
        again, changed = reduce(result, canceled, now=LATER)
        self.assertFalse(changed)
        self.assertEqual(again, result)

    def test_deadline_or_price_change_cannot_revive_same_occurrence(self):
        source = maxpain_alert()
        state, _ = reduce(None, source, now=source['expires_at'])
        for key, value in (('entry', '93.5'), ('expires_at', LATER)):
            changed = deepcopy(source); changed[key] = value
            error, reason = ((contract.ContractError, 'APPROVED_ALERT_TIMES')
                if key == 'expires_at' else (store.PlanStoreError, 'IMMUTABLE_PLAN_CHANGED'))
            with self.subTest(key=key), self.assertRaisesRegex(
                    error, reason):
                reduce(state, changed, now=LATER)

    def test_same_legacy_occurrence_is_not_admitted_as_a_second_protocol(self):
        legacy = maxpain_message()
        legacy_now = contract.iso_ms(contract.moment_ms(legacy['source_as_of']) + 1)
        legacy_before = contract.iso_ms(contract.moment_ms(legacy['created_at']) - 1)
        state, _ = reduce(None, legacy, now=legacy_now, not_before=legacy_before)
        approved_ms = (contract.moment_ms(legacy['arm_at']) // 60_000 + 1) * 60_000
        approved = maxpain_alert(approved_ms=approved_ms)
        approved['proof'] = {key: legacy['proof'][key] for key in contract.PROOF_FIELDS}
        approved['original_target'] = legacy['original_target']
        approved['occurrence_id'] = contract.occurrence_id(approved)
        contract.validate(approved)
        self.assertEqual(legacy['occurrence_id'], approved['occurrence_id'])
        with self.assertRaisesRegex(store.PlanStoreError, 'IMMUTABLE_PLAN_CHANGED'):
            reduce(state, approved, now=contract.iso_ms(approved_ms + 1),
                   not_before=legacy_before)

    def test_concurrent_approved_copies_and_cancel_finish_in_one_tombstone(self):
        journal = FakeJournal(); receiver = store.PlanStore(journal)
        alert = maxpain_alert()
        canceled = maxpain_alert(kind='CANCEL', cancel_reason='SOURCE_TAKE_TOUCHED')
        def receive(message):
            return receiver.ingest(message, now=NOW, not_before=BEFORE)
        with ThreadPoolExecutor(max_workers=6) as pool:
            list(pool.map(receive, [alert, canceled, alert, alert, canceled, alert]))
        state = receiver.load(alert['occurrence_id'])
        self.assertEqual(state['entry_permission'], 'RETIRED')
        self.assertLessEqual(state['revision'], 2)
        self.assertEqual(len(journal.records), 1)

    def test_real_local_file_restart_after_unknown_commit_keeps_single_occurrence(self):
        with tempfile.TemporaryDirectory(prefix='approved-alert-offline-') as directory:
            path = Path(directory) / 'inbox.sqlite3'
            receiver = open_software_store(path, create=True)
            receiver.journal.lose_commit_ack_once = True
            source = maxpain_alert()
            with self.assertRaisesRegex(store.PlanStoreError, 'COMMIT_ACK_UNKNOWN'):
                receiver.ingest(source, now=NOW, not_before=BEFORE)
            restarted = open_software_store(path)
            receipt = restarted.ingest(source, now=LATER, not_before=BEFORE)
            self.assertEqual((receipt['status'], receipt['revision']), ('DUPLICATE', 1))
            self.assertEqual(restarted.load(source['occurrence_id'])['entry_permission'], 'WAITING')


if __name__ == '__main__':
    unittest.main()
