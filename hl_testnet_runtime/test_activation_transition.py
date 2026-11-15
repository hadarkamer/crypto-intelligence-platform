"""Operational pauses on the real manager; only storage/venue ports isolated."""
from copy import deepcopy
from decimal import Decimal
import os
import unittest
from unittest.mock import patch

import approved_alert_contract as contract
from approved_alert_fixtures import formula_alert, selected_formula_alert
from . import experimental_execution_dispatch as boundary
from . import test_experimental_live_runtime as support
from .experimental_execution_runtime import _working_entry_active

T = support.T


class ActivationTransitionContract:
    real_pg = False
    setUp = support.RuntimeContract.setUp
    make_worker = support.RuntimeContract.make_worker
    cycle = support.RuntimeContract.cycle
    restart = support.RuntimeContract.restart

    def message(self, family):
        if family == 'hype_row71205':
            approved = ((T + 1799999) // 1800000) * 1800000 + 60000
            self.venue.t = max(self.venue.t, approved + 1000)
            return formula_alert(family, approved_ms=approved)
        return selected_formula_alert(family, reference_ms=T, approved_ms=T + 5000,
                                      cycle='activation-' + family)

    def seed_old_entry(self, family='ct009'):
        msg = self.message(family)
        account = self.store.load()['routes']['short_account']
        self.venue.mark[self.venue._key(account, msg['symbol'])] = msg['entry']
        # Construct only the exact pre-upgrade persisted state. Product policy
        # runs unpatched during the transition being tested.
        with patch.object(contract, 'new_entry_allowed', return_value=True), \
                patch.object(contract, 'entry_retirement_required', return_value=False):
            self.worker.receive([msg])
            self.assertEqual(self.cycle()['operation'], 'ENTRY')
            self.cycle(False)
        return msg

    def test_direct_paused_receipt_is_retired_and_duplicate_survives_restart(self):
        for family in ('ct009', 'oconf0034_13', 'hype_row71205'):
            msg = self.message(family)
            result = self.worker.receive([msg])
            self.assertEqual(result['receipts'][0]['entry_permission'], 'RETIRED')
            before = self.store.load()['sources'][msg['occurrence_id']]
            self.restart(); self.worker.receive([msg, msg]); self.cycle()
            after = self.store.load()['sources'][msg['occurrence_id']]
            self.assertEqual(before, after)
            self.assertEqual(after['terminal_reason'], 'FORMULA_NEW_ENTRIES_SUSPENDED')
        self.assertEqual(self.venue.requests, [])

    def test_restart_retires_accepted_gtc_without_source_cancel_message(self):
        msg = self.seed_old_entry()
        self.restart()
        result = self.cycle(False)
        self.assertEqual(result['operation'], 'CANCEL')
        source = self.store.load()['sources'][msg['occurrence_id']]
        self.assertEqual(source['entry_permission'], 'RETIRED')
        self.assertIsNone(source['cancellation'])
        self.assertFalse(_working_entry_active(source, self.venue.t))
        self.cycle(False); self.restart(); self.cycle(False)
        self.assertEqual([r['proposal']['operation'] for r in self.venue.requests], ['ENTRY', 'CANCEL'])
        self.assertEqual(self.store.load()['trades'][msg['occurrence_id']]['phase'], 'CANCELED_WITHOUT_FILL')

    def test_partial_fill_is_protected_before_only_entry_remainder_is_canceled(self):
        msg = self.seed_old_entry('oconf0034_13')
        entry = self.venue.oid('ENTRY')
        quantity = (Decimal(self.venue.orders[entry]['view']['wire_order']['s']) / 2).quantize(Decimal('.01'))
        self.venue.fill(entry, str(quantity))
        for _ in range(4): self.cycle(False)
        self.assertEqual([r['proposal']['operation'] for r in self.venue.requests],
                         ['ENTRY', 'CREATE_EXIT', 'CREATE_EXIT', 'CANCEL'])
        self.assertEqual(self.venue.requests[-1]['proposal']['leg'], 'ENTRY')
        for leg in ('STOP', 'TAKE_PROFIT'):
            row = self.venue.orders[self.venue.oid(leg)]['view']
            self.assertEqual(row['status'], 'OPEN')
            self.assertEqual(Decimal(row['wire_order']['s']), quantity)
        trade = self.store.load()['trades'][msg['occurrence_id']]
        self.assertEqual(trade['phase'], 'OPEN')
        self.assertFalse(trade['exit_fills'])

    def test_lost_cancel_reply_is_not_repeated_after_restart(self):
        msg = self.seed_old_entry()
        self.venue.lose_reply = True
        self.cycle(False)
        self.restart()
        for _ in range(3): self.cycle(False)
        self.assertEqual([r['proposal']['operation'] for r in self.venue.requests], ['ENTRY', 'CANCEL'])
        self.assertEqual(self.store.load()['trades'][msg['occurrence_id']]['phase'], 'CANCELED_WITHOUT_FILL')

    def test_unfilled_owned_entry_cancel_does_not_need_a_new_price(self):
        self.seed_old_entry()
        original = self.provider.collect
        def collect(*args, **kwargs):
            context = original(*args, **kwargs)
            context['marks'] = {}
            return context
        with patch.object(self.provider, 'collect', side_effect=collect):
            self.assertEqual(self.cycle(False)['operation'], 'CANCEL')
        self.assertEqual(self.venue.requests[-1]['proposal']['leg'], 'ENTRY')

    def test_final_sender_refuses_frozen_paused_entry_but_allows_history(self):
        msg = self.seed_old_entry()
        current = self.store.load()['sources'][msg['occurrence_id']]
        with self.assertRaisesRegex(boundary.BoundaryError, 'FORMULA_NEW_ENTRIES_DISABLED'):
            boundary._source(msg, current, self.venue.t, entry=True)
        self.assertEqual(boundary._source(msg, current, self.venue.t, entry=False), msg)


class ActivationTransitionTests(ActivationTransitionContract, unittest.TestCase):
    pass


@unittest.skipUnless(os.environ.get('HL_JOURNAL_CI_URL'), 'Disposable PostgreSQL required')
class ActivationTransitionPostgresTests(ActivationTransitionContract, unittest.TestCase):
    real_pg = True
