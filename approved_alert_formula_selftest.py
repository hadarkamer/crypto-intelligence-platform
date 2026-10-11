"""Existing formula decisions through the approved transport; no live I/O."""
from copy import deepcopy
from datetime import timedelta
import unittest
from unittest.mock import patch

import approved_alert_contract as contract
import approved_alert_outbox as outbox
import approved_alert_producer as producer
import experimental_execution_bridge as bridge
import sol_g65_experimental_store as g65
import xrp_r2732_experimental_store as xrp
import hype_row71205_experimental_store as hype
from approved_alert_producer_selftest import OutboxDatabase, approved_env
from experimental_execution_source_audit_selftest import BASE, CONFIG, SUBSCRIPTION, SCOPE, ms

STORES = {'sol_g65': g65, 'r2732': xrp, 'hype_row71205': hype}


def formula_fixture(family, *, enable=True, reference=100.):
    store = STORES[family]
    db = OutboxDatabase()
    decision = BASE + timedelta(minutes=30)
    entry = decision + timedelta(minutes=1)
    now = entry + timedelta(seconds=10)
    with patch.object(store, '_connect', db.connect), patch.dict('os.environ', approved_env() if enable else {}, clear=True), \
         patch('experimental_execution_forwarder.notify_source_commit') as wake:
        store.initialize_scope(SUBSCRIPTION, BASE, config_sha256=CONFIG)
        store.reserve_signal(SUBSCRIPTION, decision, entry, reference,
            {'valid': True, 'signal': True}, now, config_sha256=CONFIG)
        pending = store.snapshot(SUBSCRIPTION)
        if family == 'sol_g65':
            assert not db.outbox
            now = entry + timedelta(minutes=1)
            store.advance_position(SUBSCRIPTION, [dict(open_at=entry, open=reference,
                high=reference*1.006, low=reference, close=reference*1.004)],
                now, config_sha256=CONFIG)
        state = store.snapshot(SUBSCRIPTION)
        calls = wake.call_count
    return dict(state=state, pending=pending, database=db, store=store,
        key=store.key_for(SUBSCRIPTION), entry=entry, now=now, now_ms=ms(now), fence_ms=ms(BASE), wake_calls=calls)


class FormulaTransportTests(unittest.TestCase):
    def test_actual_sources_preserve_all_prices_and_formula_state(self):
        for family in STORES:
            for price in (100., 123.456789, .1532498):
                with self.subTest(family=family, price=price):
                    f = formula_fixture(family, reference=price)
                    before = deepcopy(f['state'])
                    msg, = producer.approved_messages(f['state'], now_ms=ms(f['now']), fence_ms=ms(BASE))
                    self.assertEqual(f['state'], before)
                    p = f['state']['intents'][0]['payload']
                    keys = ('entry_price', 'original_stop_price', 'take_price') if family == 'sol_g65' else (
                        'entry_price_decimal', 'stop_price_decimal', 'take_price_decimal')
                    self.assertEqual([msg[k] for k in ('entry', 'stop', 'take_profit')],
                                     [contract.price(p[k]) for k in keys])
                    self.assertEqual(contract.validate(msg), msg)
                    with f['database'].connect() as conn:
                        self.assertEqual(outbox.read(conn, [f['key']]), [msg])
                    self.assertEqual(f['wake_calls'], 1)
                    self.assertEqual(ms(f['now'])-contract.moment_ms(msg['approved_at']),
                                     0 if family == 'sol_g65' else 10000)
                    self.assertIn(f['key'], bridge.approved_source_keys({SCOPE}))

    def test_pending_g65_never_emits_and_expiry_is_original_fill_confirmation(self):
        f = formula_fixture('sol_g65')
        self.assertEqual(producer.approved_messages(f['pending'], now_ms=ms(f['now']), fence_ms=1), [])
        msg, = producer.approved_messages(f['state'], now_ms=ms(f['now']), fence_ms=1)
        self.assertEqual(msg['approved_at'], f['state']['intents'][0]['payload']['fill_confirmed_at'])
        expiry = contract.moment_ms(msg['expires_at'])
        self.assertEqual(expiry, ms(f['now'])+90000)
        self.assertEqual(producer.approved_messages(f['state'], now_ms=expiry, fence_ms=1), [])
        self.assertEqual(producer.approved_messages(f['state'], now_ms=ms(f['now']), fence_ms=ms(f['now'])+1), [])

    def test_disabled_transport_preserves_formula_outputs(self):
        for family in STORES:
            f = formula_fixture(family, enable=False)
            self.assertEqual(f['database'].outbox, {})
            self.assertEqual(f['wake_calls'], 0)
            self.assertEqual(len(f['state']['intents']), 1)

    def test_original_cancellation_survives_notification_pruning_and_ack(self):
        for family in STORES:
            f = formula_fixture(family)
            with f['database'].connect() as conn:
                msg, = outbox.read(conn, [f['key']])
                outbox.acknowledge(conn, [f['key']], msg)
                state = deepcopy(f['state']); state.update(active=None, history=[], intents=[],
                    last_exit_at=(f['now']+timedelta(minutes=1)).isoformat())
                self.assertTrue(outbox.synchronize(conn, f['key'], state, ms(f['now'])+60000))
                canceled, = outbox.read(conn, [f['key']])
                self.assertEqual(canceled['kind'], 'CANCEL')
                self.assertEqual(contract.plan_digest(canceled), contract.plan_digest(msg))
                outbox.acknowledge(conn, [f['key']], canceled)
                self.assertFalse(outbox.synchronize(conn, f['key'], f['state'], ms(f['now'])+60000))
                self.assertEqual(outbox.read(conn, [f['key']]), [])

    def test_commit_failure_never_wakes_sender_or_retains_outbox(self):
        for family, store in STORES.items():
            f = formula_fixture(family, enable=False); db = f['database']
            db.fail_commit = True
            with patch.dict('os.environ', approved_env(), clear=True), \
                 patch('experimental_execution_forwarder.notify_source_commit') as wake:
                with self.assertRaisesRegex(RuntimeError, 'COMMIT_FAILURE'):
                    with outbox.source_transaction(db.connect, None) as (conn, changes):
                        outbox.synchronize_source(conn, f['key'], f['state'], f['now'], changes)
                wake.assert_not_called()
            self.assertEqual(db.outbox, {})


if __name__ == '__main__': unittest.main()
