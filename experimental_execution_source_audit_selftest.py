"""Offline evidence for the missing experimental notification/execution bridge.

This is an AUDIT, not a proposed production adapter. Fixtures use the actual
producer state transitions with an in-memory database. Nothing calls Telegram,
an exchange, a live database, or modifies production configuration.
"""
from contextlib import ExitStack
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
import socket
import sys
from types import ModuleType
import unittest
from unittest.mock import patch

import alert_cards_forwarder as forwarder
import alert_cards_wire as wire
import sol_proximity_experimental_signal as maxpain
import sol_proximity_experimental_store as maxpain_store
import xrp_r2732_experimental_store as r2732
from maxpain_experimental_specs import HYPE_LONG_TF, SPECS
from xrp_r2732_experimental_store_selftest import MemoryDatabase

SCOPE = '1' * 64
SUBSCRIPTION = 'general-watch:' + SCOPE
BASE = datetime(2026, 10, 6, 15, 0, tzinfo=timezone.utc)
CONFIG = 'a' * 64
M = maxpain.MINUTE


def ms(value):
    return int(value.timestamp() * 1000)


def iso_ms(value):
    return datetime.fromtimestamp(value / 1000, timezone.utc).isoformat()


def r2732_fixture():
    """Produce the real durable DELIVERED shape; only Telegram is simulated."""
    # Import the real renderer lazily: source-only readers do not need workers.
    from xrp_r2732_experimental_worker import render_alert
    db = MemoryDatabase()
    decision, entry = BASE + timedelta(minutes=15), BASE + timedelta(minutes=16)
    now = entry + timedelta(seconds=10)
    text = render_alert(100, ms(entry))
    with patch.object(r2732, '_connect', db.connect):
        r2732.initialize_scope(SUBSCRIPTION, BASE, config_sha256=CONFIG)
        r2732.reserve_signal(SUBSCRIPTION, decision, entry, 100,
            {'fixture_only': True}, now, text=text, config_sha256=CONFIG)
        claimed = r2732.claim_pending(SUBSCRIPTION, now, config_sha256=CONFIG)
        assert claimed is not None
        assert r2732.finish_attempt(SUBSCRIPTION, claimed['intent_id'],
            claimed['attempt_token'], 'DELIVERED', now + timedelta(seconds=1),
            message_id=123, config_sha256=CONFIG)
        state = r2732.snapshot(SUBSCRIPTION)
    return dict(key=r2732.key_for(SUBSCRIPTION), scope=SUBSCRIPTION, state=state,
                intent=state['intents'][0], database=db, now=now + timedelta(seconds=1))


def _bundle(spec, now, target):
    rows, slots = [], []
    for tf in maxpain.TIMEFRAMES:
        rows.append(dict(timeframe=tf, current_price=100., short_max_pain=target,
            long_max_pain=90., source_observed_at_utc=iso_ms(now),
            price_fetched_at_utc=iso_ms(now), short_liquidation_amount=100,
            long_liquidation_amount=100,
            price_source=f'HYPERLIQUID_{spec.coin}_PERPETUAL_TRADE_1M',
            price_market='perpetual', price_pair=spec.coin + '-PERP',
            price_instrument=spec.coin))
        for side, level in (('SHORT', target), ('LONG', 90.)):
            slots.append(dict(timeframe=tf, source_side=side, target_price=level,
                status='SCORED', score=70., components={'target_proximity': 25.}))
    return dict(cycle_id=str(now), computed_at_utc=iso_ms(now),
                coins={spec.coin: dict(maxpain=slots,
                    sources={'maxpain_operational_rows': rows})})


def _decoded(spec, now, target):
    end = now // M * M
    bars = ([[t, 100, 110, 90, 100] for t in range(end - 1440 * M, end, M)]
            if spec.require_range24 else None)
    return maxpain.decode_bundle(_bundle(spec, now, target), now, spec, bars)


def maxpain_fixture(spec=HYPE_LONG_TF, *, gap=False, lag_seconds=10):
    """Real prospective ingestion and closed-minute touch; real durable receipt."""
    from sol_proximity_experimental_worker import render_alert
    db = MemoryDatabase()
    scope = SUBSCRIPTION if spec.coin == 'SOL' else SUBSCRIPTION + ':' + spec.rule_id
    base = ms(BASE)
    now = base + 10_000
    first_target = 100 + spec.lower_pct + .05
    target = 100 + (spec.lower_pct + spec.upper_pct) / 2
    with patch.object(maxpain_store, '_connect', db.connect):
        maxpain_store.initialize_scope(scope, now, config_sha256=CONFIG)
        maxpain_store.ingest(scope, _decoded(spec, now, first_target),
            [[base, 100, 100.01, 99.99, 100]], now, config_sha256=CONFIG)
        maxpain_store.advance(scope, [[base, 100, 100.01, 99.99, 100]],
            base + M + 10_000, config_sha256=CONFIG)
        now += M
        maxpain_store.ingest(scope, _decoded(spec, now, target),
            [[base + M, 100, 100.01, 99.99, 100]], now, config_sha256=CONFIG)
        pending_state = maxpain_store.snapshot(scope)
        assert len(pending_state['active']) == 1
        plan = pending_state['active'][0]
        entry = plan['entry_price']
        opened = entry - .25 if gap else entry + .1
        rows = [[base + M, 100, 100.01, 99.99, 100],
                [base + 2 * M, opened, entry + .2, entry - .3, entry]]
        now = base + 3 * M + lag_seconds * 1000
        maxpain_store.advance(scope, rows, now, config_sha256=CONFIG)
        state = maxpain_store.snapshot(scope)
        if state['intents']:
            claimed = maxpain_store.claim_pending(scope, now, config_sha256=CONFIG)
            assert claimed is not None
            # The exact renderer is used, but production does not store its text.
            text = render_alert(claimed['payload'])
            assert maxpain_store.finish_attempt(scope, claimed['intent_id'],
                claimed['attempt_token'], 'DELIVERED', now + 1000, message_id=124,
                config_sha256=CONFIG)
            state = maxpain_store.snapshot(scope)
            intent = state['intents'][0]
        else:
            intent, text = None, None
    return dict(key=maxpain_store.key_for(scope), scope=scope, state=state,
                intent=intent, rendered_text=text, database=db,
                now=datetime.fromtimestamp((now + 1000) / 1000, timezone.utc))


class ReadOnlySource:
    """A strict stand-in for psycopg: only the current reader's SQL is allowed."""
    def __init__(self, values):
        self.values, self.calls, self.connections = deepcopy(values), [], []

    def connect(self, dsn, **kwargs):
        self.connections.append((dsn, kwargs))
        source = self
        class Connection:
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def execute(self, sql, params):
                source.calls.append((sql, params))
                if sql.startswith('SELECT value FROM bot_settings WHERE key='):
                    value = source.values.get(params[0])
                    class Row:
                        def fetchone(self):
                            return None if value is None else {'value': value}
                    return Row()
                if 'FROM dual_cvd65_intents' in sql and sql.lstrip().startswith('SELECT'):
                    class Rows:
                        def fetchall(self): return []
                    return Rows()
                raise AssertionError('No other SQL authorized in audit')
        return Connection()


def run_current_reader(values, now):
    source = ReadOnlySource(values)
    psycopg, rows = ModuleType('psycopg'), ModuleType('psycopg.rows')
    psycopg.connect, rows.dict_row = source.connect, object()
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None): return now
    with ExitStack() as stack:
        stack.enter_context(patch.dict(sys.modules, {'psycopg': psycopg, 'psycopg.rows': rows}))
        stack.enter_context(patch.object(forwarder, '_source_dsn', return_value='postgresql://audit-only'))
        stack.enter_context(patch.object(forwarder, 'datetime', Clock))
        result = forwarder.read_delivered({SCOPE}, BASE)
    return result, source


class SourceBridgeAudit(unittest.TestCase):
    def setUp(self):
        # Any accidental network access fails the audit immediately.
        guard = patch.object(socket.socket, 'connect', side_effect=AssertionError('Offline audit only'))
        guard.start(); self.addCleanup(guard.stop)

    def test_delivered_r2732_and_all_five_maxpain_namespaces_are_omitted(self):
        fixtures = [r2732_fixture()] + [maxpain_fixture(spec) for spec in SPECS.values()]
        values = {f['key']: json.dumps(f['state']) for f in fixtures}
        result, source = run_current_reader(values, max(f['now'] for f in fixtures))
        self.assertEqual(result, [])
        queried = {params[0] for sql, params in source.calls if 'bot_settings' in sql}
        self.assertTrue(set(values).isdisjoint(queried))
        self.assertEqual(queried, {'manual-four-formulas-outbox-v1:' + SCOPE,
            'u21-xrp-experimental-cap1-v1:' + hashlib.sha256(SUBSCRIPTION.encode()).hexdigest()})
        self.assertEqual(len(source.calls), 3)
        self.assertIn('default_transaction_read_only=on', source.connections[0][1]['options'])
        self.assertEqual(source.values, values)

    def test_old_manual_positive_control_is_forwarded_by_same_reader(self):
        from alert_cards_forwarder_selftest import producer_v5_delivery
        item = producer_v5_delivery()
        item.update(acknowledged_at=(BASE + timedelta(minutes=5)).isoformat())
        values = {'manual-four-formulas-outbox-v1:' + SCOPE: json.dumps({
            'version': 'manual-four-formulas-outbox-v1', 'intents': [item]})}
        result, _ = run_current_reader(values, BASE + timedelta(minutes=10))
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['intent_id'], item['intent_id'])

    def test_r2732_freezes_lock_and_90_second_reference_expiry(self):
        f = r2732_fixture(); intent = f['intent']; p = intent['payload']
        self.assertEqual(intent['status'], 'DELIVERED')
        self.assertEqual(wire.moment(intent['expires_at']) - wire.moment(p['entry_at']), timedelta(seconds=90))
        self.assertEqual(wire.moment(p['entry_at']) - wire.moment(p['decision_at']), timedelta(minutes=1))
        self.assertEqual(p['price_source'], 'HYPERLIQUID_XRP_PERPETUAL_TRADE_1M')
        self.assertEqual(p['lock_trigger_price'], p['entry_price'] - 2 * p['original_risk_distance'])
        self.assertEqual(p['lock_stop_price'], p['entry_price'] - .5 * p['original_risk_distance'])
        self.assertIn('החל מהדקה הבאה', intent['text'])
        for field in ('exchange_order_id', 'exchange_fill_id', 'account_address'):
            self.assertNotIn(field, p)

    def test_r2732_active_profit_lock_updates_without_new_delivered_message(self):
        f = r2732_fixture(); frozen = deepcopy(f['intent'])
        entry = wire.moment(frozen['payload']['entry_at'])
        with patch.object(r2732, '_connect', f['database'].connect):
            r2732.advance_position(SUBSCRIPTION, [dict(open_at=entry, open=100,
                high=100.1, low=98.9, close=99)], entry + timedelta(minutes=1))
            state = r2732.snapshot(SUBSCRIPTION)
        self.assertEqual(state['intents'], [frozen])
        self.assertEqual(state['active']['lock_effective_at'], (entry + timedelta(minutes=1)).isoformat())
        self.assertEqual(state['active']['stop_price'], frozen['payload']['lock_stop_price'])

    def test_r2732_observation_frees_capacity_without_exchange_execution_evidence(self):
        f = r2732_fixture(); entry = wire.moment(f['intent']['payload']['entry_at'])
        with patch.object(r2732, '_connect', f['database'].connect):
            r2732.advance_position(SUBSCRIPTION, [dict(open_at=entry, open=100,
                high=100.1, low=91.9, close=92)], entry + timedelta(minutes=1))
            state = r2732.snapshot(SUBSCRIPTION)
        self.assertIsNone(state['active'])
        self.assertEqual(state['intents'][0]['status'], 'DELIVERED')
        self.assertEqual(state['history'][-1]['outcome'], 'TP')

    def test_maxpain_receipt_is_post_closed_minute_and_does_not_store_exact_text(self):
        f = maxpain_fixture(); i = f['intent']; p = i['payload']
        self.assertEqual(i['status'], 'DELIVERED')
        self.assertEqual(i['expires_ms'] - p['fill_ms'], 150_000)
        self.assertGreaterEqual(i['acknowledged_ms'] - p['fill_ms'], M)
        self.assertNotIn('text', i)
        self.assertIn('הנגיעה אינה אישור מילוי או מחיר זמין כעת', f['rendered_text'])
        self.assertEqual(p['source_quote']['price_source'], 'HYPERLIQUID_HYPE_PERPETUAL_TRADE_1M')
        self.assertEqual(p['expires_ms'] - p['arm_ms'], 24 * 60 * M)
        self.assertNotEqual(p['expires_ms'], i['expires_ms'])

    def test_maxpain_gap_observation_price_can_differ_from_original_planned_entry(self):
        f = maxpain_fixture(gap=True); p = f['intent']['payload']
        self.assertEqual(p['entry_price'], 98.)
        self.assertEqual(p['fill_price'], 97.75)
        self.assertEqual(p['stop_price'], 95.)
        self.assertEqual(p['take_price'], 100.5)
        self.assertIn('97.75', f['rendered_text'])

    def test_maxpain_old_touch_keeps_observation_but_does_not_create_replay_alert(self):
        f = maxpain_fixture(lag_seconds=90)
        self.assertIsNone(f['intent'])
        self.assertEqual(f['state']['active'][0]['status'], 'OPEN')
        self.assertEqual(f['state']['intents'], [])

    def test_maxpain_identity_and_current_five_family_scopes_are_distinct(self):
        fixtures = [maxpain_fixture(spec) for spec in SPECS.values()]
        self.assertEqual(len({f['key'] for f in fixtures}), 5)
        self.assertEqual({f['intent']['payload']['rule_id'] for f in fixtures},
                         {spec.rule_id for spec in SPECS.values()})
        for f in fixtures:
            i = f['intent']; p = i['payload']
            self.assertEqual(i['intent_id'], p['position_id'])
            self.assertEqual(i['position_id'], p['position_id'])
            self.assertEqual(len(i['intent_id']), 64)
            self.assertIn('episode_generation', p)
            self.assertIn('target_price', p)
            self.assertIn('liquidity_growth', p)
            # Boolean permission is not the actual multitimeframe liquidity proof.
            self.assertNotIn('liquidity_growth_proof', p)


if __name__ == '__main__':
    unittest.main()
