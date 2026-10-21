"""Selected DOGE source -> approved durable transport, with no live I/O."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timezone
import json
import os
import unittest
from unittest.mock import patch

import alert_delivery_policy as roster
import approved_alert_contract as contract
import approved_alert_producer as producer
import approved_alert_outbox as outbox
import experimental_execution_bridge as bridge
import experimental_execution_transport as transport
import doge_partial_experimental_worker as doge
import sol_proximity_experimental_store as store
from approved_alert_fixtures import doge_partial_alert
from approved_alert_producer_selftest import OutboxDatabase, approved_env
import approved_alert_producer_selftest as pg_support
from experimental_execution_source_audit_selftest import SCOPE, SUBSCRIPTION
from experimental_execution_transport_selftest import config, ack
from doge_partial_experimental_selftest import pending, bar, BASE, M


def doge_fixture(*, target=102, enabled=True):
    """Advance the actual frozen reducer inside its actual source transaction."""
    db = OutboxDatabase()
    scope = SUBSCRIPTION + ':' + doge.RULE_ID
    state = pending(target)
    state.update(version=store.VERSION, config_sha256=doge.SOURCE_CONFIG_SHA256)
    for position in state['active']:
        position['source_quote'] = dict(price_source='HYPERLIQUID_DOGE_PERPETUAL_TRADE_1M',
            price_instrument='DOGE', price_market='perpetual', price_pair='DOGE-PERP')
    key = store.key_for(scope)
    db.values[key] = json.dumps(state)
    when = BASE + 2*M
    entry = state['active'][0]['entry_price']
    now = when + M + 10_000
    with patch.object(store, '_connect', db.connect), \
            patch.dict('os.environ', approved_env() if enabled else {}, clear=True), \
            patch('experimental_execution_forwarder.notify_source_commit') as wake:
        doge.StoreAPI().advance(scope, [bar(when, entry, entry+.1, entry-.1, entry)], now,
                                config_sha256=doge.SOURCE_CONFIG_SHA256)
        state = store.snapshot(scope)
    return dict(database=db, key=key, scope=scope, state=state, now_ms=now,
                fence_ms=BASE, wake_calls=wake.call_count)


class DogeApprovedTests(unittest.TestCase):
    def setUp(self):
        for name in ('socket.socket.connect', 'socket.socket.connect_ex', 'http.client.HTTPSConnection'):
            guard = patch(name, side_effect=AssertionError('OFFLINE_ONLY'))
            guard.start(); self.addCleanup(guard.stop)

    def project(self, f, *, now=None):
        return producer.approved_messages(f['state'], now_ms=f['now_ms'] if now is None else now,
                                          fence_ms=f['fence_ms'])

    def test_every_selected_formula_has_contract_and_discoverable_source(self):
        represented = set(contract.SPECS) | {rule for rule, _ in contract.FORMULAS.values()}
        self.assertLessEqual(roster.SELECTED_EXPERIMENTAL_RULES, represented)
        self.assertEqual(len(roster.SELECTED_EXPERIMENTAL_RULES), 4)
        self.assertIn(store.key_for(SUBSCRIPTION+':'+doge.RULE_ID), bridge.approved_source_keys({SCOPE}))
        self.assertNotIn(doge.RULE_ID, bridge.contract.SPECS)

    def test_both_directions_commit_prices_before_sender_wake(self):
        for target, side in ((102, 'LONG'), (98, 'SHORT')):
            with self.subTest(side=side):
                f = doge_fixture(target=target)
                before = deepcopy(f['state'])
                msg, = self.project(f)
                p = f['state']['intents'][0]['payload']
                self.assertEqual(msg['side'], side)
                self.assertEqual(msg['rule_id'], doge.RULE_ID)
                for dest, source in [('entry','fill_price'),('stop','stop_price'),('take_profit','take_price')]:
                    self.assertEqual(msg[dest], producer.displayed_price(p[source]))
                self.assertEqual(msg['policy']['partial_take_price'], producer.displayed_price(p['partial_take_price']))
                self.assertEqual(msg['policy']['hold_until'], contract.iso_ms(p['hold_until_ms']))
                self.assertEqual(msg['policy']['partial_fraction'], '0.75')
                self.assertEqual(f['state'], before)
                self.assertTrue(f['state']['notification_only'])  # Frozen reducer metadata, not a transport gate.
                self.assertEqual(f['wake_calls'], 1)
                with f['database'].connect() as conn:
                    self.assertEqual(outbox.read(conn, [f['key']]), [msg])

    def test_disabled_bridge_creates_no_execution_work(self):
        f = doge_fixture(enabled=False)
        self.assertFalse(f['database'].outbox)
        self.assertEqual(f['wake_calls'], 0)
        self.assertEqual(len(f['state']['intents']), 1)

    def test_original_freshness_and_fence_prevent_historical_replay(self):
        f = doge_fixture(enabled=False)
        msg, = self.project(f)
        expiry = contract.moment_ms(msg['expires_at'])
        for now in (expiry, expiry+86_400_000):
            self.assertEqual(self.project(f, now=now), [])
            with f['database'].connect() as conn:
                self.assertFalse(outbox.synchronize(conn, f['key'], f['state'], now))
        self.assertEqual(producer.approved_messages(f['state'], now_ms=f['now_ms'],
            fence_ms=contract.moment_ms(msg['approved_at'])+1), [])

    def test_source_partial_retires_unfilled_entry_without_rewriting_exit_contract(self):
        for target in (102,98):
            f = doge_fixture(target=target)
            original, = self.project(f)
            state = f['state']; p = state['active'][0]
            when = BASE+3*M
            bars = [bar(when,100,101,99.9,100.8)] if target==102 else [bar(when,100,100.1,99,99.2)]
            doge.reducer.advance(state,bars,when+M)
            self.assertTrue(state['active'][0]['partial_taken'])
            self.assertFalse(producer.source_open(state,p))
            msg, = self.project(f, now=when+M)
            self.assertEqual(msg['kind'], 'CANCEL')
            self.assertEqual(contract.plan_digest(msg), contract.plan_digest(original))
            state['intents'] = []  # Retained outbox cancellation must survive source pruning.
            with f['database'].connect() as conn:
                outbox.acknowledge(conn,[f['key']],original)
                self.assertTrue(outbox.synchronize(conn,f['key'],state,when+M))
                cancel, = outbox.read(conn,[f['key']])
                self.assertEqual(cancel['kind'],'CANCEL')
                self.assertEqual(cancel['policy'],original['policy'])
                outbox.acknowledge(conn,[f['key']],cancel)
                self.assertFalse(outbox.synchronize(conn,f['key'],state,when+M))

    def test_duplicate_concurrent_projection_and_sender_restart_do_not_repeat(self):
        f=doge_fixture(); db=f['database']; values=[]
        def synchronize(_):
            with db.connect() as conn:
                outbox.synchronize(conn,f['key'],f['state'],f['now_ms'])
        with ThreadPoolExecutor(2) as pool:list(pool.map(synchronize,range(4)))
        self.assertEqual(len(db.outbox),1)
        fence=datetime.fromtimestamp(BASE/1000,timezone.utc)
        env=approved_env(); cfg=config(mode=transport.APPROVED_MODE,fence=fence)
        def reader(scopes, *_args, **kwargs):
            return bridge.read_approved(scopes,fence,connect=db.connect,env=env,now=kwargs.get('now'))
        def acknowledge(scopes,value,**kwargs):
            return bridge.acknowledge_approved(scopes,value,env=env,connect=db.connect)
        with patch('alert_cards_forwarder._source_dsn',return_value='offline-only'):
            one=transport.Sender().tick({SCOPE},cfg,now_ms=f['now_ms'],reader=reader,
                post=lambda v,*_a,**_k:(values.append(v) or ack(v)),acknowledge=acknowledge)
            two=transport.Sender().tick({SCOPE},cfg,now_ms=f['now_ms'],reader=reader,
                post=lambda *_a,**_k:self.fail('duplicate send'),acknowledge=acknowledge)
        self.assertEqual(one['recorded'],1)
        self.assertEqual(two['attempted'],0)
        self.assertEqual(len(values),1)

    def test_contract_rejects_incomplete_or_changed_exit_terms_and_source(self):
        for side in ('LONG','SHORT'):
            good=doge_partial_alert(side=side)
            for key,value in [('partial_fraction','1'),('partial_take_price',good['entry']),
                              ('stop_policy','TRAIL'),('hold_until',good['approved_at'])]:
                changed=deepcopy(good);changed['policy'][key]=value
                with self.assertRaises(contract.ContractError):contract.validate(changed)
            changed=deepcopy(good);changed['proof']['source_config_sha256']='a'*64
            with self.assertRaises(contract.ContractError):contract.validate(changed)
            changed=deepcopy(good);changed['policy'].pop('partial_take_price')
            with self.assertRaises(contract.ContractError):contract.validate(changed)

    def test_source_fingerprint_preserved_and_strategy_edits_require_migration(self):
        self.assertEqual(doge.source_config_hash(), contract.DOGE_SOURCE_CONFIG_SHA256)
        with patch.object(doge.parent,'config_hash',return_value='a'*64):
            with self.assertRaisesRegex(ValueError,'explicit migration'):doge.source_config_hash()
        with patch.object(doge.Path,'read_bytes',return_value=b'changed reducer'):
            with self.assertRaisesRegex(ValueError,'explicit migration'):doge.source_config_hash()

    def test_source_hold_deadline_withdraws_remaining_entry_without_extending_clock(self):
        f=doge_fixture(); original,=self.project(f)
        p=f['state']['active'][0]; deadline=p['hold_until_ms']
        f['state']['bar_cursor_ms']=deadline-M
        doge.reducer.advance(f['state'],[bar(deadline,100,100.1,99.9,100)],deadline+M)
        self.assertEqual(f['state']['history'][-1]['status'],'TIME_EXIT_24H')
        cancel,=self.project(f,now=deadline+M)
        self.assertEqual(cancel['kind'],'CANCEL')
        self.assertEqual(cancel['policy']['hold_until'],original['policy']['hold_until'])
        self.assertEqual(contract.plan_digest(cancel),contract.plan_digest(original))

    def test_status_reports_separate_execution_bridge_truthfully(self):
        with patch.dict('os.environ',approved_env(),clear=True):
            status=doge.WORKER.status()
        self.assertFalse(status['notification_only'])
        self.assertFalse(status['live_order_execution'])  # Producer does not submit exchange orders.
        self.assertEqual(status['execution_forwarding'],'APPROVED_ALERTS_CONFIGURED')
        with patch.dict('os.environ',{},clear=True):
            self.assertTrue(doge.WORKER.status()['notification_only'])


@unittest.skipUnless(os.environ.get('TEST_DATABASE_URL'),'Explicit local/CI PostgreSQL required')
class DogeApprovedPostgreSQLTests(unittest.TestCase):
    setUpClass=classmethod(pg_support.ApprovedOutboxPostgreSQLTests.setUpClass.__func__)
    tearDownClass=classmethod(pg_support.ApprovedOutboxPostgreSQLTests.tearDownClass.__func__)

    def test_concurrent_source_commit_partial_cancel_and_tombstone_survive_restart(self):
        f=doge_fixture(enabled=False); seen=[]
        with self.connect() as conn:
            conn.execute('INSERT INTO bot_settings(key,value) VALUES(%s,%s)',
                         (f['key'],json.dumps(f['state'])))
        def after_commit(key):
            with self.connect() as conn:
                seen.extend(outbox.read(conn,[key]))
        with patch.object(store,'_connect',self.connect),patch.dict('os.environ',approved_env()), \
                patch('experimental_execution_forwarder.notify_source_commit',side_effect=after_commit):
            def sync(_):
                return store.transact(f['scope'],f['now_ms'],doge.SOURCE_CONFIG_SHA256,lambda state:None)
            with ThreadPoolExecutor(max_workers=4) as pool:list(pool.map(sync,range(4)))
            self.assertEqual(len(seen),1)
            original=seen[0]
            with self.connect() as conn:
                outbox.acknowledge(conn,[f['key']],original)
            when=BASE+3*M
            doge.StoreAPI().advance(f['scope'],[bar(when,100,101,99.9,100.8)],when+M,
                                    config_sha256=doge.SOURCE_CONFIG_SHA256)
        with self.connect() as conn:
            cancel,=outbox.read(conn,[f['key']])
            self.assertEqual(cancel['kind'],'CANCEL')
            self.assertEqual(contract.plan_digest(cancel),contract.plan_digest(original))
            outbox.acknowledge(conn,[f['key']],cancel)
        # A new connection plus a replay of the pre-partial source cannot revive it.
        with self.connect() as conn:
            self.assertFalse(outbox.synchronize(conn,f['key'],f['state'],f['now_ms']))
            row=conn.execute('SELECT canceled,payload_json FROM approved_alert_execution_outbox_mainnet').fetchone()
            self.assertTrue(row['canceled']);self.assertIsNone(row['payload_json'])


if __name__ == '__main__':unittest.main()
