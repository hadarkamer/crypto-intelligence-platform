"""Real file transactions for LOCAL rehearsal; no PostgreSQL or venue claims."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import experimental_execution_contract as contract
from experimental_execution_fixtures import r2732_message, maxpain_message
from . import card_lifecycle as life, experimental_plan_store as plans
from . import experimental_local_controller as local
from .experimental_plan_sqlite_test_support import open_software_store, SQLiteTestError
from .test_r2732_entry import fixture, T, NOW, ROUTES, META
from .test_r2732_stop_dispatch import order, fill, public_result


class LocalControllerTests(unittest.TestCase):
    def setUp(self):
        for target in ('http.client.HTTPSConnection', 'socket.create_connection',
                       'hyperliquid_testnet_executor._wallet', 'hyperliquid_testnet_executor._signed_body'):
            guard = patch(target, side_effect=AssertionError('NO_NETWORK_OR_SIGNER'))
            guard.start(); self.addCleanup(guard.stop)
        self.directory = tempfile.TemporaryDirectory(); self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name)/'controller.sqlite'
        self.store = open_software_store(self.path, create=True)
        _, self.message, self.market, self.source_market, self.ownership = fixture()
        self.cid = self.message['occurrence_id']
        self.store.ingest(self.message, now=contract.iso_ms(NOW), not_before=contract.iso_ms(T-60000))
        self.controller = self.new_controller(self.store)
        self.initial = self.controller.initialize(now_ms=NOW)

    def new_controller(self, store=None):
        return local.LocalController(store or open_software_store(self.path), self.cid,
                                     ROUTES, not_before_ms=T-60000)

    def prepare(self, controller=None, *, now_ms=NOW):
        c = controller or self.controller
        value = c.load()
        return c.prepare_entry(value['source_revision'], META, self.market, self.source_market,
                               self.ownership, now_ms=now_ms)

    def begin(self, prepared=None, controller=None, *, now_ms=NOW):
        c = controller or self.controller
        p = self.prepare(c, now_ms=now_ms) if prepared is None else prepared
        return c.begin_entry(p['result']['proposal_id'], p['source_revision'], META,
                             self.market, self.source_market, self.ownership, now_ms=now_ms)

    def cancel(self, at=NOW+1):
        value = r2732_message(entry=2.3, decision_ms=T-60000, as_of_ms=at,
            kind='CANCEL', cancel_reason='SOURCE_EXIT')
        self.store.ingest(value, now=contract.iso_ms(at), not_before=contract.iso_ms(T-60000))
        return value

    def attach(self, begun=None, *, change_snapshot=None, metadata=META):
        begun = self.begin() if begun is None else begun
        proposal = begun['result']['unsigned_proposal']
        q = proposal['quantity']; p = proposal['execution']['prepared']['execution']
        owner = dict(card_id=self.cid, card_digest=life.digest(proposal['source']),
            account=ROUTES['short_account']['account'], role='short_account', symbol='XRP', side='SHORT',
            planned_quantity=q, prices={k: p[k] for k in ('entry','stop','take_profit')},
            orders=dict(ENTRY=['10'], STOP=['11'], TAKE_PROFIT=['12']), environment='testnet')
        snapshot = dict(environment='testnet', account=owner['account'], symbol='XRP', at_ms=T+60000,
            history_complete=True, orders_complete=True, position_quantity='-'+q,
            fills=[fill(owner,q,at=NOW+1)], open_orders=[order(owner,'STOP',q),order(owner,'TAKE_PROFIT',q)],
            terminal_orders=[dict(account=owner['account'],symbol='XRP',oid='10',state='FILLED',
                                  filled_quantity=q,at_ms=NOW+1)])
        if change_snapshot is not None:
            change_snapshot(snapshot)
        attached = self.controller.attach_simulated_protection(self.controller.load()['source_revision'],
            owner, snapshot, metadata, now_ms=T+60000)
        return attached

    def ready_stop(self, attached=None):
        attached = self.attach() if attached is None else attached
        bar = dict(open_at_ms=T,open='2.3',high='2.3',low='2.277',close='2.28')
        advanced = self.controller.advance_stop(attached['source_revision'],[bar],now_ms=T+60000)
        return self.controller.reserve_stop(advanced['source_revision'],META,
                    dict(mark_price='2.28',at_ms=T+60000),now_ms=T+60000)

    def test_initialization_is_explicit_idempotent_and_never_resets_attempt(self):
        self.assertFalse(self.initial['dispatch_enabled'])
        self.assertFalse(self.initial['global_account_dispatch_connected'])
        prepared = self.prepare(); begun = self.begin(prepared)
        fresh = self.new_controller()
        restarted = fresh.initialize(now_ms=NOW)
        self.assertEqual(restarted['state'], begun['state'])
        self.assertIsNotNone(restarted['state']['entry']['records'][self.cid]['request'])
        with self.assertRaises(life.LifecycleError): self.prepare(fresh)

    def test_preparation_and_attempt_are_committed_before_unsigned_result(self):
        p = self.prepare()
        self.assertEqual(self.store.load(self.cid)['strategy']['prepared']['proposal_id'],p['result']['proposal_id'])
        begun = self.begin(p)
        record = self.store.load(self.cid)['strategy']['entry']['records'][self.cid]['request']
        self.assertEqual(record['request_id'],begun['result']['unsigned_proposal']['proposal_id'])
        self.assertEqual(record['phase'],'OUTCOME_UNKNOWN')
        self.assertFalse(begun['result']['live_dispatch_authorized'])
        self.assertNotIn('action',begun['result']['unsigned_proposal'])
        self.assertEqual(begun['order_requests_sent'],0)

    def test_event_failure_rolls_back_attempt_and_returns_no_unsigned_proposal(self):
        p = self.prepare(); before = self.store.load(self.cid)
        self.store.journal.fail_event_once = True
        with self.assertRaisesRegex(SQLiteTestError,'EVENT_WRITE_FAILURE'): self.begin(p)
        self.assertEqual(self.store.load(self.cid),before)
        self.assertIsNone(self.store.load(self.cid)['strategy']['entry']['records'][self.cid]['request'])

    def test_lost_commit_ack_keeps_attempt_consumed_after_reopen(self):
        p = self.prepare(); original = self.store.change_strategy
        def lose_ack(*args,**kwargs):
            self.store.journal.lose_commit_ack_once = True
            return original(*args,**kwargs)
        with patch.object(self.store,'change_strategy',side_effect=lose_ack):
            with self.assertRaisesRegex(SQLiteTestError,'COMMIT_ACK_UNKNOWN'): self.begin(p)
        fresh = self.new_controller()
        state = fresh.load()
        self.assertIsNotNone(state['state']['entry']['records'][self.cid]['request'])
        with self.assertRaises((life.LifecycleError,local.LocalControllerError)): self.begin(p,fresh)
        with self.assertRaises(life.LifecycleError): self.prepare(fresh)

    def test_two_controllers_only_one_same_occurrence_attempt_commits(self):
        prepared = self.prepare()
        def consume(_):
            try:
                return self.begin(prepared,self.new_controller())['result']['live_dispatch_authorized']
            except (local.LocalControllerError,plans.PlanStoreError,life.LifecycleError):
                return 'BLOCKED'
        with ThreadPoolExecutor(max_workers=2) as pool:
            values = list(pool.map(consume,range(2)))
        self.assertEqual(values.count(False),1)
        self.assertEqual(values.count('BLOCKED'),1)

    def test_cancellation_before_begin_blocks_even_with_fresh_revision(self):
        p = self.prepare(); self.cancel()
        current = self.controller.load()
        with self.assertRaises(life.LifecycleError):
            self.controller.begin_entry(p['result']['proposal_id'],current['source_revision'],META,
                self.market,self.source_market,self.ownership,now_ms=NOW+1)
        self.assertIsNone(self.store.load(self.cid)['strategy']['entry']['records'][self.cid]['request'])
        with self.assertRaises(life.LifecycleError): self.prepare(now_ms=NOW+1)

    def test_cancellation_between_read_and_commit_wins_revision_cas(self):
        prepared = self.prepare(); original = self.store.change_strategy
        def race(*args,**kwargs):
            self.cancel()
            return original(*args,**kwargs)
        with patch.object(self.store,'change_strategy',side_effect=race):
            with self.assertRaisesRegex(plans.PlanStoreError,'CONCURRENT_RELOAD'):
                self.begin(prepared,now_ms=NOW+1)
        value = self.store.load(self.cid)
        self.assertEqual(value['entry_permission'],'RETIRED')
        self.assertIsNone(value['strategy']['entry']['records'][self.cid]['request'])

    def test_waiting_label_never_extends_source_deadline(self):
        self.assertEqual(self.store.load(self.cid)['entry_permission'],'WAITING')
        future = T+90000
        market = {**self.market,'at_ms':future}; source = {**self.source_market,'at_ms':future}
        own = {**self.ownership,'at_ms':future}
        with self.assertRaisesRegex(life.LifecycleError,'EXPIRED'):
            self.controller.prepare_entry(self.controller.load()['source_revision'],META,market,source,own,now_ms=future)

    def test_wrong_account_source_or_occurrence_in_fresh_saved_strategy_is_rejected(self):
        for field in ('account','occurrence_id','plan_digest','routes_digest'):
            with self.subTest(field=field):
                store = open_software_store(Path(self.directory.name)/(field+'.sqlite'),create=True)
                store.ingest(self.message,now=contract.iso_ms(NOW),not_before=contract.iso_ms(T-60000))
                c = self.new_controller(store); state = c.initialize(now_ms=NOW)
                bad = deepcopy(state['state'])
                bad[field] = '0x'+'f'*40 if field=='account' else 'f'*64
                store.change_strategy(self.cid,state['source_revision'],bad,now=contract.iso_ms(NOW))
                with self.assertRaises(local.LocalControllerError): c.load()

    def test_foreign_stop_binding_cannot_attach_to_consumed_entry(self):
        begun = self.begin(); proposal = begun['result']['unsigned_proposal']
        from .test_r2732_stop_dispatch import binding, initial
        wrong = binding(proposal['source'])
        for field,value in (('card_id','f'*64),('account','0x'+'f'*40),('planned_quantity','1')):
            with self.subTest(field=field):
                candidate = deepcopy(wrong); candidate[field] = value
                with self.assertRaises((life.LifecycleError,local.LocalControllerError)):
                    self.controller.attach_simulated_protection(begun['source_revision'],candidate,
                        initial()['snapshot'],META,now_ms=T+60000)

    def test_source_cancellation_preserves_owned_stop_and_allows_protective_amendment(self):
        attached = self.attach(); original = deepcopy(attached['state']['stop']['original_binding'])
        self.cancel(T+60000)
        refreshed = self.controller.load()
        self.assertEqual(refreshed['state']['entry']['records'][self.cid]['source_record']['entry_permission'],'RETIRED')
        reserved = self.ready_stop(refreshed)
        rid = reserved['result']['request_id']
        begun = self.controller.begin_stop(rid,reserved['source_revision'],META,
            dict(mark_price='2.28',at_ms=T+60000),now_ms=T+60000)
        self.assertEqual(begun['result']['unsigned_action']['type'],'batchModify')
        self.assertEqual(begun['state']['stop']['original_binding'],original)
        self.assertFalse(begun['production_ready'])
        self.assertEqual(begun['state']['stop']['requests'][-1]['phase'],'ATTEMPTED')

    def test_simulated_handoff_cannot_reuse_fills_or_terminal_orders_before_attempt(self):
        begun = self.begin()
        for collection in ('fills','terminal_orders'):
            for when in (NOW-1,NOW):
                with self.subTest(collection=collection,when=when):
                    def earlier(snapshot): snapshot[collection][0]['at_ms'] = when
                    with self.assertRaisesRegex(local.LocalControllerError,'NOT_AFTER_ATTEMPT'):
                        self.attach(begun,change_snapshot=earlier)
                    self.assertIsNone(self.controller.load()['state']['stop'])

    def test_simulated_handoff_metadata_cannot_change_consumed_entry_market_or_precision(self):
        begun = self.begin()
        for metadata in ({'universe':[{'name':'BTC','szDecimals':1},{'name':'XRP','szDecimals':1}]},
                         {'universe':[{'name':'XRP','szDecimals':2}]}):
            with self.subTest(metadata=metadata):
                with self.assertRaisesRegex(local.LocalControllerError,'STOP_NOT_BOUND_TO_CONSUMED_ENTRY'):
                    self.attach(begun,metadata=metadata)
                self.assertIsNone(self.controller.load()['state']['stop'])

    def test_stop_lost_reply_restart_reconciles_without_second_attempt(self):
        reserved = self.ready_stop(); rid = reserved['result']['request_id']
        begun = self.controller.begin_stop(rid,reserved['source_revision'],META,
            dict(mark_price='2.28',at_ms=T+60000),now_ms=T+60000)
        unknown = self.controller.stop_reply(rid,begun['source_revision'],None,now_ms=T+60000)
        reopened = self.new_controller()
        with self.assertRaises(life.LifecycleError):
            reopened.begin_stop(rid,unknown['source_revision'],META,
                dict(mark_price='2.28',at_ms=T+60000),now_ms=T+60000)
        snapshot,lookup = public_result(unknown['state']['stop'],at=T+60002)
        observed = reopened.observe_stop(unknown['source_revision'],snapshot,lookup=lookup,now_ms=T+60002)
        self.assertIsNone(observed['state']['stop']['pending'])
        self.assertEqual(observed['state']['stop']['requests'][-1]['phase'],'OBSERVED')
        self.assertFalse(local.stops.review(observed['state']['stop'],now_ms=T+60002)['needs_review'])

    def test_stop_begin_write_failure_does_not_consume_unsigned_action(self):
        reserved = self.ready_stop(); before = self.store.load(self.cid)
        self.store.journal.fail_event_once = True
        with self.assertRaises(SQLiteTestError):
            self.controller.begin_stop(reserved['result']['request_id'],reserved['source_revision'],META,
                dict(mark_price='2.28',at_ms=T+60000),now_ms=T+60000)
        self.assertEqual(self.store.load(self.cid),before)

    def test_stop_begin_lost_commit_ack_consumes_attempt_without_returning_wire(self):
        reserved = self.ready_stop(); rid = reserved['result']['request_id']
        original = self.store.change_strategy
        def lose_ack(*args,**kwargs):
            self.store.journal.lose_commit_ack_once = True
            return original(*args,**kwargs)
        with patch.object(self.store,'change_strategy',side_effect=lose_ack):
            with self.assertRaisesRegex(SQLiteTestError,'COMMIT_ACK_UNKNOWN'):
                self.controller.begin_stop(rid,reserved['source_revision'],META,
                    dict(mark_price='2.28',at_ms=T+60000),now_ms=T+60000)
        fresh = self.new_controller(); state = fresh.load()
        self.assertEqual(state['state']['stop']['requests'][-1]['phase'],'ATTEMPTED')
        with self.assertRaisesRegex(life.LifecycleError,'ALREADY_CONSUMED'):
            fresh.begin_stop(rid,state['source_revision'],META,
                dict(mark_price='2.28',at_ms=T+60000),now_ms=T+60000)

    def test_software_only_and_maxpain_are_hard_boundaries(self):
        self.store.domain = 'testnet'
        with self.assertRaisesRegex(local.LocalControllerError,'SOFTWARE_JOURNAL'):
            self.new_controller(self.store)
        self.store.domain = 'software'
        maxpain = maxpain_message(created_ms=T+1000)
        self.store.ingest(maxpain,now=contract.iso_ms(T+1000),not_before=contract.iso_ms(T-60000))
        c = local.LocalController(self.store,maxpain['occurrence_id'],ROUTES,not_before_ms=T-60000)
        with self.assertRaisesRegex(local.LocalControllerError,'R2732_SOURCE'): c.initialize(now_ms=NOW)
