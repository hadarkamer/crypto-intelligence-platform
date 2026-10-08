"""Independent cross-blocker acceptance, using the real Testnet transitions.

All exchange/network and wallet constructors are blocked. The Testnet store is
the existing transactional in-memory test port; requests never reach a venue.
These scenarios intentionally combine gates that separate unit suites exercise.
"""
from copy import deepcopy
from contextlib import contextmanager
from unittest.mock import patch, Mock
import time
import unittest

from . import experimental_execution_runtime as isolated
from . import experimental_live_runtime as live
from .experimental_live_dispatch import DefinitelyNotSubmitted, LiveDispatchError
from .test_approved_alert_lifecycle import alert, cancellation, CurrentOnlyExchange, BASE
from . import test_experimental_live_runtime as support


class BlockerAcceptanceTests(unittest.TestCase):
    # Reuse fixture construction, not its test methods: discovery counts only
    # this independent acceptance matrix once.
    make_worker = support.RuntimeContract.make_worker
    cycle = support.RuntimeContract.cycle
    restart = support.RuntimeContract.restart
    real_pg = False

    def setUp(self):
        support.RuntimeContract.setUp(self)
        self.venue = CurrentOnlyExchange(BASE + 61_000)
        self.make_worker(not_before=BASE - 60_000)

    def receive(self, *messages):
        self.worker.receive(messages)

    def entries(self):
        return [r for r in self.venue.requests if r['proposal']['operation'] == 'ENTRY']

    def never_sent(self, reason='DISPATCH_CONTEXT_CHANGED_RECONCILE_FIRST'):
        def fail(request, *, admission):
            claimed = self.store.claim_transport(request, 'c' * 32)
            raise DefinitelyNotSubmitted(claimed, LiveDispatchError(reason))
        return patch.object(self.port, 'send', side_effect=fail)

    def test_old_bad_candidate_does_not_starve_later_valid_sol_or_mutate_terms(self):
        old = alert(symbol='HYPE', approved_ms=BASE, cycle='stale-price-first')
        good = alert(symbol='SOL', approved_ms=BASE + 60_000, cycle='valid-sol')
        self.receive(old, good)
        lane = isolated._lane(self.store.load()['routes']['long_account'], 'HYPE')
        self.venue.mark[lane] = '90'
        result = self.cycle()
        self.assertEqual(result['operation'], 'ENTRY')
        value = self.store.load()
        self.assertEqual(self.entries()[0]['proposal']['symbol'], 'SOL')
        self.assertNotIn(old['occurrence_id'], value['trades'])
        self.assertEqual(value['sources'][good['occurrence_id']]['source'], good)
        self.assertEqual(value['entry_blocked'][old['occurrence_id']], 'TESTNET_MARK_OUTSIDE_ORIGINAL_EXITS')
        def decisions():
            return [e for e in self.store.load()['events']
                    if e.get('occurrence_id') == old['occurrence_id']
                    and e['kind'] == 'ENTRY_DEFERRED']
        initial = deepcopy(decisions())
        self.assertEqual(len(initial), 1)
        self.cycle()
        self.assertEqual(decisions(), initial, 'Repeated identical refusal must not flood the card')
        self.venue.mark[lane] = old['entry']
        self.assertEqual(self.cycle()['operation'], 'ENTRY')
        self.assertEqual(decisions(), initial, 'Resolved refusal remains in the durable card history')
        self.assertTrue(any(e['kind'] == 'ENTRY_ADMISSION_PASSED'
            and e.get('occurrence_id') == old['occurrence_id'] for e in self.store.load()['events']))

    def test_unclassified_admission_fault_remains_atomic_and_fails_closed(self):
        msg = alert(approved_ms=BASE + 60_000)
        self.receive(msg)
        prior = self.store.load()
        with patch.object(isolated.IsolatedExecutionRuntime, '_admit',
                side_effect=isolated.RuntimeError('UNKNOWN_JOURNAL_CORRUPTION')):
            with self.assertRaisesRegex(isolated.RuntimeError, 'UNKNOWN_JOURNAL_CORRUPTION'):
                self.cycle()
        self.assertEqual(self.store.load(), prior)
        self.assertFalse(self.venue.requests)

    def test_unknown_short_attempt_fences_short_but_allows_fresh_long_sol(self):
        short = alert(symbol='HYPE', side='SHORT', approved_ms=BASE + 60_000,
                      cycle='unknown-short')
        self.receive(short)
        with patch.object(self.port, 'send', side_effect=TimeoutError('UNCLASSIFIED_OUTCOME')):
            self.assertEqual(self.cycle()['status'], 'OUTCOME_UNKNOWN_RECONCILIATION_REQUIRED')
        peer_short = alert(symbol='SOL', side='SHORT', approved_ms=BASE + 60_000,
                           cycle='must-wait-short')
        long = alert(symbol='SOL', approved_ms=BASE + 60_000, cycle='independent-long')
        self.receive(peer_short, long)
        self.restart()
        result = self.cycle()
        self.assertEqual(result['operation'], 'ENTRY')
        self.assertEqual([(r['proposal']['role'], r['proposal']['symbol']) for r in self.entries()],
                         [('long_account', 'SOL')])
        self.assertNotIn(peer_short['occurrence_id'], self.store.load()['trades'])
        unknown = [r for r in self.store.load()['requests'].values()
                   if r['proposal']['card_id'] == short['occurrence_id']]
        self.assertEqual(len(unknown), 1)
        self.assertEqual(unknown[0]['phase'], 'OUTCOME_UNKNOWN')

    def test_unknown_same_account_attempt_prevents_another_symbol_entry(self):
        first = alert(symbol='HYPE', approved_ms=BASE + 60_000, cycle='unknown-long')
        self.receive(first)
        with patch.object(self.port, 'send', side_effect=TimeoutError('MAY_HAVE_SENT')):
            self.cycle()
        self.receive(alert(symbol='SOL', approved_ms=BASE + 60_000, cycle='same-account'))
        self.restart()
        self.cycle()
        self.assertFalse(self.entries())
        self.assertEqual(len(self.store.load()['requests']), 1)

    def test_certified_transient_unsent_retries_once_after_restart_with_new_identity(self):
        msg = alert(symbol='SOL', approved_ms=BASE + 60_000, cycle='retry-sol')
        self.receive(msg)
        with self.never_sent():
            self.assertEqual(self.cycle()['status'], 'DEFINITELY_NOT_SUBMITTED_REOBSERVE')
        first = next(iter(self.store.load()['requests'].values()))
        self.assertEqual(first['phase'], 'ABORTED_UNSENT')
        self.assertFalse(self.entries())
        self.restart()
        self.cycle()
        self.assertFalse(self.entries(), 'Backoff must prevent a tight retry loop')
        self.venue.t += 1_001
        self.assertEqual(self.cycle()['operation'], 'ENTRY')
        values = self.store.load()['requests']
        self.assertEqual(len(values), 2)
        self.assertEqual(values[first['request_id']], first, 'Attempt history is immutable')
        next_request = self.entries()[0]
        self.assertNotEqual(first['request_id'], next_request['request_id'])
        self.assertNotEqual(first['nonce'], next_request['nonce'])
        self.assertNotEqual(first['proposal']['action']['orders'][0]['c'],
                            next_request['proposal']['action']['orders'][0]['c'])
        for key in ('entry', 'stop', 'take_profit', 'expires_at'):
            self.assertEqual(self.store.load()['sources'][msg['occurrence_id']]['source'][key], msg[key])
        self.cycle()
        self.assertEqual(len(self.entries()), 1)

    def test_unsent_then_source_cancel_never_reenters(self):
        msg = alert(approved_ms=BASE + 60_000, cycle='retry-canceled')
        self.receive(msg)
        with self.never_sent():
            self.cycle()
        self.venue.t += 1_001
        self.receive(cancellation(msg, self.venue.t))
        self.restart()
        self.cycle()
        self.assertFalse(self.entries())
        self.assertEqual(len(self.store.load()['requests']), 1)

    def test_unsent_then_original_expiry_never_gets_a_fresh_window(self):
        msg = alert(approved_ms=BASE + 60_000, cycle='retry-expired')
        self.receive(msg)
        with self.never_sent():
            self.cycle()
        self.venue.t = BASE + 150_001
        self.restart()
        self.cycle()
        self.assertFalse(self.entries())
        self.assertEqual(len(self.store.load()['requests']), 1)

    def test_nontransient_certified_failure_remains_terminal(self):
        msg = alert(approved_ms=BASE + 60_000, cycle='signer-bad')
        self.receive(msg)
        with self.never_sent('ROLE_LOCAL_SIGNING_FAILED'):
            self.cycle()
        self.venue.t += 1_001
        self.restart()
        self.cycle()
        self.assertFalse(self.entries())
        self.assertEqual(len(self.store.load()['requests']), 1)

    def test_three_certified_unsent_attempts_are_bounded_and_never_hit_http(self):
        msg = alert(approved_ms=BASE + 60_000, cycle='retry-cap')
        self.receive(msg)
        with self.never_sent():
            for attempt in range(live.MAX_ENTRY_ATTEMPTS):
                self.assertEqual(self.cycle()['status'], 'DEFINITELY_NOT_SUBMITTED_REOBSERVE')
                self.venue.t += live.ENTRY_RETRY_DELAY_MS + 1
                self.restart()
        self.cycle()
        value = self.store.load()
        self.assertEqual(len(value['requests']), live.MAX_ENTRY_ATTEMPTS)
        self.assertEqual(value['trades'][msg['occurrence_id']]['phase'], 'CANCELED_WITHOUT_FILL')
        self.assertFalse(self.entries())
        self.assertEqual(len({r['nonce'] for r in value['requests'].values()}), live.MAX_ENTRY_ATTEMPTS)

    def test_unsent_status_without_positive_certificate_never_authorizes_retry(self):
        msg = alert(approved_ms=BASE + 60_000, cycle='missing-proof')
        self.receive(msg)
        with self.never_sent():
            self.cycle()
        def remove_certificate(value):
            next(iter(value['requests'].values())).pop('certified_unsent')
        self.store.mutate(remove_certificate)
        self.venue.t += live.ENTRY_RETRY_DELAY_MS + 1
        self.restart()
        self.cycle()
        self.assertFalse(self.entries())
        self.assertEqual(len(self.store.load()['requests']), 1)

    def test_partial_account_collection_can_enter_only_the_verified_account(self):
        long = alert(symbol='SOL', approved_ms=BASE + 60_000, cycle='verified-long')
        short = alert(symbol='HYPE', side='SHORT', approved_ms=BASE + 60_000,
                      cycle='unverified-short')
        self.receive(long, short)
        original = self.provider.collect
        def partial(value, **kwargs):
            context = original(value, **kwargs)
            good = value['routes']['long_account']
            bad = value['routes']['short_account']
            context['inventory_accounts'] = [good]
            context['account_inventory_at_ms'] = {good: self.venue.t}
            context['inventory_account_errors'] = {bad: 'ACCOUNT_READ_FAILED'}
            context['account_entry_blocked'] = {bad: 'ACCOUNT_READ_FAILED'}
            context['snapshots'] = [s for s in context['snapshots'] if s['account'] == good]
            return context
        with patch.object(self.provider, 'collect', side_effect=partial):
            result = self.cycle()
        self.assertEqual(result['operation'], 'ENTRY')
        self.assertEqual(self.entries()[0]['proposal']['role'], 'long_account')
        self.assertNotIn(short['occurrence_id'], self.store.load()['trades'])


class ConcretePortAcceptanceTests(unittest.TestCase):
    """Cross the actual provider, safety receipt and final signing boundary."""

    def fixture(self):
        from .test_experimental_live_safety import LiveSafetyTests
        fixture = LiveSafetyTests(methodName='runTest')
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        fixture.ready_feed()
        fixture.start_supervisor()
        return fixture

    def test_other_account_inventory_failure_does_not_abort_verified_long_before_transport(self):
        from .test_experimental_execution_runtime import T
        fixture = self.fixture()
        fx = fixture.fx
        msg = alert(symbol='SOL', approved_ms=T, cycle='raw-partial-account')
        fx.worker.receive([msg])
        own = fx.release['routes']['long_account']
        other = fx.release['routes']['short_account']
        fx.oracle.mark[isolated._lane(own, 'SOL')] = msg['entry']
        original = fx.raw.read
        def partial(kind, account=None, **kwargs):
            if (account or kwargs.get('user')) == other and kind == 'clearinghouseState':
                raise ValueError('ACCOUNT_INVENTORY_UNAVAILABLE')
            return original(kind, account, **kwargs)
        with patch.object(fx.raw, 'read', side_effect=partial):
            result = fx.worker.run_once(entries_enabled=True)
        self.assertEqual(result.get('operation'), 'ENTRY', result)
        self.assertEqual(len(fx.http), 1)
        trade = fx.store.load()['trades'][msg['occurrence_id']]
        self.assertEqual(trade['account'], own)
        receipt = fixture.capability._receipt(fx.store.load())
        self.assertTrue(fixture.capability._continuous(receipt, own))
        self.assertFalse(fixture.capability._continuous(receipt, other))

    def test_real_context_failure_preserves_retry_reason_then_replays_fresh_gate(self):
        from .experimental_live_provider import ProviderError
        from .test_experimental_execution_runtime import T
        fixture = self.fixture()
        fx = fixture.fx
        msg = alert(symbol='SOL', approved_ms=T, cycle='raw-unsent-retry')
        fx.worker.receive([msg])
        own = fx.release['routes']['long_account']
        fx.oracle.mark[isolated._lane(own, 'SOL')] = msg['entry']
        with patch.object(fx.port, 'context_loader',
                side_effect=ProviderError('FRESH_COLLECTOR_CHECKPOINT_REQUIRED_BEFORE_DISPATCH')):
            result = fx.worker.run_once(entries_enabled=True)
        self.assertEqual(result['status'], 'DEFINITELY_NOT_SUBMITTED_REOBSERVE')
        self.assertFalse(fx.http)
        first = next(iter(fx.store.load()['requests'].values()))
        self.assertEqual(first['unsent_reason'], 'FRESH_COLLECTOR_CHECKPOINT_REQUIRED_BEFORE_DISPATCH')
        fx.oracle.t += live.ENTRY_RETRY_DELAY_MS + 1
        result = fx.worker.run_once(entries_enabled=True)
        self.assertEqual(result.get('operation'), 'ENTRY', result)
        self.assertEqual(len(fx.http), 1)
        self.assertEqual(fx.store.load()['requests'][first['request_id']], first)


class ColdLegacyAcceptanceTests(unittest.TestCase):
    def setUp(self):
        from .test_experimental_live_provider import ProviderTests
        from .test_card_lifecycle import binding, closed
        from . import experimental_live_startup as startup
        from . import card_lifecycle as life
        from .experimental_plan_store import reduce_source
        from .test_experimental_execution_runtime import T
        self.fx = ProviderTests(methodName='runTest')
        self.fx.setUp()
        self.addCleanup(self.fx.doCleanups)
        owner = binding()  # Proven-final DOGE, several weeks before this fixture's clock.
        self.row = dict(account=owner['account'], symbol='DOGE', bucket=life.digest('cold-doge'),
            revision=1, pending=None, emergency=None, bindings=[owner],
            evidence=dict(bindings=[deepcopy(owner)], snapshot=closed(owner)))
        self.fx.legacy.rows = [self.row]
        self.routes = self.fx.state['routes']
        rows = {a: self.fx.legacy.for_account(a) for a in self.routes.values()}
        self.fx.state[startup.RECEIPT] = dict(schema=startup.RECEIPT, release_id='a' * 64,
            routes=deepcopy(self.routes), not_before_ms=self.fx.state['not_before_ms'],
            legacy_fingerprint=startup._legacy_fingerprint_rows(rows), flat_verified_at_ms=T)
        self.msg = alert(symbol='SOL', approved_ms=T, cycle='cold-doge-fresh-sol')
        self.fx.state['sources'][self.msg['occurrence_id']] = reduce_source(None, self.msg,
            now=isolated.contract.iso_ms(self.fx.exchange.t),
            not_before=isolated.contract.iso_ms(self.fx.state['not_before_ms']), domain='testnet')[0]
        self.fx.exchange.mark[isolated._lane(owner['account'], 'SOL')] = self.msg['entry']

    def test_final_doge_gap_does_not_read_old_history_or_block_sol(self):
        original = deepcopy(self.row)
        context = self.fx.provider.collect(self.fx.state, entries_enabled=True)
        # This read-only provider fixture has no running safety capability.
        # SOL reaches that independent gate; DOGE cannot replace it with an
        # irrelevant historical/listing refusal.
        self.assertEqual(context['entry_blocked'][self.msg['occurrence_id']],
                         'VERIFIED_SUPERVISOR_AND_FEED_CAPABILITY_REQUIRED')
        self.assertNotIn(self.routes['long_account'], context['account_entry_blocked'])
        self.assertFalse(context['blocked_lanes'])
        self.assertFalse(any(call[0] in ('userFillsByTime', 'orderStatus', 'lookup')
                             for call in self.fx.raw.calls))
        self.assertEqual(self.row, original, 'Historical proof and its timestamp remain untouched')

    def test_reappearing_final_doge_order_fences_account_despite_cold_history(self):
        oid = int(self.row['bindings'][0]['orders']['STOP'][0])
        self.fx.raw.extra_orders = [dict(coin='DOGE', oid=oid)]
        context = self.fx.provider.collect(self.fx.state, entries_enabled=True)
        self.assertEqual(context['account_entry_blocked'][self.routes['long_account']],
                         'FINAL_ACCOUNT_ORDER_REAPPEARED_RECONCILE_FIRST')
        self.assertNotIn(self.msg['occurrence_id'], context['entry_accounts'])

    def test_changed_pending_invalidates_retirement_receipt_and_fences_account(self):
        self.row['pending'] = 'f' * 64
        context = self.fx.provider.collect(self.fx.state, entries_enabled=True)
        self.assertIn(self.routes['long_account'], context['account_entry_blocked'])
        self.assertNotIn(self.msg['occurrence_id'], context['entry_accounts'])

    def test_unowned_current_position_fences_account_with_empty_historical_work(self):
        self.fx.raw.extra_positions = [dict(position=dict(coin='DOGE', szi='1'))]
        context = self.fx.provider.collect(self.fx.state, entries_enabled=True)
        self.assertEqual(context['account_entry_blocked'][self.routes['long_account']],
                         'UNOWNED_ACCOUNT_POSITION_NO_NEW_ENTRY')
        self.assertNotIn(self.msg['occurrence_id'], context['entry_accounts'])


class PaginatedBudgetAcceptanceTests(unittest.TestCase):
    """Use the real batch ticket contract; only its SQL owner is replaced."""
    def setUp(self):
        from . import card_sync_evidence as sync, request_budget as budget
        self.sync, self.budget = sync, budget
        self.account = '0x' + '1' * 40
        test = self
        class Reader:
            def __init__(reader):
                reader.batch = None
                reader.calls = []
                reader.fail_right = False
            def read(reader, kind, account, *, start=None, end=None, oid=None):
                body = dict(type=kind, user=account)
                if kind == 'userFillsByTime':
                    body.update(startTime=start, endTime=end, aggregateByTime=False)
                else:
                    body['oid'] = int(oid)
                reader.batch.acquire('/info', body, priority='protection')
                reader.calls.append((kind, start, end, oid))
                if reader.fail_right and start == 16:
                    raise sync.SyncError('PUBLIC_READ_UNAVAILABLE')
                return [{'time': 15}] * 2000 if start == 1 and end == 30 else []
            def reserve_history_split(reader, account, start, middle, end):
                reader.batch.reserve_extra([
                    dict(type='userFillsByTime', user=account, startTime=a, endTime=b,
                         aggregateByTime=False) for a, b in ((start, middle), (middle+1, end))])
        self.reader = Reader()
        self.cache = sync.ObservationReadCache(self.reader)
        self.parent = dict(type='userFillsByTime', user=self.account, startTime=1,
                           endTime=30, aggregateByTime=False)

    @contextmanager
    def funded(self, bodies):
        owner = Mock()
        entries = [(self.budget._observation_key(body), str(n),
                    self.budget.request_weight('/info', body)) for n, body in enumerate(bodies)]
        self.reader.batch = self.budget.ObservationBatch(owner, entries, 'protection',
                                                       time.monotonic_ns() + 10**10)
        try:
            yield owner
        finally:
            self.reader.batch.close()
            self.reader.batch = None

    def test_cached_split_tree_never_reserves_children_against_unclaimed_parent(self):
        reader = self.cache.pass_reader(0)
        with self.funded([self.parent]):
            self.assertEqual(self.sync.history(reader, self.account, 1, 30), [])
        calls = len(self.reader.calls)
        unrelated = dict(type='orderStatus', user=self.account, oid=99)
        plan = self.cache.missing_plan([self.parent, unrelated])
        self.assertEqual(plan, [unrelated])
        with self.funded(plan) as owner:
            self.assertEqual(self.sync.history(reader, self.account, 1, 30), [])
            owner._fund_observation.assert_not_called()
        self.assertEqual(len(self.reader.calls), calls)

    def test_failed_child_refunds_nothing_uncertain_and_redeclares_parent_before_retry(self):
        reader = self.cache.pass_reader(0)
        self.reader.fail_right = True
        with self.funded([self.parent]) as failed_owner:
            with self.assertRaisesRegex(self.sync.SyncError, 'PUBLIC_READ_UNAVAILABLE'):
                self.sync.history(reader, self.account, 1, 30)
        failed_owner._release_unclaimed_observation.assert_not_called()
        self.reader.fail_right = False
        plan = self.cache.missing_plan([self.parent])
        self.assertEqual(plan, [self.parent], 'Incomplete cached parent must regain funding authority')
        with self.funded(plan) as owner:
            self.assertEqual(self.sync.history(reader, self.account, 1, 30), [])
            owner._fund_observation.assert_called_once()
        self.assertEqual(sum(start == 1 and end == 30 for _, start, end, _ in self.reader.calls), 2)
        self.assertEqual(sum(start == 1 and end == 15 for _, start, end, _ in self.reader.calls), 1)


if __name__ == '__main__':
    unittest.main()
