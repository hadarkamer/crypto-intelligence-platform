"""Collection-local admission reuse; every transport and signer is isolated."""
from collections import Counter
from copy import deepcopy
import threading
import unittest
from unittest.mock import patch

import approved_alert_contract as contract
from approved_alert_fixtures import maxpain_alert
from . import experimental_execution_runtime as runtime
from .experimental_live_provider import ProviderError
from .experimental_plan_store import reduce_source
from . import test_experimental_live_provider as provider_fixtures
from .test_experimental_execution_runtime import T


class EntryReadReuseTests(unittest.TestCase):
    setUp = provider_fixtures.ProviderTests.setUp

    def candidate(self, *, symbol='HYPE', side='LONG', cycle='first'):
        message = maxpain_alert(approved_ms=T)
        message.update(symbol=symbol, side=side,
            rule_id=next(k for k,v in contract.SPECS.items() if v[0] == symbol))
        if side == 'SHORT':
            message['stop'], message['take_profit'] = message['take_profit'], message['stop']
        message['policy']['source_price'] = f'HYPERLIQUID_{symbol}_PERPETUAL_TRADE_1M'
        message['proof']['cycle_id'] = cycle
        message['occurrence_id'] = contract.occurrence_id(message)
        message = contract.validate(message)
        record, _ = reduce_source(None, message, now=contract.iso_ms(self.exchange.t),
            not_before=contract.iso_ms(self.state['not_before_ms']), domain='testnet')
        self.state['sources'][message['occurrence_id']] = record
        account = self.state['routes']['long_account' if side == 'LONG' else 'short_account']
        self.exchange.mark[runtime._lane(account, symbol)] = message['entry']
        self.provider.safety = object()
        return message['occurrence_id']

    def collect(self):
        return self.provider.collect(self.state, entries_enabled=True)

    def test_two_candidates_reuse_reads_without_changing_capacity_or_inventory(self):
        first = self.candidate()
        second = self.candidate(symbol='SOL', cycle='second')
        original = self.provider._capacity
        def separate(*args, **kwargs):
            kwargs['entry_reads'] = {}
            return original(*args, **kwargs)
        with patch.object(self.provider, '_capacity', side_effect=separate):
            before = self.collect()
        baseline = Counter(call[0] for call in self.raw.calls)
        self.raw.calls.clear()
        after = self.collect()
        counts = Counter(call[0] for call in self.raw.calls)
        self.assertEqual(after, before)
        self.assertEqual(set(after['capacity']), {first, second})
        self.assertEqual(sum(baseline.values()), 25)
        self.assertEqual(sum(counts.values()), 21)
        self.assertEqual(counts['frontendOpenOrders'], 4)
        self.assertEqual(counts['clearinghouseState'], 5)  # Four inventory + one balance.
        self.assertEqual(counts['userAbstraction'], baseline['userAbstraction'])
        self.assertEqual(counts['activeAssetData'], 2)
        self.assertEqual(counts['userRateLimit'], 1)

    def test_same_asset_raw_capacity_reused_but_plans_and_returns_independent(self):
        first = self.candidate()
        second = self.candidate(cycle='second')
        after = self.collect()
        self.assertEqual(len(self.raw.calls), 20)
        self.assertEqual(sum(c[0] == 'activeAssetData' for c in self.raw.calls), 1)
        after['capacity'][first]['active']['availableToTrade'][0] = '0'
        self.assertEqual(after['capacity'][second]['active']['availableToTrade'][0], '50000')

    def test_distinct_accounts_do_not_share_capacity_identity_or_allowance(self):
        self.candidate()
        self.candidate(side='SHORT', cycle='second')
        self.assertEqual(self.collect()['entry_blocked'], {})
        counts = Counter(call[0] for call in self.raw.calls)
        self.assertEqual(counts['userRole'], 4)
        self.assertEqual(counts['userRateLimit'], 2)
        self.assertEqual(counts['activeAssetData'], 2)

    def test_next_collection_rereads_even_unchanged_revision_and_sees_changed_data(self):
        first = self.candidate()
        self.candidate(cycle='second')
        self.collect()
        self.raw.calls.clear()
        original = self.raw.read
        def changed(kind, *args, **kwargs):
            result = original(kind, *args, **kwargs)
            if kind == 'activeAssetData':
                result['availableToTrade'] = ['40000', '40000']
            return result
        with patch.object(self.raw, 'read', side_effect=changed):
            result = self.collect()
        self.assertEqual(result['capacity'][first]['available'], '40000')
        self.assertEqual(len(self.raw.calls), 20)
        self.state['revision'] += 1
        self.raw.extra_orders = [dict(coin='DOGE', oid=999)]
        changed = self.collect()
        self.assertEqual(changed['basis_revision'], self.state['revision'])
        self.assertIn(first, changed['entry_blocked'])
        self.assertNotIn(first, changed['capacity'])

    def test_failure_is_not_cached_and_later_card_and_cycle_recover(self):
        first = self.candidate()
        second = self.candidate(cycle='second')
        original = self.raw.read
        attempts = []
        def fail_once(kind, *args, **kwargs):
            if kind == 'activeAssetData':
                attempts.append(kind)
                if len(attempts) == 1:
                    raise ValueError('ISOLATED_CAPACITY_READ_FAILED')
            return original(kind, *args, **kwargs)
        with patch.object(self.raw, 'read', side_effect=fail_once):
            result = self.collect()
        self.assertIn(first, result['entry_blocked'])
        self.assertIn(second, result['capacity'])
        self.assertEqual(len(attempts), 2)
        recovered = self.collect()
        self.assertEqual(recovered['entry_blocked'], {})
        self.assertEqual(len(recovered['capacity']), 2)

    def test_cached_capacity_preserves_earliest_read_time(self):
        first = self.candidate()
        second = self.candidate(cycle='second')
        observed = []
        def tick(kind):
            observed.append((kind, self.exchange.t))
            self.exchange.t += 10
        self.raw.mutate = tick
        result = self.collect()
        first_at = result['capacity'][first]['at_ms']
        second_at = result['capacity'][second]['at_ms']
        self.assertLess(second_at, self.exchange.t)
        self.assertEqual(second_at, next(at for kind,at in observed if kind == 'userRole'))
        self.assertLess(first_at, self.exchange.t)

    def test_malformed_raw_response_does_not_poison_later_card_or_other_account(self):
        first_long = self.candidate()
        failed_short = self.candidate(side='SHORT', cycle='short-first')
        second_long = self.candidate(cycle='long-second')
        recovered_short = self.candidate(side='SHORT', cycle='short-second')
        short_account = self.state['routes']['short_account']
        long_account = self.state['routes']['long_account']
        original = self.raw.read
        short_reads = []
        def malformed_once(kind, *args, **kwargs):
            value = original(kind, *args, **kwargs)
            if kind == 'activeAssetData' and kwargs['user'] == short_account:
                short_reads.append(kind)
                if len(short_reads) == 1:
                    value['coin'] = 'UNEXPECTED'
            return value
        with patch.object(self.raw, 'read', side_effect=malformed_once):
            result = self.collect()
        self.assertEqual(set(result['entry_blocked']), {failed_short})
        self.assertEqual(set(result['capacity']), {first_long, second_long, recovered_short})
        counts = Counter(c[1] for c in self.raw.calls if c[0] == 'activeAssetData')
        self.assertEqual(counts[long_account], 1)
        self.assertEqual(counts[short_account], 2)

    def test_slow_collection_is_still_expired_and_next_pass_has_no_stale_cache(self):
        self.candidate()
        self.candidate(cycle='second')
        modes = []
        def expire(kind):
            if kind == 'userAbstraction':
                modes.append(kind)
                if len(modes) == 4:
                    self.exchange.t += 15001
        self.raw.mutate = expire
        expired=self.collect()
        self.assertEqual(expired['inventory_accounts'],[])
        self.assertEqual(set(expired['account_entry_blocked'].values()),{'ACCOUNT_COLLECTION_EXPIRED'})
        self.assertEqual(expired['snapshots'],[])
        self.raw.mutate = None
        self.raw.calls.clear()
        result = self.collect()
        self.assertEqual(len(result['capacity']), 2)
        self.assertEqual(len(self.raw.calls), 20)

    def test_account_mode_changes_do_not_reuse_prior_mode_capacity(self):
        self.candidate()
        self.candidate(cycle='second')
        original = self.raw.read
        modes = []
        def mode_change(kind, *args, **kwargs):
            result = original(kind, *args, **kwargs)
            if kind == 'userAbstraction':
                modes.append(kind)
                return 'disabled' if len(modes) <= 3 else 'unifiedAccount'
            if kind == 'spotClearinghouseState':
                return dict(balances=[dict(coin='USDC', token=0, total='40000', hold='0')])
            return result
        with patch.object(self.raw, 'read', side_effect=mode_change):
            result = self.collect()
        self.assertEqual(result['entry_blocked'], {})
        self.assertEqual(sum(c[0] == 'activeAssetData' for c in self.raw.calls), 2)
        self.assertEqual(sum(c[0] == 'userRateLimit' for c in self.raw.calls), 2)

    def test_mode_returning_to_original_discards_its_old_samples(self):
        self.candidate()
        self.candidate(cycle='second')
        self.candidate(cycle='third')
        original = self.raw.read
        modes = []
        def mode_change(kind, *args, **kwargs):
            result = original(kind, *args, **kwargs)
            if kind == 'userAbstraction':
                modes.append(kind)
                return 'unifiedAccount' if 4 <= len(modes) <= 6 else 'disabled'
            if kind == 'spotClearinghouseState':
                return dict(balances=[dict(coin='USDC', token=0, total='40000', hold='0')])
            return result
        with patch.object(self.raw, 'read', side_effect=mode_change):
            result = self.collect()
        self.assertEqual(result['entry_blocked'], {})
        self.assertEqual(len(result['capacity']), 3)
        counts = Counter(call[0] for call in self.raw.calls)
        self.assertEqual(counts['activeAssetData'], 3)
        self.assertEqual(counts['userRole'], 6)
        self.assertEqual(counts['userRateLimit'], 3)

    def test_default_final_mode_probe_remains_uncached_after_reuse(self):
        from experimental_execution_fixtures import r2732_message
        from . import two_account_execution as roles
        from .experimental_market_context import MarketSnapshot
        from .test_experimental_execution_runtime import META
        account = roles.PHANTOM
        self.provider.env['HL_TESTNET_SHORT_ACCOUNT_ADDRESS'] = account
        original = self.raw.read
        modes = []
        def mode_change(kind, *args, **kwargs):
            if kind == 'userAbstraction':
                modes.append(kind)
                return 'disabled' if len(modes) == 4 else 'default'
            if kind == 'userRole':
                return dict(role='user') if kwargs['user'] == account else dict(role='agent', data=dict(user=account))
            return original(kind, *args, **kwargs)
        market = MarketSnapshot(self.raw.read('metaAndAssetCtxs'), observed_at_ms=self.exchange.t)
        message = r2732_message(entry=2.3, decision_ms=T-60000)
        kwargs = dict(buckets=[], inventory=dict(orders=[], positions=dict(assetPositions=[])),
            market=market, entry_reads={})
        with patch.object(self.raw, 'read', side_effect=mode_change):
            self.provider._capacity(message, account, 'short_account', META, **kwargs)
            with self.assertRaisesRegex(ValueError, 'ACCOUNT_MODE_CHANGED_RECHECK'):
                self.provider._capacity(message, account, 'short_account', META, **kwargs)
        self.assertEqual(len(modes), 4)

    def test_stalled_entry_collection_cannot_block_protection_collection(self):
        self.candidate()
        entered, release = threading.Event(), threading.Event()
        completed, errors = [], []
        def stall(kind):
            if kind == 'activeAssetData' and threading.current_thread().name == 'entry-fixture':
                entered.set()
                if not release.wait(3):
                    raise AssertionError('FIXTURE_WAIT_TIMED_OUT')
        self.raw.mutate = stall
        def run(entries):
            try:
                completed.append((entries, self.provider.collect(deepcopy(self.state), entries_enabled=entries)))
            except BaseException as exc:
                errors.append(exc)
        entry = threading.Thread(target=run, args=(True,), name='entry-fixture', daemon=True)
        protection = threading.Thread(target=run, args=(False,), name='protection-fixture', daemon=True)
        entry.start()
        try:
            self.assertTrue(entered.wait(2))
            protection.start()
            protection.join(timeout=2)
            self.assertFalse(protection.is_alive())
            self.assertTrue(entry.is_alive())
            self.assertEqual([enabled for enabled,_ in completed], [False])
        finally:
            release.set()
            entry.join(timeout=3)
            if protection.ident is not None:
                protection.join(timeout=3)
        self.assertFalse(entry.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(len(completed), 2)

    def test_idle_collections_keep_two_inventory_passes_without_unused_market_read(self):
        self.provider.collect(self.state)
        self.assertEqual(len(self.raw.calls), 8)
        self.provider.collect(self.state)
        self.assertEqual(len(self.raw.calls), 16)
        self.assertNotIn('metaAndAssetCtxs',[c[0] for c in self.raw.calls])


if __name__ == '__main__':
    unittest.main()
