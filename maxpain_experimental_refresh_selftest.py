"""New source/range/tier and migration regression tests; offline only."""
import asyncio
from copy import deepcopy
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import sol_proximity_experimental_signal as s
import sol_proximity_experimental_store as store
import sol_proximity_experimental_worker as worker
from maxpain_experimental_specs import *
from sol_proximity_experimental_selftest import BASE, M, bar, bundle, pending
from hype_row71205_experimental_store_selftest import MemoryDatabase


def fixture(spec, now, target, *, source=100, complete=True):
    b = bundle(now, target, source=source, complete=complete)
    b['coins'][spec.coin] = b['coins'].pop('SOL')
    for i, row in enumerate(b['coins'][spec.coin]['sources']['maxpain_operational_rows']):
        row['short_liquidation_amount'] = 100*2**i
        row['long_liquidation_amount'] = 100*2**i
    return b


def decoded(spec, now, target, **kw):
    b = fixture(spec, now, target, **kw)
    end = now//M*M
    rows = [bar(t, 100, 104, 96, 100) for t in range(end-1440*M, end, M)] if spec.require_range24 else None
    return s.decode_bundle(b, now, spec, rows)


class Definitions(unittest.TestCase):
    def test_distance_boundaries_both_directions_and_no_score_gate(self):
        for spec in SPECS.values():
            for target, expected in ((100+spec.lower_pct, True), (100+spec.upper_pct, False),
                                     (100+spec.lower_pct-.00001, False)):
                d = decoded(spec, BASE+10000, target)
                self.assertEqual(any(r['eligible'] for r in d['rows']), expected, (spec, target))
        b = fixture(SOL_RANGE24, BASE+10000, 102)
        for slot in b['coins']['SOL']['maxpain']:
            slot['components']['target_proximity'] = 0
        bars = [bar(t, 100, 104, 96, 100) for t in range(BASE-1440*M, BASE, M)]
        self.assertTrue(any(r['eligible'] for r in s.decode_bundle(b, BASE+10000, SOL_RANGE24, bars)['rows']))
        d = decoded(XRP_SHORT_TF, BASE+10000, 99, source=100)
        self.assertFalse(any(r['eligible'] for r in d['rows']))

    def test_score_slot_identity_remains_mandatory_without_score_cutoff(self):
        for spec in (SOL_RANGE24, HYPE_LONG_TF):
            for change in ({'target_price':999}, {'status':'INACTIVE_TARGET'}, {'score':None}):
                now = BASE+10000
                target = 100+(spec.lower_pct+spec.upper_pct)/2
                b = fixture(spec, now, target)
                for slot in b['coins'][spec.coin]['maxpain']:
                    slot.update(change)
                bars = [bar(t, 100, 104, 96, 100) for t in range(BASE-1440*M, BASE, M)] if spec.require_range24 else None
                self.assertFalse(any(r['eligible'] for r in s.decode_bundle(b, now, spec, bars)['rows']))

    def test_geometry_exact_source_midpoint(self):
        for spec, values in ((SOL_RANGE24, (96, 102, 90)), (HYPE_LONG_TF, (96, 101, 90)),
                             (DOGE_LONG_TF, (96, 101, 90)), (XRP_SHORT_TF, (99, 102, 90))):
            p = s.levels(100, 102, spec)
            self.assertEqual(tuple(p[k] for k in ('entry_price', 'take_price', 'stop_price')), values)
        p = s.levels(100, 98, DOGE_LONG_TF)
        self.assertEqual([p[k] for k in ('entry_price', 'take_price', 'stop_price')], [104, 99, 110])

    def test_range_is_closed_complete_before_decision(self):
        now = BASE+10000
        b = fixture(SOL_RANGE24, now, 102)
        bars = [bar(t, 100, 102, 99, 100) for t in range(BASE-1440*M, BASE, M)]
        self.assertTrue(any(r['eligible'] for r in s.decode_bundle(b, now, SOL_RANGE24, bars)['rows']))
        with self.assertRaises(ValueError):
            s.decode_bundle(b, now, SOL_RANGE24, bars[1:]+[bar(BASE, 100, 105, 99, 100)])
        bars = [bar(t) for t in range(BASE-1440*M, BASE, M)]
        self.assertFalse(any(r['eligible'] for r in s.decode_bundle(b, now, SOL_RANGE24, bars)['rows']))

    def test_complete_source_generation_required_and_tf_bands(self):
        for spec in SPECS.values():
            self.assertFalse(any(r['eligible'] for r in decoded(spec, BASE+10000, 100+(spec.lower_pct+spec.upper_pct)/2, complete=False)['rows']))
            d = decoded(spec, BASE+10000, 100+(spec.lower_pct+spec.upper_pct)/2)
            self.assertEqual({r['timeframe'] for r in d['rows'] if r['eligible']}, set(spec.timeframes))

    def test_original_target_cancels_partial_tp_plan(self):
        st = pending()
        p = st['active'][0]
        p.update(take_price=101, take_fraction=.5)
        s.advance(st, [bar(BASE+M), bar(BASE+2*M, 100, 101.5, 99, 100)], BASE+3*M)
        self.assertEqual(st['active'][0]['status'], 'PENDING')
        s.advance(st, [bar(BASE+3*M, 100, 102.1, 99, 100)], BASE+4*M)
        self.assertFalse(st['active'])
        self.assertEqual(st['history'][-1]['status'], 'TARGET_BEFORE_ENTRY')

    def test_partial_take_order_ambiguity_no_fabricated_alert(self):
        st = pending()
        st['active'][0].update(take_price=101, take_fraction=.5)
        s.advance(st, [bar(BASE+M), bar(BASE+2*M, 100, 101.5, 95, 99)], BASE+3*M)
        self.assertEqual(st['active'][0]['status'], 'UNKNOWN')
        self.assertEqual(st['active'][0]['unknown_reason'], 'ENTRY_PARTIAL_TAKE_ORDER_UNKNOWN')
        self.assertFalse(st['intents'])


class Growth(unittest.TestCase):
    def proof(self):
        d = decoded(XRP_SHORT_TF, BASE+10000, 103)
        incoming = next(r for r in d['rows'] if r['source_side'] == 'SHORT' and r['timeframe'] == '48h')
        previous = dict(target_price=103., timeframe='12h', direction=1)
        return d, incoming, previous

    def test_full_current_chain_exact_old_target_and_each_tier(self):
        d, row, old = self.proof()
        self.assertTrue(s.growth_allows(row, old, d['rows']))
        self.assertFalse(s.growth_allows(row, {**old, 'direction': -1}, d['rows']))
        self.assertFalse(s.growth_allows(row, {**old, 'target_price': 103.00001}, d['rows']))
        self.assertFalse(s.growth_allows(row, old, [r for r in d['rows'] if r['timeframe'] != '24h']))
        for target in d['rows']:
            if target['source_side'] == 'SHORT':
                target['liquidation_amount'] = {'12h':100, '24h':115, '48h':138}.get(target['timeframe'], 1000)
        self.assertTrue(s.growth_allows(row, old, d['rows']))
        next(r for r in d['rows'] if r['timeframe']=='24h' and r['source_side']=='SHORT')['liquidation_amount'] = 114.999
        self.assertFalse(s.growth_allows(row, old, d['rows']))

    def test_unrelated_target_missing_amount_and_wrong_direction_block(self):
        for field, val in [('target_price', 104), ('liquidation_amount', None), ('provider_valid', False)]:
            d, row, old = self.proof()
            r = next(r for r in d['rows'] if r['timeframe']=='24h' and r['source_side']=='SHORT')
            r[field] = val
            self.assertFalse(s.growth_allows(row, old, d['rows']))

    def test_consumed_growth_legs_not_reused_without_new_larger_proof(self):
        spec = XRP_SHORT_TF
        now = BASE+10000
        st = s.initial(now)
        s.ingest(st, decoded(spec, now, 103), now, [bar(BASE)])
        s.advance(st, [bar(BASE)], BASE+M+10000)
        s.ingest(st, decoded(spec, BASE+M+10000, 102.5), BASE+M+10000, [bar(BASE+M)])
        self.assertEqual(len(st['active']), 3)
        s.advance(st, [bar(BASE+M), bar(BASE+2*M, 98.75, 99, 98.5, 98.75)], BASE+3*M)
        self.assertEqual(len(st['episodes']['102.5']['filled_legs']), 3)
        st['active'].clear()
        s.ingest(st, decoded(spec, BASE+3*M, 102.5), BASE+3*M, [bar(BASE+3*M)])
        self.assertEqual(st['active'], [])

    def test_no_growth_doge_one_plan_but_hype_proved_tiers_admit(self):
        for spec, expected in ((DOGE_LONG_TF, 1), (HYPE_LONG_TF, 4)):
            now = BASE+10000
            st = s.initial(now)
            s.ingest(st, decoded(spec, now, 100+spec.lower_pct+.1), now, [bar(BASE)])
            s.advance(st, [bar(BASE)], BASE+M+10000)
            s.ingest(st, decoded(spec, BASE+M+10000, 100+spec.lower_pct+.2), BASE+M+10000, [bar(BASE+M)])
            self.assertEqual(len(st['active']), expected)


class Migration(unittest.TestCase):
    def legacy(self):
        st = pending()
        template = st['active'][0]
        st['active'] = []
        for i in range(10):
            p = {**deepcopy(template), 'position_id':str(i), 'status':'OPEN' if i<2 else 'PENDING'}
            if i < 2:
                p.update(fill_price=96, fill_ms=BASE+2*M)
            st['active'].append(p)
        st.update(version=store.VERSION, config_sha256=store.LEGACY_SOL_CONFIG_SHA256)
        return st

    def test_two_filled_preserved_eight_pending_cancelled_idempotent_restart(self):
        db = MemoryDatabase()
        now = BASE+3*M
        with patch.object(store, '_connect', db.connect):
            store.initialize_scope('migrate', now, config_sha256=store.LEGACY_SOL_CONFIG_SHA256)
            before = self.legacy()
            store.transact('migrate', now, store.LEGACY_SOL_CONFIG_SHA256, lambda st: st.update(before))
            after = store.initialize_scope('migrate', now, config_sha256=worker.CONFIG_SHA256, migrate_legacy_sol=True)
            self.assertEqual(len(after['active']), 2)
            self.assertEqual(after['formula_migration']['cancelled_pending'], 8)
            for p, old in zip(after['active'], before['active'][:2]):
                self.assertEqual([p[k] for k in ('entry_price','stop_price','take_price','fill_price')],
                                 [old[k] for k in ('entry_price','stop_price','take_price','fill_price')])
                self.assertTrue(p['legacy_formula'])
            self.assertEqual(after['counts'], {})
            again = store.initialize_scope('migrate', now+M, config_sha256=worker.CONFIG_SHA256, migrate_legacy_sol=True)
            self.assertEqual(after['formula_migration'], again['formula_migration'])
            with self.assertRaises(ValueError):
                store.transact('migrate', now+M, store.LEGACY_SOL_CONFIG_SHA256, lambda st: st.update(bad=True))

    def test_unknown_predecessor_and_inflight_refuse(self):
        st = self.legacy()
        st['config_sha256'] = 'unrecognized'
        with self.assertRaises(ValueError):
            store.migrate_sol_state(st, BASE+3*M, worker.CONFIG_SHA256)
        st = self.legacy()
        st['intents'] = [dict(status='IN_FLIGHT', attempt_ms=BASE+3*M)]
        with self.assertRaises(ValueError):
            store.migrate_sol_state(st, BASE+3*M, worker.CONFIG_SHA256)


class Source(unittest.IsolatedAsyncioTestCase):
    async def test_new_worker_scope_and_hype_source_route(self):
        for coin, w in worker.ADDITIONAL_WORKERS.items():
            self.assertIn(w.spec.rule_id, w.scope_for(123))
            self.assertFalse(w.status()['live_order_execution'])
        with patch.object(worker.hyperliquid, 'fetch_rows', return_value=[bar(BASE)]) as fetch:
            self.assertEqual(worker.fetch_rows('HYPE', BASE, BASE+M), [bar(BASE)])
            fetch.assert_called_once_with('HYPE', BASE, BASE+M)
        with patch.object(worker.requests, 'get', side_effect=AssertionError('No other route')):
            with patch.object(worker.hyperliquid, 'fetch_rows', side_effect=ValueError('Unavailable')):
                with self.assertRaises(ValueError):
                    worker.fetch_rows('HYPE', BASE, BASE+M)

if __name__ == '__main__':
    unittest.main()
