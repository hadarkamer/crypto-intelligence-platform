"""Reuse only complete collection-local history; all inputs are software fixtures."""
from copy import deepcopy
import unittest
from unittest.mock import patch

from . import card_sync_evidence as sync, experimental_external_activity as ext
from .test_experimental_execution_runtime import ROUTES, T


ACCOUNT = ROUTES['long_account']['account']
OTHER = ROUTES['short_account']['account']
END = T + sync.OVERLAP_MS


class HistoryReader:
    def __init__(self, count=500):
        self.calls = []
        self.rows = [dict(tid=n+1, oid=n+1, coin='SOL', sz='1', px='95',
                          fee='0', feeToken='USDC', side='B', time=T+120*n)
                     for n in range(count)]

    def read(self, kind, account, *, oid=None, start=None, end=None):
        self.calls.append((kind, account, start, end))
        return deepcopy([r for r in self.rows if start <= r['time'] <= end])


class CachedHistoryTests(unittest.TestCase):
    def setUp(self):
        for target in ('http.client.HTTPSConnection', 'socket.socket.connect'):
            guard = patch(target, side_effect=AssertionError('NO_EXTERNAL_IO'))
            guard.start(); self.addCleanup(guard.stop)
        self.reader = HistoryReader()
        self.cache = sync.ObservationReadCache(self.reader)
        sync.history(self.cache.pass_reader(0), ACCOUNT, T, END)
        self.root = self.cache._key('userFillsByTime', ACCOUNT, start=T, end=END)
        self.right = self.cache._key('userFillsByTime', ACCOUNT,
                                     start=(T+END)//2+1, end=END)

    def test_complete_split_history_removes_three_duplicate_reads(self):
        self.assertEqual(len(self.reader.calls), 3)
        state = dict(routes={r:v['account'] for r,v in ROUTES.items()}, trades={}, requests={})
        observation = ext.observe(state, {ACCOUNT:[]},
            {ACCOUNT:dict(orders=[], positions=dict(assetPositions=[]))},
            self.cache, now_ms=END, fill_reader=self.reader)
        self.assertEqual(len(self.reader.calls), 3)
        self.assertTrue(observation['accounts'][ACCOUNT]['history_complete'])
        self.assertEqual(len(observation['accounts'][ACCOUNT]['fills']), 500)

    def test_missing_child_returns_none_without_transport(self):
        del self.cache.samples[0][self.right]
        self.assertIsNone(ext.cached_history(self.cache, ACCOUNT, T, END))
        self.assertEqual(len(self.reader.calls), 3)

    def test_other_account_and_second_pass_cannot_complete_missing_child(self):
        child = self.cache.samples[0].pop(self.right)
        self.cache.samples[1][self.right] = child
        self.cache.samples[0][(self.right[0], OTHER, *self.right[2:])] = child
        self.assertIsNone(ext.cached_history(self.cache, ACCOUNT, T, END))
        self.assertIsNone(ext.cached_history(self.cache, OTHER, T, END))
        self.assertEqual(len(self.reader.calls), 3)

    def test_wrong_window_child_cannot_cover_gap(self):
        child = self.cache.samples[0].pop(self.right)
        wrong = (*self.right[:3], self.right[3]+1, self.right[4])
        self.cache.samples[0][wrong] = child
        self.assertIsNone(ext.cached_history(self.cache, ACCOUNT, T, END))
        self.assertEqual(len(self.reader.calls), 3)

    def test_malformed_or_out_of_window_pages_are_not_reused(self):
        before = deepcopy(self.cache.samples[0])
        for malformed in (None, [None], [{'time':'invalid'}], [{'time':END+1}]):
            with self.subTest(page=malformed):
                self.cache.samples[0].clear(); self.cache.samples[0].update(deepcopy(before))
                self.cache.samples[0][self.right] = malformed
                self.assertIsNone(ext.cached_history(self.cache, ACCOUNT, T, END))
        self.assertEqual(len(self.reader.calls), 3)

    def test_saturated_leaf_without_complete_children_is_not_reused(self):
        self.cache.samples[0][self.right] = [dict(time=END)] * 2000
        self.assertIsNone(ext.cached_history(self.cache, ACCOUNT, T, END))
        key = self.cache._key('userFillsByTime', ACCOUNT, start=END, end=END)
        self.cache.samples[0].clear(); self.cache.samples[0][key] = [dict(time=END)] * 2000
        self.assertIsNone(ext.cached_history(self.cache, ACCOUNT, END, END))
        self.assertEqual(len(self.reader.calls), 3)

    def test_superset_interval_is_filtered_and_return_value_is_detached(self):
        rows = ext.cached_history(self.cache, ACCOUNT, T+120, END-120)
        self.assertEqual(len(rows), 499)
        self.assertTrue(all(T+120 <= r['time'] <= END-120 for r in rows))
        rows[0]['sz'] = '999'
        self.assertTrue(all(r['sz']=='1' for r in ext.cached_history(self.cache, ACCOUNT, T, END)))
        self.assertIsNone(ext.cached_history(self.cache, ACCOUNT, T, END+1))
        self.assertEqual(len(self.reader.calls), 3)

    def test_single_empty_or_unsaturated_page_remains_reusable(self):
        for rows in ([], [self.reader.rows[0]]):
            with self.subTest(rows=rows):
                self.cache.samples[0].clear(); self.cache.samples[0][self.root] = rows
                self.assertEqual(ext.cached_history(self.cache, ACCOUNT, T, END), rows)
        self.assertEqual(len(self.reader.calls), 3)


if __name__ == '__main__':
    unittest.main()
