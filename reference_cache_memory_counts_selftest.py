"""Offline checks for bounded, content-free reference-cache diagnostics."""
import unittest
from unittest.mock import patch

import coinglass_history_backfill as history


def entry(rows=3, windows=1):
    return {'rows': rows, 'windows': {
        str(n): {'price_composition_samples': [(123.456, 0.75)] * 2,
                 'oi_composition_samples': [(987.654, 0.25)]}
        for n in range(windows)}}


class ReferenceCacheMemoryCountsTests(unittest.TestCase):
    def count(self, cache):
        with patch.object(history, '_REFERENCE_CACHE', cache), \
             patch.object(history, '_history_rows', side_effect=AssertionError('NO_DB')), \
             patch.object(history, 'calculate_reference_ranges', side_effect=AssertionError('NO_REBUILD')):
            result = history.reference_cache_memory_counts()
        self.assertTrue(all(type(value) is int for value in result.values()))
        return result

    def test_empty_and_unavailable_entries(self):
        self.assertEqual(self.count({}), dict(cache_symbols=0, sampled_symbols=0,
            history_rows=0, windows=0, price_samples=0, oi_samples=0, incomplete=0))
        result = self.count({'PRIVATE_SYMBOL': {'available': False, 'windows': {}}})
        self.assertEqual(result['cache_symbols'], 1)
        self.assertEqual(result['history_rows'], 0)
        self.assertEqual(result['incomplete'], 0)

    def test_exact_aggregate_without_values_or_mutation(self):
        cache = {'PRIVATE_A': entry(3, 2), 'PRIVATE_B': entry(5, 1)}
        before = repr(cache)
        self.assertEqual(self.count(cache), dict(cache_symbols=2, sampled_symbols=2,
            history_rows=8, windows=3, price_samples=6, oi_samples=3, incomplete=0))
        self.assertEqual(repr(cache), before)

    def test_sample_arrays_are_not_iterated_or_copied(self):
        class LengthOnlyList(list):
            def __iter__(self):
                raise AssertionError('NO_SAMPLE_ITERATION')
            def __getitem__(self, key):
                raise AssertionError('NO_SAMPLE_ACCESS')
        samples = LengthOnlyList([object(), object(), object()])
        cache = {'PRIVATE': {'rows': 3, 'windows': {'30m': {
            'price_composition_samples': samples, 'oi_composition_samples': samples}}}}
        result = self.count(cache)
        self.assertEqual((result['price_samples'], result['oi_samples']), (3, 3))
        self.assertEqual(result['incomplete'], 0)
        self.assertIs(cache['PRIVATE']['windows']['30m']['price_composition_samples'], samples)

    def test_symbol_and_window_work_is_bounded_and_marked_partial(self):
        result = self.count({str(n): entry(3, 9) for n in range(65)})
        self.assertEqual(result, dict(cache_symbols=65, sampled_symbols=64,
            history_rows=192, windows=512, price_samples=1024, oi_samples=512, incomplete=1))

    def test_concurrent_size_change_preserves_partial_counts_without_raising(self):
        class ChangingCache(dict):
            def values(self):
                iterator = iter(super().values())
                yield next(iterator)
                self['ADDED_CONCURRENTLY'] = entry()
                yield next(iterator)
        result = self.count(ChangingCache({'FIRST': entry(), 'SECOND': entry()}))
        self.assertEqual(result['cache_symbols'], 2)
        self.assertEqual(result['sampled_symbols'], 1)
        self.assertEqual(result['history_rows'], 3)
        self.assertEqual(result['incomplete'], 1)

    def test_bad_entry_or_array_is_sanitized_and_later_entries_still_count(self):
        cache = {'BAD': {'rows': True, 'windows': {'30m': {
            'price_composition_samples': 'SECRET_VALUE', 'oi_composition_samples': None}}},
            'BROKEN': None, 'GOOD': entry(5, 1)}
        result = self.count(cache)
        self.assertEqual(result['history_rows'], 5)
        self.assertEqual((result['price_samples'], result['oi_samples']), (2, 1))
        self.assertEqual(result['incomplete'], 1)


if __name__ == '__main__':
    unittest.main()
