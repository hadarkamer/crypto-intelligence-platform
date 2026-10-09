"""Shared REST/websocket fill facts; no transport, database or exchange access."""
from copy import deepcopy
import unittest

from . import card_lifecycle as life, card_sync_evidence as evidence


ACCOUNT = '0x' + '1' * 40
AT = 1800000000000


def raw_fill(**changes):
    value = dict(coin='SOL', tid=118906512037719, oid=90542681,
                 px='18.4350', sz='93.530', side='B', time=AT,
                 fee='0.010', feeToken='USDC')
    value.update(changes)
    return value


def normalize(row):
    return evidence.normalize_fill(row, ACCOUNT, 'SOL', AT-1000, AT+1000)


class FillNormalizationTests(unittest.TestCase):
    def test_rest_and_websocket_wire_metadata_produce_same_exact_saved_schema(self):
        rest = raw_fill()
        websocket = raw_fill(startPosition='26.86', dir='Open Long', closedPnl='0.0',
            hash='0x'+'a'*64, crossed=False, builderFee='0.01',
            liquidation=dict(markPx=18.4, method='market'))
        expected = dict(account=ACCOUNT, symbol='SOL', oid='90542681',
            fill_id='hl:118906512037719', quantity='93.530', price='18.4350',
            fee='0.010', fee_token='USDC', side='B', at_ms=AT)
        self.assertEqual(normalize(rest), expected)
        self.assertEqual(normalize(websocket), expected)
        self.assertEqual(evidence.merge_fills([], [rest], ACCOUNT, 'SOL', AT-1000, AT+1000),
                         [expected])
        snapshot = dict(environment='testnet', account=ACCOUNT, symbol='SOL', at_ms=AT,
            history_complete=False, orders_complete=False, position_quantity='0',
            fills=[expected], open_orders=[], terminal_orders=[])
        self.assertEqual(life.validate_snapshot(snapshot), (ACCOUNT, 'SOL'))

    def test_normalization_does_not_mutate_input_or_return_wire_references(self):
        row = raw_fill(builderFee='0.01', liquidation=dict(method='market'))
        original = deepcopy(row)
        result = normalize(row)
        result['quantity'] = '1'
        self.assertEqual(row, original)

    def test_rebate_zero_fee_and_both_sides_keep_exact_values(self):
        for fee in ('-0.00300', '0', '0.0000', '0.123'):
            for side in ('A', 'B'):
                with self.subTest(fee=fee, side=side):
                    row = normalize(raw_fill(fee=fee, side=side, builderFee='999'))
                    self.assertEqual(row['fee'], fee)
                    self.assertEqual(row['side'], side)

    def test_unrelated_symbol_is_filtered_before_economic_validation(self):
        self.assertIsNone(normalize(dict(coin='ETH')))
        self.assertEqual(evidence.merge_fills([], [dict(coin='ETH')],
                         ACCOUNT, 'SOL', AT-1000, AT+1000), [])

    def test_invalid_symbol_and_non_mapping_are_rejected(self):
        for row in (None, [], 'SOL', {}, raw_fill(coin=None), raw_fill(coin=1)):
            with self.subTest(row=row):
                with self.assertRaisesRegex(evidence.SyncError, 'INVALID_FILL_SYMBOL'):
                    normalize(row)

    def test_integer_identifier_validation_preserves_legacy_limits(self):
        for changes in (dict(tid=True), dict(tid='1'), dict(tid=-1), dict(tid=None),
                        dict(oid=True), dict(oid='1'), dict(oid=0), dict(oid=-1),
                        dict(oid=2**64), dict(oid=None)):
            with self.subTest(changes=changes):
                with self.assertRaisesRegex(evidence.SyncError, 'EXACT_FILL_IDENTIFIERS_REQUIRED'):
                    normalize(raw_fill(**changes))
        self.assertEqual(normalize(raw_fill(tid=0, oid=2**64-1))['fill_id'], 'hl:0')

    def test_quantity_price_and_fee_require_bounded_finite_decimal_strings(self):
        invalid_positive = (None, True, 1, 1.5, 'NaN', 'Infinity', '0', '-1',
                            '1e19', '0.0000000000000000001', '1'*81)
        for field in ('sz', 'px'):
            for value in invalid_positive:
                with self.subTest(field=field, value=value):
                    with self.assertRaises(life.LifecycleError):
                        normalize(raw_fill(**{field:value}))
        for value in (None, True, 1, 'NaN', 'Infinity', '1e19', '1'*81):
            with self.subTest(fee=value):
                with self.assertRaises(life.LifecycleError):
                    normalize(raw_fill(fee=value))

    def test_fee_token_side_and_time_validation(self):
        for token in (None, '', 'USDC ', 'USDC/ETH', 1):
            with self.subTest(token=token):
                with self.assertRaisesRegex(life.LifecycleError, 'INVALID_IDENTIFIER'):
                    normalize(raw_fill(feeToken=token))
        for side in ('buy', 'a', '', None, True):
            with self.subTest(side=side):
                with self.assertRaisesRegex(evidence.SyncError, 'INVALID_FILL_SIDE'):
                    normalize(raw_fill(side=side))
        for at in (None, True, str(AT), 0, -1, 10**15):
            with self.subTest(at=at):
                with self.assertRaisesRegex(life.LifecycleError, 'INVALID_TIME'):
                    normalize(raw_fill(time=at))
        for at in (AT-1001, AT+1001):
            with self.assertRaisesRegex(evidence.SyncError, 'FILL_TIME_OUTSIDE_WINDOW'):
                normalize(raw_fill(time=at))
        for at in (AT-1000, AT+1000):
            self.assertEqual(normalize(raw_fill(time=at))['at_ms'], at)

    def test_identical_websocket_and_rest_replays_merge_once(self):
        row = raw_fill()
        previous = [normalize(row)]
        original = deepcopy(previous)
        replay = raw_fill(hash='0x'+'b'*64, builderFee='0.005', crossed=True)
        self.assertEqual(evidence.merge_fills(previous, [row, replay],
                         ACCOUNT, 'SOL', AT-1000, AT+1000), previous)
        self.assertEqual(previous, original)

    def test_conflicting_replay_within_batch_or_against_saved_fill_is_rejected(self):
        for changes in (dict(sz='1'), dict(px='19'), dict(side='A'), dict(fee='1'),
                        dict(feeToken='OTHER'), dict(time=AT+1), dict(oid=90542682),
                        dict(sz='93.53')):
            # The last case deliberately preserves legacy exact-string facts.
            for saved in (False, True):
                with self.subTest(changes=changes, saved=saved):
                    previous = [normalize(raw_fill())] if saved else []
                    rows = [raw_fill(**changes)] if saved else [raw_fill(), raw_fill(**changes)]
                    with self.assertRaisesRegex(evidence.SyncError, 'FILL_FACT_CHANGED'):
                        evidence.merge_fills(previous, rows, ACCOUNT, 'SOL', AT-1000, AT+1000)

    def test_distinct_partial_fills_on_same_order_and_time_are_not_aggregated(self):
        rows = [raw_fill(tid=2, sz='3'), raw_fill(tid=1, sz='7')]
        result = evidence.merge_fills([], rows, ACCOUNT, 'SOL', AT-1000, AT+1000)
        self.assertEqual([row['fill_id'] for row in result], ['hl:1', 'hl:2'])
        self.assertEqual([row['quantity'] for row in result], ['7', '3'])

    def test_sparse_delta_normalizes_without_weakening_rest_overlap_check(self):
        previous = [normalize(raw_fill(tid=1))]
        delta = raw_fill(tid=2, time=AT+1)
        self.assertEqual(normalize(delta)['fill_id'], 'hl:2')
        with self.assertRaisesRegex(evidence.SyncError, 'PREVIOUS_FILL_MISSING_IN_OVERLAP'):
            evidence.merge_fills(previous, [delta], ACCOUNT, 'SOL', AT-1000, AT+1000)
        # Fills before the audited interval remain retained without requiring a replay.
        previous[0]['at_ms'] = AT-1001
        result = evidence.merge_fills(previous, [delta], ACCOUNT, 'SOL', AT-1000, AT+1000)
        self.assertEqual([row['fill_id'] for row in result], ['hl:1', 'hl:2'])


if __name__ == '__main__':
    unittest.main()
