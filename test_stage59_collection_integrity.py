import unittest
from coinglass_dom_reader import _rows_fingerprint

class CollectionIntegrityTests(unittest.TestCase):
    def test_fingerprint_changes_when_liquidity_changes(self):
        a = [{"symbol":"BTC","max_short_price":100,"short_amount_usd":1,"max_long_price":90,"long_amount_usd":2}]
        b = [{"symbol":"BTC","max_short_price":100,"short_amount_usd":3,"max_long_price":90,"long_amount_usd":2}]
        self.assertNotEqual(_rows_fingerprint(a), _rows_fingerprint(b))

    def test_fingerprint_is_stable_for_same_rows(self):
        rows = [{"symbol":"BTC","max_short_price":100,"short_amount_usd":1,"max_long_price":90,"long_amount_usd":2}]
        self.assertEqual(_rows_fingerprint(rows), _rows_fingerprint(list(rows)))

if __name__ == '__main__':
    unittest.main()
