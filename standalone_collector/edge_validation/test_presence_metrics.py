"""A visible weak band is not the same as a missing current band."""
import unittest
from test_edge_zones import fixture, run
from edge_zones import audit_saved_zones

class PresenceTests(unittest.TestCase):
    def test_weak_current_band_not_misreported_as_absent(self):
        m=run(fixture([(100,108,.30,100,480)]))
        z=[{'side':'above','price_low':78300,'price_high':78450,'intensity':'normal'}]
        r=audit_saved_zones(m,z)[0]
        self.assertEqual(r['verdict'],'present_below_candidate_threshold')
        self.assertGreater(r['current_presence_fraction'],0)
        self.assertEqual(r['edge_support_fraction'],0)

    def test_historical_weak_band_does_not_gain_current_presence(self):
        m=run(fixture([(100,108,.30,100,420)]))
        z=[{'side':'above','price_low':78300,'price_high':78450,'intensity':'normal'}]
        r=audit_saved_zones(m,z)[0]
        self.assertEqual(r['verdict'],'unsupported_current_edge')
        self.assertEqual(r['current_presence_fraction'],0)

if __name__=='__main__':unittest.main()
