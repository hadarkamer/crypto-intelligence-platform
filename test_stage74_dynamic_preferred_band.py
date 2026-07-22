import unittest

import alert_engine


class Stage74DynamicPreferredBandTests(unittest.TestCase):
    def test_preferred_ceiling_mapping(self):
        self.assertEqual(alert_engine._preferred_distance_ceiling(2.5), 1.3)
        self.assertEqual(alert_engine._preferred_distance_ceiling(2.7), 1.4)
        self.assertEqual(alert_engine._preferred_distance_ceiling(3.0), 1.5)
        self.assertEqual(alert_engine._preferred_distance_ceiling(3.5), 1.7)
        self.assertEqual(alert_engine._preferred_distance_ceiling(4.0), 2.0)

    def test_25_point_band_expands_by_threshold(self):
        self.assertEqual(alert_engine._target_proximity_points(1.3, 2.5), 25.0)
        self.assertEqual(alert_engine._target_proximity_points(1.31, 2.5), 20.0)
        self.assertEqual(alert_engine._target_proximity_points(1.4, 2.7), 25.0)
        self.assertEqual(alert_engine._target_proximity_points(1.5, 3.0), 25.0)
        self.assertEqual(alert_engine._target_proximity_points(1.7, 3.5), 25.0)
        self.assertEqual(alert_engine._target_proximity_points(2.0, 4.0), 25.0)

    def test_existing_outer_bands_remain(self):
        self.assertEqual(alert_engine._target_proximity_points(0.79, 4.0), 17.0)
        self.assertEqual(alert_engine._target_proximity_points(1.8, 3.5), 20.0)
        self.assertEqual(alert_engine._target_proximity_points(2.1, 3.5), 15.0)
        self.assertEqual(alert_engine._target_proximity_points(3.6, 3.5), 0.0)


if __name__ == "__main__":
    unittest.main()
