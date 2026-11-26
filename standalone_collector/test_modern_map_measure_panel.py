from unittest import TestCase
from unittest.mock import Mock

from modern_map_measure_panel import measure_panel
from model1_execution import StageFailure


class MeasurePanelTest(TestCase):
    def setUp(self):
        self.control = Mock()
        self.data = {'active': True, 'counts': {'box': 1, 'leader': 2, 'reference': 1}, 'values': {
            'box': [['61,418', '61,680', '0.44%', '91.1M']],
            'leader': [['61,500', '51.0M'], ['61500', '51M']], 'reference': [['60,036']]}}
        self.control.evaluate.return_value = self.data

    def test_duplicate_rendered_labels_agree(self):
        ref, total, leader, lo, hi = measure_panel(None, self.control)
        self.assertEqual((ref, total, lo, hi), ('60,036', '91.1M', 61418, 61680))
        self.assertIn(leader['raw_value'], ('51M', '51.0M'))
        self.control.evaluate.assert_called_once()

    def test_conflicting_or_missing_preserves_stage_evidence(self):
        self.data['values']['leader'][1][1] = '52M'
        for cause in ('conflicting_values', 'number_missing', 'label_missing'):
            with self.assertRaises(StageFailure) as caught:
                measure_panel(None, self.control)
            detail = caught.exception.measure_detail
            self.assertEqual((detail['reason'], detail['field']), (cause, 'leader'))
            if cause == 'conflicting_values':
                self.data['values']['leader'] = []
            else:
                self.data['counts']['leader'] = 0

    def test_inactive_control_is_rejected(self):
        self.data['active'] = False
        with self.assertRaises(StageFailure) as caught:
            measure_panel(None, self.control)
        self.assertEqual(caught.exception.measure_detail['reason'], 'right_edge_inactive')
