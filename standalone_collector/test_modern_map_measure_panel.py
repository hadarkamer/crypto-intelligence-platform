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

    def test_optional_leader_retains_printed_bounds_without_fabricating_member(self):
        self.data['values']['leader'] = []
        self.data['values']['box'] = [['61,680.00', '61,418.0', '0.44%', '91.10M']]
        answer = measure_panel(None, self.control, require_leader=False)
        self.assertEqual(answer, ('60,036', '91.10M', None, 61418, 61680))
        self.assertEqual(answer.raw_bounds, ('61,418.0', '61,680.00'))
        self.data['values']['reference'] = []
        with self.assertRaises(StageFailure) as caught:
            measure_panel(None, self.control, require_leader=False)
        self.assertEqual(caught.exception.measure_detail['field'], 'reference')

    def test_optional_still_returns_valid_leader_and_rejects_disagreement(self):
        answer = measure_panel(None, self.control, require_leader=False)
        self.assertIsNotNone(answer[2])
        self.assertEqual(answer.raw_bounds, ('61,418', '61,680'))
        self.data['values']['leader'][1][1] = '50M'
        with self.assertRaises(StageFailure) as caught:
            measure_panel(None, self.control, require_leader=False)
        self.assertEqual(caught.exception.measure_detail['reason'], 'conflicting_values')
        self.data['values']['leader'] = []
        self.data['values']['box'] = []
        with self.assertRaises(StageFailure) as caught:
            measure_panel(None, self.control, require_leader=False)
        self.assertEqual(caught.exception.measure_detail['field'], 'box')


class LatestBinMeasurement(TestCase):
    @staticmethod
    def reading(low, high, value='37.2M'):
        from modern_map_measure_panel import _resolve
        return _resolve({'active':True,'counts':{'box':1,'leader':2,'reference':1},'values':{
            'box':[[low,high,'0.22%',value]],'leader':[],'reference':[['60,677']]}},require_leader=False)

    def test_inner_box_preserves_independently_displayed_price_and_amount(self):
        from modern_map_evidence import box_member
        measurement=self.reading('60,285','60,313')
        self.assertEqual(box_member(measurement,'60,299','43.56','61,519'),
                         {'raw_price':'60,299','raw_value':'37.2M'})
        for lower,upper,price,cause in [
                ('60,260','60,310','60,299','box_not_within_native_bin'),
                ('60,297','60,299','60,299','box_not_within_native_bin'),
                ('60,285','60,313',None,'native_price_not_printed'),
                ('60,285','60,313','60,302','price_not_on_native_grid')]:
            with self.subTest(cause=cause,low=lower),self.assertRaises(StageFailure) as caught:
                box_member(self.reading(lower,upper),price,'43.56','61,519')
            self.assertEqual(caught.exception.measure_detail['reason'],cause)

    def test_full_domain_validates_independent_maximum_and_all_displayed_ties(self):
        from modern_map_evidence import verify_covered_maximum
        rail=[{'raw_price':'60,299','raw_value':'37.2M'},{'raw_price':'61,519','raw_value':'37.2M'}]
        groups=[{'members':rail[:]}]
        verify_covered_maximum(groups,None,'37.2M',rail)
        for members,source_max,ties in [
                (rail[:1],'37.2M',rail),(rail,'39.2M',rail),
                (rail,None,rail),(rail,'37.2M',[])]:
            with self.subTest(members=len(members),source_max=source_max),self.assertRaises(StageFailure):
                verify_covered_maximum([{'members':members}],None,source_max,ties)
        smaller={'raw_price':'60,100','raw_value':'1.0M'}
        with self.assertRaises(StageFailure):
            verify_covered_maximum([{'members':rail+[smaller]}],smaller,None,[])
