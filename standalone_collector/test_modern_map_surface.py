"""Only the verified, unique Close control on the chart-overlapping card can click."""
from unittest import TestCase
from unittest.mock import Mock
from modern_map_surface import prepare_surface


def handle(cards=0,eligible=0,unknown=()):
    state=Mock()
    state.evaluate.return_value=dict(cards=cards,eligible=eligible,covered=[[3,18,105,72]] if cards else [],
                                     unknown=list(unknown))
    close,card=Mock(),Mock()
    state.get_property.side_effect=lambda key:Mock(as_element=lambda:close if key=='close' else card)
    return state,close,card


class PromotionTests(TestCase):
    def test_visible_normal_dismissal_confirmed_before_capture(self):
        old,close,card=handle(1,1)
        new,_,_=handle()
        page=Mock()
        page.evaluate_handle.side_effect=[old,new]
        facts=prepare_surface(page,(0,0,370,205),lambda:None,lambda n:n)
        close.click.assert_called_once_with(timeout=1500)
        card.wait_for_element_state.assert_called_once_with('hidden',timeout=1500)
        self.assertEqual(facts,dict(cards=0,eligible=0,covered=[],unknown=[],close_attempts=1))

    def test_unknown_or_ambiguous_controls_leave_the_original_obstruction_reported(self):
        for n,k,foreign in ((1,2,()),(1,0,()),(2,1,()),(0,0,[[13,8,45,40]]),(1,1,[[13,8,45,40]])):
            with self.subTest(cards=n,eligible=k):
                old,close,_=handle(n,k,foreign)
                page=Mock()
                page.evaluate_handle.return_value=old
                facts=prepare_surface(page,(0,0,370,205),lambda:None,lambda ms:ms)
                close.click.assert_not_called()
                self.assertEqual(facts['close_attempts'],0)
                self.assertTrue(facts['unknown'] or facts['covered'])
