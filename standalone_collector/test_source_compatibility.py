"""Observed Model 3 redirect and current source controls; no provider calls."""
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock,patch

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE/'runtime'))
import capture_readiness as readiness
import model1_execution as execution
import heatmap_models as models
from heatmap_models import select_visible_control,source_url

MODEL3='https://www.coinglass.com/liquidation-heatmap-model3?coin=BTC&type=symbol'


class Controls:
    def __init__(self,items):self.items=items
    def count(self):return len(self.items)
    @property
    def first(self):return self.items[0]
    def nth(self,index):return self.items[index]


class SourceCompatibilityTests(unittest.TestCase):
    def test_observed_redirect_and_legacy_routes_preserve_exact_model(self):
        for model in (1,2,3):
            self.assertTrue(readiness.valid_source(source_url(model),model))
        self.assertTrue(readiness.valid_source(MODEL3,3))
        for model in (1,2):self.assertFalse(readiness.valid_source(MODEL3,model))
        for hours in (12,24,48):
            page=Mock(url=MODEL3)
            page.evaluate.return_value={'ready':True,'reason':'render-checks-passed','label':f'{hours} hour'}
            self.assertTrue(readiness.wait_for_render(page,f'{hours}h',3)['ready'])
            page.wait_for_function.assert_not_called()

    def test_canonical_alias_never_accepts_other_source_or_scope(self):
        invalid=[MODEL3.replace('https:','http:'),MODEL3.replace('www.coinglass.com','evil.example'),
            MODEL3.replace('www.coinglass.com','www.coinglass.com.evil.example'),
            MODEL3.replace('www.coinglass.com','user@www.coinglass.com'),
            MODEL3.replace('www.coinglass.com','www.coinglass.com:444'),
            MODEL3.replace('coin=BTC','coin=ETH'),MODEL3.replace('type=symbol','type=pair'),
            MODEL3+'&coin=BTC',MODEL3+'&type=symbol',MODEL3.replace('model3','model2'),
            MODEL3.replace('/liquidation-heatmap-model3','/liquidationmap')]
        for url in invalid:
            with self.subTest(url=url):self.assertFalse(readiness.valid_source(url,3))

    def test_redirected_model3_login_gate_fails_immediately_after_selection(self):
        page=Mock(url=MODEL3);page.evaluate.side_effect=[False,True]
        calls=[]
        def _select_timeframe(page,timeframe):calls.append(timeframe)
        with patch.object(models,'HEATMAP_MODEL',3),self.assertRaises(execution.StageFailure) as caught:
            execution.control(_select_timeframe,page,'12h')
        self.assertEqual(caught.exception.code,'source_login_required')
        self.assertEqual(caught.exception._model1_capture_phase,'select_12h')
        self.assertEqual(calls,['12h'])
        page.wait_for_function.assert_not_called();page.wait_for_timeout.assert_not_called()
        page.screenshot.assert_not_called()

    def page(self,buttons=(),tabs=(),label='Model 3'):
        page=Mock()
        def by_role(role,**kwargs):
            self.assertTrue(kwargs['name'].fullmatch(label))
            return Controls(buttons if role=='button' else tabs)
        page.get_by_role.side_effect=by_role
        return page

    def test_existing_visible_button_remains_first_choice(self):
        button=Mock();button.is_visible.return_value=True
        tab=Mock();tab.is_visible.return_value=True
        page=self.page([button],[tab])
        self.assertTrue(select_visible_control(page,'Model 3'))
        button.click.assert_called_once_with(timeout=2000);tab.click.assert_not_called()
        self.assertEqual(page.get_by_role.call_count,1)

    def test_visible_exact_model_tab_follows_hidden_button(self):
        button=Mock();button.is_visible.return_value=False
        tab=Mock();tab.is_visible.return_value=True
        page=self.page([button],[tab])
        self.assertTrue(select_visible_control(page,'Model 3'))
        button.click.assert_not_called();tab.click.assert_called_once_with(timeout=2000)
        page.wait_for_timeout.assert_called_once_with(500)

    def test_symbol_tab_and_second_visible_match_use_normal_click(self):
        hidden=Mock();hidden.is_visible.return_value=False
        visible=Mock();visible.is_visible.return_value=True
        page=self.page([], [hidden,visible],label='Symbol')
        self.assertTrue(select_visible_control(page,'Symbol'))
        hidden.click.assert_not_called();visible.click.assert_called_once_with(timeout=2000)

    def test_absent_control_adds_no_wait_or_click(self):
        page=self.page()
        self.assertFalse(select_visible_control(page,'Model 3'))
        page.wait_for_timeout.assert_not_called()


if __name__=='__main__':unittest.main()
