"""Selected-map readiness, shared deadlines and same-frame geometry, offline."""
import ast
import json
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE/'runtime'))
import capture_readiness as readiness
import model1_execution as execution
from heatmap_models import source_url


def markup(hours=12,*,model=1,notice='',header_section=False,source_class=True):
    # M1's saved source has this component/controls/chart-sibling hierarchy;
    # the hash suffix is deliberately immaterial. Model3 uses shared ancestry.
    header=f'<h1>BTC Liquidation Heatmap</h1><button role="combobox">{hours} hour</button>'
    if header_section:header='<section id="header">'+header+'</section>'
    axis='<div data-axis="y" role="button"><div>87000</div><div>77000</div></div>' if model==3 else ''
    return ('<div id="panel" '+('class="LiqHeatmap2_box1__fixture"' if source_class else '')+'>'
        +header+'<div id="chart" style="position:relative;width:800px;height:400px">'
        '<canvas width="800" height="400"></canvas>'+axis+notice+'</div></div>')


NOTICE='<div id="empty" style="position:absolute;left:300px;top:180px">No data</div>'


class SelectedMapDOMTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from playwright.sync_api import sync_playwright
        cls.driver=sync_playwright().start()
        cls.browser=cls.driver.chromium.launch(headless=True)

    @classmethod
    def tearDownClass(cls):
        cls.browser.close();cls.driver.stop()

    def page(self,html,model=1):
        context=self.browser.new_context(viewport={'width':1600,'height':1300 if model==3 else 1000})
        page=context.new_page()
        # Every request is fulfilled from this test string, never the source.
        page.route('**/*',lambda route:route.fulfill(status=200,content_type='text/html',body=html))
        page.goto(source_url(model))
        self.addCleanup(context.close)
        return page

    def test_all_nine_model_horizon_combinations_preserve_source_and_model3_axis_contract(self):
        for model in (1,2,3):
            for hours in (12,24,48):
                with self.subTest(model=model,hours=hours):
                    page=self.page(markup(hours,model=model,source_class=model!=3),model)
                    value=readiness.wait_for_render(page,f'{hours}h',model,2000)
                    self.assertTrue(value['ready'])
                    self.assertEqual(value['label'],f'{hours} hour')
                    self.assertEqual(readiness.geometry_from_state(value)['width'],800)
                    self.assertEqual('price_axis_ticks' in value,model==3)
                    if model==3:self.assertEqual(value['price_axis_ticks'],[77000,87000])

    def test_empty_active_component_does_not_borrow_larger_unrelated_canvas(self):
        page=self.page('<div class="LiqHeatmap2_box1__fixture"><h1>BTC Liquidation Heatmap</h1>'
            '<button role="combobox">12 hour</button><div style="height:400px">'+NOTICE+'</div></div>'
            '<aside><canvas width="1200" height="700"></canvas></aside>')
        value=readiness.current_state(page,'12h',1)
        self.assertFalse(value['ready'])
        self.assertEqual(value['_render_observation']['canvas_count'],0)
        self.assertIsNone(readiness.geometry_from_state(value))
        self.assertIn(value['reason'],('no-data','no-chart'))

    def test_header_only_section_does_not_hide_its_sibling_chart(self):
        for source_class in (True,False):
            with self.subTest(source_class=source_class):
                page=self.page(markup(header_section=True,source_class=source_class))
                value=readiness.wait_for_render(page,'12h',1,2000)
                self.assertTrue(value['ready'])
                self.assertEqual(readiness.geometry_from_state(value)['width'],800)

    def test_unrelated_large_chart_notice_and_below_chart_status_do_not_block_selected_map(self):
        page=self.page(markup()+
            '<aside style="position:absolute;left:900px;top:0"><canvas width="1200" height="700"></canvas>'
            '<div role="status">No data</div><div role="status">Too many requests</div></aside>'
            '<div role="status" style="position:absolute;top:1100px">No data</div>')
        value=readiness.current_state(page,'12h',1)
        self.assertTrue(value['ready'])
        self.assertFalse(value['_render_observation']['visible_nodata'])
        self.assertFalse(value['_render_observation']['visible_request_limit'])
        self.assertEqual(readiness.geometry_from_state(value)['width'],800)

    def test_many_hidden_or_unrelated_leaves_cannot_hide_a_real_empty_notice(self):
        noise=''.join('<span style="display:none">unrelated</span>' for _ in range(700))
        page=self.page(markup(notice=noise+NOTICE))
        value=readiness.current_state(page,'12h',1)
        self.assertFalse(value['ready']);self.assertEqual(value['reason'],'no-data')

    def test_loading_and_resize_finish_before_the_bound_geometry_and_original_png(self):
        page=self.page(markup(notice=NOTICE))
        initial=readiness.current_state(page,'12h',1)
        self.assertEqual(initial['reason'],'no-data')
        old=initial['_render_observation']['canvas_rect']
        page.evaluate("""() => setTimeout(()=>{
            document.querySelector('#empty').remove();
            document.querySelector('#panel').style.marginTop='23.5px';
            document.querySelector('#chart').style.width='820px';
            document.querySelector('canvas').style.width='820px';
        },60)""")
        state=readiness.wait_for_render(page,'12h',1,2000)
        self.assertTrue(state['ready'])
        geometry=readiness.geometry_from_state(state)
        self.assertEqual(geometry['width'],820)
        self.assertNotEqual(geometry['y'],old['y'])
        with tempfile.TemporaryDirectory() as directory:
            image=Path(directory)/'original.png'
            with patch.object(page,'screenshot',wraps=page.screenshot) as shot:
                page.screenshot(path=str(image),full_page=True)
                before=image.read_bytes()
                verdict=readiness.verify_saved_render(page,'12h',1,state,image,Mock(summary=lambda:{}))
                self.assertTrue(verdict['ready']);shot.assert_called_once()
                self.assertEqual(before,image.read_bytes())
            retained=json.loads(image.with_suffix('.render.json').read_text())
            self.assertEqual(retained['render_observations'][0]['canvas_rect'],
                retained['render_observations'][1]['canvas_rect'])

    def test_persistent_no_data_uses_one_bounded_wait_and_never_reloads(self):
        page=self.page(markup(notice=NOTICE))
        started=time.monotonic()
        with patch.object(page,'goto',wraps=page.goto) as navigate,patch.object(page,'reload',wraps=page.reload) as reload:
            state=readiness.wait_for_render(page,'12h',1,150)
            navigate.assert_not_called();reload.assert_not_called()
        self.assertFalse(state['ready']);self.assertEqual(state['reason'],'no-data')
        self.assertLess(time.monotonic()-started,1)

    def test_login_gate_appearing_during_paint_does_not_spend_the_remaining_budget(self):
        page=self.page(markup()+'<div id="gate" role="dialog" hidden>Log in to unlock full data</div>')
        evaluate=page.evaluate
        def change_during_paint(script,*args):
            if script==readiness.RENDER_PAINT_SCRIPT:
                evaluate("() => document.querySelector('#gate').hidden=false")
            return evaluate(script,*args)
        started=time.monotonic()
        with patch.object(page,'evaluate',side_effect=change_during_paint):
            value=readiness.wait_for_render(page,'12h',1,2000)
        self.assertEqual(value['reason'],'login-required');self.assertFalse(value['ready'])
        self.assertLess(time.monotonic()-started,1)

    def test_geometry_change_during_capture_keeps_png_but_stops_before_hover(self):
        for model in (1,2,3):
            with self.subTest(model=model):
                page=self.page(markup(model=model,source_class=model!=3),model)
                state=readiness.wait_for_render(page,'12h',model,2000)
                with tempfile.TemporaryDirectory() as directory:
                    image=Path(directory)/'original.png';image.write_bytes(b'original evidence')
                    page.evaluate("() => document.querySelector('#panel').style.transform='translateY(0.5px)'")
                    with self.assertRaises(execution.StageFailure) as caught:
                        readiness.verify_saved_render(page,'12h',model,state,image,Mock(summary=lambda:{}))
                    self.assertEqual(caught.exception.code,'source_not_readable')
                    self.assertEqual(caught.exception._model1_source_readiness['reason'],'geometry-changed')
                    self.assertEqual(image.read_bytes(),b'original evidence')

    def test_model3_axis_change_still_rejects_even_when_geometry_is_identical(self):
        page=self.page(markup(model=3,source_class=False),3)
        state=readiness.wait_for_render(page,'12h',3,2000)
        page.evaluate("() => document.querySelector('[data-axis=y]').innerHTML='<div>88000</div><div>78000</div>'")
        with tempfile.TemporaryDirectory() as directory:
            image=Path(directory)/'original.png';image.write_bytes(b'original evidence')
            with self.assertRaises(execution.StageFailure) as caught:
                readiness.verify_saved_render(page,'12h',3,state,image,Mock(summary=lambda:{}))
            self.assertEqual(caught.exception._model1_source_readiness['reason'],'render-pending')


class ReadinessBudgetAndOrderTests(unittest.TestCase):
    def test_wait_and_paint_share_the_original_budget_without_fixed_delay(self):
        page=Mock(url=source_url(1))
        not_ready={'ready':False,'reason':'no-data','label':'12 hour'}
        ready={'ready':True,'reason':'render-checks-passed','label':'12 hour'}
        states=iter((not_ready,ready,ready))
        page.evaluate.side_effect=lambda script,*args: True if script==readiness.RENDER_PAINT_SCRIPT else next(states)
        with patch.object(readiness.time,'monotonic',side_effect=(0,0,59.8)):
            self.assertTrue(readiness.wait_for_render(page,'12h',1)['ready'])
        self.assertEqual(page.wait_for_function.call_args.kwargs['timeout'],60000)
        paints=[call for call in page.evaluate.call_args_list if call.args[0]==readiness.RENDER_PAINT_SCRIPT]
        self.assertEqual(len(paints),1)
        self.assertLessEqual(paints[0].args[1]['timeout'],200)
        page.wait_for_timeout.assert_not_called();page.goto.assert_not_called();page.reload.assert_not_called()

    def test_generated_flow_binds_ready_geometry_before_the_same_single_screenshot(self):
        from market_vision import coinglass_heatmap_capture as capture
        source=Path(capture.__file__).read_text()
        function=next(node for node in ast.parse(source).body if isinstance(node,ast.FunctionDef) and node.name=='capture_heatmaps')
        text=ast.unparse(function)
        ordered=('close_heatmap_settings(page)','dismiss_cookie_banner(page, model=HEATMAP_MODEL)',
            'wait_for_render(page, timeframe, HEATMAP_MODEL)','geometry_from_state(render_state)',
            'page.screenshot(','verify_saved_render(','capture_column(')
        positions=[text.index(value) for value in ordered]
        self.assertEqual(positions,sorted(positions))
        shots=[node for node in ast.walk(function) if isinstance(node,ast.Call) and ast.unparse(node.func)=='page.screenshot']
        self.assertEqual(len(shots),1)
        self.assertEqual(ast.unparse(next(k.value for k in shots[0].keywords if k.arg=='full_page')),'HEATMAP_MODEL != 3')
        self.assertNotIn('chart_rect = page.evaluate',text)


if __name__=='__main__':unittest.main()
