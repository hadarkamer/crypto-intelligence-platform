"""Diagnostic observations preserve the existing render decisions and calls."""
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE/'runtime'))
import capture_readiness as readiness
from heatmap_models import source_url


class RenderObservationTests(unittest.TestCase):
    def observed(self,nodata,at):
        return {'ready':not nodata,'reason':'no-data' if nodata else 'render-checks-passed','label':'12 hour',
            '_render_observation':{'observed_ms':at,'visible_nodata':nodata,
                'visible_request_limit':False,'canvas_count':1,
                'canvas_rect':{'x':300.5,'y':535.796875,'width':1255,'height':730},
                'viewport':{'width':1600,'height':1000},'scroll_x':0,'scroll_y':556,'page_width':1600}}

    def test_resolved_no_data_is_not_bound_to_a_stale_before_screenshot_state(self):
        page=Mock(url=source_url(1))
        states=iter([self.observed(True,100.25),self.observed(False,180.75),
            self.observed(False,200.5),self.observed(False,240.75)])
        page.evaluate.side_effect=lambda script,*args: True if script==readiness.RENDER_PAINT_SCRIPT else next(states)
        state=readiness.wait_for_render(page,'12h',1)
        with tempfile.TemporaryDirectory() as directory:
            image=Path(directory)/'coinglass_btc_heatmap_12h.png'
            image.write_bytes(b'original')
            verdict=readiness.verify_saved_render(page,'12h',1,state,image,Mock(summary=lambda:{}))
            self.assertEqual(verdict,{'ready':True,'reason':'render-checks-passed','label':'12 hour'})
            saved=json.loads(image.with_suffix('.render.json').read_text())
            observations=saved['render_observations']
            self.assertEqual([v['stage'] for v in observations],['before_screenshot','after_screenshot'])
            self.assertEqual([v['visible_nodata'] for v in observations],[False,False])
            self.assertEqual([v['observed_ms'] for v in observations],[200.5,240.75])
            self.assertEqual(observations[0]['canvas_rect']['y'],535.796875)
            self.assertEqual(image.read_bytes(),b'original')
            self.assertLess(image.with_suffix('.render.json').stat().st_size,8192)
        self.assertEqual(page.evaluate.call_count,5)
        page.wait_for_function.assert_called_once()
        page.wait_for_timeout.assert_not_called()
        page.goto.assert_not_called();page.reload.assert_not_called();page.screenshot.assert_not_called()

    def test_missing_and_untrusted_observations_do_not_change_original_state(self):
        page=Mock(url=source_url(1))
        raw=self.observed(True,1)
        raw['_render_observation']={'visible_nodata':True,'observed_ms':float('nan'),
            'canvas_count':True,'canvas_rect':{'x':1,'y':float('inf'),'width':10,'height':20},
            'url':'https://private/?token=SECRET','cookies':'SECRET','text':'SECRET'}
        page.evaluate.return_value=raw
        result=readiness.current_state(page,'12h',1)
        self.assertFalse(result['ready'])
        self.assertEqual(result['reason'],'no-data')
        self.assertEqual(result['_render_observation'],{'visible_nodata':True})
        clean=readiness.safe_render_observations([
            {**raw,**raw['_render_observation'],'stage':'before_screenshot','label':'SECRET'},
            {'stage':'SECRET','visible_nodata':True},
            {'stage':'after_screenshot','visible_nodata':False}])
        self.assertEqual(len(clean),1)
        self.assertNotIn('SECRET',json.dumps(clean,allow_nan=False))
        self.assertNotIn('canvas_rect',clean[0])

    def test_real_dom_no_data_blocks_only_the_selected_chart_region(self):
        from playwright.sync_api import sync_playwright
        with sync_playwright() as driver:
            browser=driver.chromium.launch(headless=True)
            try:
                page=browser.new_page(viewport={'width':1000,'height':800})
                page.route('**/*',lambda route:route.abort())
                markup='<section><button role="combobox">12 hour</button><div style="position:relative;width:800px;height:300px"><canvas width="800" height="300"></canvas>{}</div></section>'
                page.set_content(markup.format('<img alt="nodata" style="position:absolute;left:100px;top:100px;width:40px;height:40px"><div role="status" style="position:absolute;left:100px;top:150px">Too many requests</div>'))
                observed=page.evaluate(readiness.STATE_SCRIPT)
                self.assertFalse(observed['ready'])
                self.assertEqual(observed['reason'],'request-limit')
                self.assertTrue(observed['_render_observation']['visible_nodata'])
                self.assertTrue(observed['_render_observation']['visible_request_limit'])
                self.assertEqual(observed['_render_observation']['canvas_count'],1)
                page.set_content(markup.format('<img alt="nodata" style="display:none;width:40px;height:40px">'))
                hidden=page.evaluate(readiness.STATE_SCRIPT)
                self.assertTrue(hidden['ready'])
                self.assertFalse(hidden['_render_observation']['visible_nodata'])
                page.set_content(markup.format('')+'<div role="status">No data</div>')
                unrelated=page.evaluate(readiness.STATE_SCRIPT)
                self.assertTrue(unrelated['ready'])
                self.assertFalse(unrelated['_render_observation']['visible_nodata'])
                page.set_content('<button role="combobox">12 hour</button><img alt="nodata" style="width:40px;height:40px">')
                absent=page.evaluate(readiness.STATE_SCRIPT)
                self.assertFalse(absent['ready'])
                self.assertEqual(absent['reason'],'no-chart')
                self.assertFalse(absent['_render_observation']['visible_nodata'])
                self.assertEqual(absent['_render_observation']['canvas_count'],0)
            finally:browser.close()


if __name__=='__main__':unittest.main()
