"""Offline DOM replay of the real 15 px extent mismatch with synthetic, private-data-free pixels."""
import unittest
from playwright.sync_api import sync_playwright
from modern_map_reader import chart_box, market_surface, position_chart
from model1_execution import StageFailure

class NativeGeometry(unittest.TestCase):
    def test_real_firefox_scroll_crop_and_restore(self):
        with sync_playwright() as pw, pw.firefox.launch(headless=True) as browser:
            ctx = browser.new_context(viewport={'width':1440, 'height':1280}, device_scale_factor=1,
                                      locale='en-US', timezone_id='UTC', offline=True)
            requests = []
            ctx.on('request',lambda r:requests.append(r.url))
            page = ctx.new_page()
            page.set_content('''<!DOCTYPE html><style>
                body{height:1600px;margin:0;background:#101422}
                canvas{position:absolute;top:495px;left:17px}
                button{position:absolute;top:80px;left:24px}
            </style><button>Model 1</button><canvas width="1074" height="800"></canvas>
            <script>const g=document.querySelector("canvas").getContext("2d");
            g.fillStyle="#1c2348"; g.fillRect(0,0,1074,800);
            g.fillStyle="#df4620"; g.fillRect(0,0,1074,2);
            g.fillStyle="#38e8cf"; g.fillRect(0,798,1074,2);</script>''')
            original = chart_box(page)
            self.assertEqual(original, (17,495,1074,800))
            with self.assertRaises(StageFailure) as before:
                market_surface(page,original,[],None)
            self.assertEqual(before.exception.reason,'map_clip_outside_viewport')
            page.evaluate('window.scopeGuard=0')
            def guard():
                self.assertEqual(page.url,'about:blank')
                page.evaluate('window.scopeGuard++')
            with position_chart(page,guard) as shown:
                self.assertEqual(shown,(17,480,1074,800))
                self.assertEqual(page.evaluate('scrollY'),15)
                control=page.locator('button').bounding_box()
                self.assertEqual(control['y'],65)
                with market_surface(page,shown,[control],guard) as image:
                    self.assertEqual(image.getpixel((20,480)),(223,70,32))
                    self.assertEqual(image.getpixel((20,1279)),(56,232,207))
                self.assertGreaterEqual(page.evaluate('window.scopeGuard'),6)
            self.assertEqual(page.evaluate('scrollY'),0)
            page.locator('button').evaluate("e=>e.style.top='4px'")
            with self.assertRaises(StageFailure) as clipped, position_chart(page,guard) as shown:
                market_surface(page,shown,[page.locator('button').bounding_box()],guard)
            self.assertEqual(clipped.exception.reason,'map_clip_outside_viewport')
            self.assertEqual(clipped.exception.market_clip['rect_index'],1)
            self.assertEqual(page.evaluate('scrollY'),0)
            self.assertEqual(requests,[])
            print('Offline Firefox',browser.version,'before=495+800>1280 after=480+800=1280; both edge rows verified; clipped control rejected; rollback=0; requests=0')
