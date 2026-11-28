"""Bind Coin mode to the semantic state of its visible Pair/Coin control group."""
from unittest import TestCase

from playwright.sync_api import sync_playwright

from model1_execution import StageFailure
from modern_map_reader import choice, choice_target, select_choice, symbol_control


class CoinModeTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.driver=sync_playwright().start()
        cls.browser=cls.driver.firefox.launch(headless=True)

    @classmethod
    def tearDownClass(cls):
        cls.browser.close()
        cls.driver.stop()

    def setUp(self):
        self.page=self.browser.new_page()
        self.addCleanup(self.page.close)

    def render(self,mode,extra=''):
        self.page.set_content(f'''<header>{mode}{extra}<button aria-label="Coin">BTC</button></header>
            <script>window.clicks=[];document.addEventListener('click',e=>clicks.push(e.target.innerText));</script>''')

    def test_css_only_aria_only_and_agreeing_selected_signals(self):
        for coin,pair in [('class="ui_on__source"',''),('aria-selected="true"','aria-selected="false"'),
                          ('aria-pressed="true" data-state="on" class="ui_selected__toggle"','data-state="off"'),
                          ('data-state="active"','data-state="inactive"')]:
            with self.subTest(coin=coin):
                self.render(f'<ul><li {pair}><span>Pair</span></li><li {coin}><span>Coin</span></li></ul>')
                self.assertTrue(choice(self.page,'Coin'))
                self.assertEqual(choice_target(self.page,'Coin').inner_text(),'Coin')
                self.assertEqual(symbol_control(self.page,'BTC').inner_text(),'BTC')
                select_choice(self.page,'Coin',requested=False)
                self.assertEqual(self.page.evaluate('()=>clicks'),[])

    def test_conflicting_states_duplicate_or_absent_options_rejected(self):
        for body in [
            '<li>Pair</li><li class=ui_on__toggle aria-selected=false>Coin</li>',
            '<li>Pair</li><li aria-selected=true aria-pressed=false>Coin</li>',
            '<li>Pair</li><li data-state=inactive class=active>Coin</li>',
            '<li>Pair</li><li class=active><button aria-pressed=false>Coin</button></li>',
            '<li>Pair</li><li aria-pressed=mixed>Coin</li>',
            '<li class=on>Pair</li><li class=on>Coin</li>',
            '<li>Pair</li><li>Coin</li>', '<li class=active>Coin</li>',
            '<li>Other</li><li class=active>Coin</li>', '<li>Pair</li><li>Pair</li><li class=active>Coin</li>',
            '<li>Pair</li><li class=active>Coin</li><li>Other</li>',
        ]:
            with self.subTest(body=body):
                self.render('<ul>'+body+'</ul>')
                self.assertFalse(choice(self.page,'Coin'))
                with self.assertRaises(StageFailure):choice_target(self.page,'Coin')
        group='<ul><li>Pair</li><li class=active>Coin</li></ul>'
        self.render(group,group)
        self.assertFalse(choice(self.page,'Coin'))
        self.render(group,'<ul hidden><li>Coin</li><li class=active>Pair</li></ul>')
        self.assertTrue(choice(self.page,'Coin'))

    def test_sizing_label_is_one_option_mixed_label_is_unresolved(self):
        for copies in ('<span hidden>Coin</span>', '<span aria-hidden="true">Coin</span>', '<span>Coin</span>'):
            with self.subTest(copies=copies):
                self.render(f'<ul><li><span>Pair</span><span>Pair</span></li>'
                            f'<li class=active><span>Coin</span>{copies}</li></ul>')
                self.assertTrue(choice(self.page,'Coin'))
        self.render('<ul><li>Pair</li><li class=active><span>Coin</span><span>Pair</span></li></ul>')
        self.assertFalse(choice(self.page,'Coin'))

    def test_transient_conflict_rechecks_without_clicking_and_pair_can_switch(self):
        self.render('<div role=tablist><button id=pair role=tab aria-selected=true>Pair</button>'
                    '<button id=coin role=tab aria-selected=false>Coin</button></div>')
        page=self.page
        self.assertFalse(choice(page,'Coin'))
        page.evaluate("""()=>{coin.onclick=()=>{pair.setAttribute('aria-selected','false');
                      coin.setAttribute('aria-selected','true')}}""")
        select_choice(page,'Coin',requested=False)
        self.assertEqual(page.evaluate('()=>clicks'),['Coin'])
        self.assertTrue(choice(page,'Coin'))
        page.evaluate("""()=>{window.clicks=[];coin.className='ui_on__toggle';coin.setAttribute('aria-selected','false');
                      setTimeout(()=>coin.setAttribute('aria-selected','true'),220)}""")
        select_choice(page,'Coin',requested=False)
        self.assertTrue(choice(page,'Coin'))
        self.assertEqual(page.evaluate('()=>clicks'),[])
