"""Isolated tests for the numeric reader's public map identity and rendered-model binding."""
from contextlib import ExitStack
import tempfile
from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock, patch

import dedicated_map_flow as flow
import modern_map_reader as reader
from model1_execution import StageFailure
from modern_map_checkpoint import _observation_receipts, partial_record
from modern_map_basic_numbers import basic_source_url


class MarketPage:
    def __init__(self, url='about:blank'):
        self.url = url
        self.model = 1
        self.range = '48H'
        self.rail_model = 1
        self.plot_model = 1
        self.loading = False
        self.navigations = []
        self.switches = []
        self.frames = []

    def goto(self, url, **options):
        self.navigations.append(url)
        self.url = url
        self.model = self.rail_model = self.plot_model = 1
        return Mock(status=200)

    def set_default_timeout(self, ms):
        pass

    def get_by_text(self, text, **options):
        if hasattr(text, 'pattern') and 'Loading' in text.pattern:
            return [1] if self.loading else []
        return [] if text is reader.GATE else [1]


class NavigationTests(TestCase):
    def simulate(self, page, target, *, stuck=None, observe=None, reset=False):
        def wait(page, read, *args, **kw):
            for _ in range(8):
                try:
                    value = read()
                    if value: return value
                except StageFailure as error:
                    if error.code != 'source_not_readable': raise
                if page.frames:
                    page.model,page.rail_model,page.plot_model,page.loading=page.frames.pop(0)
            raise StageFailure('source_not_readable')

        def select(page, name, **kw):
            if not name.startswith('Model '):
                return
            m = int(name[-1])
            if m == page.model:
                return
            old = page.model
            page.switches.append((old,m))
            page.url = reader.map_url('BTC',m)
            page.frames = [(m,old,old,False), (m,old,old,True),
                           (m, old if stuck=='sidebar' else m, old if stuck=='chart' else m, False)]
            if stuck == 'selector':
                page.frames = [(old,m,m,False)]

        def choice(page, label):
            return label == 'Coin' or label == f'Model {page.model}'

        def side(page):
            return dict(raw_price_step=str(11 * page.rail_model), raw_above_total='2', raw_below_total='1',
                        source_time='2026-11-27T16:09:00Z')

        def range_control(page, name):
            return Mock(**{'inner_text.return_value':page.range[:-1]+' hour'})

        def select_range(page, timeframe):
            page.range = timeframe
            return {'label': timeframe[:-1]+' hour', 'value':timeframe}

        with ExitStack() as stack:
            mocks = dict(wait_for=wait, choice_target=Mock(return_value=True), prefer_dark=Mock(),
                select_choice=select, choice=choice, visible=lambda values:values,
                symbol_control=Mock(return_value=True), option_control=range_control, details=side,
                rail_anchor=lambda p:50*p.rail_model, _rendered_chart=lambda p,g:bytes([10*p.plot_model])*2304,
                select_range=select_range)
            for name, fn in mocks.items():
                stack.enter_context(patch.object(reader, name, fn))
            return reader.open_verified_map(page, 'BTC', target, '48H',
                        guard=lambda:None, observe=observe, reset_to_entry=reset)

    def test_one_document_selects_models_and_returns_with_changed_side_and_plot(self):
        page=MarketPage()
        for m in (1,2,1,3,1):
            result=self.simulate(page,m)
            self.assertEqual(result['raw_price_step'],str(11*m))
            self.assertEqual(page.url,reader.map_url('BTC',m))
        self.assertEqual(page.navigations, [reader.map_url('BTC')])
        self.assertEqual(page.switches,[(1,2),(2,1),(1,3),(3,1)])

    def test_stale_chart_rail_or_selector_keeps_target_unreadable(self):
        for stuck in ('chart','sidebar','selector'):
            with self.subTest(stuck=stuck):
                events=[]
                page=MarketPage()
                with self.assertRaises(StageFailure):
                    self.simulate(page, 3, stuck=stuck, observe=lambda _,d: events.append(d))
                rec=_observation_receipts(events)['map_navigation']
                self.assertEqual(rec['observed_url'],reader.map_url('BTC',3))
                self.assertEqual(rec['phase'],'model_plot_settling')
                self.assertEqual(rec['selected_model'],1 if stuck=='selector' else 3)

    def test_untrusted_prior_is_not_read_and_dynamic_coin_starts_own_document(self):
        page=MarketPage('https://www.coinglass.com/LiquidationHeatMap/BTC?mode=coin&extra=private')
        events=[]
        with self.assertRaises(StageFailure) as failure:
            self.simulate(page,2,observe=lambda _,d: events.append(d))
        self.assertEqual(failure.exception.code,'source_navigation_changed')
        self.assertFalse(page.navigations)
        self.assertNotIn('private',str(_observation_receipts(events)))
        cross=MarketPage(reader.map_url('ETH',3))
        self.simulate(cross,2)
        self.assertEqual(cross.navigations,[reader.map_url('BTC')])
        landing=MarketPage('https://www.coinglass.com/signin')
        self.simulate(landing,1,reset=True)
        self.assertEqual(landing.navigations,[reader.map_url('BTC')])

    def test_explicit_auth_and_cooldown_evidence_retain_authority(self):
        target=flow.MapTarget('ETH',3,'48H')
        page=Mock(url=reader.map_url('ETH',3))
        for code in ('source_rate_limited','auth_challenge','source_login_required'):
            with self.subTest(code=code), patch.object(flow,'guard_surface',side_effect=StageFailure(code)):
                with self.assertRaises(StageFailure) as caught:
                    flow._checked_action(Mock(side_effect=StageFailure('source_not_readable')), page,target)
                self.assertEqual(caught.exception.code,code)
                self.assertEqual(caught.exception.first_failure_code,'source_not_readable')
        with patch.object(flow,'guard_surface',side_effect=StageFailure('source_navigation_changed')):
            original=StageFailure('source_not_readable')
            with self.assertRaises(StageFailure) as caught:
                flow._checked_action(Mock(side_effect=original),page,target)
            self.assertIs(caught.exception,original)
            self.assertEqual(caught.exception.secondary_guard_code,'source_navigation_changed')

    def test_shared_provider_block_on_previous_model_stops_without_auth_from_wrong_map(self):
        target=flow.MapTarget('BTC',3,'48H')
        prior=Mock(url=reader.map_url('BTC',2))
        for signal, expected in [('login_rate_limited','source_rate_limited'),
                                 ('login_captcha_required','auth_challenge'),
                                 ('login_form_visible','source_navigation_changed')]:
            with self.subTest(signal=signal), patch.object(flow,'visible_login_blocker',return_value=signal):
                with self.assertRaises(StageFailure) as stopped:
                    flow.guard_surface(prior,target)
                self.assertEqual(stopped.exception.code,expected)
        prior.url='https://untrusted.example/LiquidationHeatMap/BTC?mode=coin&model=3'
        with patch.object(flow,'visible_login_blocker') as inspect:
            with self.assertRaises(StageFailure) as stopped:
                flow.guard_surface(prior,target)
            inspect.assert_not_called()
            self.assertEqual(stopped.exception.code,'source_navigation_changed')

    def test_failed_confirmation_cannot_open_an_auth_incident(self):
        page=Mock(url=reader.map_url('ETH',3))
        session=Mock(state={'cookies':[{}]})
        session.source_cooldown.return_value={'code':'allowed'}
        with patch.object(flow,'open_verified_map',side_effect=StageFailure('source_login_required')), \
             patch.object(flow,'guard_surface',side_effect=StageFailure('source_navigation_changed')):
            with self.assertRaises(StageFailure) as caught:
                flow.collect_with_dedicated_session(session,Mock(),page,flow.MapTarget('ETH',3,'48H'),Mock())
        self.assertEqual(caught.exception.code,'source_not_readable')
        self.assertEqual(caught.exception.first_failure_code,'source_login_required')
        session.auth_start.assert_not_called()
        session.credentials.assert_not_called()

    def test_route_binding_and_diagnostic_record_are_dimension_safe(self):
        for coin in ('BTC','ETH'):
            for m in (1,2,3):
                expect=f'https://www.coinglass.com/LiquidationHeatMap/{coin}?mode=coin'+(f'&model={m}' if m>1 else '')
                self.assertEqual(reader.map_url(coin,m),expect)
                self.assertEqual(basic_source_url(coin,m),expect)
        bad=StageFailure('source_not_readable')
        bad.secondary_guard_code='source_navigation_changed'
        with tempfile.TemporaryDirectory() as path:
            record=partial_record('job',{},[],[],bad,Path(path)/'no.png',identity=('ETH',3,'1W'))
        self.assertEqual(record['failure']['condition'],'source_not_readable')
        self.assertEqual(record['failure']['secondary_guard_code'],'source_navigation_changed')
        self.assertIsNone(record['image_sha256'])

    def test_lane_commits_original_partial_and_logs_identity_before_replacing_page(self):
        import asyncio
        import json
        from uuid import uuid4
        from unittest.mock import AsyncMock
        import modern_map_cycle_child as child
        jobs=[str(uuid4()),str(uuid4())]
        sequence=[]
        pending=[]
        for job in jobs:
            pending.extend(([job,'BTC',3,'48H','trace_source'], 'committed'))
        pending.append(None)
        async def read_page(page, target, image, observe):
            err=StageFailure('source_not_readable')
            err.basic_reason='old_plot_remains'
            err.secondary_guard_code='source_navigation_changed'
            observe('stage',dict(name='map_navigation',status='failed',condition=err.basic_reason,
                navigation={'expected_url':reader.map_url('BTC',3),'observed_url':page.raw.url,
                            'phase':'model_plot_settling','selected_model':3,'mode_coin':True,
                            'loading':False,'plot_populated':True}))
            raise err
        async def close():
            sequence.append('physical-close')
        async def write(stream, message):
            sequence.append(message)
        def capture_line(line, *args, **kwargs):
            if line.startswith('COINGLASS_CYCLE_FAILURE '):
                sequence.append(json.loads(line.split(' ',1)[1]))
        root=Path(self.enterContext(tempfile.TemporaryDirectory()))
        pages=[]
        for job in jobs:
            (root/job).mkdir()
            raw=Mock(url=reader.map_url('BTC',3), close=AsyncMock(side_effect=close))
            pages.append(raw)
        ctx=Mock()
        ctx.raw.browser.is_connected.return_value=True
        ctx.raw.new_page=AsyncMock(side_effect=pages)
        ctx.wrap.side_effect=lambda raw:Mock(raw=raw)
        account=Mock(read=AsyncMock(side_effect=read_page))
        with patch.object(child,'read',new=AsyncMock(side_effect=pending)), patch.object(child,'write',new=write), \
             patch('builtins.print',side_effect=capture_line):
            asyncio.run(child.lane(Mock(),Mock(),ctx,account,root,index=1))
        reports=[e for e in sequence if isinstance(e,dict)]
        self.assertEqual(len(reports),2)
        for job, report in zip(jobs, reports):
            partial=json.loads((root/job/'measured-partial.json').read_text())
            self.assertEqual(report['job_id'],job)
            self.assertEqual(report['last_failed_stage'],'map_navigation')
            self.assertEqual(report['secondary_guard_code'],'source_navigation_changed')
            self.assertEqual(partial['failure']['condition'],'old_plot_remains')
            self.assertEqual(partial['map_navigation']['phase'],'model_plot_settling')
            self.assertEqual(partial['image_sha256'],None)
            self.assertEqual(sequence[sequence.index(report)+1],'physical-close')
        self.assertEqual(sequence.count(['save','P']),2)
        self.assertEqual(sequence.count(['done','source_not_readable']),2)
        self.assertEqual([r['page_number'] for r in reports],[1,2])
