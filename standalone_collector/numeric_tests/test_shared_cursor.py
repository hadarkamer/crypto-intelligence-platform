"""Real headless Firefox, one context, two pages, and the collector's greenlet adapter."""
import asyncio
from datetime import datetime, timezone
from unittest import IsolatedAsyncioTestCase
from weakref import WeakValueDictionary

from playwright.async_api import async_playwright
from model1_execution import StageFailure
from modern_map_cursor import read_cursor_pair
from modern_map_cycle_browser import ReaderPageAPI, run_reader, wait
from test_cursor_columns import HTML


class SharedCursorTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        driver = await async_playwright().start()
        self.addAsyncCleanup(driver.stop)
        browser = await driver.firefox.launch()
        self.addAsyncCleanup(browser.close)
        context = await browser.new_context(viewport={'width': 820, 'height': 510}, timezone_id='UTC')
        cache = WeakValueDictionary()
        adapter = ReaderPageAPI(context, cache)
        self.pages = []
        for _ in range(2):
            page = await context.new_page()
            await page.set_content(HTML)
            await page.evaluate("""()=>{cfg={prior:'11/25 21:30',latest:'11/27 21:30',
                relative:'2 d 0 min before the latest',layer:'canvas'};draw()}""")
            self.pages.append(adapter.wrap(page))

    async def compete(self, role, fail=False):
        first, second = self.pages
        pointer_requested, peer_inspected = asyncio.Event(), asyncio.Event()
        frames = []
        overlap = []
        second_move = None

        class Watch:
            def __init__(self, handle):
                self.handle = handle

            def __getattr__(self, name):
                return getattr(self.handle, name)

            def evaluate(self, expression, *args):
                frame = self.handle.evaluate(expression, *args)
                if expression == 'w=>w.cursorRead()' and not pointer_requested.is_set():
                    headers = frame['tooltip']
                    phase = 'latest' if headers and headers[0][1].startswith('Latest column') else 'prior'
                    if headers and phase == role:
                        frames.append(frame)
                        pointer_requested.set()
                        wait(peer_inspected.wait())
                return frame

        class Page:
            def __getattr__(self, name):
                return getattr(first, name)

            def evaluate_handle(self, expression, *args):
                return Watch(first.evaluate_handle(expression, *args))

        async def peer():
            nonlocal second_move
            await pointer_requested.wait()
            # Issue the ordinary Playwright move. A separate non-pointer read on the
            # same page is free to complete, including while that input is waiting.
            second_move = asyncio.create_task(run_reader(second.mouse.move, 5, 20))
            await asyncio.sleep(0)
            await run_reader(second.evaluate, '()=>document.querySelector("canvas").width')
            overlap.append(not second_move.done())
            peer_inspected.set()
            await second_move

        worker = asyncio.create_task(peer())
        try:
            if fail:
                await first.raw.evaluate("()=>{cfg.fakeLatest='30 min before the latest'}")
            return await run_reader(read_cursor_pair, Page(), (20, 40, 640, 380), (31, 58, 610, 375),
                datetime(2026, 11, 27, 21, 42, tzinfo=timezone.utc), '1W', '30m')
        finally:
            await asyncio.wait_for(worker, 6)
            self.assertEqual(len(frames), 1)
            self.assertEqual(overlap, [True])
            self.assertTrue(second_move.done())

    async def test_peer_native_input_cannot_displace_either_cursor_sample(self):
        for role in ('prior', 'latest'):
            with self.subTest(role=role):
                _, _, proof = await self.compete(role)
                self.assertEqual(proof['latest_time'], '2026-11-27T21:30:00Z')

    async def test_wrong_neighbor_is_rejected_and_peer_finishes(self):
        with self.assertRaises(StageFailure) as caught:
            await self.compete('prior', fail=True)
        self.assertEqual(caught.exception.basic_reason, 'latest_cursor_time_unconfirmed')

    async def test_cleanup_cannot_hide_first_cursor_or_account_failure(self):
        from contextlib import contextmanager
        from unittest.mock import patch
        from modern_map_reader import PAINT
        tab = self.pages[0]
        original_handle = tab.evaluate_handle
        cleanup_errors = []

        class Watch:
            def __init__(self, handle):
                self.handle = handle

            def __getattr__(self, name):
                return getattr(self.handle, name)

            def evaluate(self, expression, *args):
                value = self.handle.evaluate(expression, *args)
                if expression == 'w=>w.restore()':
                    cleanup_errors.append(True)
                    raise RuntimeError('test cleanup transport loss')
                return value

        def wrap(expression, *args):
            handle = original_handle(expression, *args)
            return Watch(handle) if expression == PAINT else handle

        def read():
            return read_cursor_pair(tab, (20, 40, 640, 380), (31, 58, 610, 375),
                 datetime(2026, 11, 27, 21, 42, tzinfo=timezone.utc), '1W', '30m')

        @contextmanager
        def blocked():
            raise StageFailure('source_rate_limited')
            yield

        with patch.object(tab, 'evaluate_handle', wrap):
            await tab.raw.evaluate("()=>{cfg.fakeLatest='30 min before the latest'}")
            with self.assertRaises(StageFailure) as local:
                await run_reader(read)
            self.assertEqual(local.exception.basic_reason, 'latest_cursor_time_unconfirmed')
            with patch.object(tab, 'pointer_scope', blocked), self.assertRaises(StageFailure) as shared:
                await run_reader(read)
            self.assertEqual(shared.exception.code, 'source_rate_limited')
        self.assertEqual(len(cleanup_errors), 2)

    async def test_contended_input_uses_reader_budget_before_native_move(self):
        from modern_map_reader import map_access_budget
        first, second = self.pages
        held, release = asyncio.Event(), asyncio.Event()
        await second.raw.evaluate("()=>{window.inputCount=0;c.addEventListener('pointermove',()=>inputCount++)}")

        def owner():
            with first.pointer_scope():
                held.set()
                wait(release.wait())

        def bounded_move():
            with map_access_budget(.05):
                second.mouse.move(447, 216)

        task = asyncio.create_task(run_reader(owner))
        try:
            await held.wait()
            with self.assertRaises(StageFailure) as timed_out:
                await asyncio.wait_for(run_reader(bounded_move), 1.5)
            self.assertEqual(timed_out.exception.code, 'source_timeout')
            self.assertEqual(await second.raw.evaluate('()=>inputCount'), 0)
        finally:
            release.set()
            await task
        await run_reader(second.mouse.move, 447, 216)
        self.assertEqual(await second.raw.evaluate('()=>inputCount'), 1)
