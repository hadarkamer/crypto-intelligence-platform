"""Check the cooperative adapter using actual public Playwright handles, without contacting a site."""
import asyncio
from contextvars import ContextVar
from pathlib import Path
import unittest
from weakref import WeakValueDictionary

from playwright.async_api import Page, async_playwright

from modern_map_cycle_browser import ReaderPageAPI, run_reader


class CyclePlaywrightAPI(unittest.IsolatedAsyncioTestCase):
    async def test_concurrent_reader_calls_keep_page_and_context_identity(self):
        # Two actual Playwright Page wrappers. The underlying async transport
        # uses a barrier so the test fails if a worker blocks the owner's loop.
        identity = ContextVar("map_identity", default=None)
        gate = asyncio.Event()
        pending = []
        class InMemoryTransport:
            _loop = asyncio.get_running_loop()
            def __init__(self, title):
                self.name = title
            async def title(self):
                pending.append(self.name)
                if len(pending) == 2:
                    gate.set()
                await asyncio.wait_for(gate.wait(), 2)
                return self.name
        cache = WeakValueDictionary()
        wrap = ReaderPageAPI(None, cache).wrap
        pages = [wrap(Page(InMemoryTransport(s))) for s in ("BTC:1:12H", "DOGE:3:1W")]
        def check(page, expected):
            identity.set(expected)
            self.assertIs(wrap(page.raw), page)
            self.assertEqual(page.title(), expected)
            self.assertEqual(identity.get(), expected)
        await asyncio.gather(*(run_reader(check, p, key) for p, key in zip(pages, ("BTC:1:12H", "DOGE:3:1W"))))
        self.assertCountEqual(pending, ("BTC:1:12H", "DOGE:3:1W"))
        self.assertIsNone(identity.get())

    async def test_firefox_two_tabs_with_offline_html(self):
        # The driver runs in either case. The full DOM test requires the
        # packaged engine, and never downloads an alternative browser.
        async with async_playwright() as pw:
            if not Path(pw.firefox.executable_path).is_file():
                self.skipTest("packaged Playwright Firefox executable absent")
            browser = await pw.firefox.launch(headless=True, timeout=20000)
            try:
                context = await browser.new_context(offline=True, locale="en-US", timezone_id="UTC")
                try:
                    await context.route("**/*", lambda route: route.abort())
                    cache = WeakValueDictionary()
                    wrapped = ReaderPageAPI(context, cache)
                    cache[context] = wrapped
                    pages = [wrapped.wrap(await context.new_page()) for _ in range(2)]
                    def check(page, name):
                        page.set_content(f"<main><output>{name}</output></main>")
                        node = page.locator("output")
                        self.assertEqual(node.inner_text(), name)
                        handle = node.element_handle()
                        try:
                            self.assertEqual(page.evaluate("node => node.textContent", handle), name)
                        finally:
                            handle.dispose()
                        self.assertIs(page.context, wrapped)
                    await asyncio.gather(run_reader(check,pages[0],"BTC:1:12H"),run_reader(check,pages[1],"DOGE:3:1W"))
                    await run_reader(check,pages[0],"ZEC:2:48H")
                    self.assertEqual(await pages[1].raw.locator("output").inner_text(), "DOGE:3:1W")
                    await pages[0].raw.close()
                    self.assertFalse(pages[1].raw.is_closed())
                finally:
                    await context.close()
            finally:
                await browser.close()
