"""Page ownership regressions; fake Playwright only, no network or browser."""
import asyncio
import unittest
from unittest.mock import AsyncMock, patch

import coinglass_dom_reader as reader


class FakePage:
    def __init__(self, *, fail_at=None, error=None, block_at=None, close_error=None):
        self.fail_at = fail_at
        self.error = error
        self.block_at = block_at
        self.close_error = close_error
        self.entered = asyncio.Event()
        self.calls = []
        self.close_calls = 0
        self.closed = False
        self.first = self

    async def _operation(self, name):
        if self.block_at == name:
            self.entered.set()
            await asyncio.Event().wait()
        if self.fail_at == name:
            raise self.error

    async def goto(self, url, **kwargs):
        self.calls.append(("goto", url, kwargs))
        await self._operation("goto")

    async def wait_for_timeout(self, milliseconds):
        self.calls.append(("wait", milliseconds))
        await self._operation("initial_wait" if milliseconds == 8000 else "modal_wait")

    def get_by_text(self, label, **kwargs):
        self.calls.append(("label", label, kwargs))
        return self

    async def click(self, **kwargs):
        self.calls.append(("click", kwargs))
        await self._operation("click")

    async def close(self):
        self.close_calls += 1
        self.closed = True
        if self.close_error is not None:
            raise self.close_error


class FakeContext:
    def __init__(self, pages):
        self.pages = pages
        self.created = []
        self.max_active = 0

    async def new_page(self):
        page = self.pages[len(self.created)]
        self.created.append(page)
        self.max_active = max(
            self.max_active, sum(not candidate.closed for candidate in self.created)
        )
        return page


class PageOwnershipTests(unittest.IsolatedAsyncioTestCase):
    async def test_navigation_failure_closes_page_and_preserves_error(self):
        error = reader.PlaywrightTimeoutError("navigation fixture timeout")
        page = FakePage(fail_at="goto", error=error)
        with self.assertRaises(reader.PlaywrightTimeoutError) as result:
            await reader._new_ready_page(FakeContext([page]), "fixture://page")
        self.assertIs(result.exception, error)
        self.assertEqual(page.close_calls, 1)

    async def test_initial_wait_failure_closes_page_and_preserves_error(self):
        error = RuntimeError("setup fixture failed")
        page = FakePage(fail_at="initial_wait", error=error)
        with self.assertRaises(RuntimeError) as result:
            await reader._new_ready_page(FakeContext([page]), "fixture://page")
        self.assertIs(result.exception, error)
        self.assertEqual(page.close_calls, 1)

    async def _check_cancellation(self, stage):
        page = FakePage(block_at=stage)
        task = asyncio.create_task(
            reader._new_ready_page(FakeContext([page]), "fixture://page")
        )
        await asyncio.wait_for(page.entered.wait(), timeout=1)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(page.close_calls, 1)

    async def test_navigation_cancellation_closes_page(self):
        await self._check_cancellation("goto")

    async def test_initial_wait_cancellation_closes_page(self):
        await self._check_cancellation("initial_wait")

    async def test_modal_click_cancellation_closes_page(self):
        await self._check_cancellation("click")

    async def test_modal_wait_cancellation_closes_page(self):
        await self._check_cancellation("modal_wait")

    async def test_cleanup_failure_does_not_replace_setup_failure(self):
        error = reader.PlaywrightTimeoutError("original fixture failure")
        for cleanup_error in (RuntimeError("close failed"), asyncio.CancelledError()):
            with self.subTest(cleanup_error=type(cleanup_error).__name__):
                page = FakePage(fail_at="goto", error=error, close_error=cleanup_error)
                with self.assertRaises(reader.PlaywrightTimeoutError) as result:
                    await reader._new_ready_page(FakeContext([page]), "fixture://page")
                self.assertIs(result.exception, error)
                self.assertEqual(page.close_calls, 1)

    async def test_success_transfers_handle_without_changing_setup(self):
        page = FakePage()
        result = await reader._new_ready_page(FakeContext([page]), "fixture://page")
        self.assertIs(result, page)
        self.assertEqual(page.close_calls, 0)
        expected = [
            ("goto", "fixture://page", {"wait_until": "domcontentloaded", "timeout": 60_000}),
            ("wait", 8000),
        ]
        for label in ["Accept", "I Agree", "Got it", "Close", "×"]:
            expected.extend([
                ("label", label, {"exact": True}),
                ("click", {"timeout": 1000}),
                ("wait", 400),
            ])
        self.assertEqual(page.calls, expected)
        await result.close()
        self.assertEqual(page.close_calls, 1)

    async def test_optional_modal_failure_still_returns_open_handle(self):
        page = FakePage(fail_at="click", error=reader.PlaywrightTimeoutError("no modal"))
        result = await reader._new_ready_page(FakeContext([page]), "fixture://page")
        self.assertIs(result, page)
        self.assertEqual(page.close_calls, 0)
        self.assertEqual(sum(call[0] == "click" for call in page.calls), 5)
        await result.close()
        self.assertEqual(page.close_calls, 1)

    async def test_failed_retries_do_not_accumulate_open_pages(self):
        pages = [
            FakePage(fail_at="goto", error=reader.PlaywrightTimeoutError("fixture timeout"))
            for _ in range(3)
        ]
        context = FakeContext(pages)
        with patch.object(reader.asyncio, "sleep", new=AsyncMock()):
            result = await reader._retry_timeframe_on_fresh_page(
                context, "fixture://page", "24h", None, attempts=3
            )
        self.assertFalse(result["verified"])
        self.assertEqual(len(context.created), 3)
        self.assertEqual(context.max_active, 1)
        self.assertEqual([page.close_calls for page in pages], [1, 1, 1])

    async def test_successful_retry_keeps_caller_cleanup_exactly_once(self):
        page = FakePage()
        accepted = {"verified": True, "rows": [{"symbol": "FIXTURE"}]}
        with patch.object(reader, "read_timeframe", new=AsyncMock(return_value=accepted)):
            result = await reader._retry_timeframe_on_fresh_page(
                FakeContext([page]), "fixture://page", "24h", None, attempts=3
            )
        self.assertIs(result, accepted)
        self.assertEqual(page.close_calls, 1)


if __name__ == "__main__":
    unittest.main()
