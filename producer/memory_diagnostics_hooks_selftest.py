"""Diagnostics failures must not alter real page ownership or main boundaries."""
import ast
import asyncio
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import coinglass_dom_reader as reader
from coinglass_dom_cleanup_selftest import FakeContext, FakePage


class DomDiagnosticsFailureTests(unittest.IsolatedAsyncioTestCase):
    def failing_sink(self):
        return patch.object(reader.runtime_memory_diagnostics, "emit_memory_sample",
                            side_effect=RuntimeError("diagnostic fixture failure"))

    async def test_success_keeps_page_owned_by_caller_when_diagnostics_fail(self):
        page = FakePage()
        with self.failing_sink() as sink:
            result = await reader._new_ready_page(FakeContext([page]), "fixture://page")
        self.assertIs(result, page)
        self.assertEqual(page.close_calls, 0)
        self.assertEqual(page.calls[0], (
            "goto", "fixture://page", {"wait_until": "domcontentloaded", "timeout": 60_000}))
        self.assertEqual(sum(call[0] == "click" for call in page.calls), 5)
        self.assertTrue(sink.called)
        await result.close()
        self.assertEqual(page.close_calls, 1)

    async def test_navigation_error_and_cleanup_survive_diagnostic_failure(self):
        error = reader.PlaywrightTimeoutError("original navigation fixture timeout")
        page = FakePage(fail_at="goto", error=error)
        with self.failing_sink(), self.assertRaises(reader.PlaywrightTimeoutError) as result:
            await reader._new_ready_page(FakeContext([page]), "fixture://page")
        self.assertIs(result.exception, error)
        self.assertEqual(page.close_calls, 1)

    async def test_cancellation_and_cleanup_survive_diagnostic_failure(self):
        page = FakePage(block_at="goto")
        with self.failing_sink():
            task = asyncio.create_task(
                reader._new_ready_page(FakeContext([page]), "fixture://page"))
            try:
                await asyncio.wait_for(page.entered.wait(), timeout=1)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
            finally:
                if not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
        self.assertEqual(page.close_calls, 1)


def load_main_diagnostic_hook(counter, sink, state):
    """Run the production helper body without importing application startup."""
    source = Path(__file__).with_name("main.py")
    tree = ast.parse(source.read_text())
    nodes = [node for node in tree.body
             if isinstance(node, ast.FunctionDef) and node.name == "_record_runtime_memory"]
    if len(nodes) != 1:
        raise AssertionError("Expected one production diagnostic hook")
    scope = {
        "coinglass_history_backfill": SimpleNamespace(reference_cache_memory_counts=counter),
        "runtime_memory_diagnostics": SimpleNamespace(emit_memory_sample=sink),
        "WATCH_RUNTIME": state,
    }
    module = ast.fix_missing_locations(ast.Module(body=nodes, type_ignores=[]))
    exec(compile(module, str(source), "exec"), scope)
    return scope["_record_runtime_memory"]


class MainDiagnosticsFailureTests(unittest.TestCase):
    def test_cache_counter_failure_does_not_change_watch_state_or_reach_sink(self):
        state = {"scan_in_progress": True, "cycle_stage": "collecting_inputs"}
        before = dict(state)
        counter = Mock(side_effect=RuntimeError("counter fixture failure"))
        sink = Mock()
        hook = load_main_diagnostic_hook(counter, sink, state)
        self.assertIsNone(hook("watch_start"))
        counter.assert_called_once_with()
        sink.assert_not_called()
        self.assertEqual(state, before)

    def test_sink_failure_does_not_change_watch_state_or_escape(self):
        state = {"scan_in_progress": False, "cycle_stage": None}
        before = dict(state)
        counter = Mock(return_value={"cache_symbols": 2, "incomplete": 0})
        sink = Mock(side_effect=RuntimeError("sink fixture failure"))
        hook = load_main_diagnostic_hook(counter, sink, state)
        self.assertIsNone(hook("watch_end"))
        counter.assert_called_once_with()
        sink.assert_called_once_with("watch_end", counters={
            "cache_symbols": 2, "incomplete": 0, "active_watch": 0})
        self.assertEqual(state, before)


if __name__ == "__main__":
    unittest.main()
