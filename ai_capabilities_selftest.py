"""Deterministic tests for candidate web research and CoinGlass Vision wiring."""

from __future__ import annotations

import asyncio
import os

import ai_agent
import ai_market_vision
import ai_telegram
import ai_tools


def _test_tool_registration() -> None:
    agent = ai_agent.BotAIAgent(api_key="test-only")
    payload = agent._base_payload([{"role": "user", "content": "test"}])
    function_names = {
        tool.get("name")
        for tool in payload["tools"]
        if isinstance(tool, dict) and tool.get("type") == "function"
    }
    hosted_types = {
        tool.get("type") for tool in payload["tools"] if isinstance(tool, dict)
    }
    assert "scan_coinglass_market" in function_names
    assert "web_search" in hosted_types
    assert "code_interpreter" in hosted_types
    assert payload.get("include") == [
        "web_search_call.action.sources",
        "code_interpreter_call.outputs",
    ]
    assert "scan_coinglass_market" in ai_tools.tool_names()


def _test_source_rendering() -> None:
    payload = {
        "output": [
            {
                "type": "web_search_call",
                "action": {
                    "type": "search",
                    "sources": [
                        {"type": "url", "url": "https://example.com/a"},
                    ],
                },
            },
            {
                "type": "message",
                "content": [
                    {
                        "type": "output_text",
                        "text": "תשובה citeturn0search0",
                        "annotations": [
                            {
                                "type": "url_citation",
                                "url": "https://example.com/a",
                                "title": "Example A",
                            },
                            {
                                "type": "url_citation",
                                "url": "https://example.com/b",
                                "title": "Example B",
                            },
                        ],
                    }
                ],
            },
        ]
    }
    answer = ai_agent._answer_with_sources(payload)
    assert "cite" not in answer
    assert answer.count("https://example.com/a") == 1
    assert answer.count("https://example.com/b") == 1
    assert "מקורות" in answer


def _test_telegram_cleanup() -> None:
    source = "## כותרת\n**חשוב** ו־`get_oi_state`\n[מקור](https://example.com)"
    rendered = ai_telegram._telegram_text(source)
    assert "##" not in rendered
    assert "**" not in rendered
    assert "`" not in rendered
    assert "get_oi_state" in rendered
    assert "https://example.com" in rendered


def _test_vision_status_redaction() -> None:
    old_cookie = os.environ.get("COINGLASS_COOKIE_HEADER")
    try:
        os.environ["COINGLASS_COOKIE_HEADER"] = "secret-cookie-value"
        state = ai_market_vision.status()
        assert state["authenticated_session_configured"] is True
        assert "secret-cookie-value" not in repr(state)
        assert state["writes_bot_state"] is False
        assert state["persists_screenshots"] is False
        assert state["browser"]["auto_install"] is True
    finally:
        if old_cookie is None:
            os.environ.pop("COINGLASS_COOKIE_HEADER", None)
        else:
            os.environ["COINGLASS_COOKIE_HEADER"] = old_cookie


def _test_vision_cache() -> None:
    old_enabled = os.environ.get("AI_COINGLASS_VISION_ENABLED")
    original_scan = ai_market_vision._scan_sync
    calls = []

    def fake_scan(symbol: str, view: str):
        calls.append((symbol, view))
        return {"symbol": symbol, "requested_view": view, "read_only": True}

    async def run() -> None:
        ai_market_vision._CACHE.clear()
        first = await ai_market_vision.scan(symbol="BTC", view="heatmap")
        second = await ai_market_vision.scan(symbol="BTC", view="heatmap")
        assert first["cache"]["hit"] is False
        assert second["cache"]["hit"] is True

    try:
        os.environ["AI_COINGLASS_VISION_ENABLED"] = "1"
        ai_market_vision._scan_sync = fake_scan
        asyncio.run(run())
        assert calls == [("BTC", "heatmap")]
    finally:
        ai_market_vision._scan_sync = original_scan
        ai_market_vision._CACHE.clear()
        if old_enabled is None:
            os.environ.pop("AI_COINGLASS_VISION_ENABLED", None)
        else:
            os.environ["AI_COINGLASS_VISION_ENABLED"] = old_enabled


def run() -> None:
    _test_tool_registration()
    _test_source_rendering()
    _test_telegram_cleanup()
    _test_vision_status_redaction()
    _test_vision_cache()
    print("AI candidate capabilities self-test: PASS")


if __name__ == "__main__":
    run()
