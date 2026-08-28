"""Read-only tools exposed to the GPT agent.

Candidate-stage rule: tools in this module may read existing bot state and run
existing analysis functions, but they must not change trading rules, scores,
thresholds, alerts, watches, database schema, or scheduled jobs.
"""

from __future__ import annotations

import asyncio
import json
import os
from typing import Any, Awaitable, Callable, Dict

import ai_history_research
import ai_market_vision
import coinglass_flow_engine
import coinglass_oi_regime_service
import market_confidence_engine


TOOL_SPECS = [
    {
        "type": "function",
        "name": "get_oi_state",
        "description": (
            "Read the bot's latest already-computed Price+OI regime for one crypto symbol. "
            "Use this for questions about current OI, price/OI relationship, windows, or regime. "
            "This tool does not refresh or change data."
        ),
        "parameters": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "symbol": {
                    "type": "string",
                    "description": "Crypto symbol such as BTC, ETH, SOL, HYPE, DOGE, BNB or XRP.",
                }
            },
            "required": ["symbol"],
        },
        "strict": True,
    },
    {
        "type": "function",
        "name": "get_cvd_state",
        "description": (
            "Read the bot's current Futures CVD and Spot CVD analysis for one crypto symbol. "
            "Use this for current flow, CVD windows/families, impulse, trend, structure, or early shift. "
            "This is read-only and does not collect new data."
        ),
        "parameters": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "symbol": {
                    "type": "string",
                    "description": "Crypto symbol such as BTC, ETH, SOL, HYPE, DOGE, BNB or XRP.",
                }
            },
            "required": ["symbol"],
        },
        "strict": True,
    },
    {
        "type": "function",
        "name": "get_market_state",
        "description": (
            "Read the current combined market-evidence snapshot used by the bot for a symbol. "
            "It combines Price+OI and CVD evidence against an optional expected price direction. "
            "This is observational only and never changes a score or alert."
        ),
        "parameters": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "symbol": {
                    "type": "string",
                    "description": "Crypto symbol such as BTC, ETH, SOL, HYPE, DOGE, BNB or XRP.",
                },
                "expected_direction": {
                    "type": "string",
                    "enum": ["LONG", "SHORT", "NEUTRAL"],
                    "description": "Direction to compare current evidence against. Use NEUTRAL when none was specified.",
                },
            },
            "required": ["symbol", "expected_direction"],
        },
        "strict": True,
    },
    {
        "type": "function",
        "name": "research_market_history",
        "description": (
            "Research existing historical market data for one symbol over a bounded lookback window. "
            "Returns compact Price/OI, Futures CVD, Spot CVD, OI-regime and technical-signal summaries instead of raw tables. "
            "Use this for questions such as how BTC or SOL behaved over the last N hours/days or to compare historical flow evidence."
        ),
        "parameters": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "symbol": {
                    "type": "string",
                    "description": "Crypto symbol such as BTC, ETH, SOL, HYPE, DOGE, BNB or XRP.",
                },
                "lookback_hours": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 2160,
                    "description": "Historical lookback in hours, up to 90 days. Convert user-requested days to hours.",
                },
            },
            "required": ["symbol", "lookback_hours"],
        },
        "strict": True,
    },
    {
        "type": "function",
        "name": "get_market_context_at_time",
        "description": (
            "Inspect the bot's stored market evidence nearest to an exact historical UTC timestamp for one symbol. "
            "Returns nearest Price/OI, Futures/Spot CVD, OI regime, technical signals and available Max-Pain snapshot rows. "
            "Use this when researching what the market looked like around a known event or alert time."
        ),
        "parameters": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "symbol": {
                    "type": "string",
                    "description": "Crypto symbol such as BTC, ETH, SOL, HYPE, DOGE, BNB or XRP.",
                },
                "timestamp_iso": {
                    "type": "string",
                    "description": "Exact event timestamp in ISO-8601, preferably with Z or an explicit UTC offset.",
                },
                "window_minutes": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 1440,
                    "description": "How many minutes before and after the event to search for nearest stored evidence.",
                },
            },
            "required": ["symbol", "timestamp_iso", "window_minutes"],
        },
        "strict": True,
    },
    {
        "type": "function",
        "name": "scan_coinglass_market",
        "description": (
            "Capture and visually analyze the live CoinGlass BTC maps. "
            "The heatmap view covers 12h and 24h; the liquidation-map view separately reads "
            "Binance BTC/USDT, aggregate exchanges and Hyperliquid. Use this when the user asks "
            "to scan or inspect the current visible CoinGlass maps. The scan is read-only, can "
            "take a few minutes, and reports relative visual concentration rather than invented dollar totals. "
            "Heatmap is public; Liquidation Map availability depends on an authenticated browser session."
        ),
        "parameters": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "symbol": {
                    "type": "string",
                    "enum": ["BTC"],
                    "description": "The validated visual POC currently supports BTC only.",
                },
                "view": {
                    "type": "string",
                    "enum": ["heatmap", "liquidation_map", "all"],
                    "description": "Which live CoinGlass view to capture and analyze.",
                },
                "force_refresh": {
                    "type": "boolean",
                    "description": "True only when the user explicitly asks for a fresh capture; otherwise reuse a recent cached scan.",
                },
            },
            "required": ["symbol", "view", "force_refresh"],
        },
        "strict": True,
    },
    {
        "type": "function",
        "name": "get_ai_capabilities",
        "description": (
            "Return the tools currently approved for the candidate AI agent and the current safety boundary. "
            "Use this when the user asks what the AI can do right now."
        ),
        "parameters": {
            "type": "object",
            "additionalProperties": False,
            "properties": {},
            "required": [],
        },
        "strict": True,
    },
]


def _symbol(value: Any) -> str:
    symbol = str(value or "").strip().upper()
    if not symbol or len(symbol) > 16 or not symbol.replace("-", "").isalnum():
        raise ValueError("Invalid crypto symbol")
    return symbol


def _json_safe(value: Any) -> Any:
    """Round-trip arbitrary engine results into JSON-safe data."""
    return json.loads(json.dumps(value, default=str, ensure_ascii=False))


def _bounded(value: Any, max_chars: int = 60000) -> Any:
    """Prevent an unexpectedly large engine payload from flooding model context."""
    safe = _json_safe(value)
    raw = json.dumps(safe, ensure_ascii=False, separators=(",", ":"))
    if len(raw) <= max_chars:
        return safe
    return {
        "truncated": True,
        "reason": f"tool payload exceeded {max_chars} characters",
        "preview": raw[:max_chars],
    }


async def _get_oi_state(args: Dict[str, Any]) -> Any:
    symbol = _symbol(args.get("symbol"))
    result = await asyncio.to_thread(coinglass_oi_regime_service.latest, symbol)
    return _bounded({"symbol": symbol, "result": result})


async def _get_cvd_state(args: Dict[str, Any]) -> Any:
    symbol = _symbol(args.get("symbol"))
    result = await asyncio.to_thread(coinglass_flow_engine.analyze_symbol, symbol)
    return _bounded({"symbol": symbol, "result": result})


async def _get_market_state(args: Dict[str, Any]) -> Any:
    symbol = _symbol(args.get("symbol"))
    expected = str(args.get("expected_direction") or "NEUTRAL").strip().upper()
    if expected not in {"LONG", "SHORT", "NEUTRAL"}:
        raise ValueError("expected_direction must be LONG, SHORT or NEUTRAL")

    snapshot = await asyncio.to_thread(market_confidence_engine.capture_snapshot, [symbol])
    captured = (snapshot or {}).get(symbol) or {}
    result = await asyncio.to_thread(
        market_confidence_engine.combine,
        symbol,
        expected,
        captured.get("regime") or {},
        captured.get("flow") or {},
    )
    return _bounded(
        {
            "symbol": symbol,
            "expected_direction": expected,
            "regime": captured.get("regime") or {},
            "flow": captured.get("flow") or {},
            "combined": result,
        }
    )


async def _research_market_history(args: Dict[str, Any]) -> Any:
    symbol = _symbol(args.get("symbol"))
    hours = int(args.get("lookback_hours"))
    result = await asyncio.to_thread(ai_history_research.historical_summary, symbol, hours)
    return _bounded(result, max_chars=45000)


async def _get_market_context_at_time(args: Dict[str, Any]) -> Any:
    symbol = _symbol(args.get("symbol"))
    timestamp_iso = str(args.get("timestamp_iso") or "").strip()
    window_minutes = int(args.get("window_minutes"))
    result = await asyncio.to_thread(
        ai_history_research.context_at_time,
        symbol,
        timestamp_iso,
        window_minutes,
    )
    return _bounded(result, max_chars=45000)


async def _scan_coinglass_market(args: Dict[str, Any]) -> Any:
    symbol = _symbol(args.get("symbol"))
    if symbol != "BTC":
        raise ValueError("CoinGlass Vision currently supports BTC only")
    view = str(args.get("view") or "all").strip().lower()
    force_refresh = bool(args.get("force_refresh"))
    result = await ai_market_vision.scan(
        symbol=symbol,
        view=view,
        force_refresh=force_refresh,
    )
    return _bounded(result, max_chars=60000)


async def _get_ai_capabilities(_: Dict[str, Any]) -> Any:
    web_enabled = os.getenv("AI_WEB_SEARCH_ENABLED", "1").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    code_enabled = os.getenv("AI_CODE_INTERPRETER_ENABLED", "1").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    return {
        "mode": "candidate_read_only",
        "approved_tools": [
            "get_oi_state",
            "get_cvd_state",
            "get_market_state",
            "research_market_history",
            "get_market_context_at_time",
            "scan_coinglass_market",
            "web_search",
            "code_interpreter",
            "get_ai_capabilities",
        ],
        "live_external_research": {
            "enabled": web_enabled,
            "preferred_sources": [
                "cryptojungle.co.il",
                "sosovalue.com",
                "youtube.com",
                "unbias.fyi",
                "x.com/unbias_fyi",
            ],
            "citations": "source links are appended to Telegram answers",
            "persistence": "not archived in the candidate lab",
        },
        "coinglass_vision": ai_market_vision.status(),
        "quantitative_calculation": {
            "enabled": code_enabled,
            "tool": "code_interpreter",
            "container": "ephemeral 1 GB, calculation-only",
            "bot_or_database_access": False,
        },
        "not_yet_connected": [
            "historical alert outcomes (requires timestamped Research Events)",
            "persistent external exchange/index context archive",
            "persistent global news context archive",
            "scheduled natural-language tasks",
        ],
        "prohibited_in_candidate": [
            "changing scores or thresholds",
            "changing confirmation logic",
            "starting or stopping Watch without an explicit approved tool",
            "editing code or database schema",
            "placing trades",
        ],
    }


_EXECUTORS: Dict[str, Callable[[Dict[str, Any]], Awaitable[Any]]] = {
    "get_oi_state": _get_oi_state,
    "get_cvd_state": _get_cvd_state,
    "get_market_state": _get_market_state,
    "research_market_history": _research_market_history,
    "get_market_context_at_time": _get_market_context_at_time,
    "scan_coinglass_market": _scan_coinglass_market,
    "get_ai_capabilities": _get_ai_capabilities,
}


async def execute_tool(name: str, arguments: Dict[str, Any]) -> Any:
    executor = _EXECUTORS.get(str(name))
    if executor is None:
        raise ValueError(f"Unknown or unapproved AI tool: {name}")
    return await executor(arguments)


def tool_names() -> list[str]:
    return list(_EXECUTORS)


def vision_status() -> dict[str, Any]:
    return ai_market_vision.status()
