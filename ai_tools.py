"""Read-only tools exposed to the GPT agent.

Candidate-stage rule: tools in this module may read existing bot state and run
existing analysis functions, but they must not change trading rules, scores,
thresholds, alerts, watches, database schema, or scheduled jobs.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Awaitable, Callable, Dict

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


async def _get_ai_capabilities(_: Dict[str, Any]) -> Any:
    return {
        "mode": "candidate_read_only",
        "approved_tools": [
            "get_oi_state",
            "get_cvd_state",
            "get_market_state",
            "get_ai_capabilities",
        ],
        "not_yet_connected": [
            "historical alert research",
            "CoinGlass visual scanner from AI Lab",
            "SoSoValue",
            "YouTube",
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
    "get_ai_capabilities": _get_ai_capabilities,
}


async def execute_tool(name: str, arguments: Dict[str, Any]) -> Any:
    executor = _EXECUTORS.get(str(name))
    if executor is None:
        raise ValueError(f"Unknown or unapproved AI tool: {name}")
    return await executor(arguments)


def tool_names() -> list[str]:
    return list(_EXECUTORS)
