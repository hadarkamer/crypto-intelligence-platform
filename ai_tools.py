"""Read-only analytical tools exposed to the production GPT agent.

Production rule: tools in this module may read existing bot state, historical
market evidence and the alert Research Archive. They must not change trading
rules, scores, thresholds, alerts, watches, database schema or scheduled jobs.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Awaitable, Callable, Dict

import ai_alert_research
import ai_history_research
import coinglass_flow_engine
import coinglass_oi_regime_service
import market_confidence_engine
import research_feature_matrix
import research_formula_store
import research_historical_replay


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
        "name": "research_alert_history",
        "description": (
            "Analyze delivered bot alerts and their measured outcomes from the production Research Archive. "
            "Returns sample sizes, direction-adjusted fixed-horizon performance by alert type, and recent alert rows. "
            "Use this for questions about which alerts worked, failed, repeated, or performed better. "
            "Never generalize from a small sample, and never treat reconstructed market history as a delivered alert."
        ),
        "parameters": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "symbol": {
                    "type": ["string", "null"],
                    "description": "Optional crypto symbol. Use null to analyze all archived symbols.",
                },
                "lookback_days": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 3650,
                    "description": "Archive lookback in days.",
                },
                "horizon_minutes": {
                    "type": "integer",
                    "enum": [60, 240, 720, 1440],
                    "description": "Outcome horizon: 1h, 4h, 12h or 24h.",
                },
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 50,
                    "description": "Maximum recent delivered alerts returned.",
                },
            },
            "required": ["symbol", "lookback_days", "horizon_minutes", "limit"],
        },
        "strict": True,
    },
    {
        "type": "function",
        "name": "get_alert_context",
        "description": (
            "Inspect one archived delivered alert by Research Event ID. "
            "Returns its immutable decision-time engine snapshot, measured outcomes, and the nearest stored OI/CVD/market context. "
            "Use this to explain why one alert was generated and what happened afterward without look-ahead bias."
        ),
        "parameters": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "event_id": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Research Event ID returned by research_alert_history.",
                },
                "window_minutes": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 1440,
                    "description": "Bounded time window for surrounding stored market evidence.",
                },
            },
            "required": ["event_id", "window_minutes"],
        },
        "strict": True,
    },
    {
        "type": "function",
        "name": "research_formula_groups",
        "description": (
            "Search verified canonical spot one-minute alert outcomes for candidate formula groups. "
            "Groups by exact signal combination, alert type, symbol, or score band and returns sample size, "
            "baseline comparison, hit rate, MFE, MAE percentiles, speed, target progress, rarity share and sample event IDs. "
            "Use it to discover high-probability, low-adverse-movement, fast candidate setups. "
            "It is exploratory evidence, not permission to activate a live formula."
        ),
        "parameters": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "symbol": {
                    "type": ["string", "null"],
                    "description": "Optional crypto symbol. Use null to search across all archived symbols.",
                },
                "lookback_days": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 3650,
                    "description": "Research Archive lookback in days.",
                },
                "horizon_minutes": {
                    "type": "integer",
                    "enum": [60, 240, 720, 1440],
                    "description": "Price-path outcome horizon to compare.",
                },
                "group_by": {
                    "type": "string",
                    "enum": ["signal_combination", "event_type", "symbol", "score_band"],
                    "description": "Reproducible candidate dimension to aggregate.",
                },
                "minimum_samples": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 1000,
                    "description": "Minimum observations per returned candidate group.",
                },
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 100,
                    "description": "Maximum candidate groups returned.",
                },
            },
            "required": [
                "symbol",
                "lookback_days",
                "horizon_minutes",
                "group_by",
                "minimum_samples",
                "limit"
            ],
        },
        "strict": True,
    },
    {
        "type": "function",
        "name": "get_alert_price_path",
        "description": (
            "Fetch the canonical spot one-minute path for one archived delivered alert after a completed horizon. "
            "Returns full-path MFE, MAE, speed and target metrics plus a bounded candle sample. "
            "Use it to inspect the exact post-alert path or a formula counterexample. Binance Spot USDT is default; "
            "HYPE uses Hyperliquid HYPE/USDT spot. No futures path or silent fallback is allowed."
        ),
        "parameters": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "event_id": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Research Event ID returned by an alert or formula research tool.",
                },
                "horizon_minutes": {
                    "type": "integer",
                    "enum": [60, 240, 720, 1440],
                    "description": "Completed post-alert path horizon.",
                },
                "max_points": {
                    "type": "integer",
                    "minimum": 10,
                    "maximum": 120,
                    "description": "Maximum sampled candles returned to model context; metrics always use the full path.",
                },
            },
            "required": ["event_id", "horizon_minutes", "max_points"],
        },
        "strict": True,
    },
    {
        "type": "function",
        "name": "research_feature_matrix",
        "description": (
            "Build bounded, versioned research rows that place decision-time raw Price/OI, Futures CVD and Spot CVD "
            "features beside the bot's captured model/score features and verified later canonical spot outcome labels. "
            "Includes prior-alert repetition/breadth and UTC time features. Every input feature is timestamped at or "
            "before the alert; later outcomes are kept in a separate label object. Use this to search for formulas "
            "that may outperform the bot's existing score construction without assuming those scores are optimal."
        ),
        "parameters": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "symbol": {
                    "type": ["string", "null"],
                    "description": "Optional crypto symbol. Use null to sample all archived symbols.",
                },
                "event_type": {
                    "type": ["string", "null"],
                    "description": "Optional exact archived event type. Use null for every alert type.",
                },
                "direction": {
                    "type": ["string", "null"],
                    "enum": ["LONG", "SHORT", None],
                    "description": "Optional expected price direction. Use null to return LONG and SHORT rows.",
                },
                "lookback_days": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 3650,
                    "description": "Research Archive lookback in days.",
                },
                "horizon_minutes": {
                    "type": "integer",
                    "enum": [60, 240, 720, 1440],
                    "description": "Verified canonical spot outcome horizon used as the label.",
                },
                "window_profile": {
                    "type": "string",
                    "enum": ["core", "extended"],
                    "description": (
                        "core returns 30m/1h/4h/12h/24h raw windows; extended also returns 48h/72h/7d."
                    ),
                },
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 25,
                    "description": "Maximum feature rows returned to model context.",
                },
            },
            "required": [
                "symbol",
                "event_type",
                "direction",
                "lookback_days",
                "horizon_minutes",
                "window_profile",
                "limit"
            ],
        },
        "strict": True,
    },
    {
        "type": "function",
        "name": "research_historical_replay_status",
        "description": (
            "Read coverage and provenance of the neutral historical Price/OI/CVD opportunity replay. "
            "It reports which symbols, horizons and dates have canonical Binance Spot or HYPE Hyperliquid "
            "MFE/MAE labels, without returning or storing one-minute candle history. Use it to verify that "
            "a formula search has broad historical coverage before interpreting results."
        ),
        "parameters": {
            "type": "object",
            "additionalProperties": False,
            "properties": {},
            "required": [],
        },
        "strict": True,
    },
    {
        "type": "function",
        "name": "research_formula_registry",
        "description": (
            "Read the versioned automatic formula registry and its latest chronological discovery/holdout metrics. "
            "Returns reproducible conditions, probability evidence, MFE/MAE efficiency, speed, sample rarity, "
            "multiple-testing correction and lifecycle stage. This tool never promotes or activates a formula."
        ),
        "parameters": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "stage": {
                    "type": ["string", "null"],
                    "enum": [
                        "DISCOVERED", "BACKTESTED", "HOLDOUT_PASSED", "SHADOW",
                        "APPROVED", "LIVE", "RETIRED", None
                    ],
                    "description": "Optional exact lifecycle stage; use null for all active formulas.",
                },
                "direction": {
                    "type": ["string", "null"],
                    "enum": ["LONG", "SHORT", None],
                    "description": "Optional formula direction; use null for both.",
                },
                "horizon_minutes": {
                    "type": ["integer", "null"],
                    "enum": [60, 240, 720, 1440, None],
                    "description": "Optional canonical spot outcome horizon; use null for all.",
                },
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 100,
                    "description": "Maximum formulas returned.",
                },
            },
            "required": ["stage", "direction", "horizon_minutes", "limit"],
        },
        "strict": True,
    },
    {
        "type": "function",
        "name": "research_formula_shadow",
        "description": (
            "Read formula lifecycle counts and recent matches observed after a formula entered Shadow. "
            "Shadow matches and rolling metrics are observational, with an automatic ceiling of "
            "SHADOW_PENDING_EXPLICIT_APPROVAL. They never promote a formula. LIVE requires a separate "
            "frozen prospective review, explicit immutable human approval, runtime enablement and chat opt-in."
        ),
        "parameters": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 100,
                    "description": "Maximum recent Shadow matches returned.",
                }
            },
            "required": ["limit"],
        },
        "strict": True,
    },
    {
        "type": "function",
        "name": "get_ai_capabilities",
        "description": (
            "Return the tools currently approved for the production AI analysis layer and the current safety boundary. "
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


async def _research_alert_history(args: Dict[str, Any]) -> Any:
    result = await asyncio.to_thread(
        ai_alert_research.research_alert_history,
        symbol=args.get("symbol"),
        lookback_days=int(args.get("lookback_days")),
        horizon_minutes=int(args.get("horizon_minutes")),
        limit=int(args.get("limit")),
    )
    return _bounded(result, max_chars=45000)


async def _get_alert_context(args: Dict[str, Any]) -> Any:
    result = await asyncio.to_thread(
        ai_alert_research.alert_context,
        int(args.get("event_id")),
        int(args.get("window_minutes")),
    )
    return _bounded(result, max_chars=50000)


async def _research_formula_groups(args: Dict[str, Any]) -> Any:
    result = await asyncio.to_thread(
        ai_alert_research.research_formula_groups,
        symbol=args.get("symbol"),
        lookback_days=int(args.get("lookback_days")),
        horizon_minutes=int(args.get("horizon_minutes")),
        group_by=str(args.get("group_by")),
        minimum_samples=int(args.get("minimum_samples")),
        limit=int(args.get("limit")),
    )
    return _bounded(result, max_chars=50000)


async def _get_alert_price_path(args: Dict[str, Any]) -> Any:
    result = await asyncio.to_thread(
        ai_alert_research.alert_price_path,
        int(args.get("event_id")),
        int(args.get("horizon_minutes")),
        int(args.get("max_points")),
    )
    return _bounded(result, max_chars=50000)


async def _research_feature_matrix(args: Dict[str, Any]) -> Any:
    result = await asyncio.to_thread(
        research_feature_matrix.research_feature_matrix,
        symbol=args.get("symbol"),
        event_type=args.get("event_type"),
        direction=args.get("direction"),
        lookback_days=int(args.get("lookback_days")),
        horizon_minutes=int(args.get("horizon_minutes")),
        window_profile=str(args.get("window_profile")),
        limit=int(args.get("limit")),
    )
    return _bounded(result, max_chars=55000)


async def _research_formula_registry(args: Dict[str, Any]) -> Any:
    result = await asyncio.to_thread(
        research_formula_store.formula_registry,
        stage=args.get("stage"),
        direction=args.get("direction"),
        horizon_minutes=args.get("horizon_minutes"),
        limit=int(args.get("limit")),
    )
    return _bounded(result, max_chars=55000)


async def _research_historical_replay_status(_: Dict[str, Any]) -> Any:
    result = await asyncio.to_thread(research_historical_replay.status)
    return _bounded(result, max_chars=45000)


async def _research_formula_shadow(args: Dict[str, Any]) -> Any:
    result = await asyncio.to_thread(
        research_formula_store.shadow_status,
        int(args.get("limit")),
    )
    return _bounded(result, max_chars=45000)


async def _get_ai_capabilities(_: Dict[str, Any]) -> Any:
    archive = await asyncio.to_thread(ai_alert_research.archive_status)
    return {
        "mode": "production_analysis_read_only",
        "primary_analytical_objective": (
            "Discover reproducible candidate formulas for the widest practical directional "
            "movement with high probability, low adverse excursion, fast favorable progress "
            "and strong risk/reward. Rolling future-Shadow evidence is observational. Live alerts "
            "require a separate frozen prospective review, explicit immutable human approval "
            "and an opted-in Telegram destination."
        ),
        "approved_tools": [
            "get_oi_state",
            "get_cvd_state",
            "get_market_state",
            "research_market_history",
            "get_market_context_at_time",
            "research_alert_history",
            "get_alert_context",
            "research_formula_groups",
            "get_alert_price_path",
            "research_feature_matrix",
            "research_historical_replay_status",
            "research_formula_registry",
            "research_formula_shadow",
            "get_ai_capabilities",
        ],
        "alert_archive": archive,
        "outcome_path": {
            "sources": {
                "default": "Binance Spot USDT",
                "HYPE": "Hyperliquid HYPE/USDT spot (@107)",
            },
            "resolution": "1m closed candles",
            "historical_imports": (
                "allowed when source, pair, market, resolution and quality provenance are retained"
            ),
            "metrics": [
                "fixed-horizon return",
                "MFE",
                "MAE",
                "time to first progress",
                "time to MFE",
                "target progress",
                "target timing",
            ],
        },
        "formula_research": {
            "available_now": [
                "exact archived signal-combination groups",
                "alert-type comparison",
                "symbol comparison",
                "score-band comparison",
                "baseline, rarity, MFE/MAE, speed and target metrics",
                "versioned no-lookahead raw/model feature matrix",
                "neutral historical raw Price/OI/CVD opportunity replay with canonical spot labels",
                "prior-alert repetition and cross-symbol breadth features",
                "UTC hour plus exact per-window ACTIVE/WEEKEND session composition",
                "automatic bounded single/pair/triple search, with opt-in stable-parent hierarchical expansion to four/five conditions",
                "frozen chronological discovery/holdout validation",
                "wide-movement percentile, probability, MFE, MAE, speed, sample-size and q-value ranking",
                "versioned formula lifecycle registry",
                "future Shadow observation with a SHADOW_PENDING_EXPLICIT_APPROVAL ceiling",
                "Telegram delivery support for explicitly human-approved LIVE formulas in opted-in chats",
            ],
            "next_required_stages": [
                "accumulate a materially longer out-of-sample alert history",
                "accumulate enough future Shadow observations for a frozen prospective review",
                "record explicit immutable human approval before any LIVE transition",
                "enable the destination chat with /ai_alerts_on",
            ],
        },
        "historical_limitations": {
            "real_research_events_begin": "2026-08-28",
            "older_telegram_messages": (
                "importable only from a Telegram Desktop JSON export into an isolated legacy-message table; "
                "they are not silently treated as complete Research Events"
            ),
            "HYPE": "verified through the explicit Hyperliquid HYPE/USDT spot route",
        },
        "lab_only_not_connected": [
            "external exchange/index context archive",
            "global news context archive",
            "CoinGlass visual scanner from AI Lab",
            "web search",
            "SoSoValue",
            "YouTube",
            "scheduled natural-language tasks",
        ],
        "prohibited": [
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
    "research_alert_history": _research_alert_history,
    "get_alert_context": _get_alert_context,
    "research_formula_groups": _research_formula_groups,
    "get_alert_price_path": _get_alert_price_path,
    "research_feature_matrix": _research_feature_matrix,
    "research_historical_replay_status": _research_historical_replay_status,
    "research_formula_registry": _research_formula_registry,
    "research_formula_shadow": _research_formula_shadow,
    "get_ai_capabilities": _get_ai_capabilities,
}


async def execute_tool(name: str, arguments: Dict[str, Any]) -> Any:
    executor = _EXECUTORS.get(str(name))
    if executor is None:
        raise ValueError(f"Unknown or unapproved AI tool: {name}")
    return await executor(arguments)


def tool_names() -> list[str]:
    return list(_EXECUTORS)
