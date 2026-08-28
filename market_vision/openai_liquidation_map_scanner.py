"""OpenAI vision scanner for CoinGlass BTC liquidation-map screenshots.

One CoinGlass page currently exposes three useful views in the same screenshot:
Binance BTC/USDT, the aggregate Bitcoin exchange map, and Hyperliquid.  The
scanner extracts each independently so one browser capture / one model call can
serve all three sources without conflating their levels.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import requests

from market_vision.openai_heatmap_scanner import (
    DEFAULT_MODEL,
    OPENAI_RESPONSES_URL,
    _extract_output_text,
    _image_url,
)


_LEVEL_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "low_price": {"type": ["number", "null"]},
        "high_price": {"type": ["number", "null"]},
        "relative_strength": {
            "type": "string",
            "enum": ["very_strong", "strong", "medium", "weak"],
        },
        "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
        "note": {"type": "string"},
    },
    "required": ["low_price", "high_price", "relative_strength", "confidence", "note"],
}

_SIDE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "visual_intensity_score": {"type": "number", "minimum": 0, "maximum": 10},
        "strongest_level": _LEVEL_SCHEMA,
        "other_levels": {
            "type": "array",
            "maxItems": 5,
            "items": _LEVEL_SCHEMA,
        },
    },
    "required": ["visual_intensity_score", "strongest_level", "other_levels"],
}

_MAP_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "title": {"type": "string"},
        "timeframe": {"type": ["string", "null"]},
        "current_price_estimate": {"type": ["number", "null"]},
        "current_price_confidence": {"type": "string", "enum": ["high", "medium", "low"]},
        "above_price": _SIDE_SCHEMA,
        "below_price": _SIDE_SCHEMA,
        "dominant_side": {
            "type": "string",
            "enum": ["above", "below", "balanced", "unclear"],
        },
        "dominance_confidence": {"type": "string", "enum": ["high", "medium", "low"]},
        "points_of_interest": {
            "type": "array",
            "maxItems": 6,
            "items": {"type": "string"},
        },
        "exact_dollar_values_available": {"type": "boolean"},
        "short_summary": {"type": "string"},
        "limitations": {"type": "array", "items": {"type": "string"}},
    },
    "required": [
        "title",
        "timeframe",
        "current_price_estimate",
        "current_price_confidence",
        "above_price",
        "below_price",
        "dominant_side",
        "dominance_confidence",
        "points_of_interest",
        "exact_dollar_values_available",
        "short_summary",
        "limitations",
    ],
}

LIQUIDATION_MAP_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "source": {"type": "string"},
        "analysis_mode": {"type": "string", "enum": ["visual_screenshot"]},
        "symbol": {"type": "string"},
        "maps": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "binance_btc_usdt": _MAP_SCHEMA,
                "bitcoin_exchange_aggregate": _MAP_SCHEMA,
                "hyperliquid_btc": _MAP_SCHEMA,
            },
            "required": [
                "binance_btc_usdt",
                "bitcoin_exchange_aggregate",
                "hyperliquid_btc",
            ],
        },
        "cross_map": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "consensus": {
                    "type": "string",
                    "enum": ["above", "below", "mixed", "unclear"],
                },
                "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
                "shared_observations": {
                    "type": "array",
                    "maxItems": 6,
                    "items": {"type": "string"},
                },
                "disagreements": {
                    "type": "array",
                    "maxItems": 6,
                    "items": {"type": "string"},
                },
                "summary": {"type": "string"},
            },
            "required": ["consensus", "confidence", "shared_observations", "disagreements", "summary"],
        },
    },
    "required": ["source", "analysis_mode", "symbol", "maps", "cross_map"],
}


SYSTEM_PROMPT = """You are the visual market-scanning layer of a crypto intelligence system.
Analyze the CoinGlass BTC Liquidation Map screenshot like a careful human analyst.
Your task is observation and structured extraction, not trade advice.

The screenshot can contain THREE separate charts. Analyze them independently:
1. Binance BTC/USDT Liquidation Map.
2. Bitcoin Exchange Liquidation Map (aggregate exchanges).
3. Hyperliquid Liquidation Map.
Do not mix a price level or bar from one chart into another chart's result.

Rules:
1. For each chart, estimate current price only from its own clearly visible chart/axis marker. Lower confidence or use null when unreadable.
2. Identify the strongest visible liquidation concentrations above and below current price for that chart.
3. Score each side on a fixed 0-10 relative visual_intensity_score. This is not dollars and not a probability.
4. Never invent exact monetary values. exact_dollar_values_available is true only if the screenshot explicitly gives monetary values tied to the relevant level or side.
5. Prefer a wider range with lower confidence over false precision.
6. points_of_interest should contain short operational observations: nearest notable concentration, strongest level, broad cluster, or obvious imbalance.
7. Compare the three charts only in cross_map after analyzing each separately. Consensus means visual agreement across the maps, not a price forecast.
8. Do not infer future price direction and do not recommend a trade.
9. Write all human-facing free-text fields (note, points_of_interest, short_summary, limitations, cross_map text) in concise Hebrew. Keep chart titles and technical identifiers as displayed when useful.
"""


def analyze_liquidation_map(
    image: str | Path,
    *,
    symbol: str = "BTC",
    model: str | None = None,
    api_key: str | None = None,
    timeout_seconds: int = 120,
) -> dict[str, Any]:
    key = api_key or os.getenv("OPENAI_API_KEY")
    if not key:
        raise RuntimeError("OPENAI_API_KEY is not configured")

    selected_model = model or DEFAULT_MODEL
    request_payload: dict[str, Any] = {
        "model": selected_model,
        "store": False,
        "reasoning": {"effort": "medium"},
        "instructions": SYSTEM_PROMPT,
        "input": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": (
                            f"Analyze every visible CoinGlass liquidation-map chart for {symbol}. "
                            "Keep Binance, aggregate exchanges, and Hyperliquid separate."
                        ),
                    },
                    {
                        "type": "input_image",
                        "image_url": _image_url(image),
                        "detail": "original",
                    },
                ],
            }
        ],
        "text": {
            "verbosity": "low",
            "format": {
                "type": "json_schema",
                "name": "coinglass_liquidation_maps_scan",
                "strict": True,
                "schema": LIQUIDATION_MAP_SCHEMA,
            },
        },
    }

    response = requests.post(
        OPENAI_RESPONSES_URL,
        headers={
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
        },
        json=request_payload,
        timeout=timeout_seconds,
    )
    if not response.ok:
        raise RuntimeError(f"OpenAI API error {response.status_code}: {response.text[:1000]}")

    parsed = json.loads(_extract_output_text(response.json()))
    parsed["model"] = selected_model
    return parsed

