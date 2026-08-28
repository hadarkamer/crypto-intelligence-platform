"""OpenAI vision scanner for CoinGlass liquidation heatmap screenshots.

The scanner is deliberately observational: it extracts visible price zones and
relative concentration, but it does not invent exact dollar totals that are not
explicitly visible in the screenshot.
"""

from __future__ import annotations

import base64
import json
import mimetypes
import os
from pathlib import Path
from typing import Any, Iterable, Mapping

import requests

OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"
DEFAULT_MODEL = os.getenv("OPENAI_MARKET_SCANNER_MODEL", "gpt-5.6")


HEATMAP_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "source": {"type": "string"},
        "analysis_mode": {"type": "string", "enum": ["visual_screenshot"]},
        "symbol": {"type": "string"},
        "scans": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "timeframe": {"type": "string"},
                    "liquidity_threshold": {"type": ["number", "null"]},
                    "current_price_estimate": {"type": ["number", "null"]},
                    "current_price_confidence": {
                        "type": "string",
                        "enum": ["high", "medium", "low"],
                    },
                    "above_price": {"$ref": "#/$defs/side"},
                    "below_price": {"$ref": "#/$defs/side"},
                    "dominant_side": {
                        "type": "string",
                        "enum": ["above", "below", "balanced", "unclear"],
                    },
                    "dominance_confidence": {
                        "type": "string",
                        "enum": ["high", "medium", "low"],
                    },
                    "exact_dollar_totals_available": {"type": "boolean"},
                    "short_summary": {"type": "string"},
                    "limitations": {"type": "array", "items": {"type": "string"}},
                },
                "required": [
                    "timeframe",
                    "liquidity_threshold",
                    "current_price_estimate",
                    "current_price_confidence",
                    "above_price",
                    "below_price",
                    "dominant_side",
                    "dominance_confidence",
                    "exact_dollar_totals_available",
                    "short_summary",
                    "limitations",
                ],
            },
        },
        "cross_timeframe": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "available": {"type": "boolean"},
                "main_changes": {"type": "array", "items": {"type": "string"}},
                "shared_zones": {"type": "array", "items": {"type": "string"}},
                "summary": {"type": "string"},
            },
            "required": ["available", "main_changes", "shared_zones", "summary"],
        },
    },
    "required": ["source", "analysis_mode", "symbol", "scans", "cross_timeframe"],
    "$defs": {
        "zone": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "low_price": {"type": ["number", "null"]},
                "high_price": {"type": ["number", "null"]},
                "relative_strength": {
                    "type": "string",
                    "enum": ["very_strong", "strong", "medium", "weak"],
                },
                "distance_from_price": {
                    "type": "string",
                    "enum": ["nearest", "near", "medium", "far"],
                },
                "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
            },
            "required": [
                "low_price",
                "high_price",
                "relative_strength",
                "distance_from_price",
                "confidence",
            ],
        },
        "side": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "visual_intensity_score": {"type": "number", "minimum": 0, "maximum": 10},
                "main_zone": {"$ref": "#/$defs/zone"},
                "secondary_zones": {
                    "type": "array",
                    "maxItems": 4,
                    "items": {"$ref": "#/$defs/zone"},
                },
            },
            "required": ["visual_intensity_score", "main_zone", "secondary_zones"],
        },
    },
}


SYSTEM_PROMPT = """You are the visual market-scanning layer of a crypto intelligence system.
Read CoinGlass Liquidation Heatmap screenshots like a careful human analyst.
Your job is observation and structured extraction, not trade advice.

Rules:
1. Read the visible price axis and current-price candles before identifying zones.
2. Identify the strongest visible liquidity bands above and below current price.
3. Use color/brightness only for RELATIVE visual concentration. Score each side on a fixed 0-10 visual_intensity_score scale, where 0 means no meaningful visible concentration and 10 means the strongest concentration visible in the supplied screenshot set. This score is not dollars and not a probability.
4. Never invent exact monetary liquidity totals. If exact dollar totals are not explicitly visible for each side, exact_dollar_totals_available must be false.
5. Do not treat the heatmap legend maximum as total liquidity on either side.
6. If a boundary cannot be read confidently, use null or a wider range and lower confidence rather than pretending precision.
7. Keep 12h and 24h separate, then compare them only if both were supplied.
8. Keep short_summary concise and observational: mention the dominant visible side plus the most important upper/lower zones. Do not infer future price direction or recommend a trade.
"""


def _image_to_data_url(path: str | Path) -> str:
    image_path = Path(path)
    if not image_path.is_file():
        raise FileNotFoundError(f"Image not found: {image_path}")
    mime = mimetypes.guess_type(image_path.name)[0] or "image/png"
    encoded = base64.b64encode(image_path.read_bytes()).decode("ascii")
    return f"data:{mime};base64,{encoded}"


def _image_url(value: str | Path) -> str:
    text = str(value)
    if text.startswith(("http://", "https://", "data:image/")):
        return text
    return _image_to_data_url(text)


def _extract_output_text(payload: Mapping[str, Any]) -> str:
    direct = payload.get("output_text")
    if isinstance(direct, str) and direct.strip():
        return direct
    for item in payload.get("output", []) or []:
        if not isinstance(item, Mapping) or item.get("type") != "message":
            continue
        for part in item.get("content", []) or []:
            if isinstance(part, Mapping) and part.get("type") == "output_text":
                text = part.get("text")
                if isinstance(text, str) and text.strip():
                    return text
    raise RuntimeError("OpenAI response did not contain output text")


def analyze_heatmap_images(
    images: Iterable[Mapping[str, Any]],
    *,
    symbol: str = "BTC",
    model: str | None = None,
    api_key: str | None = None,
    timeout_seconds: int = 120,
) -> dict[str, Any]:
    """Analyze one or more labeled heatmap screenshots.

    Each image mapping accepts:
      - image: local path, public URL, or data URL (required)
      - timeframe: e.g. "12h" or "24h" (required)
      - liquidity_threshold: optional numeric value such as 0.85
    """
    key = api_key or os.getenv("OPENAI_API_KEY")
    if not key:
        raise RuntimeError("OPENAI_API_KEY is not configured")

    selected_model = model or DEFAULT_MODEL
    content: list[dict[str, Any]] = [
        {
            "type": "input_text",
            "text": (
                f"Analyze the attached CoinGlass Liquidation Heatmap screenshots for {symbol}. "
                "Use the labels supplied before each image as authoritative metadata."
            ),
        }
    ]

    count = 0
    for image in images:
        image_value = image.get("image")
        timeframe = str(image.get("timeframe") or "unknown")
        threshold = image.get("liquidity_threshold")
        if not image_value:
            raise ValueError("Each image requires an 'image' path or URL")
        label = f"Screenshot metadata: symbol={symbol}; timeframe={timeframe}"
        if threshold is not None:
            label += f"; liquidity_threshold={threshold}"
        content.append({"type": "input_text", "text": label})
        content.append(
            {
                "type": "input_image",
                "image_url": _image_url(image_value),
                "detail": "original",
            }
        )
        count += 1

    if count == 0:
        raise ValueError("At least one heatmap image is required")

    request_payload: dict[str, Any] = {
        "model": selected_model,
        "store": False,
        "reasoning": {"effort": "medium"},
        "instructions": SYSTEM_PROMPT,
        "input": [{"role": "user", "content": content}],
        "text": {
            "verbosity": "low",
            "format": {
                "type": "json_schema",
                "name": "coinglass_heatmap_scan",
                "strict": True,
                "schema": HEATMAP_SCHEMA,
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
        preview = response.text[:1000]
        raise RuntimeError(f"OpenAI API error {response.status_code}: {preview}")

    payload = response.json()
    parsed = json.loads(_extract_output_text(payload))
    parsed["model"] = selected_model
    return parsed


def analyze_heatmap_image(
    image: str | Path,
    *,
    timeframe: str,
    symbol: str = "BTC",
    liquidity_threshold: float | None = 0.85,
    model: str | None = None,
    api_key: str | None = None,
) -> dict[str, Any]:
    """Convenience wrapper for a single screenshot."""
    return analyze_heatmap_images(
        [
            {
                "image": image,
                "timeframe": timeframe,
                "liquidity_threshold": liquidity_threshold,
            }
        ],
        symbol=symbol,
        model=model,
        api_key=api_key,
    )

