"""Adapter around the existing capture + OpenAI functions; no new provider or login flow.

The legacy modules remain unchanged. This adapter calls their existing public
signatures, then rejects unreadable or inconsistent visual estimates. No hooks,
monkey-patching, or changes to source authentication are performed.
"""
from __future__ import annotations

import hashlib
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCHEMA = "coinglass-model1.v1"
SOURCE = "https://www.coinglass.com/pro/futures/LiquidationHeatMap?coin=BTC&type=symbol"
INTENSITY = {"very_strong": "many", "strong": "many", "medium": "normal", "weak": "few"}


def number(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        raise ValueError("Invalid visual price")
    return float(value)


def normalize(raw: Any, *, timeframe: str, run_id: str, captured_at: str, image: bytes) -> dict[str, Any]:
    if timeframe not in {"12H", "24H"} or not isinstance(raw, dict):
        raise ValueError("Unsupported scan")
    if raw.get("symbol") != "BTC" or raw.get("analysis_mode") != "visual_screenshot":
        raise ValueError("Wrong analysis identity")
    scans = raw.get("scans")
    if not isinstance(scans, list) or len(scans) != 1:
        raise ValueError("Exactly one requested timeframe is required")
    scan = scans[0]
    if not isinstance(scan, dict) or str(scan.get("timeframe", "")).upper() != timeframe:
        raise ValueError("Wrong timeframe")
    if scan.get("current_price_confidence") != "high":
        raise ValueError("Current price is not readable with sufficient confidence")
    current = number(scan.get("current_price_estimate"))
    zones = []
    seen = set()
    for side, field in (("above", "above_price"), ("below", "below_price")):
        group = scan.get(field)
        if not isinstance(group, dict) or not isinstance(group.get("secondary_zones"), list) or len(group["secondary_zones"]) > 4:
            raise ValueError("Invalid zone group")
        for zone in [group.get("main_zone"), *group["secondary_zones"]]:
            if not isinstance(zone, dict):
                raise ValueError("Invalid zone")
            if zone.get("low_price") is None or zone.get("high_price") is None or zone.get("confidence") == "low":
                continue
            if zone.get("confidence") not in {"medium", "high"}:
                raise ValueError("Invalid confidence")
            low, high = number(zone["low_price"]), number(zone["high_price"])
            intensity = INTENSITY.get(zone.get("relative_strength"))
            if not intensity or high < low or (side == "above" and low <= current) or (side == "below" and high >= current):
                raise ValueError("Invalid price range or side")
            key = (side, low, high)
            if key not in seen:
                seen.add(key)
                zones.append({"side": side, "price_low": low, "price_high": high, "intensity": intensity})
    if not zones:
        raise ValueError("No readable zones")
    model = raw.get("model")
    if not isinstance(model, str) or not model or len(model) > 100:
        raise ValueError("Missing actual model identity")
    return {"schema_version": SCHEMA, "run_id": run_id, "source_url": SOURCE,
            "symbol": "BTC", "heatmap_model": 1, "timeframe": timeframe,
            "captured_at": captured_at, "source_updated_at": None,
            "provider": "OpenAI", "model": model, "usage": raw.get("usage"),
            "observed_price": current, "zones": zones,
            "evidence": {"sha256": hashlib.sha256(image).hexdigest(), "artifact_id": run_id, "content_type": "image/png"},
            "summary": str(scan.get("short_summary", ""))[:1000], "quality": "visual_estimate"}


def main(timeframe: str, run_id: str, output: str) -> None:
    if timeframe not in {"12H", "24H"}:
        raise ValueError("Unsupported timeframe")
    from market_vision.coinglass_heatmap_capture import capture_heatmaps, COINGLASS_HEATMAP_URL
    from market_vision.openai_heatmap_scanner import analyze_heatmap_images
    if COINGLASS_HEATMAP_URL != SOURCE:
        raise ValueError("Unexpected legacy source configuration")
    root = Path(output)
    root.mkdir(parents=True, exist_ok=True)
    images = capture_heatmaps(root / "capture", timeframes=(timeframe.lower(),))
    if len(images) != 1 or images[0].get("timeframe") != timeframe.lower():
        raise ValueError("Capture count or timeframe mismatch")
    image = Path(images[0]["image"]).read_bytes()
    if len(image) > 4 * 1024 * 1024 or not image.startswith(b"\x89PNG\r\n\x1a\n"):
        raise ValueError("Invalid image evidence")
    captured_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    images[0].pop("liquidity_threshold", None)
    raw = analyze_heatmap_images(images, symbol="BTC", timeout_seconds=120)
    result = normalize(raw, timeframe=timeframe, run_id=run_id, captured_at=captured_at, image=image)
    (root / "image.png").write_bytes(image)
    (root / "result.json").write_text(json.dumps(result, ensure_ascii=False, allow_nan=False), encoding="utf-8")


if __name__ == "__main__":
    try:
        main(*sys.argv[1:])
    except Exception:
        sys.exit(1)
