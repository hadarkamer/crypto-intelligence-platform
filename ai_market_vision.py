"""Read-only orchestration for the CoinGlass visual scanner.

The capture and vision modules originate from the proven ``crypto-ai-lab`` POC.
This wrapper makes them safe to call from the candidate AI agent: one scan runs
at a time, results are cached briefly, screenshots live only in a temporary
directory, and no bot/database state is changed.
"""

from __future__ import annotations

import asyncio
import copy
import os
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from market_vision.coinglass_heatmap_capture import capture_heatmaps
from market_vision.coinglass_liquidation_map_capture import capture_liquidation_map
from market_vision.heatmap_report import build_heatmap_report
from market_vision.liquidation_map_report import build_liquidation_map_report
from market_vision.openai_heatmap_scanner import analyze_heatmap_images
from market_vision.openai_liquidation_map_scanner import analyze_liquidation_map


SUPPORTED_SYMBOLS = {"BTC"}
SUPPORTED_VIEWS = {"heatmap", "liquidation_map", "all"}
_SCAN_LOCK = asyncio.Lock()
_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_BROWSER_INSTALL_LOCK = threading.Lock()
_BROWSER_STATE: dict[str, Any] = {"ready": False, "checked": False, "last_error": None}


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _cache_seconds() -> int:
    try:
        value = int(os.getenv("AI_COINGLASS_VISION_CACHE_SECONDS", "600"))
    except ValueError:
        value = 600
    return max(60, min(value, 3600))


def _symbol(value: Any) -> str:
    symbol = str(value or "BTC").strip().upper()
    if symbol not in SUPPORTED_SYMBOLS:
        raise ValueError("The validated CoinGlass visual scanner currently supports BTC only")
    return symbol


def _view(value: Any) -> str:
    view = str(value or "all").strip().lower()
    if view not in SUPPORTED_VIEWS:
        raise ValueError("view must be heatmap, liquidation_map or all")
    return view


def _authenticated_session_configured() -> bool:
    return bool(
        os.getenv("COINGLASS_STORAGE_STATE_JSON", "").strip()
        or os.getenv("COINGLASS_COOKIE_HEADER", "").strip()
    )


def status() -> dict[str, Any]:
    """Return configuration metadata without exposing credentials."""
    return {
        "enabled": _env_flag("AI_COINGLASS_VISION_ENABLED"),
        "authenticated_session_configured": _authenticated_session_configured(),
        "heatmap_available": _env_flag("AI_COINGLASS_VISION_ENABLED"),
        "liquidation_map_available": (
            _env_flag("AI_COINGLASS_VISION_ENABLED")
            and _authenticated_session_configured()
        ),
        "supported_symbols": sorted(SUPPORTED_SYMBOLS),
        "supported_views": sorted(SUPPORTED_VIEWS),
        "cache_seconds": _cache_seconds(),
        "browser": {
            "ready": bool(_BROWSER_STATE["ready"]),
            "checked": bool(_BROWSER_STATE["checked"]),
            "auto_install": _env_flag("AI_COINGLASS_AUTO_INSTALL_BROWSER", True),
            "last_error": _BROWSER_STATE["last_error"],
        },
        "writes_bot_state": False,
        "persists_screenshots": False,
    }


def _stamp(result: dict[str, Any], generated_at: str) -> dict[str, Any]:
    result["scan_generated_at_utc"] = generated_at
    return result


def _chromium_executable() -> Path:
    from playwright.sync_api import sync_playwright

    with sync_playwright() as playwright:
        return Path(playwright.chromium.executable_path)


def _ensure_chromium() -> None:
    """Install the Playwright browser lazily when Render's build omitted it."""
    with _BROWSER_INSTALL_LOCK:
        executable = _chromium_executable()
        _BROWSER_STATE["checked"] = True
        if executable.is_file():
            _BROWSER_STATE.update({"ready": True, "last_error": None})
            return

        if not _env_flag("AI_COINGLASS_AUTO_INSTALL_BROWSER", True):
            _BROWSER_STATE.update(
                {"ready": False, "last_error": "Chromium is not installed and auto-install is disabled"}
            )
            raise RuntimeError(_BROWSER_STATE["last_error"])

        try:
            completed = subprocess.run(
                [sys.executable, "-m", "playwright", "install", "chromium"],
                check=False,
                capture_output=True,
                text=True,
                timeout=360,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            message = f"Chromium installation failed: {type(exc).__name__}: {exc}"
            _BROWSER_STATE.update({"ready": False, "last_error": message[:500]})
            raise RuntimeError(message) from exc

        executable = _chromium_executable()
        if completed.returncode != 0 or not executable.is_file():
            output = (completed.stderr or completed.stdout or "no installer output").strip()
            message = f"Chromium installation failed ({completed.returncode}): {output[-1500:]}"
            _BROWSER_STATE.update({"ready": False, "last_error": message[:500]})
            raise RuntimeError(message)

        _BROWSER_STATE.update({"ready": True, "last_error": None})


def _scan_sync(symbol: str, view: str) -> dict[str, Any]:
    _ensure_chromium()
    generated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    output: dict[str, Any] = {
        "source": "CoinGlass browser capture + OpenAI Vision",
        "symbol": symbol,
        "requested_view": view,
        "scan_generated_at_utc": generated_at,
        "read_only": True,
        "results": {},
    }

    with tempfile.TemporaryDirectory(prefix="coinglass-ai-") as temp_dir:
        root = Path(temp_dir)

        if view in {"heatmap", "all"}:
            images = capture_heatmaps(root / "heatmap", timeframes=("12h", "24h"))
            heatmap = _stamp(analyze_heatmap_images(images, symbol=symbol), generated_at)
            output["results"]["heatmap"] = {
                "structured": heatmap,
                "report": build_heatmap_report(heatmap),
            }

        if view in {"liquidation_map", "all"}:
            if not _authenticated_session_configured():
                unavailable = {
                    "available": False,
                    "reason": (
                        "The authenticated CoinGlass browser session is not configured in this service"
                    ),
                }
                if view == "liquidation_map":
                    raise RuntimeError(unavailable["reason"])
                output["results"]["liquidation_map"] = unavailable
            else:
                image = capture_liquidation_map(root / "liquidation_map")
                liquidation_map = _stamp(
                    analyze_liquidation_map(image, symbol=symbol), generated_at
                )
                output["results"]["liquidation_map"] = {
                    "available": True,
                    "structured": liquidation_map,
                    "report": build_liquidation_map_report(liquidation_map),
                }

    return output


async def scan(
    *,
    symbol: str = "BTC",
    view: str = "all",
    force_refresh: bool = False,
) -> dict[str, Any]:
    """Run or reuse a bounded live visual scan."""
    if not _env_flag("AI_COINGLASS_VISION_ENABLED"):
        raise RuntimeError("CoinGlass Vision is disabled in this service")

    selected_symbol = _symbol(symbol)
    selected_view = _view(view)
    cache_key = f"{selected_symbol}:{selected_view}"
    now = time.monotonic()

    cached = _CACHE.get(cache_key)
    if not force_refresh and cached and now - cached[0] <= _cache_seconds():
        result = copy.deepcopy(cached[1])
        result["cache"] = {"hit": True, "max_age_seconds": _cache_seconds()}
        return result

    async with _SCAN_LOCK:
        now = time.monotonic()
        cached = _CACHE.get(cache_key)
        if not force_refresh and cached and now - cached[0] <= _cache_seconds():
            result = copy.deepcopy(cached[1])
            result["cache"] = {"hit": True, "max_age_seconds": _cache_seconds()}
            return result

        result = await asyncio.to_thread(_scan_sync, selected_symbol, selected_view)
        _CACHE[cache_key] = (time.monotonic(), copy.deepcopy(result))
        result["cache"] = {"hit": False, "max_age_seconds": _cache_seconds()}
        return result
