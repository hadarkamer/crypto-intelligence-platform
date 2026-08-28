"""Capture the authenticated CoinGlass BTC Liquidation Map for visual AI analysis."""

from __future__ import annotations

import re
from pathlib import Path

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError, sync_playwright

from market_vision.coinglass_heatmap_capture import (
    _dismiss_capture_blockers,
    _load_authenticated_storage_state,
    _load_cookie_header,
)

COINGLASS_LIQUIDATION_MAP_URL = "https://www.coinglass.com/pro/futures/LiquidationMap"


def _wait_for_liquidation_map(page) -> None:
    title = page.get_by_text(re.compile(r"(BTC/USDT|Bitcoin).*Liquidation Map", re.I))
    try:
        title.first.wait_for(state="visible", timeout=25000)
    except PlaywrightTimeoutError:
        pass
    page.wait_for_timeout(6000)


def capture_liquidation_map(
    output_dir: str | Path,
    *,
    url: str = COINGLASS_LIQUIDATION_MAP_URL,
) -> Path:
    """Open CoinGlass with the user's authenticated session and save a full-page screenshot."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    path = out / "coinglass_btc_liquidation_map.png"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context_options = {
            "viewport": {"width": 1600, "height": 1100},
            "device_scale_factor": 1,
            "locale": "en-US",
            "user_agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/151.0.0.0 Safari/537.36"
            ),
        }
        storage_state = _load_authenticated_storage_state()
        if storage_state is not None:
            context_options["storage_state"] = storage_state

        context = browser.new_context(**context_options)
        cookies = _load_cookie_header()
        if cookies:
            context.add_cookies(cookies)

        page = context.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        _dismiss_capture_blockers(page)
        _wait_for_liquidation_map(page)
        _dismiss_capture_blockers(page)
        page.wait_for_timeout(1000)
        page.screenshot(path=str(path), full_page=True)

        context.close()
        browser.close()

    return path

