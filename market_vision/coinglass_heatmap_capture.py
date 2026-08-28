"""Capture public CoinGlass liquidation heatmap screenshots for visual AI analysis.

This POC uses the public web page exactly as a browser would: it opens the page,
selects the requested timeframe when possible, and captures a screenshot. It does
not call private CoinGlass endpoints or attempt to recover hidden numeric data.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Iterable

from playwright.sync_api import Locator, Page, TimeoutError as PlaywrightTimeoutError, sync_playwright

COINGLASS_HEATMAP_URL = (
    "https://www.coinglass.com/pro/futures/LiquidationHeatMap?coin=BTC&type=symbol"
)


def _first_visible(locator: Locator) -> Locator | None:
    for idx in range(locator.count()):
        item = locator.nth(idx)
        try:
            if item.is_visible():
                return item
        except Exception:
            continue
    return None


def _dismiss_common_overlays(page: Page) -> None:
    labels = ["Accept", "Accept all", "Allow all", "I agree", "Agree", "Got it", "OK", "Close"]
    for label in labels:
        try:
            locator = page.get_by_role("button", name=re.compile(f"^{re.escape(label)}$", re.I))
            visible = _first_visible(locator)
            if visible is not None:
                visible.click(timeout=1500)
                page.wait_for_timeout(300)
        except Exception:
            pass


def _dismiss_capture_blockers(page: Page) -> None:
    _dismiss_common_overlays(page)
    login_text = page.get_by_text(re.compile(r"log\s*in\s+to\s+unlock\s+full\s+data", re.I))
    login_visible = _first_visible(login_text)
    if login_visible is None:
        return

    dialog = login_visible.locator("xpath=ancestor::*[@role='dialog'][1]")
    scopes = [dialog.first] if dialog.count() else []
    scopes.append(page.locator("body"))

    close_patterns = [
        re.compile(r"^close$", re.I),
        re.compile(r"^dismiss$", re.I),
        re.compile(r"^not now$", re.I),
        re.compile(r"^maybe later$", re.I),
    ]

    for scope in scopes:
        for pattern in close_patterns:
            try:
                button = _first_visible(scope.get_by_role("button", name=pattern))
                if button is not None:
                    button.click(timeout=1500)
                    page.wait_for_timeout(500)
                    if _first_visible(login_text) is None:
                        return
            except Exception:
                pass

        try:
            icon_close = _first_visible(
                scope.locator(
                    'button[aria-label*="close" i], button[title*="close" i], '
                    '[role="button"][aria-label*="close" i]'
                )
            )
            if icon_close is not None:
                icon_close.click(timeout=1500)
                page.wait_for_timeout(500)
                if _first_visible(login_text) is None:
                    return
        except Exception:
            pass

    try:
        page.keyboard.press("Escape")
        page.wait_for_timeout(500)
    except Exception:
        pass


def _select_symbol_mode(page: Page) -> None:
    try:
        symbol = page.get_by_role("button", name=re.compile(r"^Symbol$", re.I))
        if symbol.count() and symbol.first.is_visible():
            symbol.first.click(timeout=2000)
            page.wait_for_timeout(500)
    except Exception:
        pass


def _select_model_one(page: Page) -> None:
    try:
        model = page.get_by_role("button", name=re.compile(r"^Model 1$", re.I))
        if model.count() and model.first.is_visible():
            model.first.click(timeout=2000)
            page.wait_for_timeout(500)
    except Exception:
        pass


def _select_timeframe(page: Page, timeframe: str) -> None:
    wanted = timeframe.strip().lower()
    visible_label = "12 hour" if wanted in {"12h", "12 hour", "12 hours"} else "24 hour"
    wanted_pattern = re.compile(f"^{re.escape(visible_label)}$", re.I)
    any_hour = re.compile(r"\b(12|24)\s*hour\b", re.I)

    selected = _first_visible(
        page.locator('[role="combobox"], [aria-haspopup="listbox"]').filter(has_text=wanted_pattern)
    )
    if selected is not None:
        return

    try:
        page.keyboard.press("Escape")
        page.wait_for_timeout(200)
    except Exception:
        pass

    trigger: Locator | None = _first_visible(
        page.locator('[role="combobox"]').filter(has_text=any_hour)
    )
    if trigger is None:
        trigger = _first_visible(
            page.locator('button[aria-haspopup="listbox"], [aria-haspopup="listbox"]').filter(has_text=any_hour)
        )
    if trigger is None:
        label = _first_visible(page.get_by_text(any_hour))
        if label is not None:
            ancestor = label.locator(
                "xpath=ancestor-or-self::*[@role='combobox' or @aria-haspopup='listbox'][1]"
            )
            if ancestor.count() and ancestor.first.is_visible():
                trigger = ancestor.first
    if trigger is None:
        raise RuntimeError("Could not find the visible CoinGlass timeframe selector")

    try:
        try:
            trigger.click(timeout=5000)
        except Exception:
            trigger.click(timeout=5000, force=True)
        page.wait_for_timeout(300)

        option = _first_visible(page.get_by_role("option", name=wanted_pattern))
        if option is None:
            option = _first_visible(page.locator('[role="option"]').filter(has_text=wanted_pattern))
        if option is None:
            raise RuntimeError(f"Visible option '{visible_label}' did not appear after opening selector")

        option.click(timeout=5000)
        page.wait_for_timeout(1500)

        selected = _first_visible(
            page.locator('[role="combobox"], [aria-haspopup="listbox"]').filter(has_text=wanted_pattern)
        )
        if selected is None:
            raise RuntimeError(f"Timeframe selection did not settle on '{visible_label}'")
    except Exception as exc:
        raise RuntimeError(f"Could not select timeframe {visible_label}: {exc}") from exc


def _wait_for_heatmap(page: Page) -> None:
    title = page.get_by_text(re.compile(r"BTC.*Liquidation Heatmap", re.I))
    try:
        title.first.wait_for(state="visible", timeout=25000)
    except PlaywrightTimeoutError:
        return
    page.wait_for_timeout(5000)


def _load_authenticated_storage_state() -> dict | None:
    raw = os.getenv("COINGLASS_STORAGE_STATE_JSON", "").strip()
    if not raw:
        return None
    try:
        state = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError("COINGLASS_STORAGE_STATE_JSON is not valid JSON") from exc
    if not isinstance(state, dict):
        raise RuntimeError("COINGLASS_STORAGE_STATE_JSON must contain a JSON object")
    return state


def _load_cookie_header() -> list[dict]:
    """Convert an encrypted browser Cookie request header into Playwright cookies."""
    raw = os.getenv("COINGLASS_COOKIE_HEADER", "").strip()
    if not raw:
        return []

    cookies: list[dict] = []
    for chunk in raw.split(";"):
        chunk = chunk.strip()
        if not chunk or "=" not in chunk:
            continue
        name, value = chunk.split("=", 1)
        name = name.strip()
        if not name:
            continue
        cookies.append(
            {
                "name": name,
                "value": value.strip(),
                "domain": ".coinglass.com",
                "path": "/",
                "secure": True,
                "sameSite": "Lax",
            }
        )
    return cookies


def capture_heatmaps(
    output_dir: str | Path,
    *,
    timeframes: Iterable[str] = ("12h", "24h"),
    url: str = COINGLASS_HEATMAP_URL,
) -> list[dict[str, str]]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, str]] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context_options = {
            "viewport": {"width": 1600, "height": 1000},
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
        cookie_header_cookies = _load_cookie_header()
        if cookie_header_cookies:
            context.add_cookies(cookie_header_cookies)

        page = context.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        _dismiss_capture_blockers(page)
        _wait_for_heatmap(page)
        _select_model_one(page)
        _select_symbol_mode(page)

        for timeframe in timeframes:
            _dismiss_capture_blockers(page)
            _select_timeframe(page, timeframe)
            _wait_for_heatmap(page)
            _dismiss_capture_blockers(page)
            page.wait_for_timeout(500)

            path = out / f"coinglass_btc_heatmap_{timeframe}.png"
            page.screenshot(path=str(path), full_page=True)
            results.append(
                {
                    "image": str(path),
                    "timeframe": timeframe,
                    "liquidity_threshold": 0.85,
                }
            )

        context.close()
        browser.close()

    return results

