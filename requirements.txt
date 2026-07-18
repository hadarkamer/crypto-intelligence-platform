"""Hyperliquid liquidation map control probe.

Stage 8:
- Fixes the previous hanging /hyper_debug command.
- Opens the Hyperliquid map page.
- Waits for the dropdown/graph area.
- Attempts to select a symbol.
- Attempts to click refresh.
- Returns a Telegram summary even if selection fails.
- Uses strict timeouts so the bot will not keep hanging.

This is still diagnostic only:
No DB writes.
No Hyperliquid scoring yet.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

from playwright.async_api import async_playwright, Page, TimeoutError as PlaywrightTimeoutError


HYPERLIQUID_URL = os.getenv(
    "COINGLASS_HYPERLIQUID_URL",
    "https://www.coinglass.com/hyperliquid-liquidation-map",
)


def _clean_lines(text: str, limit: int = 160) -> List[str]:
    return [x.strip() for x in text.splitlines() if x.strip()][:limit]


async def _body_lines(page: Page, limit: int = 160) -> List[str]:
    try:
        text = await page.evaluate("() => document.body ? document.body.innerText : ''")
        return _clean_lines(text, limit)
    except Exception:
        return []


async def _click_refresh(page: Page) -> Dict[str, Any]:
    """Try several ways to click the refresh button without hanging."""
    attempts = []

    selectors = [
        "button:has(svg)",
        "button",
        "[role='button']",
    ]

    # Prefer buttons near the symbol selector area. We click candidates and watch for no fatal error.
    for selector in selectors:
        try:
            count = await page.locator(selector).count()
            attempts.append({"selector": selector, "count": count})
            # The refresh button is often one of the first few buttons near the chart.
            for i in range(min(count, 8)):
                try:
                    loc = page.locator(selector).nth(i)
                    text = ""
                    try:
                        text = (await loc.inner_text(timeout=500)).strip()
                    except Exception:
                        pass
                    await loc.click(timeout=1200)
                    await page.wait_for_timeout(1500)
                    attempts.append({"clicked": selector, "index": i, "text": text[:80], "ok": True})
                    return {"ok": True, "attempts": attempts}
                except Exception as exc:
                    attempts.append({"clicked": selector, "index": i, "ok": False, "error": repr(exc)[:200]})
        except Exception as exc:
            attempts.append({"selector": selector, "ok": False, "error": repr(exc)[:200]})

    return {"ok": False, "attempts": attempts}


async def _try_select_symbol(page: Page, symbol: str) -> Dict[str, Any]:
    """Best effort symbol selection, capped with short timeouts."""
    symbol = symbol.upper()
    attempts = []

    async def add(name: str, ok: bool, err: Optional[Exception] = None):
        attempts.append({"attempt": name, "ok": ok, "error": repr(err)[:220] if err else None})

    # 1) If current dropdown already shows symbol, accept it.
    lines = await _body_lines(page, 80)
    if symbol in lines:
        await add("symbol already visible in body", True)
        return {"ok": True, "method": "already_visible", "attempts": attempts}

    # 2) Try clicking exact symbol if visible.
    try:
        await page.get_by_text(symbol, exact=True).first.click(timeout=1800)
        await page.wait_for_timeout(1500)
        await add(f"click exact text {symbol}", True)
        return {"ok": True, "method": "click_exact_text", "attempts": attempts}
    except Exception as exc:
        await add(f"click exact text {symbol}", False, exc)

    # 3) Try opening dropdown by clicking text BTC/current dropdown, then click symbol.
    for opener_text in ["BTC", "ETH", "SOL"]:
        try:
            await page.get_by_text(opener_text, exact=True).first.click(timeout=1800)
            await page.wait_for_timeout(1200)
            await page.get_by_text(symbol, exact=True).first.click(timeout=1800)
            await page.wait_for_timeout(2500)
            await add(f"open dropdown via {opener_text}, click {symbol}", True)
            return {"ok": True, "method": "dropdown_text", "attempts": attempts}
        except Exception as exc:
            await add(f"open dropdown via {opener_text}", False, exc)

    # 4) Try combobox/button typing.
    for selector in ["input", "[role='combobox']", "button"]:
        try:
            loc = page.locator(selector).first
            await loc.click(timeout=1800)
            await page.keyboard.press("Control+A")
            await page.keyboard.type(symbol)
            await page.wait_for_timeout(700)
            await page.keyboard.press("Enter")
            await page.wait_for_timeout(2500)
            await add(f"type into {selector}", True)
            return {"ok": True, "method": f"type_{selector}", "attempts": attempts}
        except Exception as exc:
            await add(f"type into {selector}", False, exc)

    return {"ok": False, "method": None, "attempts": attempts}


async def _structure(page: Page) -> Dict[str, Any]:
    return await page.evaluate(
        """() => {
            const short = (s) => (s || '').toString().trim().slice(0, 100);
            const buttons = [...document.querySelectorAll('button')].slice(0, 20).map((b, i) => ({
                i,
                text: short(b.innerText || b.textContent),
                aria: short(b.getAttribute('aria-label')),
                title: short(b.getAttribute('title')),
                cls: short(b.className)
            }));
            const inputs = [...document.querySelectorAll('input')].slice(0, 20).map((b, i) => ({
                i,
                value: short(b.value),
                placeholder: short(b.getAttribute('placeholder')),
                aria: short(b.getAttribute('aria-label')),
                cls: short(b.className)
            }));
            const canvases = [...document.querySelectorAll('canvas')].map((c, i) => ({
                i, width: c.width, height: c.height, clientWidth: c.clientWidth, clientHeight: c.clientHeight
            }));
            const svgs = [...document.querySelectorAll('svg')].slice(0, 20).map((s, i) => ({
                i,
                text: short(s.textContent),
                cls: short(s.className && (s.className.baseVal || s.className))
            }));
            return {
                url: location.href,
                title: document.title,
                buttonCount: document.querySelectorAll('button').length,
                inputCount: document.querySelectorAll('input').length,
                canvasCount: document.querySelectorAll('canvas').length,
                svgCount: document.querySelectorAll('svg').length,
                buttons,
                inputs,
                canvases,
                svgs
            };
        }"""
    )


async def probe_hyperliquid_symbol(symbol: str = "BTC", headless: bool = True, max_seconds: int = 55) -> Dict[str, Any]:
    symbol = symbol.upper()
    print(f"[hyper] control probe start symbol={symbol}; url={HYPERLIQUID_URL}", flush=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=headless,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--disable-gpu",
                "--disable-extensions",
            ],
        )

        context = await browser.new_context(
            viewport={"width": 1440, "height": 1800},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/150.0.0.0 Safari/537.36"
            ),
            locale="en-US",
        )
        page = await context.new_page()
        page.set_default_timeout(3500)
        page.set_default_navigation_timeout(60000)

        try:
            await page.goto(HYPERLIQUID_URL, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(9000)
            title = await page.title()
            print(f"[hyper] opened title={title!r}; url={page.url}", flush=True)

            before_lines = await _body_lines(page, 120)
            print("[hyper] before_lines=" + json.dumps(before_lines[:60], ensure_ascii=False)[:3000], flush=True)

            select_result = await _try_select_symbol(page, symbol)
            print("[hyper] select_result=" + json.dumps(select_result, ensure_ascii=False)[:4000], flush=True)

            refresh_result = await _click_refresh(page)
            print("[hyper] refresh_result=" + json.dumps(refresh_result, ensure_ascii=False)[:4000], flush=True)

            await page.wait_for_timeout(4000)
            after_lines = await _body_lines(page, 160)
            struct = await _structure(page)

            print("[hyper] structure=" + json.dumps(struct, ensure_ascii=False)[:5000], flush=True)
            print("[hyper] after_lines=" + json.dumps(after_lines[:90], ensure_ascii=False)[:5000], flush=True)

            screenshot_path = f"/tmp/hyperliquid_{symbol}_control.png"
            try:
                await page.screenshot(path=screenshot_path, full_page=True)
                print(f"[hyper] screenshot saved to {screenshot_path}", flush=True)
            except Exception as exc:
                print(f"[hyper] screenshot failed: {repr(exc)}", flush=True)

            return {
                "ok": True,
                "symbol": symbol,
                "url": page.url,
                "title": title,
                "selected": select_result,
                "refreshed": refresh_result,
                "structure": {
                    "buttonCount": struct.get("buttonCount"),
                    "inputCount": struct.get("inputCount"),
                    "canvasCount": struct.get("canvasCount"),
                    "svgCount": struct.get("svgCount"),
                    "buttons": struct.get("buttons", [])[:8],
                    "inputs": struct.get("inputs", [])[:8],
                    "canvases": struct.get("canvases", [])[:6],
                },
                "body_preview": after_lines[:50],
            }
        except Exception as exc:
            print(f"[hyper] ERROR: {repr(exc)}", flush=True)
            return {
                "ok": False,
                "symbol": symbol,
                "error": repr(exc),
                "body_preview": await _body_lines(page, 50),
            }
        finally:
            await context.close()
            await browser.close()
            print("[hyper] control probe done", flush=True)
