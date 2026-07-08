"""Hyperliquid liquidation map probe reader.

Stage 7 is diagnostic only:
- Opens CoinGlass Hyperliquid Liquidation Map.
- Tries to select the requested symbol.
- Extracts page text + structural hints from DOM.
- Does NOT save Hyperliquid data to DB yet.
- Purpose: learn the page structure safely before building real extraction.

After testing /hyper_debug BTC, send the Render logs if parsing is incomplete.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

from playwright.async_api import async_playwright, Page


HYPERLIQUID_URL = os.getenv(
    "COINGLASS_HYPERLIQUID_URL",
    "https://www.coinglass.com/hyperliquid-liquidation-map",
)


def _clean_lines(text: str, limit: int = 180) -> List[str]:
    return [x.strip() for x in text.splitlines() if x.strip()][:limit]


async def _extract_body_lines(page: Page, limit: int = 220) -> List[str]:
    text = await page.evaluate("() => document.body ? document.body.innerText : ''")
    return _clean_lines(text, limit=limit)


async def _structural_probe(page: Page) -> Dict[str, Any]:
    return await page.evaluate(
        """() => {
            const pick = (arr) => arr.slice(0, 60).map(x => ({
                tag: x.tagName,
                text: (x.innerText || x.textContent || '').trim().slice(0, 120),
                aria: x.getAttribute('aria-label'),
                title: x.getAttribute('title'),
                role: x.getAttribute('role'),
                cls: x.className ? String(x.className).slice(0, 120) : '',
                id: x.id || ''
            }));

            const buttons = pick([...document.querySelectorAll('button')]);
            const inputs = pick([...document.querySelectorAll('input')]);
            const selects = pick([...document.querySelectorAll('select')]);
            const canvases = [...document.querySelectorAll('canvas')].map(c => ({
                width: c.width,
                height: c.height,
                clientWidth: c.clientWidth,
                clientHeight: c.clientHeight,
                cls: c.className ? String(c.className).slice(0,120) : ''
            }));
            const svgs = [...document.querySelectorAll('svg')].map(s => ({
                width: s.getAttribute('width'),
                height: s.getAttribute('height'),
                viewBox: s.getAttribute('viewBox'),
                text: (s.textContent || '').trim().slice(0, 200),
                cls: s.className ? String(s.className.baseVal || s.className).slice(0,120) : ''
            })).slice(0, 20);

            const possibleTexts = [...document.querySelectorAll('*')]
                .map(e => (e.innerText || e.textContent || '').trim())
                .filter(t => t && t.length <= 80)
                .filter(t => /BTC|ETH|SOL|Long|Short|Liquidation|Cumulative|Price|USDC|HYPE/i.test(t))
                .slice(0, 120);

            return {
                url: location.href,
                title: document.title,
                buttonCount: document.querySelectorAll('button').length,
                inputCount: document.querySelectorAll('input').length,
                selectCount: document.querySelectorAll('select').length,
                canvasCount: document.querySelectorAll('canvas').length,
                svgCount: document.querySelectorAll('svg').length,
                buttons,
                inputs,
                selects,
                canvases,
                svgs,
                possibleTexts
            };
        }"""
    )


async def _try_select_symbol(page: Page, symbol: str) -> Dict[str, Any]:
    """Best-effort symbol selection. This is intentionally broad for diagnostics."""
    symbol = symbol.upper()
    attempts = []

    async def record(name: str, ok: bool, err: Optional[Exception] = None):
        attempts.append({"attempt": name, "ok": ok, "error": repr(err) if err else None})

    # Try exact text first.
    try:
        await page.get_by_text(symbol, exact=True).first.click(timeout=3000)
        await page.wait_for_timeout(2500)
        await record(f"text exact {symbol}", True)
        return {"selected": True, "attempts": attempts}
    except Exception as exc:
        await record(f"text exact {symbol}", False, exc)

    # Try clicking known search/input/dropdown-ish elements and typing.
    for selector in [
        "input",
        "[role='combobox']",
        "button",
    ]:
        try:
            loc = page.locator(selector).first
            await loc.click(timeout=2500)
            await page.keyboard.press("Control+A")
            await page.keyboard.type(symbol)
            await page.wait_for_timeout(1000)
            await page.keyboard.press("Enter")
            await page.wait_for_timeout(3000)
            await record(f"type into {selector}", True)
            return {"selected": True, "attempts": attempts}
        except Exception as exc:
            await record(f"type into {selector}", False, exc)

    # Try URL query fallback; may or may not be supported.
    try:
        sep = "&" if "?" in HYPERLIQUID_URL else "?"
        await page.goto(f"{HYPERLIQUID_URL}{sep}symbol={symbol}", wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(8000)
        await record("url query symbol", True)
        return {"selected": True, "attempts": attempts}
    except Exception as exc:
        await record("url query symbol", False, exc)

    return {"selected": False, "attempts": attempts}


async def probe_hyperliquid_symbol(symbol: str = "BTC", headless: bool = True) -> Dict[str, Any]:
    symbol = symbol.upper()
    print(f"[hyper] probe start symbol={symbol}; url={HYPERLIQUID_URL}", flush=True)

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

        try:
            await page.goto(HYPERLIQUID_URL, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(9000)
            print(f"[hyper] opened title={await page.title()!r}; url={page.url}", flush=True)

            select_result = await _try_select_symbol(page, symbol)
            await page.wait_for_timeout(5000)

            lines = await _extract_body_lines(page, limit=220)
            structure = await _structural_probe(page)

            print("[hyper] select_result=" + json.dumps(select_result, ensure_ascii=False)[:4000], flush=True)
            print("[hyper] structure_summary=" + json.dumps({
                "url": structure.get("url"),
                "title": structure.get("title"),
                "buttonCount": structure.get("buttonCount"),
                "inputCount": structure.get("inputCount"),
                "selectCount": structure.get("selectCount"),
                "canvasCount": structure.get("canvasCount"),
                "svgCount": structure.get("svgCount"),
                "canvases": structure.get("canvases"),
                "possibleTexts": structure.get("possibleTexts", [])[:80],
            }, ensure_ascii=False)[:6000], flush=True)
            print("[hyper] body_preview=" + json.dumps(lines[:120], ensure_ascii=False)[:6000], flush=True)

            try:
                screenshot_path = f"/tmp/hyperliquid_{symbol}_debug.png"
                await page.screenshot(path=screenshot_path, full_page=True)
                print(f"[hyper] screenshot saved to {screenshot_path}", flush=True)
            except Exception as exc:
                print(f"[hyper] screenshot failed: {repr(exc)}", flush=True)

            return {
                "ok": True,
                "symbol": symbol,
                "url": page.url,
                "title": await page.title(),
                "selected": select_result,
                "line_count": len(lines),
                "body_preview": lines[:80],
                "structure": {
                    "buttonCount": structure.get("buttonCount"),
                    "inputCount": structure.get("inputCount"),
                    "selectCount": structure.get("selectCount"),
                    "canvasCount": structure.get("canvasCount"),
                    "svgCount": structure.get("svgCount"),
                    "canvases": structure.get("canvases"),
                    "possibleTexts": structure.get("possibleTexts", [])[:40],
                },
            }
        finally:
            await context.close()
            await browser.close()
            print("[hyper] probe done", flush=True)
