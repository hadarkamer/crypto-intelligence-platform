"""
CoinGlass DOM reader
--------------------
Purpose:
    Replace direct encrypted CoinGlass API collection with Playwright-based DOM extraction.

How it works:
    1. Opens CoinGlass in Chromium.
    2. Lets CoinGlass load/decrypt/render the data itself.
    3. Reads visible DOM text/tables.
    4. Returns normalized rows for the bot/database layer.

Important:
    This file is intentionally isolated from Telegram and DB logic.
    /collect should call collect_coinglass_dom_snapshot(), not the old API decrypt collector.
"""

from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError


COINGLASS_URL = "https://www.coinglass.com/liquidation-maxpain"
DEFAULT_TIMEFRAMES = ["12h", "24h", "48h", "3d", "1w", "2w", "1m"]


@dataclass
class DomRow:
    collected_at_utc: str
    timeframe: str
    source: str
    symbol: str
    raw_cells: List[str]
    price: Optional[float] = None
    max_short_price: Optional[float] = None
    max_long_price: Optional[float] = None
    short_amount_usd: Optional[float] = None
    long_amount_usd: Optional[float] = None
    extra: Optional[Dict[str, Any]] = None


def _to_float(value: str | None) -> Optional[float]:
    if value is None:
        return None
    s = str(value).strip().replace(",", "").replace("$", "").replace("%", "")
    if not s or s in {"-", "--", "none", "None", "null"}:
        return None

    multiplier = 1.0
    last = s[-1:].upper()
    if last == "K":
        multiplier = 1_000
        s = s[:-1]
    elif last == "M":
        multiplier = 1_000_000
        s = s[:-1]
    elif last == "B":
        multiplier = 1_000_000_000
        s = s[:-1]
    elif last == "T":
        multiplier = 1_000_000_000_000
        s = s[:-1]

    try:
        return float(s) * multiplier
    except ValueError:
        return None


def _looks_like_symbol(value: str) -> bool:
    if not value:
        return False
    value = value.strip()
    # Most crypto symbols are uppercase letters/numbers, 2-12 chars.
    return bool(re.fullmatch(r"[A-Z0-9]{2,12}", value))


def _normalize_timeframe(tf: str) -> str:
    aliases = {
        "12 hour": "12h",
        "24 hour": "24h",
        "48 hour": "48h",
        "3 day": "3d",
        "1 week": "1w",
        "2 week": "2w",
        "1 month": "1m",
    }
    return aliases.get(tf.strip().lower(), tf.strip())


async def _click_timeframe(page, timeframe: str) -> bool:
    """Try to select a timeframe in the CoinGlass UI."""
    labels = {
        "12h": ["12 hour", "12h", "12 Hour"],
        "24h": ["24 hour", "24h", "24 Hour"],
        "48h": ["48 hour", "48h", "48 Hour"],
        "3d": ["3 day", "3d", "3 Day"],
        "1w": ["1 week", "1w", "1 Week"],
        "2w": ["2 week", "2w", "2 Week"],
        "1m": ["1 month", "1m", "1 Month"],
    }.get(timeframe, [timeframe])

    for label in labels:
        try:
            locator = page.get_by_text(label, exact=True).first
            await locator.click(timeout=2500)
            await page.wait_for_timeout(1200)
            return True
        except Exception:
            pass

    # Fallback: some buttons are not exact text matches.
    for label in labels:
        try:
            locator = page.locator(f"text={label}").first
            await locator.click(timeout=2500)
            await page.wait_for_timeout(1200)
            return True
        except Exception:
            pass

    return False


async def _extract_tables(page) -> List[Dict[str, Any]]:
    """Extract all real HTML tables currently visible/rendered."""
    return await page.evaluate(
        """
        () => [...document.querySelectorAll('table')].map((table, index) => ({
            index,
            headers: [...table.querySelectorAll('thead th')].map(th => th.innerText.trim()),
            rows: [...table.querySelectorAll('tbody tr')]
                .map(row => [...row.querySelectorAll('td')].map(td => td.innerText.trim()))
                .filter(row => row.some(cell => cell && cell.trim() !== ''))
        }))
        """
    )


async def _extract_body_lines(page, limit: int = 2000) -> List[str]:
    """Extract visible body text lines as a fallback for virtualized/non-table UI."""
    return await page.evaluate(
        f"""
        () => document.body.innerText
            .split('\\n')
            .map(x => x.trim())
            .filter(Boolean)
            .slice(0, {limit})
        """
    )


def _parse_market_table_rows(timeframe: str, tables: List[Dict[str, Any]], collected_at: str) -> List[DomRow]:
    """
    Parse standard market table rows when available.
    This is not Max Pain yet, but it is useful as a stable fallback and future data source.
    """
    out: List[DomRow] = []
    for table in tables:
        headers = table.get("headers") or []
        rows = table.get("rows") or []
        if "Assets" not in headers or "Price" not in headers:
            continue

        for cells in rows:
            # Common structure seen in DevTools:
            # ['', 'BTC', '$63072', '+2.65%', '0.0066%', '$71.96B', ...]
            symbol = next((c for c in cells if _looks_like_symbol(c)), "")
            if not symbol:
                continue
            price_cell = cells[cells.index(symbol) + 1] if cells.index(symbol) + 1 < len(cells) else None
            out.append(
                DomRow(
                    collected_at_utc=collected_at,
                    timeframe=timeframe,
                    source="dom_market_table",
                    symbol=symbol,
                    raw_cells=cells,
                    price=_to_float(price_cell),
                    extra={"headers": headers},
                )
            )
    return out


def _parse_body_lines_for_symbols(timeframe: str, lines: List[str], collected_at: str) -> List[DomRow]:
    """
    Generic fallback parser for text-based/virtualized pages.
    It groups likely symbols with the following numeric/text lines.
    This is intentionally conservative; Max Pain-specific mapping should be added once
    the exact visible order of Max Pain fields is captured.
    """
    rows: List[DomRow] = []
    i = 0
    while i < len(lines):
        if _looks_like_symbol(lines[i]):
            symbol = lines[i]
            raw = lines[i : min(i + 12, len(lines))]
            # First money-like value after symbol is usually current price in market tables.
            price = None
            for item in raw[1:]:
                if item.startswith("$"):
                    price = _to_float(item)
                    break
            rows.append(
                DomRow(
                    collected_at_utc=collected_at,
                    timeframe=timeframe,
                    source="dom_body_text",
                    symbol=symbol,
                    raw_cells=raw,
                    price=price,
                )
            )
            i += len(raw)
        else:
            i += 1
    return rows


async def read_timeframe(page, timeframe: str) -> Dict[str, Any]:
    collected_at = datetime.now(timezone.utc).isoformat()
    tf = _normalize_timeframe(timeframe)

    clicked = await _click_timeframe(page, tf)
    await page.wait_for_timeout(2500)

    tables = await _extract_tables(page)
    body_lines = await _extract_body_lines(page)

    rows = _parse_market_table_rows(tf, tables, collected_at)

    # If no parseable HTML table exists, fallback to text grouping.
    if not rows:
        rows = _parse_body_lines_for_symbols(tf, body_lines, collected_at)

    return {
        "timeframe": tf,
        "clicked": clicked,
        "row_count": len(rows),
        "rows": [asdict(r) for r in rows],
        "debug": {
            "table_count": len(tables),
            "table_headers": [t.get("headers", []) for t in tables],
            "body_preview": body_lines[:120],
        },
    }


async def collect_coinglass_dom_snapshot(
    timeframes: Optional[List[str]] = None,
    headless: bool = True,
    url: str = COINGLASS_URL,
) -> Dict[str, Any]:
    """
    Main entry point for /collect.

    Returns:
        {
          'ok': bool,
          'rows': [...],
          'missing_timeframes': [...],
          'by_timeframe': {...},
          'debug': {...}
        }
    """
    timeframes = timeframes or DEFAULT_TIMEFRAMES
    all_rows: List[Dict[str, Any]] = []
    by_timeframe: Dict[str, Any] = {}
    missing: List[str] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=headless,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        context = await browser.new_context(
            viewport={"width": 1440, "height": 1400},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/150.0.0.0 Safari/537.36"
            ),
            locale="en-US",
        )
        page = await context.new_page()

        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            await page.wait_for_timeout(5000)

            # Try to dismiss common popups if present.
            for label in ["Accept", "I Agree", "Got it", "Close", "×"]:
                try:
                    await page.get_by_text(label, exact=True).first.click(timeout=1000)
                    await page.wait_for_timeout(500)
                except Exception:
                    pass

            for tf in timeframes:
                try:
                    result = await read_timeframe(page, tf)
                    by_timeframe[tf] = result
                    rows = result.get("rows", [])
                    if rows:
                        all_rows.extend(rows)
                    else:
                        missing.append(tf)
                except Exception as exc:
                    missing.append(tf)
                    by_timeframe[tf] = {"timeframe": tf, "error": repr(exc), "rows": []}

        finally:
            await context.close()
            await browser.close()

    return {
        "ok": len(all_rows) > 0,
        "rows": all_rows,
        "row_count": len(all_rows),
        "missing_timeframes": missing,
        "by_timeframe": by_timeframe,
        "collected_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def collect_coinglass_dom_snapshot_sync(
    timeframes: Optional[List[str]] = None,
    headless: bool = True,
    url: str = COINGLASS_URL,
) -> Dict[str, Any]:
    """Sync wrapper for code that is not async-aware."""
    return asyncio.run(
        collect_coinglass_dom_snapshot(timeframes=timeframes, headless=headless, url=url)
    )


if __name__ == "__main__":
    snapshot = collect_coinglass_dom_snapshot_sync(timeframes=["24h"], headless=True)
    print(json.dumps(snapshot, ensure_ascii=False, indent=2)[:12000])
