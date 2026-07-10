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




def _parse_number(value):
    """Convert CoinGlass strings like '$63,650', '49.57M', '+2.29%', '-$1,096.9' to float."""
    if value is None:
        return None
    s = str(value).strip()
    if not s or s in {"-", "—", "N/A", "nan", "None", "💥"}:
        return None

    # Remove common formatting while preserving sign and multiplier.
    s = s.replace(",", "")
    s = s.replace("$", "")
    s = s.replace("%", "")
    s = s.replace("≈", "")
    s = s.replace("+", "")
    s = s.strip()

    multiplier = 1.0
    if s[-1:].upper() == "K":
        multiplier = 1_000.0
        s = s[:-1]
    elif s[-1:].upper() == "M":
        multiplier = 1_000_000.0
        s = s[:-1]
    elif s[-1:].upper() == "B":
        multiplier = 1_000_000_000.0
        s = s[:-1]
    elif s[-1:].upper() == "T":
        multiplier = 1_000_000_000_000.0
        s = s[:-1]

    try:
        return float(s) * multiplier
    except Exception:
        return None


def _is_symbol_token(x: str) -> bool:
    if not x:
        return False
    x = x.strip().upper()
    if len(x) < 2 or len(x) > 12:
        return False
    if x in {"NEW", "API", "APP", "USD", "USDT", "BTC", "ETH", "SOL", "XRP", "BNB", "DOGE", "ADA", "LINK", "AVAX", "TRX", "HYPE"}:
        # Known valid symbols plus some common symbols; still allow by regex below.
        pass
    return bool(re.fullmatch(r"[A-Z0-9]{2,12}", x))


def _parse_body_maxpain_rows(lines: list[str], timeframe: str) -> list[dict]:
    """Parse CoinGlass Liquidation Max Pain body text.

    Expected repeating pattern after headers:
    rank, symbol, price,
    short_max_pain_px, short_amount, short_distance_usd, short_distance_pct, emoji,
    long_max_pain_px, long_amount, long_distance_usd, long_distance_pct, emoji
    """
    rows = []
    now = datetime.now(timezone.utc).isoformat()

    # Start after the Max Pain table header if possible.
    start = 0
    for i in range(len(lines) - 7):
        window = lines[i:i+8]
        if (
            "Ranking" in window
            and "Symbol" in window
            and "Price" in window
            and "Short Max Pain" in window
            and "Long Max Pain" in window
        ):
            start = i + 8
            break

    i = start
    while i < len(lines) - 11:
        rank = lines[i].strip()
        symbol = lines[i + 1].strip().upper()

        if not rank.isdigit() or not _is_symbol_token(symbol):
            i += 1
            continue

        price = _parse_number(lines[i + 2])
        short_px = _parse_number(lines[i + 3])
        short_amount = _parse_number(lines[i + 4])
        short_dist_abs = _parse_number(lines[i + 5])
        short_dist_pct = _parse_number(lines[i + 6])
        long_px = _parse_number(lines[i + 8])
        long_amount = _parse_number(lines[i + 9])
        long_dist_abs = _parse_number(lines[i + 10])
        long_dist_pct = _parse_number(lines[i + 11])

        if price is not None and short_px is not None and long_px is not None:
            rows.append({
                "collected_at_utc": now,
                "timeframe": timeframe,
                "source": "dom_body_maxpain",
                "rank": int(rank),
                "symbol": symbol,
                "price": price,
                "max_short_price": short_px,
                "short_amount_usd": short_amount,
                "short_distance_usd": short_dist_abs,
                "short_distance_pct": short_dist_pct,
                "max_long_price": long_px,
                "long_amount_usd": long_amount,
                "long_distance_usd": long_dist_abs,
                "long_distance_pct": long_dist_pct,
                "raw_cells": lines[i:i+13],
                "extra": None,
            })
            i += 13
        else:
            i += 1

    return rows


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



def _rows_fingerprint(rows: List[Dict[str, Any]]) -> str:
    """Stable signature used to detect stale/duplicated timeframe content."""
    sample = []
    for row in rows[:10]:
        sample.append((
            row.get("symbol"),
            row.get("max_short_price"),
            row.get("short_amount_usd"),
            row.get("max_long_price"),
            row.get("long_amount_usd"),
        ))
    return json.dumps(sample, sort_keys=True, ensure_ascii=False)


async def _active_timeframe_label(page) -> Optional[str]:
    """Best-effort detection of the currently selected timeframe tab."""
    return await page.evaluate(
        """
        () => {
            const labels = ['12 hour','24 hour','48 hour','3 day','1 week','2 week','1 month'];
            const nodes = [...document.querySelectorAll('button, [role="tab"], div, span')];
            for (const el of nodes) {
                const text = (el.innerText || el.textContent || '').trim();
                if (!labels.includes(text)) continue;

                const cls = String(el.className || '').toLowerCase();
                const ariaSelected = el.getAttribute('aria-selected');
                const dataState = el.getAttribute('data-state');
                const style = getComputedStyle(el);

                if (
                    ariaSelected === 'true' ||
                    dataState === 'active' ||
                    cls.includes('active') ||
                    cls.includes('selected') ||
                    cls.includes('current') ||
                    style.borderColor === 'rgb(65, 132, 230)' ||
                    style.color === 'rgb(65, 132, 230)'
                ) {
                    return text;
                }
            }
            return null;
        }
        """
    )


async def _click_timeframe_verified(page, timeframe: str) -> bool:
    """Click the requested tab using several selectors."""
    label_map = {
        "12h": "12 hour",
        "24h": "24 hour",
        "48h": "48 hour",
        "3d": "3 day",
        "1w": "1 week",
        "2w": "2 week",
        "1m": "1 month",
    }
    label = label_map.get(timeframe, timeframe)

    candidates = [
        page.get_by_role("tab", name=label, exact=True),
        page.get_by_role("button", name=label, exact=True),
        page.get_by_text(label, exact=True),
        page.locator("button").filter(has_text=label),
        page.locator('[role="tab"]').filter(has_text=label),
    ]

    for locator in candidates:
        try:
            target = locator.first
            await target.scroll_into_view_if_needed(timeout=2000)
            await target.click(timeout=4000, force=True)
            return True
        except Exception:
            continue
    return False


async def read_timeframe(
    page,
    timeframe: str,
    previous_fingerprint: Optional[str] = None,
) -> Dict[str, Any]:
    """Select a timeframe and accept rows only after the page really changed.

    The old bug:
    CoinGlass sometimes reported click success while the visible table still
    contained the previous timeframe. The parser then labelled stale rows as the
    new timeframe.

    This version:
    - retries the click
    - waits for active-tab/content confirmation
    - compares row fingerprints
    - refuses duplicate stale content
    """
    label_map = {
        "12h": "12 hour",
        "24h": "24 hour",
        "48h": "48 hour",
        "3d": "3 day",
        "1w": "1 week",
        "2w": "2 week",
        "1m": "1 month",
    }
    expected_label = label_map.get(timeframe, timeframe)

    last_debug = {}
    for attempt in range(1, 4):
        clicked = await _click_timeframe_verified(page, timeframe)

        # CoinGlass updates asynchronously. Poll rather than trusting click().
        for poll in range(1, 13):
            await page.wait_for_timeout(1000)

            tables = await _extract_tables(page)
            lines = await _extract_body_lines(page, limit=1200)
            rows = _parse_body_maxpain_rows(lines, timeframe)
            fingerprint = _rows_fingerprint(rows) if rows else None
            active_label = await _active_timeframe_label(page)

            changed = (
                bool(rows)
                and (
                    previous_fingerprint is None
                    or fingerprint != previous_fingerprint
                    or timeframe == "12h"
                )
            )
            active_ok = active_label in {None, expected_label}

            last_debug = {
                "table_count": len(tables),
                "table_headers": [t.get("headers", []) for t in tables],
                "body_preview": lines[:120],
                "parsed_count": len(rows),
                "attempt": attempt,
                "poll": poll,
                "active_label": active_label,
                "expected_label": expected_label,
                "fingerprint": fingerprint,
                "previous_fingerprint": previous_fingerprint,
                "clicked": clicked,
                "changed": changed,
                "active_ok": active_ok,
            }

            if changed and active_ok:
                return {
                    "timeframe": timeframe,
                    "clicked": clicked,
                    "verified": True,
                    "rows": rows,
                    "fingerprint": fingerprint,
                    "debug": last_debug,
                }

        # Retry after another click and slightly longer pause.
        await page.wait_for_timeout(1500)

    return {
        "timeframe": timeframe,
        "clicked": False,
        "verified": False,
        "rows": [],
        "fingerprint": None,
        "error": "timeframe content did not change or active tab could not be verified",
        "debug": last_debug,
    }


async def collect_coinglass_dom_snapshot(
    timeframes: Optional[List[str]] = None,
    headless: bool = True,
    url: str = COINGLASS_URL,
) -> Dict[str, Any]:
    """
    Main entry point for /collect.

    DEBUG version:
    - prints every browser/DOM step to Render logs
    - prints table headers + body preview when it cannot parse Max Pain rows
    - does not fake rows if Max Pain fields are not found
    """
    timeframes = timeframes or DEFAULT_TIMEFRAMES
    all_rows: List[Dict[str, Any]] = []
    by_timeframe: Dict[str, Any] = {}
    missing: List[str] = []
    debug_summary: Dict[str, Any] = {}

    print(f"[dom] launch browser; url={url}; headless={headless}; timeframes={timeframes}", flush=True)

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
        print("[dom] browser launched", flush=True)

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
            print("[dom] opening page", flush=True)
            await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            print(f"[dom] page opened; title={await page.title()!r}; current_url={page.url}", flush=True)

            # Let React/network render dynamic content.
            await page.wait_for_timeout(8000)
            print("[dom] waited 8s after domcontentloaded", flush=True)

            # Dismiss popups if present.
            for label in ["Accept", "I Agree", "Got it", "Close", "×"]:
                try:
                    await page.get_by_text(label, exact=True).first.click(timeout=1000)
                    print(f"[dom] dismissed popup/button: {label}", flush=True)
                    await page.wait_for_timeout(500)
                except Exception:
                    pass

            initial_tables = await _extract_tables(page)
            initial_lines = await _extract_body_lines(page, limit=250)
            print(f"[dom] initial table_count={len(initial_tables)}", flush=True)
            for t in initial_tables[:6]:
                print(
                    f"[dom] table[{t.get('index')}] headers={t.get('headers')} rows={len(t.get('rows') or [])}",
                    flush=True,
                )
            print("[dom] body_preview_first_80=" + json.dumps(initial_lines[:80], ensure_ascii=False), flush=True)

            previous_fingerprint = None
            accepted_fingerprints: Dict[str, str] = {}

            for tf in timeframes:
                print(f"[dom] ===== timeframe {tf} =====", flush=True)
                try:
                    result = await read_timeframe(
                        page,
                        tf,
                        previous_fingerprint=previous_fingerprint,
                    )
                    by_timeframe[tf] = result
                    rows = result.get("rows", [])
                    debug = result.get("debug", {})
                    print(
                        f"[dom] tf={tf} clicked={result.get('clicked')} row_count={len(rows)} "
                        f"table_count={debug.get('table_count')}",
                        flush=True,
                    )
                    for i, headers in enumerate((debug.get("table_headers") or [])[:6]):
                        print(f"[dom] tf={tf} headers[{i}]={headers}", flush=True)
                    print(
                        f"[dom] tf={tf} body_preview_first_60="
                        + json.dumps((debug.get("body_preview") or [])[:60], ensure_ascii=False),
                        flush=True,
                    )
                    fingerprint = result.get("fingerprint")
                    duplicate_of = next(
                        (
                            prior_tf
                            for prior_tf, prior_fp in accepted_fingerprints.items()
                            if fingerprint and prior_fp == fingerprint
                        ),
                        None,
                    )

                    if rows and result.get("verified") and not duplicate_of:
                        print(
                            f"[dom] tf={tf} verified=True active_label="
                            f"{debug.get('active_label')} fingerprint={fingerprint[:120] if fingerprint else None}",
                            flush=True,
                        )
                        print(
                            f"[dom] tf={tf} first_rows="
                            + json.dumps(rows[:3], ensure_ascii=False)[:4000],
                            flush=True,
                        )
                        all_rows.extend(rows)
                        accepted_fingerprints[tf] = fingerprint
                        previous_fingerprint = fingerprint
                    else:
                        missing.append(tf)
                        print(
                            f"[dom] tf={tf} REJECTED verified={result.get('verified')} "
                            f"duplicate_of={duplicate_of} error={result.get('error')} "
                            f"active_label={debug.get('active_label')}",
                            flush=True,
                        )
                except Exception as exc:
                    missing.append(tf)
                    by_timeframe[tf] = {"timeframe": tf, "error": repr(exc), "rows": []}
                    print(f"[dom] tf={tf} ERROR: {repr(exc)}", flush=True)

            # Capture a final screenshot path for Render logs. It is mostly diagnostic;
            # the file is not expected to be downloaded, but confirms page rendering.
            try:
                screenshot_path = "/tmp/coinglass_dom_debug.png"
                await page.screenshot(path=screenshot_path, full_page=True)
                print(f"[dom] screenshot saved to {screenshot_path}", flush=True)
            except Exception as exc:
                print(f"[dom] screenshot failed: {repr(exc)}", flush=True)

            debug_summary = {
                "initial_table_count": len(initial_tables),
                "initial_table_headers": [t.get("headers") for t in initial_tables[:6]],
                "initial_body_preview": initial_lines[:120],
            }

        finally:
            await context.close()
            await browser.close()
            print("[dom] browser closed", flush=True)

    print(f"[dom] done; raw_rows={len(all_rows)}; missing={missing}", flush=True)
    return {
        "ok": len(all_rows) > 0,
        "rows": all_rows,
        "row_count": len(all_rows),
        "missing_timeframes": missing,
        "by_timeframe": by_timeframe,
        "debug": debug_summary,
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
