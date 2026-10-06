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
    forbidden_fingerprints: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Read one timeframe only after tab, content and stability are verified.

    Reliability rules:
    - capture the page baseline before clicking;
    - for non-default tabs, require content to change from that baseline;
    - require the same parsed fingerprint on two consecutive polls;
    - reject a fingerprint already accepted for another timeframe;
    - validate row count and unique symbols before accepting.
    """
    label_map = {
        "12h": "12 hour", "24h": "24 hour", "48h": "48 hour",
        "3d": "3 day", "1w": "1 week", "2w": "2 week", "1m": "1 month",
    }
    expected_label = label_map.get(timeframe, timeframe)
    forbidden_fingerprints = forbidden_fingerprints or {}

    baseline_lines = await _extract_body_lines(page, limit=1200)
    baseline_rows = _parse_body_maxpain_rows(baseline_lines, "baseline")
    baseline_fp = _rows_fingerprint(baseline_rows) if baseline_rows else None

    last_debug: Dict[str, Any] = {}
    for attempt in range(1, 4):
        clicked = await _click_timeframe_verified(page, timeframe)
        stable_fp = None
        stable_count = 0

        for poll in range(1, 25):
            await page.wait_for_timeout(750)
            lines = await _extract_body_lines(page, limit=1400)
            rows = _parse_body_maxpain_rows(lines, timeframe)
            fingerprint = _rows_fingerprint(rows) if rows else None
            active_label = await _active_timeframe_label(page)

            symbols = [str(r.get("symbol") or "") for r in rows]
            unique_symbols = len(symbols) == len(set(symbols))
            row_count_ok = len(rows) >= 30
            active_ok = active_label == expected_label
            baseline_changed = timeframe == "24h" or baseline_fp is None or fingerprint != baseline_fp
            previous_changed = previous_fingerprint is None or fingerprint != previous_fingerprint
            duplicate_of = next(
                (tf for tf, fp in forbidden_fingerprints.items() if fingerprint and fp == fingerprint),
                None,
            )

            if fingerprint and fingerprint == stable_fp:
                stable_count += 1
            else:
                stable_fp = fingerprint
                stable_count = 1 if fingerprint else 0

            last_debug = {
                "parsed_count": len(rows), "attempt": attempt, "poll": poll,
                "active_label": active_label, "expected_label": expected_label,
                "fingerprint": fingerprint, "baseline_fingerprint": baseline_fp,
                "previous_fingerprint": previous_fingerprint, "clicked": clicked,
                "active_ok": active_ok, "baseline_changed": baseline_changed,
                "previous_changed": previous_changed, "stable_count": stable_count,
                "unique_symbols": unique_symbols, "row_count_ok": row_count_ok,
                "duplicate_of": duplicate_of, "body_preview": lines[:120],
            }

            if (
                active_ok and baseline_changed and previous_changed and
                stable_count >= 2 and unique_symbols and row_count_ok and not duplicate_of
            ):
                return {
                    "timeframe": timeframe, "clicked": clicked, "verified": True,
                    "rows": rows, "fingerprint": fingerprint, "debug": last_debug,
                }

        await page.wait_for_timeout(1000)

    return {
        "timeframe": timeframe, "clicked": False, "verified": False, "rows": [],
        "fingerprint": None,
        "error": "timeframe failed active-tab/content/stability/uniqueness verification",
        "debug": last_debug,
    }


async def _new_ready_page(context, url: str):
    """Open a clean CoinGlass page for a failed-timeframe retry."""
    page = await context.new_page()
    await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
    await page.wait_for_timeout(8000)

    for label in ["Accept", "I Agree", "Got it", "Close", "×"]:
        try:
            await page.get_by_text(label, exact=True).first.click(timeout=1000)
            await page.wait_for_timeout(400)
        except Exception:
            pass

    return page


async def _retry_timeframe_on_fresh_page(
    context,
    url: str,
    timeframe: str,
    previous_fingerprint: Optional[str],
    attempts: int,
    forbidden_fingerprints: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Retry a rejected timeframe on clean pages."""
    last_result: Dict[str, Any] = {
        "timeframe": timeframe,
        "clicked": False,
        "verified": False,
        "rows": [],
        "fingerprint": None,
        "error": "fresh-page retries exhausted",
        "debug": {},
    }

    for attempt in range(1, attempts + 1):
        retry_page = None
        try:
            print(
                f"[dom] tf={timeframe} fresh-page retry {attempt}/{attempts}",
                flush=True,
            )
            retry_page = await _new_ready_page(context, url)
            last_result = await read_timeframe(
                retry_page,
                timeframe,
                previous_fingerprint=previous_fingerprint,
                forbidden_fingerprints=forbidden_fingerprints,
            )

            if last_result.get("verified") and last_result.get("rows"):
                print(
                    f"[dom] tf={timeframe} retry succeeded on attempt {attempt}",
                    flush=True,
                )
                return last_result

        except Exception as exc:
            last_result = {
                "timeframe": timeframe,
                "clicked": False,
                "verified": False,
                "rows": [],
                "fingerprint": None,
                "error": repr(exc),
                "debug": {},
            }
            print(
                f"[dom] tf={timeframe} retry error: {exc!r}",
                flush=True,
            )
        finally:
            if retry_page is not None:
                try:
                    await retry_page.close()
                except Exception:
                    pass

        await asyncio.sleep(1.5)

    return last_result


async def collect_coinglass_dom_snapshot(
    timeframes: Optional[List[str]] = None,
    headless: bool = True,
    url: str = COINGLASS_URL,
) -> Dict[str, Any]:
    """Collect an atomic, fully verified seven-timeframe snapshot.

    Each timeframe is opened in its own fresh page. A timeframe is accepted only
    after the requested tab is active, the table changed from the fresh-page
    baseline when needed, and the parsed fingerprint is stable across two reads.
    Exact cross-timeframe duplicates are retried and then rejected. No partial
    snapshot is considered OK.
    """
    requested_timeframes = list(dict.fromkeys(timeframes or DEFAULT_TIMEFRAMES))
    preferred_order = ["24h", "12h", "48h", "3d", "1w", "2w", "1m"]
    collection_order = [tf for tf in preferred_order if tf in requested_timeframes]
    collection_order += [tf for tf in requested_timeframes if tf not in collection_order]

    all_rows: List[Dict[str, Any]] = []
    by_timeframe: Dict[str, Any] = {}
    missing: List[str] = []
    accepted_fingerprints: Dict[str, str] = {}

    print(
        f"[dom] isolated collection; url={url}; headless={headless}; "
        f"timeframes={collection_order}", flush=True,
    )

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=headless,
            args=[
                "--no-sandbox", "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--disable-gpu", "--disable-extensions",
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

        try:
            previous_fp: Optional[str] = None
            for tf in collection_order:
                print(f"[dom] ===== isolated timeframe {tf} =====", flush=True)
                result = await _retry_timeframe_on_fresh_page(
                    context,
                    url,
                    tf,
                    previous_fingerprint=previous_fp,
                    attempts=3,
                    forbidden_fingerprints=accepted_fingerprints,
                )
                by_timeframe[tf] = result
                debug = result.get("debug", {})
                rows = result.get("rows", [])
                fp = result.get("fingerprint")

                if result.get("verified") and rows and fp:
                    print(
                        f"[dom] tf={tf} ACCEPTED rows={len(rows)} "
                        f"active={debug.get('active_label')} stable={debug.get('stable_count')} "
                        f"unique={debug.get('unique_symbols')}", flush=True,
                    )
                    all_rows.extend(rows)
                    accepted_fingerprints[tf] = fp
                    previous_fp = fp
                else:
                    missing.append(tf)
                    print(
                        f"[dom] tf={tf} REJECTED error={result.get('error')} "
                        f"debug={json.dumps(debug, ensure_ascii=False)[:1800]}",
                        flush=True,
                    )
        finally:
            await context.close()
            await browser.close()
            print("[dom] isolated browser/context closed", flush=True)

    # Final whole-snapshot integrity checks.
    pair_counts: Dict[tuple, int] = {}
    tf_counts = {tf: 0 for tf in requested_timeframes}
    for row in all_rows:
        key = (str(row.get("symbol") or "").upper(), str(row.get("timeframe") or ""))
        pair_counts[key] = pair_counts.get(key, 0) + 1
        if key[1] in tf_counts:
            tf_counts[key[1]] += 1

    duplicate_pairs = [f"{sym}/{tf}" for (sym, tf), count in pair_counts.items() if count > 1]
    empty_or_short = [tf for tf, count in tf_counts.items() if count < 30]
    for tf in empty_or_short:
        if tf not in missing:
            missing.append(tf)
    if duplicate_pairs:
        missing = list(dict.fromkeys(missing + requested_timeframes))

    # A successful result is atomic: all requested timeframes verified, no duplicate pairs.
    ok = not missing and not duplicate_pairs
    public_order = {tf: i for i, tf in enumerate(requested_timeframes)}
    all_rows.sort(key=lambda row: (
        public_order.get(str(row.get("timeframe")), 999),
        int(row.get("rank") or 9999), str(row.get("symbol") or ""),
    ))

    print(
        f"[dom] atomic result ok={ok}; rows={len(all_rows)}; counts={tf_counts}; "
        f"missing={missing}; duplicates={duplicate_pairs}", flush=True,
    )
    return {
        "ok": ok,
        "rows": all_rows if ok else [],
        "row_count": len(all_rows) if ok else 0,
        "missing_timeframes": sorted(set(missing), key=requested_timeframes.index),
        "duplicate_pairs": duplicate_pairs,
        "timeframe_counts": tf_counts,
        "by_timeframe": by_timeframe,
        "debug": {
            "requested_timeframes": requested_timeframes,
            "collection_order": collection_order,
            "accepted_fingerprints": accepted_fingerprints,
        },
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
