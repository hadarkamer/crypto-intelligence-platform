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
import hashlib
import json
import math
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import research_dom_capture as dom_capture


COINGLASS_URL = "https://www.coinglass.com/liquidation-maxpain"
DEFAULT_TIMEFRAMES = ["12h", "24h", "48h", "3d", "1w", "2w", "1m"]


def _capture_unavailable(timeframe, reason, *, outcome='FAILED', metadata=None):
    """Diagnostics are fail-open, including a failure in their own helper."""
    try:
        return dom_capture.unavailable(timeframe, reason, outcome=outcome, metadata=metadata)
    except Exception:
        payload = {'kind': 'TIMEFRAME', 'timeframe': str(timeframe)[:32], 'outcome': outcome,
                   'unavailable_reason': 'DIAGNOSTIC_HELPER_FAILURE', 'selected_table': None,
                   'row_decisions': [], 'attempt_history': [], 'truncation': []}
        encoded = json.dumps(payload, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)
        return {'version': 'coinglass-dom-row-capture-v1', 'status': 'UNAVAILABLE',
                'payload': payload, 'payload_sha256': hashlib.sha256(encoded.encode('utf-8')).hexdigest()}


def _accepted_capture(tables, timeframe, rows, metadata):
    try:
        return dom_capture.capture_poll(tables, timeframe, rows, metadata=metadata,
                                        parse_number=_parse_number, is_symbol=_is_symbol_token)
    except Exception:
        return _capture_unavailable(timeframe, 'ACCEPTED_POLL_CAPTURE_FAILED', metadata=metadata)


def _attempt_capture(result, timeframe, history, attempt):
    """Attach bounded retry metadata without changing operational result fields."""
    try:
        capture = result.get('dom_capture') or _capture_unavailable(timeframe, 'ATTEMPT_CAPTURE_UNAVAILABLE')
        debug = result.get('debug') or {}
        history.append({'attempt': attempt, 'outcome': 'ACCEPTED' if result.get('verified') else 'FAILED',
                        'retryable': result.get('retryable'), 'poll': debug.get('poll'),
                        'parsed_count': debug.get('parsed_count'), 'capture_payload_sha256': capture['payload_sha256'],
                        'reason_code': capture['payload'].get('unavailable_reason'),
                        'capture_completed_at_utc': capture['payload'].get('metadata', {}).get('capture_completed_at_utc')})
        result['dom_capture'] = dom_capture.with_attempt_history(capture, history, attempt=attempt)
    except Exception:
        result['dom_capture'] = _capture_unavailable(timeframe, 'ATTEMPT_HISTORY_CAPTURE_FAILED')
    return result


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
    s = s.replace("+", "").replace("−", "-")
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
        value = float(s) * multiplier
        return value if math.isfinite(value) else None
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


def _parse_maxpain_table_rows(tables: list[dict], timeframe: str) -> list[dict]:
    """Read named table columns; never interpret body-text offsets as prices."""
    required = ("#", "Symbol", "Price · 24h", "Long pain below", "Short pain above")
    matches = []
    for table in tables:
        headers = [re.sub(r"\s+", " ", h).strip(" ↑↓") for h in table["headers"]]
        if all(headers.count(name) == 1 for name in required):
            matches.append((table, {name: headers.index(name) for name in required}))
    if len(matches) != 1:
        raise ValueError("unrecognized Max Pain table columns")
    table, columns = matches[0]
    rows = []
    now = datetime.now(timezone.utc).isoformat()
    for cells in table["rows"]:
        # Loading/empty-state rows span the table and contain no price fields.
        if len(cells) != len(table["headers"]):
            continue
        rank = cells[columns["#"]]["text"].strip()
        symbol = cells[columns["Symbol"]]["primary"].strip().upper()
        price_cell = cells[columns["Price · 24h"]]
        long_cell = cells[columns["Long pain below"]]
        short_cell = cells[columns["Short pain above"]]
        price_texts = [cell["primary"] for cell in (price_cell, short_cell, long_cell)]
        prices = [_parse_number(value) for value in price_texts]
        amounts = [_parse_number(cell["amount"]) for cell in (short_cell, long_cell)]
        if (not rank.isdigit() or not _is_symbol_token(symbol)
                or any("%" in value for value in price_texts)
                or any(value is None or value <= 0 for value in prices)
                or any(value is None or value < 0 for value in amounts)):
            continue
        price, short_px, long_px = prices
        rows.append({
            "collected_at_utc": now, "timeframe": timeframe,
            "source": "dom_body_maxpain", "rank": int(rank), "symbol": symbol,
            "price": price, "max_short_price": short_px, "max_long_price": long_px,
            "short_amount_usd": amounts[0], "long_amount_usd": amounts[1],
            "short_distance_usd": short_px - price,
            "short_distance_pct": (short_px / price - 1) * 100,
            "long_distance_usd": price - long_px,
            "long_distance_pct": (1 - long_px / price) * 100,
            "raw_cells": [cell["text"] for cell in cells], "extra": None,
        })
    return rows


async def _extract_tables(page) -> List[Dict[str, Any]]:
    """Read the ranking table's cell boundaries, including nested price/amount fields."""
    return await page.evaluate(
        """
        () => [...document.querySelectorAll('#mp-table table')].map(table => ({
            headers: [...table.querySelectorAll('thead th')].map(th => th.innerText.trim()),
            rows: [...table.querySelectorAll('tbody tr')].map(row =>
                [...row.querySelectorAll('td')].map(td => {
                    const value = td.querySelector('b');
                    return {
                        text: td.innerText.trim(),
                        primary: (value || td.querySelector('span') || td).innerText.trim(),
                        amount: value?.nextElementSibling?.innerText.trim() || ''
                    };
                }))
        }))
        """
    )


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
    """Require agreement between the Window selector and the ranking heading."""
    return await page.evaluate(
        r"""
        () => {
            const selected = document.querySelector('[aria-label="Window"] [role="combobox"]');
            const heading = document.querySelector('#mp-table-h');
            const label = el => el?.innerText.match(/\b(12H|24H|48H|3D|1W|2W|1M)\b/)?.[1];
            const value = label(selected);
            return value && value === label(heading) ? value.toLowerCase() : null;
        }
        """
    )


async def _click_timeframe_verified(page, timeframe: str) -> bool:
    """Select the existing Window dropdown's exact option."""
    try:
        selector = page.locator('[aria-label="Window"] [role="combobox"]')
        await selector.wait_for(state="visible", timeout=4000)
        if await _active_timeframe_label(page) == timeframe:
            return True
        await selector.click(timeout=4000)
        await page.locator('[role="listbox"]').get_by_text(timeframe.upper(), exact=True).click(timeout=4000)
        return True
    except PlaywrightTimeoutError:
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
    forbidden_fingerprints = forbidden_fingerprints or {}
    last_debug: Dict[str, Any] = {}
    try:
        baseline_rows = _parse_maxpain_table_rows(await _extract_tables(page), "baseline")
    except ValueError as exc:
        return {"timeframe": timeframe, "verified": False, "rows": [],
                "error": str(exc), "retryable": False, "debug": {},
                "dom_capture": _capture_unavailable(timeframe, 'BASELINE_SCHEMA_UNRECOGNIZED')}
    baseline_fp = _rows_fingerprint(baseline_rows) if baseline_rows else None
    baseline_label = await _active_timeframe_label(page)
    clicked = await _click_timeframe_verified(page, timeframe)
    if not clicked:
        return {"timeframe": timeframe, "verified": False, "rows": [],
                "error": "Max Pain Window option timed out", "retryable": True, "debug": {},
                "dom_capture": _capture_unavailable(timeframe, 'WINDOW_SELECTOR_TIMEOUT')}
    stable_fp = None
    stable_count = 0
    # Fresh-page retries already exist; do not multiply them by another retry loop.
    for poll in range(1, 25):
        await page.wait_for_timeout(750)
        try:
            # Keep the same single extraction and the unchanged operational
            # parser. Retain this exact poll only after its existing gate passes.
            tables = await _extract_tables(page)
            rows = _parse_maxpain_table_rows(tables, timeframe)
        except ValueError as exc:
            return {"timeframe": timeframe, "verified": False, "rows": [],
                    "error": str(exc), "retryable": False, "debug": last_debug,
                    "dom_capture": _capture_unavailable(timeframe, 'POLL_SCHEMA_UNRECOGNIZED', metadata={'poll': poll})}
        fingerprint = _rows_fingerprint(rows) if rows else None
        active_label = await _active_timeframe_label(page)
        symbols = [str(r.get("symbol") or "") for r in rows]
        unique_symbols = len(symbols) == len(set(symbols))
        row_count_ok = len(rows) >= 30
        active_ok = active_label == timeframe
        baseline_changed = baseline_label == timeframe or baseline_fp is None or fingerprint != baseline_fp
        previous_changed = previous_fingerprint is None or fingerprint != previous_fingerprint
        duplicate_of = next(
            (tf for tf, fp in forbidden_fingerprints.items() if fingerprint and fp == fingerprint), None,
        )
        if fingerprint and fingerprint == stable_fp:
            stable_count += 1
        else:
            stable_fp = fingerprint
            stable_count = 1 if fingerprint else 0
        last_debug = {
            "parsed_count": len(rows), "poll": poll, "active_label": active_label,
            "expected_label": timeframe, "clicked": clicked, "active_ok": active_ok,
            "baseline_changed": baseline_changed, "previous_changed": previous_changed,
            "stable_count": stable_count, "unique_symbols": unique_symbols,
            "row_count_ok": row_count_ok, "duplicate_of": duplicate_of,
        }
        if (active_ok and baseline_changed and previous_changed and stable_count >= 2
                and unique_symbols and row_count_ok and not duplicate_of):
            return {"timeframe": timeframe, "clicked": clicked, "verified": True,
                    "rows": rows, "fingerprint": fingerprint, "debug": last_debug,
                    "dom_capture": _accepted_capture(tables, timeframe, rows,
                        {**last_debug, 'baseline_label': baseline_label, 'fingerprint': fingerprint,
                         'baseline_fingerprint': baseline_fp, 'previous_fingerprint': previous_fingerprint,
                         'parsed_at_utc': rows[0]['collected_at_utc'] if rows else None})}
    return {
        "timeframe": timeframe, "clicked": clicked, "verified": False, "rows": [],
        "fingerprint": None,
        "error": "timeframe failed active-tab/content/stability/uniqueness verification",
        "debug": last_debug,
        "dom_capture": _capture_unavailable(timeframe, 'TIMEFRAME_VERIFICATION_FAILED', metadata=last_debug),
    }


async def _new_ready_page(context, url: str):
    """Open a clean CoinGlass page for a failed-timeframe retry."""
    page = await context.new_page()
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        await page.wait_for_timeout(8000)

        for label in ["Accept", "I Agree", "Got it", "Close", "×"]:
            try:
                await page.get_by_text(label, exact=True).first.click(timeout=1000)
                await page.wait_for_timeout(400)
            except Exception:
                pass
    except BaseException:
        # The caller owns the page only after this function returns. Close it
        # here if setup fails or is cancelled before ownership can transfer.
        try:
            await page.close()
        except BaseException:
            # Cleanup must not replace the original setup error/cancellation.
            pass
        raise

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
    capture_history = []

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
            _attempt_capture(last_result, timeframe, capture_history, attempt)

            if last_result.get("retryable") is False:
                return last_result

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
                "dom_capture": _capture_unavailable(timeframe, 'FRESH_PAGE_ATTEMPT_EXCEPTION', metadata={'error_code': type(exc).__name__}),
            }
            _attempt_capture(last_result, timeframe, capture_history, attempt)
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
                    if result.get("retryable") is False:
                        missing.extend(tf for tf in collection_order if tf not in by_timeframe)
                        break
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
    snapshot = {
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
    try:
        snapshot['dom_capture'] = dom_capture.aggregate(requested_timeframes, by_timeframe)
    except Exception:
        try:
            snapshot['dom_capture'] = dom_capture.unavailable_snapshot(requested_timeframes, 'SNAPSHOT_CAPTURE_FAILED')
        except Exception:
            payload = {'kind': 'SNAPSHOT', 'requested_timeframes': [], 'timeframes': {},
                       'unavailable_reason': 'DIAGNOSTIC_HELPER_FAILURE',
                       'original_requested_timeframe_count': len(requested_timeframes)}
            encoded = json.dumps(payload, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)
            snapshot['dom_capture'] = {'version': 'coinglass-dom-row-capture-v1', 'status': 'UNAVAILABLE',
                                       'payload': payload, 'payload_sha256': hashlib.sha256(encoded.encode('utf-8')).hexdigest()}
    return snapshot


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
