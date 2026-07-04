import asyncio
import re
import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from playwright.async_api import async_playwright
from .config import COINGLASS_MAX_PAIN_URL, TIMEFRAMES, TOP_COINS_LIMIT, SOURCE_NAME, COLLECTOR_VERSION
from .analytics import enrich_rows
from .storage import insert_snapshots

def parse_number(value: str) -> Optional[float]:
    if value is None:
        return None
    raw = value.replace(",", "").replace("$", "").strip()
    match = re.search(r"-?\d+(\.\d+)?", raw)
    if not match:
        return None
    num = float(match.group(0))
    if raw.lower().endswith("k"):
        num *= 1_000
    elif raw.lower().endswith("m"):
        num *= 1_000_000
    elif raw.lower().endswith("b"):
        num *= 1_000_000_000
    return num

async def scrape_timeframe(page, timeframe: str, collected_at: str, scrape_duration: float) -> List[Dict[str, Any]]:
    try:
        await page.get_by_text(timeframe, exact=True).click(timeout=5000)
        await page.wait_for_timeout(2500)
    except Exception:
        pass

    rows = await page.locator("table tbody tr").all()
    output = []

    for idx, row in enumerate(rows[:TOP_COINS_LIMIT], start=1):
        cells = [await c.inner_text() for c in await row.locator("td").all()]
        cells = [c.strip() for c in cells if c.strip()]
        if len(cells) < 4:
            continue

        symbol_match = re.search(r"[A-Z0-9]{2,12}", cells[0])
        symbol = symbol_match.group(0) if symbol_match else cells[0].split()[0].upper()

        numbers = [parse_number(c) for c in cells]
        numbers = [n for n in numbers if n is not None]

        current_price = numbers[0] if len(numbers) > 0 else None
        short_max_pain = numbers[1] if len(numbers) > 1 else None
        long_max_pain = numbers[2] if len(numbers) > 2 else None

        output.append({
            "collected_at": collected_at,
            "source": SOURCE_NAME,
            "collector_version": COLLECTOR_VERSION,
            "scrape_duration_seconds": scrape_duration,
            "is_valid": 1,
            "validation_errors": None,
            "symbol": symbol,
            "rank": idx,
            "timeframe": timeframe,
            "current_price": current_price,
            "short_max_pain": short_max_pain,
            "long_max_pain": long_max_pain,
        })
    return output

def validate_snapshot(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    global_errors = []
    expected_min = TOP_COINS_LIMIT * len(TIMEFRAMES) * 0.8

    if len(rows) < expected_min:
        global_errors.append(f"Expected around {TOP_COINS_LIMIT * len(TIMEFRAMES)} rows, got {len(rows)}")

    seen_timeframes = {r["timeframe"] for r in rows}
    missing_timeframes = set(TIMEFRAMES) - seen_timeframes
    if missing_timeframes:
        global_errors.append(f"Missing timeframes: {sorted(missing_timeframes)}")

    for row in rows:
        row_errors = []
        if not row.get("symbol"):
            row_errors.append("missing symbol")
        if row.get("current_price") is None:
            row_errors.append("missing current_price")
        if row.get("short_max_pain") is None:
            row_errors.append("missing short_max_pain")
        if row.get("long_max_pain") is None:
            row_errors.append("missing long_max_pain")

        if global_errors or row_errors:
            row["is_valid"] = 0
            row["validation_errors"] = "; ".join(global_errors + row_errors)[:1000]
    return rows

async def collect_once() -> int:
    start = time.time()
    collected_at = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0).isoformat()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(viewport={"width": 1440, "height": 1200})
        await page.goto(COINGLASS_MAX_PAIN_URL, wait_until="networkidle", timeout=60000)

        all_rows = []
        for timeframe in TIMEFRAMES:
            rows = await scrape_timeframe(page, timeframe, collected_at, time.time() - start)
            all_rows.extend(rows)

        await browser.close()

    all_rows = validate_snapshot(all_rows)
    all_rows = enrich_rows(all_rows)
    return insert_snapshots(all_rows)

if __name__ == "__main__":
    inserted = asyncio.run(collect_once())
    print(f"Inserted {inserted} rows")
