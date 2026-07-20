# Stage 59 — Stable atomic CoinGlass collection

## Scope
All live CoinGlass consumers use the same collector:
- `/collect`
- `/alerts`
- `/alert SYMBOL`
- `/debug SYMBOL`
- Watch

## Reliability rules
1. Every timeframe is opened in a fresh browser page.
2. The requested tab must be visibly active.
3. Non-default tabs must differ from the page baseline.
4. A table is accepted only after the same fingerprint is read twice consecutively.
5. Symbol rows within a timeframe must be unique and at least 30 rows must parse.
6. A fingerprint already accepted for another timeframe is rejected and retried on a fresh page.
7. Each timeframe receives up to three fresh-page attempts.
8. The final snapshot is atomic: if one timeframe fails, no rows are returned or saved.
9. Duplicate symbol/timeframe pairs reject the complete snapshot.
10. The existing shared scrape lock prevents overlapping collection, alerts, debug and Watch scans.

## Expected log ending
A valid scan ends with:

`[dom] atomic result ok=True; rows=...; counts=...; missing=[]; duplicates=[]`

A failed validation ends with `ok=False` and the caller must not save or score the snapshot.
