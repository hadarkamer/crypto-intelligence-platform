# Stage 87.1 — Nearest Reference Selection

## Scope
This patch changes only Price/OI reference selection and validation output.
It does not alter Max Pain scoring, Alerts, Watch, OI formulas, CVD storage, or trading logic.

## Reference selection
For every requested window, the engine now considers:
- live Price/OI snapshots;
- historical Price/OI backfill candles.

It selects the candidate with the smallest absolute time distance from:

`target_time = current_time - requested_window`

A candidate is valid only when:

`abs(candidate_time - target_time) <= 20 minutes`

Candidates may be immediately before or after the target time. This avoids a systematic preference for the previous 30-minute candle and allows historical data to replace a stale live candidate.

## Validation command
`/oi_validation SYMBOL` now displays for each window:
- requested target timestamp;
- chosen reference timestamp;
- absolute offset in seconds;
- whether the reference is before or after the target.

## Unchanged behavior
- Price/OI remains independent from Max Pain score.
- Invalid/missing references return No Data.
- Flow Foundation remains storage-only.
- No CVD/Flow decision is added.
