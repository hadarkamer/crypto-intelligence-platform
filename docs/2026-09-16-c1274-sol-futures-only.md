# C1274 — SOL Futures-only notification

Effective from the deployment of this change, the experimental C1274
notification no longer uses a Magnet event as a carrier or condition.

## Frozen rule

- Symbol: `SOL` only.
- Display and price-level threshold: `150` basis points (`1.5%`).
- Input: the Futures long time family from the frozen operational Watch score
  bundle.
- Match: Futures is available; long-family quality is at least `0.65`; the
  total Futures score has magnitude at least `25` and has the same sign as the
  long-family direction.
- Direction is direct: `BULLISH` becomes `LONG`; `BEARISH` becomes `SHORT`.
- Price reference: `FUTURES_CVD` only, at the exact source candle close. A
  missing or clock-mismatched quote remains unavailable.
- Deduplication: at most one decision per SOL Futures source candle, including
  non-matches and source revisions.

For a base price `P` the displayed levels are:

- `LONG`: stop `P × 0.985`, target `P × 1.015`.
- `SHORT`: stop `P × 1.015`, target `P × 0.985`.

The v2-to-v3 state migration cancels only old pending C1274 notifications.
In-flight and terminal attempts remain frozen for auditability. Historical
documents describing the prior Magnet-carried 1% rule remain historical and
are not rewritten.
