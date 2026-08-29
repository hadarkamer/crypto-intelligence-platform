# Formula Research Runtime v1

## Objective

Find reproducible decision-time conditions that precede the widest practical
LONG or SHORT move, while retaining high out-of-sample probability, low
adverse excursion, fast favorable progress and useful MFE/MAE efficiency.

## Evidence contract

- Inputs: immutable Research Event state plus raw Price/OI, Futures CVD and
  Spot CVD points at or before the alert.
- Labels: closed canonical spot one-minute paths at 1h, 4h, 12h and 24h.
- Binance Spot USDT is the default route. HYPE uses Hyperliquid HYPE/USDT spot
  (`@107`). The exchange and pair stay explicit in every outcome.
- The first partial minute after the alert is excluded.
- Historical price candles may be imported/backfilled from the canonical
  exchange APIs when source, market, pair, resolution, method and quality are
  retained. This is different from importing an old Telegram message that
  lacks its full immutable decision-time state.
- Complete Research Events begin on 2026-08-28. Older Telegram exports may be
  imported only into the isolated legacy-message table and are not training
  rows by default.

## Discovery

For each horizon and direction, the engine:

1. sorts verified alerts chronologically;
2. freezes the earliest 70% as discovery and latest 30% as holdout;
3. derives numeric thresholds from discovery quantiles only;
4. evaluates single, pair and triple conditions in a bounded search;
5. compares each candidate with its same-direction complement;
6. applies Benjamini-Hochberg correction across all unique candidates;
7. ranks candidates with material priority for movement width: median MFE,
   MFE percentile in the same direction/horizon universe, movement beyond p90
   MAE and a horizon-specific minimum; probability, speed, sample reliability
   and stability remain required.

Small samples remain visible but cannot pass the strict Holdout gate.
In addition, automatic Shadow promotion requires at least 72 hours across
three UTC dates in discovery and 24 hours across two UTC dates in holdout. A
high percentage from a single day therefore remains `BACKTESTED` at most.

## Lifecycle and safety

- Discovery: `DISCOVERED`, `BACKTESTED`, `HOLDOUT_PASSED`, `SHADOW`.
- Owner-policy validator: `APPROVED`, `LIVE` only after enough genuinely
  future Shadow outcomes satisfy every stored gate.
- A Shadow formula starts with the latest existing event ID and evaluates only
  genuinely future delivered alerts.
- Every check and match is idempotent and auditable.
- Shadow hits remain auditable and are never sent before validation.
- A LIVE match creates a durable delivery only for Telegram chats that opted in
  with `/ai_alerts_on`. One AI trade alert is queued per event per chat.
- Alerts are informational; there is no automatic trade execution.

## Runtime flags

- `FORMULA_DISCOVERY_ENABLED=1`
- `FORMULA_SHADOW_ENABLED=1`
- `FORMULA_LIVE_ALERTS_ENABLED=1` enables delivery after all formula and chat
  gates pass; it does not bypass validation.
- `FORMULA_DISCOVERY_HORIZONS=60,240,720,1440`
- `FORMULA_DISCOVERY_INTERVAL_SECONDS=21600`
- `FORMULA_SHADOW_POLL_SECONDS=60`

The Candidate service keeps formula workers disabled. Production uses
migrations `002_formula_research_v1.sql` and
`003_formula_autonomous_alerts_v1.sql` before enabling the workers.
