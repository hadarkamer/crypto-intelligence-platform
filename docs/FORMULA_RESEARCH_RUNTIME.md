# Formula Research Runtime v1

## Objective

Find reproducible decision-time conditions that precede a defined LONG or
SHORT move with high out-of-sample probability, low adverse excursion, fast
favorable progress and useful MFE/MAE efficiency.

## Evidence contract

- Inputs: immutable Research Event state plus raw Price/OI, Futures CVD and
  Spot CVD points at or before the alert.
- Labels: closed Binance Spot USDT one-minute paths at 1h, 4h, 12h and 24h.
- The first partial minute after the alert is excluded.
- HYPE has no Binance Spot pair and is excluded from verified outcome training.
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
7. ranks candidates by probability/Wilson lower bound, baseline improvement,
   MAE, MFE/MAE efficiency, speed, rarity, sample reliability and stability.

Small samples remain visible but cannot pass the strict Holdout gate.
In addition, automatic Shadow promotion requires at least 72 hours across
three UTC dates in discovery and 24 hours across two UTC dates in holdout. A
high percentage from a single day therefore remains `BACKTESTED` at most.

## Lifecycle and safety

- Automatic: `DISCOVERED`, `BACKTESTED`, `HOLDOUT_PASSED`, `SHADOW`.
- Human-only: `APPROVED`, `LIVE`.
- A Shadow formula starts with the latest existing event ID and evaluates only
  genuinely future delivered alerts.
- Every check and match is idempotent and auditable.
- Shadow hits are stored with `delivery_status=NOT_SENT`.
- The current runtime has no Telegram destination for formula alerts. Live
  delivery remains disabled even if a formula matches.

## Runtime flags

- `FORMULA_DISCOVERY_ENABLED=1`
- `FORMULA_SHADOW_ENABLED=1`
- `FORMULA_LIVE_ALERTS_ENABLED=0`
- `FORMULA_DISCOVERY_HORIZONS=60,240,720,1440`
- `FORMULA_DISCOVERY_INTERVAL_SECONDS=21600`
- `FORMULA_SHADOW_POLL_SECONDS=60`

The Candidate service keeps all formula workers disabled. Production enables
discovery and Shadow only after migration `002_formula_research_v1.sql` is
applied manually.
