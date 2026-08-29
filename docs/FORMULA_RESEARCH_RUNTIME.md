# Formula Research Runtime v3

## Objective

Find reproducible decision-time conditions that precede the widest practical
LONG or SHORT move, while retaining high out-of-sample probability, low
adverse excursion, fast favorable progress and useful MFE/MAE efficiency.

## Evidence contract

- Inputs: immutable Research Event state and/or neutral archived raw Price/OI,
  Futures CVD and Spot CVD observations after their source candles closed.
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
- The shared production session definition is ACTIVE from Sunday 18:00 ET
  through Friday 20:00 ET and WEEKEND otherwise. It is evaluated in
  `America/New_York`, including DST. Every input window and future outcome
  horizon carries its own exact ACTIVE/WEEKEND composition.
- CoinGlass 30-minute timestamps are interval-open timestamps. Replay exposes
  a row only after the 30-minute close plus a two-minute provider grace; its
  canonical spot path begins after that safe decision time.
- Technical metadata such as archive sample counts, point age, completeness,
  schema versions and raw UTC/DST offsets is diagnostic only and cannot become
  a formula predicate.

## Discovery

For each horizon and direction, the engine:

1. sorts verified observations chronologically;
2. freezes the earliest approximately 70% as discovery and latest 30% as
   holdout without ever splitting rows that share the same timestamp;
3. derives numeric thresholds from discovery quantiles only;
4. evaluates single, pair and triple conditions in a bounded search;
5. compares each candidate with its same-direction complement after matching
   the outcome horizon's ACTIVE/WEEKEND composition with triangular weights;
6. applies Benjamini-Hochberg correction across all unique candidates;
7. ranks candidates with material priority for movement width: median MFE,
   MFE percentile in the same direction/horizon universe, movement beyond p90
   MAE and a horizon-specific minimum; probability, speed, sample reliability
   and stability remain required. The absolute movement floor may be scaled
   down for weekend/mixed horizons only from sufficient prior raw-price
   evidence; probability, Wilson, control improvement, MAE, efficiency and
   movement-percentile gates are never relaxed.

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
- `FORMULA_DISCOVERY_DATASET_MODE=auto` prefers the neutral historical replay
  only after its minimum coverage gate; `alerts` and `historical_replay` are
  explicit bounded operator overrides.

The one-shot replay is separate from the Watch loop:

- apply migration `004_historical_opportunity_replay_v1.sql` explicitly;
- set `HISTORICAL_REPLAY_BACKFILL=1` only for the backfill command;
- optionally bound symbols, dates, horizons, chunk size and anchor count;
- run `python research_historical_replay.py`;
- one-minute candles are discarded after calculation; PostgreSQL retains only
  compact reference/return/MFE/MAE/speed summaries and provenance.

The Candidate service keeps formula workers disabled. Production uses
migrations `002_formula_research_v1.sql`,
`003_formula_autonomous_alerts_v1.sql` and
`004_historical_opportunity_replay_v1.sql` before enabling the workers.

Formula schema v5 retires an earlier non-LIVE cohort only after at least four
symbols independently have 250 anchors, 14 UTC dates and 336 hours of span for
that horizon. Sparse symbols are reported but excluded from discovery until
they pass the same per-symbol gate. Before that gate, a result is capped at
`BACKTESTED` and the earlier cohort remains auditable and active.

Hyperliquid's official candle endpoint exposes only its most recent 5000
candles. HYPE replay therefore uses only exact one-minute observations still
inside that window; older HYPE anchors are excluded rather than approximated or
labeled from another venue.
