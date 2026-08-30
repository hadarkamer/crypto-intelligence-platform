# Formula Research Runtime v4 — adaptive evidence

## Objective

Find reproducible decision-time conditions that currently precede a useful
LONG or SHORT bias. A formula may qualify through high directional probability
or through strong favorable/adverse movement asymmetry. It does not need to be
perfect, and a deep adverse tail is disclosed rather than automatically used
to reject an otherwise material edge.

## Evidence contract

- Inputs: immutable Research Event state and/or neutral archived raw Price/OI,
  Futures CVD and Spot CVD observations after their source candles closed.
- Labels: closed canonical spot one-minute paths at 1h, 4h, 12h and 24h.
- Future price movement, MFE and MAE are labels only. They are never formula
  conditions, Shadow match inputs or decision-time width-calibration sources.
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
- Statistical evidence is counted in outcome-blind Market Episodes, not raw
  alerts. After exact frozen cohorts collapse, the first forecast start opens
  a fixed 24-hour window (or the formula horizon if longer). All symbols and
  later matches inside it remain auditable but carry total evidence weight one.
  A new episode can start only when that forecast window no longer overlaps.
- Market Episode counts are formula-local. Counts from different formulas or
  horizons must never be added as if they were independent proof. Formula
  family deduplication separately compares their compact time intervals so
  shifted formulas supported by the same broad moves collapse to one champion.
- Discovery reads a rolling 120-day archive by default. Current relevance uses
  the latest 21 days with a 14-day half-life after episode collapse. Both raw
  recent counts and Kish effective sample sizes are reported.
- Absolute dollar CVD changes remain auditable inputs but are forbidden as v7
  discovery predicates across symbols. Discovery uses prior-only, same-symbol,
  session-composition-matched percentile forms instead. This prevents a dollar
  cutoff learned from a large market from being treated as equivalent evidence
  for a smaller market.

## Discovery

For each horizon and direction, the engine:

1. sorts verified observations chronologically;
2. freezes the earliest approximately 70% as discovery and latest 30% as
   holdout without ever splitting rows that share the same timestamp;
3. derives numeric thresholds from discovery quantiles only;
4. evaluates single, pair and triple conditions in a bounded search by default;
5. only when `FORMULA_DISCOVERY_HIERARCHICAL_ENABLED=1`, expands a bounded
   beam of stable triple parents to four and then five conditions. A nested,
   timestamp-safe chronological fit/screen split inside discovery selects that
   hierarchy and requires incremental gain in both parts; the outer holdout is
   not inspected until the complete hypothesis family has been frozen;
6. prevents correlated-family stacking without a frozen written exception and
   always forbids combining composite Max Pain evidence with its components;
7. tests probability and asymmetry for every candidate, then corrects all
   `2 × candidate` hypotheses together in one Benjamini-Hochberg family before
   final holdout validation begins. Each route keeps its own mapped q-value;
8. collapses exact/overlapping evidence fingerprints into deterministic formula
   families and persists one champion per family. Single-coin effects remain
   eligible; cross-symbol breadth is not a family-grouping requirement;
9. compares each candidate with its same-direction complement after matching
   the outcome horizon's ACTIVE/WEEKEND composition with triangular weights;
10. blends 70% cumulative historical quality with 30% current relevance. Both
    parts are route-aware. Risk, speed, width, p90/p95 MAE, sample reliability
    and stability remain visible. Weekend calibration may adjust only the
    absolute favorable-width floor.

Small samples remain visible but cannot become research-ready. Historical
holdout acceptance requires common independence, currentness, width, baseline
and data-integrity gates, then either:

- Probability: at least 60% current weighted hit rate, weighted Wilson lower
  bound 45%, at least five percentage points over controls, MFE/MAE at least
  1.10 and joint-family q at most 0.20.
- Asymmetry: hit rate at least 45%, favorable excursion exceeds adverse in at
  least 70% of valid pairs, dominance Wilson at least 40%, at least five points
  over paired controls, MFE/MAE at least 2.0, positive paired median edge and
  joint-family q at most 0.20.

Prospective research readiness uses the stricter probability floors 65%/50%
and asymmetry Wilson floor 45%, with at least 12 independent matches, 12
independent controls and six effective recent match/control episodes. Three to
five effective recent episodes may be labelled `EARLY_CURRENT_EDGE`, never
`RESEARCH_READY`. Missing current p90 or p95 MAE fails closed; a known deep p90
tail is a mandatory warning, not a standalone rejection.

The former 72-hour/three-UTC-date rule is not a research-maturity gate because
Market Episodes now prevent one rise from creating repeated proof. It remains
unchanged inside the separate legacy owner/LIVE approval contract until that
contract is explicitly redesigned and migrated.

## Lifecycle and safety

- Discovery: `DISCOVERED`, `BACKTESTED`, `HOLDOUT_PASSED`, `SHADOW`.
- Automatic rolling evaluation has a hard ceiling of
  `SHADOW_PENDING_EXPLICIT_APPROVAL`. This is an observational readiness state;
  it never changes a formula to `APPROVED` or `LIVE`.
- Registry and Shadow status expose `RESEARCH_READY`, `EARLY_CURRENT_EDGE`,
  accepted route, missing gates, rolling metrics and episode counts. The old
  LIVE-review readiness is reported separately and is not an alias for the new
  research contract.
- After enough genuinely future evidence exists, a separate prospective review
  freezes a predeclared cutoff and evaluates that fixed sample. Only an
  explicit, immutable human approval may then activate `APPROVED` or `LIVE`.
- A Shadow formula starts with the latest existing event ID and evaluates only
  genuinely future delivered alerts.
- Every check and match is idempotent and auditable.
- Shadow hits remain auditable and are never sent before validation.
- A LIVE match creates a durable delivery only for Telegram chats that opted in
  with `/ai_alerts_on`. One AI trade alert is queued per event per chat.
- Alerts are informational; there is no automatic trade execution.

## Operational change protocol

Every change is handled as one explicitly approved stage with a fixed scope and
exit criteria. GitHub, Render and PostgreSQL baselines are collected once and
in parallel before that stage. If no external fingerprint changes, later
preflight checks verify only the branch SHA/tree, active deploy and critical
schema/watermark instead of repeating the full baseline.

- Classify findings as a live fault, expected warning, historical error or
  evidence that is still maturing. A live fault interrupts the planned stage:
  apply the smallest isolated correction, verify it, report it and stop for
  approval before continuing.
- Keep one writer and one coherent commit per stage. Do not mix a fault fix,
  cleanup, refactor and capability expansion. Prefer an existing file or
  function; add a boundary only when ownership or an independent self-test
  requires it.
- Run targeted checks while editing, then compile every tracked Python module
  and run every tracked `*_selftest.py` exactly once after the patch stabilizes.
  Do not repeat the full suite unless the code changes again.
- Move the same verified commit to both production branches, allow their
  deploys to proceed in parallel and perform one bounded post-deploy check:
  exact SHA, no new current-deploy error, preserved database invariants and one
  startup for every required worker. Documentation-only checkpoints do not
  justify a deploy.
- When future evidence is required, close the stage as `WAITING_DATA` with the
  earliest UTC recheck, required horizons, missing eligible sample count and
  exact database watermark. Do not poll repeatedly before that condition.
- Every stage report states what was checked, what changed, commit/deploy/schema,
  tests passed, what deliberately did not change, data still maturing and the
  single next step requiring explicit approval. LIVE or Telegram delivery
  remains forbidden without frozen prospective review and explicit approval.

A chat handoff uses the same compact checkpoint: verified UTC, common branch
SHA/tree, live deploy IDs, database/schema watermarks, current stage and gate,
known expected warnings, unfinished work and the one approval required next.
Live values belong in that timestamped handoff, not as mutable status in this
runtime contract.

## Runtime flags

- `FORMULA_DISCOVERY_ENABLED=1`
- `FORMULA_SHADOW_ENABLED=1`
- `FORMULA_LIVE_ALERTS_ENABLED=1` enables delivery only for a formula with an
  explicit immutable human LIVE approval and an opted-in chat; it never
  promotes a Shadow formula or bypasses the frozen prospective review.
- `FORMULA_DISCOVERY_HORIZONS=60,240,720,1440`
- `FORMULA_DISCOVERY_INTERVAL_SECONDS=21600`
- `FORMULA_DISCOVERY_LOOKBACK_DAYS=120` is the adaptive default. An explicit
  environment override remains bounded to 1–3650 days.
- `FORMULA_DISCOVERY_HIERARCHICAL_ENABLED=1` explicitly enables the bounded
  stable-parent four/five-condition beam; absent/false preserves the default
  single/pair/triple search.
- `FORMULA_SHADOW_POLL_SECONDS=60`
- `PROSPECTIVE_ANCHORS_ENABLED=1` opts the production service into the silent,
  UTC-minute-aligned prospective sampler. Each eligible 30-minute slot is
  idempotent and persists an atomic LONG/SHORT `DECISION_SAMPLE` pair only when
  the Research schema and all required decision-time sources are valid.
  Missing official prices remain missing per symbol; this flag never enables
  Telegram delivery or LIVE promotion.
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
`004_historical_opportunity_replay_v1.sql`, followed by
`005_formula_shadow_safety_v1.sql`, before enabling the workers.

Prospective sampling additionally requires the additive first-touch and neutral
anchor migrations (`006` and `008`). Its worker starts only after startup schema
verification succeeds. `/health` exposes its last official-price coverage,
missing symbols, persistence summary and next aligned minute without exposing
any delivery action.

Formula schema v6 retires an earlier non-LIVE cohort only after at least four
symbols independently have 250 anchors, 14 UTC dates and 336 hours of span for
that horizon. Sparse symbols are reported but excluded from discovery until
they pass the same per-symbol gate. Before that gate, a result is capped at
`BACKTESTED` and the earlier cohort remains auditable and active.

Hyperliquid's official candle endpoint exposes only its most recent 5000
candles. HYPE replay therefore uses only exact one-minute observations still
inside that window; older HYPE anchors are excluded rather than approximated or
labeled from another venue.

Formula schema v7 adds the two-route acceptance contract, rolling relevance
and Market Episodes. Existing v5/v6 Shadow formulas remain observable under
their frozen contracts. Only an exact current v7 runtime may satisfy the LIVE
runtime identity check, and Stage 1 neither creates a Telegram experiment nor
enables LIVE delivery.

The scheduler is intentionally unchanged in this stage: after a complete
multi-horizon Discovery cycle finishes, the worker sleeps six hours. Therefore
the next cycle starts approximately `previous runtime + 6h`, not at a fixed UTC
clock time. Fixed-time/adaptive scheduling is a separate operational stage.
