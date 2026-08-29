# Formula Discovery Roadmap

## Primary objective

The production AI Research layer exists to discover reproducible conditions
that precede movement in a defined direction with:

1. high out-of-sample probability;
2. movement as wide as practical, measured absolutely and relative to the
   same direction/horizon universe;
3. low adverse movement before the favorable move;
4. fast favorable progress where possible;
5. attractive reward relative to the adverse path;
6. enough observations to distinguish a repeatable edge from an anecdote.

Ideas supplied by operators are hypothesis seeds, never mandatory assumptions.
The search may use raw measurements, the bot's current models/scores, alternative
family weighting, or combinations of all of them. LONG and SHORT are evaluated
independently.

## Formula lifecycle

`DISCOVERED -> BACKTESTED -> HOLDOUT_PASSED -> SHADOW -> APPROVED -> LIVE`

Runtime status (2026-08-29): neutral historical raw-opportunity replay,
automatic discovery, chronological holdout, multiple-testing correction,
wide-move ranking, versioned registry and future Shadow evaluation are
implemented. GPT cannot approve a formula. A separate
deterministic owner-policy validator may promote a frozen formula to `LIVE`
only after enough future outcomes pass every gate.

- `DISCOVERED`: reproducible conditions and discovery-set metrics exist.
- `BACKTESTED`: full path metrics, counterexamples and baseline comparisons exist.
- `HOLDOUT_PASSED`: the frozen formula passes a later chronological sample.
- `SHADOW`: evaluated on new live events without sending a trading alert.
- `APPROVED`: the stored owner policy and future Shadow gates approve alert
  eligibility.
- `LIVE`: the versioned formula may emit an alert to explicitly opted-in chats.

No stage transition is inferred from GPT prose. Each transition must be stored
and auditable.

## Work order

### 1. Canonical outcome path — implemented

- Binance Spot `USDT`, closed 1-minute candles by default.
- HYPE uses explicit Hyperliquid HYPE/USDT spot (`@107`) candles.
- One fetch per alert up to the maximum due horizon, then internal slices for
  1h / 4h / 12h / 24h.
- Fixed return, MFE, MAE, time to first progress, time to MFE, target progress,
  target hit and timing.
- Source, pair, resolution, samples, quality and method version retained.
- First partial minute excluded to avoid movement from before the alert.
- No source may be silently relabelled. Historical candle imports are allowed
  only with complete provenance and quality fields.

### 2. Research feature matrix — core implemented

The first version now builds one bounded row per delivered-alert decision time
with separate feature and label sections:

- raw archive: prior-only Price/OI changes and Futures/Spot CVD values and
  changes over 30m, 1h, 4h, 12h and 24h; the extended profile also adds 48h,
  72h and 7d;
- captured event inputs: decision price/target plus available Max Pain distance,
  liquidity, gap, cluster and Magnet inputs frozen into the event;
- model: existing scores, families, confirmations and Magnet/Combined state;
- label: verified later canonical spot return, MFE, MAE, speed and target result,
  kept structurally separate from every input feature.

It also derives without look-ahead:

- symbol and expected direction;
- DST-safe New-York local hour, weekday and market-time buckets; raw UTC hour
  and offset remain diagnostic and are forbidden formula predicates;
- the production bot's exact `America/New_York` ACTIVE/WEEKEND session, plus
  a separate composition ratio for every 30m, 1h, 4h, 12h and 24h input
  window and for the future outcome horizon;
- prior-only historical Price/OI/Futures CVD/Spot CVD percentiles matched to
  each individual window's session composition rather than a binary UTC day;
- prior alert/repeat counts at 30m, 2h and 6h;
- same-setup repetition and cross-symbol directional breadth;
- strategy/code version and data-quality flags for audit only.

Every raw lookup selects the newest stored row at or before the alert. A point
more than 45 minutes old is missing, and no nearest-future row is permitted.
Raw histories are read in bounded batches; they are not copied into each event.

The absolute wide-move floor may be reduced for thinner weekend/mixed horizons
only when a prior raw-price calibration has enough effective samples. The
probability, Wilson, improvement, adverse-excursion, efficiency and relative
movement gates are unchanged.

The neutral replay removes the alert-only selection bias for raw Price/OI/CVD:
each eligible 30-minute observation is evaluated in both LONG and SHORT from
canonical one-minute spot paths. It stores compact outcomes rather than candle
history and samples evenly across independently coverage-ready symbols and time
for formula search. A sparse symbol cannot borrow the history of other symbols
to pass the gate. Existing model/score features remain available in the
delivered-alert matrix. Remaining extensions include explicit
strengthening/weakening deltas, BTC-to-alt context, range position and full
decision-time reconstruction of model scores where their historical source
inputs are complete.

### 3. Candidate search

Search simple conditions first, then bounded interactions. Candidate operators
may include ranges, direction agreement, counts, order, time gaps, averages and
cross-symbol context. Complexity must be penalized so a complicated formula is
not preferred merely because it fits the discovery sample.

For every candidate report:

- exact executable conditions;
- sample count and archive share (rarity);
- baseline and improvement over baseline;
- directional hit definition and rate;
- median/mean MFE and MAE;
- MAE p75/p90/p95 for candidate stop-survival studies;
- median time to progress and MFE;
- target/partial-target results where relevant;
- coin, direction, timeframe, session and regime coverage;
- strongest failures and invalidation conditions.

### 4. Validation

- Split chronologically; never randomly mix future observations into training.
- Freeze each formula before evaluating holdout data.
- Enforce minimum samples and report uncertainty.
- Compare with simpler baselines and account for the number of hypotheses tried.
- Re-test across coins/periods only where pooling is logically defensible.
- Prefer stable performance across windows over a single exceptional period.

### 5. Shadow and live gates

Holdout-passed candidates run in Shadow against future events. Shadow records
every occurrence, including failures. The owner-approved deterministic policy
requires future sample, control, temporal, probability, width and risk gates.
Only then may the versioned formula become LIVE. A chat must separately opt in,
and every message states formula version, direction, horizon, risk evidence,
rarity, sample count and validation state. It never executes a trade.

## Current boundary

The production AI can inspect replay coverage, explore verified path
aggregates, fetch alert paths, inspect raw/model feature rows, enumerate bounded
single/pair/triple formulas, correct for multiple testing, rank wide movements,
validate chronologically, observe formulas in Shadow and deliver validated LIVE
matches to opted-in Telegram chats. Remaining operational work is completing
the historical backfill, rerunning every horizon, then accumulating genuinely
future Shadow outcomes; optional context features remain listed above.
