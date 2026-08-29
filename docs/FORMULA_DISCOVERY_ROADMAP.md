# Formula Discovery Roadmap

## Primary objective

The production AI Research layer exists to discover reproducible conditions
that precede movement in a defined direction with:

1. high out-of-sample probability;
2. low adverse movement before the favorable move;
3. fast favorable progress where possible;
4. attractive reward relative to the adverse path;
5. enough observations to distinguish a repeatable edge from an anecdote.

Ideas supplied by operators are hypothesis seeds, never mandatory assumptions.
The search may use raw measurements, the bot's current models/scores, alternative
family weighting, or combinations of all of them. LONG and SHORT are evaluated
independently.

## Formula lifecycle

`DISCOVERED -> BACKTESTED -> HOLDOUT_PASSED -> SHADOW -> APPROVED -> LIVE`

- `DISCOVERED`: reproducible conditions and discovery-set metrics exist.
- `BACKTESTED`: full path metrics, counterexamples and baseline comparisons exist.
- `HOLDOUT_PASSED`: the frozen formula passes a later chronological sample.
- `SHADOW`: evaluated on new live events without sending a trading alert.
- `APPROVED`: a human explicitly approves production alert eligibility.
- `LIVE`: the versioned formula may emit an alert when its conditions occur.

No stage transition is inferred from GPT prose. Each transition must be stored
and auditable.

## Work order

### 1. Canonical outcome path — implemented

- Binance Spot `USDT`, closed 1-minute candles.
- One fetch per alert up to the maximum due horizon, then internal slices for
  1h / 4h / 12h / 24h.
- Fixed return, MFE, MAE, time to first progress, time to MFE, target progress,
  target hit and timing.
- Source, pair, resolution, samples, quality and method version retained.
- First partial minute excluded to avoid movement from before the alert.
- No exchange fallback may be labelled as Binance Spot.

### 2. Research feature matrix — next

Build one versioned row per decision timestamp with two parallel feature sets:

- raw: Price/OI changes, Futures and Spot CVD values/changes, raw Max Pain
  distances/averages, liquidity balances, gaps, cluster values and other stored
  measurements;
- model: existing scores, families, confirmations, Magnet/Combined states and
  alternative Research-only regroupings/weights.

Also derive without look-ahead:

- symbol and expected direction;
- UTC hour, weekday/weekend and defined market sessions;
- repeat count, spacing, recency and strengthening/weakening;
- order of different alert families;
- breadth across symbols and agreement with BTC;
- range position and distance from relevant targets where source data supports it;
- strategy/code version and data-quality flags.

Alert events alone are selected by existing rules. Bounded near-miss and matched
control samples are therefore required before alternative thresholds can be
judged fairly.

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

Holdout-passed candidates run in shadow against future events. Shadow records
every occurrence, including rejected/failed paths. Only an explicitly approved,
versioned formula may become a live alert condition. Its alert must state the
formula version, direction, expected target/exit logic, risk evidence, rarity,
sample count and validation state.

## Current boundary

The production AI can now explore verified path aggregates by exact signal
combination, event type, symbol and score band, and can fetch a bounded direct
Binance Spot path for an individual archived alert. It cannot yet autonomously
scan the full raw feature space, generate sequences, correct for multiple
hypothesis testing or promote a formula through the lifecycle. Those are stages
2–5 above.
