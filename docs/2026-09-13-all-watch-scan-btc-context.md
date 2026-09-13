# All-Watch prior BTC context, adapter v3

One bounded implementation step within research from all scans. It extends the
unchanged catalog from82 to106 supported definitions, with192 explicitly
unsupported. It does not discover formulas or change scores, trading, delivery,
the source population, price outcomes, inverse orientation or BTC wave counting.

## Exact applicable predicates

Two original fields account for24 additional definitions, including inverses:

- `historical.closed_1m.1h.btc_direction`: six UP/DOWN/FLAT definitions.
- `historical.closed_1m.4h.btc_market_regime`: eighteen definitions crossing the
  three existing total-score>=65 baselines with UP/DOWN/RANGE BTC context.

The first field is the exact sign of the previous completed one-hour BTC Spot
return, from first open to last close. FLAT means exactly zero. The second uses
the existing fixed four-hour range-efficiency rule: absolute net displacement
divided by high-low range must be at least0.50 for UP/DOWN; otherwise RANGE.
These definitions are reused from `research_past_price_features.py`, including
its method and regime versions. They are not selected or tuned from outcomes.

The cutoff is `floor(usable_from_utc, minute)`. Every contributing candle must
close before that cutoff, and the complete one-minute sequence is required
independently for each lookback. The current minute is excluded, even though
the outcome entry is in the following full minute. Calculating at that later
entry would introduce prices unavailable at the original scan decision.

Only the established `BINANCE_SPOT_TRADE_1M` BTC route is read. A240-minute
cache-only path is shared across all coin jobs for each selected scan in one
pass. No provider, outcome, membership or own-coin price read is needed. This
context is valid for HYPE as well; HYPE's outcome remains Hyperliquid PERP TRADE
and no HYPE Spot/PERP source substitution is made.

## Immutable evidence and bounded processing

Adapter: `research_watch_scan_formula_btc_context.py`.
Evaluation version: `watch-scan-formulas-v3-btc-context`.
Feature version: `watch-captured-total-maxpain-and-btc-context-features-v3`.
Context version: `watch-prior-btc-spot-closed-1m-v1`.

The shared context binds consumer, source/population version, scan identity,
bundle/parent hashes and usable time. It retains source/window proofs, causal
cutoff, original calculation versions, feature availability and a canonical
context hash. Calculation time does not change context identity. Each sample
contains212 decisions =106 original definitions ×2 base directions. Existing82
decisions, missing-feature lists and inverse behavior remain exactly unchanged.
BTC state is an absolute market observation and stays the same for both base
directions; inverse candidates flip only their outcome direction.

Missing or invalid price evidence never becomes FLAT, RANGE or a zero return.
An incomplete4h window can coexist with a known complete1h window. The frozen
sample records the available cache evidence used for this descriptive evaluation;
READY means complete decision coverage, including explicit UNKNOWN decisions,
and does not assert that all price features were available. Existing immutable
samples are not silently rewritten by later historical enrichment. A database
read error is retryable and is not mislabeled as missing market evidence.

The worker retains its16-coin/30-second budget. Each source read uses a savepoint;
a failed scan's context is not reread eight times during the same pass and does
not block another scan. Failed coin jobs retry after five minutes. Successful
context evidence is calculated once per scan/pass and shared by its coin jobs.
The first successfully committed READY coin freezes the scan's shared BTC context.
Subsequent passes and coin retries reuse that validated evidence, even if a cache
gap has since been filled, so a scan never mixes different BTC states or coverage
between its coins merely because processing happened at different times.

Migration050 only extends the immutable sample coverage check to212 decisions.
The existing active-version transition and `_by_version` audit views are reused.
V2 remains current until every accepted scan/coin has a READY v3 sample, then
the final sample and pointer commit atomically. V1/v2 samples and catalog entries
are retained. Migration049 must not be rerun after050, because049 only knows
the older payload lengths; explicit deployment always applies050 only.

## Remaining contracts and sequencing

The216 definitions unsupported before this step had these disjoint contracts:

| Contract | Definitions before v3 |
|---|---:|
| Causal prior asset/BTC prices | 76 |
| Selected-item timeframe liquidity, including Combined conjunction | 34 |
| Combined top-item average | 22 |
| Captured confirmation status | 16 |
| Entry/score-change/family-order sequences | 46 |
| Deferred MaxPain components | 10 |
| Same-timeframe selected/opposite score difference | 2 |
| Missing15m/1h/4h MaxPain horizons | 8 |
| Standalone Combined event type | 2 |

V3 implements24 of the76 prior-price definitions. The52 own-price/relative-price
definitions still need an exact Spot contract; a HYPE PERP path cannot be
relabelled as the original Spot feature. Other MaxPain predicates tied to a
selected item need an explicit timeframe key sharing the same outcome, not a
new arbitrary aggregate. Combined or confirmation states cannot be invented
from absence of delivery. Missing short MaxPain horizons cannot be substituted.

Cluster/CVD component hypothesis research, B4 and240m family normalization remain
deferred. The ordered five-topic plan is still in topic1, research from all scans.
Continuous new-formula discovery, historical archive completion, HYPE completion
and validation on new cases remain later topics.

## Verification

Tests cover original-calculation parity, causal cutoffs and future-price poison,
source/identity/hash validation, partial lookbacks, inverse and legacy equality.
Split-pass testing fills an archive gap after the first coin samples freeze and
verifies that all remaining coins reuse the same original context.
Disposable PostgreSQL tests check cache sharing, version transition, preserved
v1/v2 evidence, source error savepoints/retry and HYPE's unchanged outcome route.
Full CI and a final live evidence checkpoint are required for the rollout.
Before implementation, all19 captured scans had a complete BTC240-minute cache.
