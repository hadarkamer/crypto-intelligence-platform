# All-Watch prior asset Spot context, adapter v4

This is one implementation step within research from all scans. It connects
52 more original definitions, increasing supported coverage from 106 to 158 of
the unchanged 298-definition catalog. The other 140 remain explicitly unsupported.
It does not invent formulas, tune conditions, change scores or launch discovery.

## Exact original predicates

The additional 52 definitions include their original inverse identities:

| Predicate group | Definitions |
|---|---:|
| Prior asset UP/DOWN/FLAT in 15m, 30m, 1h, 4h, 12h and 24h | 36 |
| Prior asset 4h UP/DOWN/RANGE regime | 6 |
| Prior 1h asset return strictly above/below BTC return | 4 |
| Captured Spot CVD >=65 crossed with prior 1h alignment | 6 |

Nine existing feature keys supply these predicates: six lookback `.direction`
fields, `historical.closed_1m.4h.market_regime`,
`historical.closed_1m.1h.relative_strength_pct` and
`historical.closed_1m.1h.alignment`. The existing past-price method calculates
each closed lookback independently. Return uses first open to last close;
direction is its exact sign. FLAT means exactly zero. The fixed 0.50
range-efficiency rule supplies the regime. Relative strength subtracts the
same-window frozen BTC return; alignment is relative to the base direction.
Inverse candidates retain those base predicates and flip only outcome direction.

## Causal sources and HYPE availability

All windows end at `floor(usable_from_utc, minute) - 1 millisecond`. The current
minute and the later outcome-entry minute are excluded. Exact closed one-minute
continuity and source provenance are required separately for every lookback.
The worker reads one bounded 1,440-minute Binance Spot cache path per applicable
coin job. Missing history never becomes zero, FLAT or RANGE. A short window may
remain known even when a longer one is incomplete. No provider, outcome, event
or wave-membership read enters the calculation.

Own Spot lookbacks are enabled for BTC, ETH, SOL, BNB, XRP, DOGE and ZEC.
The original predicates explicitly require Spot; HYPE's active PERP outcome path
cannot be silently substituted. The HYPE job makes no own-price cache or provider
request and records `HYPE_SPOT_LOOKBACK_NOT_ENABLED_FOR_WATCH` for all nine new
features. Its existing 106 definitions, captured available Spot CVD and shared
BTC context remain unchanged. A known false Spot-CVD conjunct still produces
NO_MATCH even when its own-price alignment is unavailable; all other tri-state
rules remain intact. This step does not complete the later HYPE integration topic.

## Version preservation and shared BTC context

Evaluation version: `watch-scan-formulas-v4-asset-context`.
Feature version: `watch-captured-total-maxpain-btc-and-asset-context-features-v4`.
Asset context version: `watch-prior-asset-spot-closed-1m-v1`.

For each scan, the worker first looks for the frozen BTC context in a READY v4
sample, then in a READY predecessor v3 sample. Only if neither exists may it
build BTC context from the cache. Missing or invalid frozen context causes a
retry; it does not authorize rebuilding. This preserves all 106 prior decisions
even when the price archive has gained previously missing minutes.

The asset context binds immutable scan/source identity, symbol, usable time,
cutoff, original calculation versions and the validated BTC-context hash. It
stores six own-window proofs and separate feature/availability maps for LONG and
SHORT. For BTC itself, own 1h and 4h windows reuse the frozen shared BTC windows
exactly, including missing coverage. BTC relative to itself is therefore zero
when known; a missing shared hour cannot manufacture a self-relative result.

Each coin has 316 frozen decisions =158 definitions ×2 base directions. READY
means completed decision coverage, which may include explicit UNKNOWN values.
Successful samples are immutable; later historical enrichment cannot rewrite
their original decision context. Per-coin source-read failures remain retryable
and isolated by savepoints. The worker retains the 16-coin/30-second budget.

Migration051 only extends the immutable payload coverage check to 316 decisions.
V3 stays current until the automatic bounded worker completes all accepted
scan/coin samples in v4. The final sample and active pointer commit atomically.
Original current view names expose only the active version, while `_by_version`
views retain all earlier evidence. No outcome, selection, source or BTC-wave
definition changes. The same 298 original definitions and full catalog hash
remain frozen. Do not rerun older coverage migrations after051; use the explicit
single-migration installer for deployment.

## Verification and next scope

Tests cover original-method parity, six-window gaps and causal future-candle
exclusion, alignment and inverse orientation, relative BTC evidence, HYPE
unavailability, source/context hashes and exact previous-decision preservation.
Disposable PostgreSQL tests exercise version activation, predecessor BTC reuse,
bounded cache reads, partial coverage and per-coin SQL-error recovery.

Preflight found 34 accepted scans with complete 1,440-minute histories for every
enabled Spot coin, and complete frozen BTC contexts. Full CI, automatic production
backfill and independent raw-price verification follow implementation.

The remaining 140 definitions require timeframe liquidity (34), Combined top-item
averages (22), captured confirmation states (16), sequences (46), deferred MaxPain
components (10), a same-timeframe score difference (2), absent short MaxPain
horizons (8), or standalone Combined event type (2). These need their exact
original semantics, not invented aggregate or delivery states.

The ordered plan remains in topic1, research from all scans. Continuous discovery,
historical archive completion, HYPE completion and validation on new cases are
later topics. Cluster/CVD component hypothesis research, B4 and 240m family
normalization remain deferred. This descriptive evidence cannot trigger trading,
promotions or prospective acceptance.
