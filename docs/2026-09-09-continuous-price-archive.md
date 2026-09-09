# Continuous price archive and automatic wave reporting

The September 9 audit found a complete BTC minute archive but no equivalent
durable minute archive for the other seven research assets. Outcome workers
downloaded paths repeatedly and retained labels/coverage rather than all bars.

The alleged 14-hour live Price/OI outage was a diagnostic error. The inspected
`oi_price_history` table is refreshed daily. Actual Price/OI observations in
`oi_regime_snapshots` continued every 30 minutes. This change preserves the
existing CoinGlass cadence and reports those two freshness contracts separately.

## Storage and readers

Migration 044 adds immutable route/symbol/minute OHLC records, durable bounded
history cursors and explicit missing-range retries. Seven Binance Spot assets
(BTC, ETH, SOL, BNB, XRP, DOGE, ZEC) and Hyperliquid HYPE perpetual TRADE are
collected continuously. The requested research start is August 16 in Israel
(August 15 21:00 UTC). Older HYPE minutes outside the provider retention limit
remain explicitly unavailable; previously archived bars do not expire.

Current tails are collected before historical work. Exact missing-minute
ranges, rather than successful HTTP requests, determine path completeness.
Duplicate conflicting bars cannot rewrite persisted evidence. Readers use
stored bars first and download only absent pages, keeping Spot, perpetual TRADE
and MARK sources separate. Existing unsupported archive symbols retain their
original provider route. No alert reference, BTC membership or statistical
acceptance threshold is replaced by this storage change.

## Automatic full-wave report

Migration 045 stores incremental full-wave report jobs and their frozen source
population. The worker reads archived prices without issuing provider requests.
Closed records are reused only after source identity and causal end boundaries
are checked. Active records are provisional; missing representatives block a
complete headline rather than being replaced with later successful alerts.

The `MaxPain_Wave_Live` tab receives eight thresholds through the existing
durable Sheet outbox. It is a separate explicit Spot + HYPE perpetual TRADE
report. The prior `MaxPain_Wave_Recovery` tab remains the dated MARK snapshot;
neither version silently changes the source contract of the other.

Probability uses decided outcomes only. Asymmetry in this full-wave contract
is the ratio of summed full-path favorable/adverse excursions on the same
complete cohort; it is not a separately selected cohort per threshold. BTC
parents remain the independence unit across coins and repeated alerts.

## Rollout

Enable `RESEARCH_PRICE_ARCHIVE_ENABLED` only with migration 044 installed.
Install 045 before enabling the full-wave report worker. Schema application is
restricted to the exact additive migrations for this rollout. Existing Watch,
Price/OI, captures, native fixed-window labels and their Sheet delivery remain
under their existing worker locks and queues.

Acceptance checks: immutable-source/gap/restart tests, real PostgreSQL tests,
exact deployed commit, minute archive continuity and latest-tail advancement,
explicit unavailable historical HYPE range, full report generation plus Sheet
acknowledgement, and source/formula/outcome freshness checked independently.
