# All Watch scans — causal measurement adapter

This is the second implementation step within **מחקר מכל הסקירות**. The user
authorized one step at a time. It extends PR33 intake; it does not start the
later continuous-discovery, archive-completion, HYPE-completion or new-case
formula-validation topics. Cluster/CVD component hypothesis research, B4 and
240m family normalization remain deferred.

## Measurement contract

- Population: `watch-all-scan-observations-v1`; source: accepted
  `watch-operational-scores-v2` receipts, including low/zero scores, inactive
  targets, missing inputs and scans without qualifying MaxPain scores.
- Adapter: `watch-scan-measurements-v1`; entry:
  `watch-next-full-minute-route-open-v1`.
- Entry is the open of the minute strictly after `usable_from_utc`:
  `floor(usable_from_utc, minute) + 1 minute`, including an exact minute boundary.
  It is a reproducible research entry, not a recorded fill or the older quote
  used to calculate the operational score. Entry is admitted only once its
  complete closed candle is available.
- Seven coins use `BINANCE_SPOT_TRADE_1M`. HYPE uses the already archived
  `HYPERLIQUID_HYPE_PERP_TRADE_1M`. The latter is a perpetual trade-price route,
  distinct from Binance Futures MARK and Hyperliquid Spot @107. This policy does
  not alter the separate price provenance frozen with operational HYPE scores.
- Both analysis directions use the existing `ordered-first-touch-v7` calculator.
  Thresholds are 25/50/75/100/125/150/175/200 bps; windows are 60/240/720/1440
  minutes. Same-candle two-sided hits remain unresolved. Gaps remain missing.
- Full-window MFE/MAE and their ratio require a complete mature window. These
  differ from excursions measured only up to a first touch. Zero MAE gives an
  undefined ratio, not infinity. Future aggregation must use sum(MFE)/sum(MAE)
  on the same complete cohort, not an average of per-row ratios.
- Each coin/direction/window is measured once. Seven MaxPain timeframe links
  share that result: 64 window records and 512 threshold results per scan,
  regardless of the number of score-slot joins.

## Causal BTC membership and eligibility

The adapter reuses `btc-parent-close-reversal-200bps-v1` and its actual latest
closed BTC source bar at **usable_from_utc**, before the later entry minute.
There is no outcome-dependent parent selection or new wave segmentation.
Missing BTC evidence retries; unverified boundaries remain explicitly
ineligible. Price results are retained even when wave evidence is missing.
The existing membership term `LIVE` denotes a confirmed causal parent under
that policy; it does not reclassify this population as delivered alerts or
prospective formula evidence. Historical/forward capture markers remain intact.

Several scans or coins in the same parent are not independent formula evidence.
No formula is evaluated or promoted by this adapter. All result views retain
`qualifies_as_prospective_formula_evidence = false`.

## Bounded execution and persistence

`research_watch_scan_measurement_worker` uses a transaction advisory lock,
a finite cyclic receipt scan (32 IDs plus a reserve of two recent receipts),
and at most 16 coin jobs per 30-second pass. Queue creation and cursor movement
are atomic; repeated laps recover lower IDs committed late. The existing
collector fills the price archive and the existing BTC worker maintains waves.
The new worker performs no network requests and makes no synthetic alert events.

Complete prefixes are revisited at the next measurement-window boundary.
Missing evidence retries with a bounded delay. A per-coin savepoint isolates
invalid evidence and preserves earlier results. Completed window records,
established entry provenance and confirmed BTC membership are protected against
changes by a database trigger. Existing frozen price bars also reject revisions.

Migration `047_watch_scan_measurements.sql` adds two research tables and three
views: window results, threshold results and links to frozen score slots.
The worker inherits `RESEARCH_OUTCOME_ENRICHMENT_ENABLED`; an explicit
`RESEARCH_WATCH_SCAN_MEASUREMENT_ENABLED` overrides it. A missing migration
prevents only this new worker from starting.

## Verification and continuation

Pure tests cover source/time gates, horizon maturity, missing candles,
same-candle ambiguity, inverse symmetry and exact route separation. PostgreSQL
tests use disposable local/CI databases for durable enqueue, rollback, late
commit recovery, replay, completed-result protection and all-slot joins.
Production verification uses only natural worker passes and read-only queries.

The next step remains within all-scan research: connect a versioned formula
evaluation/comparison reader to the measured population, preserving feature
availability, price route, historical/forward phase and BTC-wave grouping.
Do not count this implementation or a large joined row count as formula success.
