# Stage 103 — Synced Watch, Magnet Watch and Liquidity V2

## Runtime contract

1. One shared coordinator serves regular Watch and Magnet Watch.
2. The first cycle runs immediately. Later deadlines are aligned to each UTC
   half-hour plus the closed-CVD publication grace; scan duration is never added
   to the next deadline and missed deadlines are not replayed concurrently.
3. Each cycle starts exactly one Max Pain DOM scan and joins exactly one
   in-process Price+OI task and one CVD task. PostgreSQL advisory locks retain
   cross-process exclusion during Render deploy overlap.
4. DOM and derivatives refresh may run concurrently. Scoring/report generation
   begins only after both complete or the bounded derivatives timeout expires.
5. CVD uses closed 30-minute candles only. The five-minute Flow loop is a poll
   for a newly published closed candle. A no-change poll performs no continuous
   CVD rebuild or database rewrite. A real write invalidates the Flow cache.
6. Failure is fail-safe: Max Pain can still be reported, while the existing
   freshness rules keep stale/unavailable Price+OI or CVD outside Confirmation.

## Commands

- `/watch_magnet_v1 BTC`
- `/watch_magnet_v1_status`
- `/watch_magnet_v1_stop BTC` or `/watch_magnet_v1_stop`
- `/oi_refresh`
- `/market_state BTC [LONG|SHORT]`

## Liquidity V2 (Magnet shadow path only)

- First available timeframe: named baseline (normally 12h), using its observed
  cumulative amounts directly.
- Later timeframe: candidate and opposite amounts minus the previous available
  cumulative timeframe.
- A materially negative difference is marked `NON_MONOTONIC` and excluded; it
  is never silently converted into positive evidence.
- Liquidity Edge is calculated from the widest available cumulative timeframe
  directly, without time reduction: candidate gross liquidity vs opposite gross
  liquidity.
- Consistency is `abs(sum(signed layer imbalance)) / sum(abs(layer imbalance))`
  using the valid non-overlapping layers only.
- A non-monotonic layer remains visible as a data-quality warning, but it does
  not change the gross cumulative Liquidity Edge.
- Distance and historical reach probability are not used.

## Explicit non-changes

- No legacy score formula, component weight or threshold changed.
- Magnet Quality geometry and thresholds did not change.
- Confirmation remains a gate over Price+OI and Futures CVD; Spot remains
  secondary context.
- Historical Backfill remains isolated from the live OI snapshot table.
- No open CVD candle is eligible.
