# Stage 88.2 — Continuous CVD rebuild and quality tolerance

## Scope

This is a narrow correction only. It does not change Alerts, Watch, Max Pain,
LONG/SHORT selection, Flow conclusions, directional baselines, or historical
percentile rules.

## 1. Rebuild on skipped backfill

`/flow_backfill` still skips API downloads when a symbol/market is already
current. Before returning the skipped result, it now recomputes
`continuous_cum_vol_delta_usd` from all stored raw rows:

```
delta_t = buy_volume_usd_t - sell_volume_usd_t
continuous_CVD_n = sum(delta_t, t=1..n)
```

This repairs stale continuous-CVD values without deleting data and without
calling CoinGlass again.

## 2. Quality-check tolerance

The independent cumulative Buy-Sell sum is compared with the stored continuous
CVD using:

```
tolerance_usd = max(1000, abs(independent_cvd_usd) * 0.0001)
```

That is an absolute floor of $1,000 or a relative tolerance of 0.01%, whichever
is larger. Large stale-series mismatches still fail, while insignificant
floating-point/storage differences no longer create false warnings.

## Operational verification

1. Run `/flow_backfill` once after deployment. Existing complete markets may be
   marked skipped, but their continuous CVD is rebuilt.
2. Run `/flow_state BTC`.
3. The previous multi-billion-dollar mismatch warning should disappear if the
   raw Buy/Sell rows are internally consistent.
