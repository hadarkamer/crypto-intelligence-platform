# Stage 88.1 — Flow clarity and directional CVD baselines

## Scope

This patch changes only three Stage 88 presentation/analytics details. It does
not modify Alerts, Watch, Max-Pain scoring, LONG/SHORT selection, BTC
confirmation, position sizing or Stage 89 integration.

## 1. Layer names

The displayed hierarchy is now:

- Impulse: current 30m Buy-Sell delta.
- Momentum: 30m + 1h CVD windows.
- Trend: 4h + 12h + 24h CVD windows.
- Structure: 48h + 72h + 7d CVD windows.

The underlying confirmation rule is unchanged: a family is confirmed only
when at least two meaningful/strong member windows agree.

## 2. Data-quality reasons

`/flow_state SYMBOL` now prints the reason behind PASS/WARNING. Checks include:

- whether continuous CVD equals the independent cumulative sum of Buy-Sell;
- number of missing 30-minute intervals;
- largest detected timestamp gap.

Quality remains read-only and never changes existing trade logic.

## 3. Separate bullish and bearish baselines

Historical positive and negative CVD changes are no longer mixed into one
absolute distribution.

For each exact `symbol × market × timeframe`:

- positive CVD changes build a Bullish distribution;
- negative CVD changes are converted to magnitudes and build a Bearish
  distribution.

A current positive change is compared only with the Bullish distribution. A
current negative change is compared only with the Bearish distribution.

The percentile thresholds remain unchanged:

- below P25: NOISE;
- P25–P75: NORMAL;
- P75–P90: MEANINGFUL;
- P90 and above: STRONG.

`/flow_stats SYMBOL` displays Bullish and Bearish P25/P50/P75/P90 separately.
