# Stage 88 — Futures and Spot CVD Flow Engine

## Scope

Stage 88 reads the Stage 87.2 raw 30-minute CoinGlass flow tables and creates
read-only Futures Flow and Spot Flow analytics. It does not alter Alerts,
Watch, Max-Pain scoring, BTC confirmation, LONG/SHORT selection or position
sizing.

## Single-family rule

Buy/Sell and CVD are the same information family:

`Delta = Taker Buy Volume - Taker Sell Volume`

`Continuous CVD = cumulative sum of Delta`

The 30m Delta is displayed as the current impulse but is never counted as an
additional independent vote beside the 30m CVD change.

## Windows

30m, 1h, 4h, 12h, 24h, 48h, 72h and 7d.

For window W:

`CVD_change(W) = ContinuousCVD(now) - ContinuousCVD(reference W ago)`

The reference must be within 20 minutes of the requested target.

## Historical baseline

For each exact `symbol × market × window`, Stage 88 calculates the distribution
of absolute rolling CVD changes and derives P25, Median, P75 and P90.

- below P25: NOISE / Neutral
- P25 to P75: NORMAL; displayed but not directional evidence
- P75 to P90: MEANINGFUL evidence
- at or above P90: STRONG evidence

At least 100 historical changes are required.

## Timeframe families

- Short: 30m, 1h
- Medium: 4h, 12h, 24h
- Broad: 48h, 72h, 7d

A family is Confirmed only when at least two meaningful/strong windows agree
and no meaningful window opposes them. Opposing meaningful windows produce
MIXED. One meaningful window produces EVIDENCE, not confirmation.

## Early Shift

Early Shift is raised only when the Short family points in the opposite
direction from a confirmed Medium and/or Broad family.

## Data quality

The locally continuous CVD is independently checked against the cumulative sum
of stored Buy-Sell deltas. Missing 30m intervals are reported. Quality warnings
do not alter any existing trading logic.

## Commands

- `/flow_state BTC` — Futures and Spot flow conclusions.
- `/flow_stats BTC` — historical P25/P50/P75/P90 by market and timeframe.

## Explicit non-goals

Stage 88 does not create Market Confidence, does not combine Positioning with
Futures/Spot Flow, and does not modify Alerts or Watch. That remains Stage 89.
