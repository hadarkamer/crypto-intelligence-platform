# Stage 57 — Directional consensus, directional clusters and debug validation

## Changes

- Consensus is calculated for the direction of each alert.
- LONG and SHORT clusters are calculated independently.
- Duplicate symbol/timeframe rows are removed before scoring.
- Every score is checked against the sum of its four components.
- Cluster size is checked against the number of timeframes supporting the alert direction.
- `/debug BTC` runs a fresh scan and displays consensus, cluster members, duplicate removals and score-sum checks for each timeframe.

## Manual check

Run:

```text
/collect
/debug BTC
/alert BTC
```

For a 3 LONG / 4 SHORT configuration:

- LONG alerts must show 3/7 and 9.86/23 consensus points.
- SHORT alerts must show 4/7 and 13.14/23 consensus points.
- LONG cluster members may only be LONG timeframes.
- SHORT cluster members may only be SHORT timeframes.
