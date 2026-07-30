# Stage 86 — Display all active Max Pain targets

## Required behavior

Every active, uncrossed Max Pain target remains visible in alerts and watch scans, regardless of distance.

- Distance below 0.8%: display the alert and award 0 Max Pain proximity points.
- Distance inside the configured scoring band: display the alert and award proximity points according to the existing table.
- Distance above the symbol-specific upper limit, including above 4%: display the alert and award 0 Max Pain proximity points.

Distance is therefore a score component only. It is not an eligibility or display filter.

## Unchanged

- Crossed or missing targets remain unavailable.
- Consensus, BTC confirmation, Cluster and Gap calculations remain unchanged.
- Top 8 commands and all existing watch commands remain unchanged.
- Ranking continues to use the final score after proximity points are calculated.
