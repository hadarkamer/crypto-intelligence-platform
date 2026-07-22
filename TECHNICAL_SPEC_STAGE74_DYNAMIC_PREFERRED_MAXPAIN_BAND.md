# Stage 74 — Dynamic preferred Max Pain band

## Scope

Only the 25-point Max Pain proximity band was changed. Gap, watch logic, cluster, consensus, BTC confirmation and all other score components remain unchanged.

## Dynamic Max Pain thresholds and preferred bands

| Coin group | Max Pain eligibility threshold | 25-point band |
|---|---:|---:|
| BTC | 2.5% | 0.8%–1.3% |
| ETH | 2.7% | 0.8%–1.4% |
| Rank 3–10 | 3.0% | 0.8%–1.5% |
| Rank 11–20 | 3.5% | 0.8%–1.7% |
| Rank 21+ / unranked | 4.0% | 0.8%–2.0% |

The lower edge remains fixed at 0.8%. Only the upper edge expands for coins with a larger permitted Max Pain distance.

## Remaining bands

- Below 0.5% or above the coin threshold: 0 points.
- 0.5% to below 0.8%: 17 points.
- Above the dynamic preferred ceiling through 2.0%: 20 points.
- Above 2.0% through the coin threshold: 15 points.
