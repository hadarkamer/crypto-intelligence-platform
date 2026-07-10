Stage 14 — Alert Formula Correction

Fixes:
- Removes Setup Strength from alert priority.
- Removes asset-to-asset absolute liquidity ranking.
- Uses normalized liquidity inside each coin/timeframe.
- Shows liquidity balance explicitly in /alert_check and /alert_explain.

New priority formula:
- Distance: 0..45
- Consensus: 0..30
- Liquidity balance/concentration: 0..25
Total: 0..100

Liquidity formula:
NearShare% = Near-side liquidity / (Near + Opposite liquidity) * 100

Output includes:
- NearShare%
- Near/Far ratio
- LiqPts

Test:
1. /collect
2. /alert_check
3. /alert_explain XRP 12h
