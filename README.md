# Stage 20 — Alert Score v2

Implemented:
- New 5-component score
- Consensus and BTC Like combined
- Target clustering
- Historical liquidity density
- Liquidity balance as bonus/penalty
- Data-quality notes outside the score
- Multiple-alert notes without score impact
- Separate alert cards per timeframe
- No .pyc files

Test:
1. /collect
2. /alert_check 10
3. /alert_explain BTC 24h
4. /watch_now
