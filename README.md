Stage 6 Decision Engine Patch

Adds:
- decision_engine.py
- /score SYMBOL
- /score_top [limit]

Purpose:
First transparent setup-strength engine using existing Max Pain data only.

Components:
1. CONSENSUS: up to 35 points
2. NEAR_MAX_PAIN: up to 25 points
3. LIQUIDITY_BALANCE: up to 20 points
4. MARKET_BIAS: up to 10 points
5. BTC_LIKE: up to 10 points

No Hyperliquid yet.
No automatic alerts yet.
No changes to collection.

Test:
/collect
/score BTC
/score_top
