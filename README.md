Stage 9 Manual Alert Engine Patch

Adds:
- alert_engine.py
- /alert_check [limit]

This is manual only:
- no automatic 15-minute watch yet
- no DB writes for alerts yet
- no Hyperliquid dependency

Alert types:
1. NEAR_MAX_PAIN
2. LIQUIDITY_IMBALANCE_NEAR_SIDE
3. EXTREME_GAP
4. HIGH_LIQUIDITY_CLOSE_DISTANCE
5. HIGH_SETUP_STRENGTH

Test:
/collect
/alert_check
/alert_check 30
