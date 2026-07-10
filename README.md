Stage 16 — Alert Cards + SKHY Filter

Changes:
1. SKHY is added to the non-crypto blacklist.
2. /alert_check no longer uses a wide table.
3. Each opportunity is shown as a readable Telegram card.
4. Each card includes:
   - Coin and timeframe
   - Closest side
   - Priority
   - Distance
   - Consensus
   - Near-side liquidity concentration
   - Near/Far ratio
   - Score component breakdown
   - Alert types
5. Automatic watch messages include the same score breakdown.
6. Long outputs are split safely into multiple Telegram messages.

Test:
1. /collect
2. /alert_check
3. /alert_check 15
4. /watch_now
