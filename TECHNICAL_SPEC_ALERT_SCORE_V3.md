# Technical Specification — Alert Quality Score V3

Directional Alignment = Consensus (0..12) + BTC Like (0..5) + Market (0..3).
Specific Distance = 0..25.
Adjusted High Liquidity Close Distance = 0..25.
Liquidity Balance = clamp(balance*30, -10, +20).
Target Clustering = 0..10.
Final score range: 0..100.

Daily commands: /collect, /alerts, /coin BTC, /watch_on, /watch_status, /watch_stop.


## Stage 27 — Aggregate Market Schema

Market uses every valid asset/timeframe indication in the current snapshot.

For a SHORT alert:
market_support_pct = total SHORT indications / total indications * 100

For a LONG alert:
market_support_pct = total LONG indications / total indications * 100

Market points:
market_points = clamp((market_support_pct - 50) / 50 * 3, 0, 3)

This is not a binary label and does not borrow direction from a different
timeframe. It represents the strength of the full market schema.
