# Technical Specification — Alert Quality Score V3

Directional Alignment = Consensus (0..12) + BTC Like (0..5) + Market (0..3).
Specific Distance = 0..25.
Adjusted High Liquidity Close Distance = 0..25.
Liquidity Balance = clamp(balance*30, -10, +20).
Target Clustering = 0..10.
Final score range: 0..100.

Daily commands: /collect, /alerts, /coin BTC, /watch_on, /watch_status, /watch_stop.
