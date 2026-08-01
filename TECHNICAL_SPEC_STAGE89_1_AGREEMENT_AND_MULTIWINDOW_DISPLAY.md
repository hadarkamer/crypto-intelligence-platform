# Stage 89.1 — Agreement + Multi-window Evidence Display

- Removed 40/35/25 weights and the artificial /100 alignment score.
- Market Evidence now counts three independent families: Price+OI, Futures Flow, Spot Flow.
- Alerts and all Watch paths show all eight CVD windows for Futures and Spot.
- Every CVD window shows the current signed CVD change value, its historical magnitude class, and the window conclusion. The compact Telegram display omits the redundant “ΔCVD” label.
- Price+OI now calculates 30m, 1h, 4h, 12h, 24h, 48h, 72h and 7d.
- Overall Price+OI uses a strict majority of the windows currently available.
- No Max-Pain score, ranking, LONG/SHORT selection, Watch or alert trigger was changed.
- /market_state is no longer registered for normal use.
