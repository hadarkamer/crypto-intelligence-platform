# Stage 21 — Automatic Alerts Final

Changes:
- Liquidity Density removed from Alert Score and alert display.
- Historical liquidity baselines removed from the alert workflow.
- HIGH_LIQUIDITY_CLOSE_DISTANCE removed because it depended on Liquidity Density.
- New raw maximum score: 65.
- Priority is normalized to 0..100 from:
  - Proximity 0..20
  - Directional Alignment 0..20
  - Target Clustering 0..15
  - Liquidity Balance -10..+10
- Automatic Watch is silent unless it sends a real new alert.
- /watch_status shows last/next scan and last scan results.
- /watch_stop remains an alias for /watch_off.

Tests:
1. /collect
2. /alert_check 10
3. /watch_now
4. /watch_on
5. /watch_status
6. /watch_stop
