Stage 19 — Alert annotations and data-quality notes

Implemented:
- Data quality is not part of Priority.
- Each alert remains separate by coin and timeframe.
- Each alert notes additional alerts for the same coin in other timeframes.
- Opposite-direction alerts are explicitly marked.
- Multiple alerts do not add score.
- Data-quality problems are displayed in Hebrew at the end of the alert only.
- Data-quality notes do not add or subtract score.
- Same behavior in /alert_check, /alert_explain and automatic Watch alerts.

Tests:
1. /collect
2. /alert_check 10
3. /alert_explain BTC 24h
4. /watch_now
