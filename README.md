Stage 10 — Live Price Diagnostic

Adds:
- live_price_provider.py
- /price_check
- /price_check BTC

Purpose:
Verify live-price coverage before changing alert calculations.

This stage does NOT:
- modify DB rows
- change /alert_check
- run automatic alerts

Test:
1. /collect
2. /price_check
3. /price_check BTC
