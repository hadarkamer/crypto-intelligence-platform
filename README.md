Stage 8 Hyperliquid Control Probe Patch

Goal:
Fix the hanging /hyper_debug command and verify that the bot can control the Hyperliquid page.

What it does:
/hyper_debug BTC
- opens https://www.coinglass.com/hyperliquid-liquidation-map
- waits for page load
- tries to select BTC
- tries to click refresh
- returns Telegram summary even if selection/refresh fails
- logs detailed [hyper] diagnostics
- does not save data to DB

Test:
/hyper_debug BTC

Send back:
- Telegram output
- Render lines starting with [hyper]
