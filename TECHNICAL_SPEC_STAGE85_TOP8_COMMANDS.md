# Stage 85 — Top 8 commands

Added two Telegram commands restricted to the fixed core list:

- `/alerts_top8 [limit]` — one live scan, filtered to BTC, ETH, SOL, HYPE, DOGE, ZEC, BNB and XRP.
- `/watch_on_top8` — persistent Watch using the same scan cadence and scoring as regular Watch, but filtered to those eight symbols only.

`/watch_stop` stops the active regular or Top-8 Watch loop. Existing `/alerts`, `/watch_on`, and targeted `/watch_on SYMBOL TARGET` behavior is unchanged.
