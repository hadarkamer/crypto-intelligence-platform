# Stage 73 — Targeted Watch Uses Normal Alerts

## Scope

The existing Gap calculation and all alert scoring remain unchanged.

## Targeted watch cycle

`/watch_on SOL 160` keeps an independent SOL watch with target 160.
Every five minutes one protected collection cycle is shared by all active
specific watches. For the watched symbol, the bot selects the strongest normal
alert for each configured timeframe and sends the standard alert card in the
fixed order:

`12h, 24h, 48h, 3d, 1w, 2w, 1m`.

The target progress footer is appended only to the final normal alert card of
the cycle. If a timeframe has no generated Max Pain opportunity, the bot reports
that missing timeframe rather than inventing an alert.

## Target completion

Target evaluation occurs after the normal timeframe alerts are sent. When the
price reaches or crosses the target, a separate short completion summary is sent
last. Only that symbol watch is removed automatically; all other specific
watches and the general watch remain active.

## Concurrency

The existing shared scrape lock remains in force. Multiple symbol watches use
one manager loop and one collection per five-minute cycle, preventing competing
CoinGlass scans.
