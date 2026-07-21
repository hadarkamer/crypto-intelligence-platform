# Stage 72 — Targeted Multi-Watch

## Scope

Stage 72 is based on Stage 70. The existing Relative Gap formula and all Max Pain scoring logic remain unchanged.

## Commands

- `/watch_on` — starts the existing general market Watch.
- `/watch_stop` — stops only the general market Watch.
- `/watch_on SOL 160` — starts or updates an independent SOL watch with target price 160.
- `/watch_stop SOL` — stops only the SOL watch.
- `/watch_status` — shows the general Watch status and active targeted symbols.

Several targeted watches may run at the same time. Each symbol stores its own start price, target, previous price, start time and last scan time.

## Target behavior

The direction is inferred from the target relative to the activation price:

- target above activation price: reached when current price is greater than or equal to target;
- target below activation price: reached when current price is less than or equal to target.

Every five minutes, the bot sends the normal highest-scoring Max Pain alert card for each watched symbol, plus:

- target price;
- whether the symbol moved closer to or farther from the target since the previous check;
- remaining distance in percent;
- progress relative to the activation price.

When the target is reached, the bot sends a summary and removes only that symbol watch.

## Scan protection

All targeted symbols are served by one manager task. A cycle performs one complete seven-timeframe CoinGlass scan and reuses the same result for every active targeted symbol.

The targeted manager, general Watch, `/alerts`, and `/collect` use the existing shared `SCRAPE_LOCK`. Therefore two CoinGlass scans cannot run concurrently. Each symbol watch has separate state and stopping or completing one symbol cannot stop another.

## Unchanged logic

- Relative Gap is unchanged from Stage 70.
- Alert scoring is unchanged.
- Dynamic Max Pain distance thresholds are unchanged.
- General `/watch_on` behavior and interval are unchanged.
