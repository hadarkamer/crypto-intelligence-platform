# Stage 76 — Dual-direction score selection and alert layout

## Direction selection

For every symbol and every Max Pain timeframe, the engine calculates a complete LONG score and a complete SHORT score independently.

The displayed direction is the direction with the higher complete score:

Selected direction = argmax(Score LONG, Score SHORT)

Max Pain distance is used only as a deterministic tie-breaker when the two complete scores are equal. It no longer selects the direction before scoring.

## Bitcoin confirmation

BTC is evaluated with the same dual-direction method. For each timeframe, altcoin confirmation uses BTC's selected direction and complete score from that exact timeframe. It does not use BTC's all-timeframe average.

## Directional averages

LONG and SHORT averages remain separate. The alert displays:

- the selected direction's score for the current timeframe;
- the selected direction's average across all available timeframes;
- the opposite direction's score for the current timeframe;
- the opposite direction's average across all available timeframes.

## Commands affected

The updated direction selection is used by all consumers of build_opportunities, including:

- /alerts
- /alerts SYMBOL
- /alert SYMBOL (backward-compatible alias)
- /alerts_liq
- general Watch
- /watch_on SYMBOL TARGET

## Alert layout

All principal scores and component scores are displayed on a separate line below their label and in Telegram bold formatting. Alert-card send operations use HTML parse mode.

## Regression validation

Stage 76 adds tests ensuring:

- the selected direction never has a lower complete score than the available opposite direction;
- BTC confirmation uses the selected complete BTC score from the same timeframe;
- the opposite-direction average is available for alert formatting.
