# Stage 92 — Directional Alert and Separate Confirmation

## Directional command

The command supports:

- `/alert BTC`
- `/alert BTC long`
- `/alert BTC short`

A requested direction is scored with the exact existing directional calculation. The command does not add or remove points and does not alter ordinary automatic direction selection. It only selects the requested side for the displayed BTC/SYMBOL cards. The opposite direction score and average remain visible in the normal alert template.

## Separate confirmation notification

After an ordinary alert card, a short HTML Telegram notification is sent when the status changes to one of:

- `CONFIRMED`
- `STRONG_CONFIRMED`
- `CONFLICT`

The state key is `symbol + timeframe + side`. Repeated scans with the same status do not resend the special notification. A changed status, direction, or timeframe can create a new notification.

The notification includes symbol, direction, timeframe, score, Price+OI, Futures CVD, Spot CVD, and the confirmation label.

## Unchanged

No scoring formula, Max Pain calculation, CVD/OI calculation, ranking, Watch threshold, or database market data is changed.
