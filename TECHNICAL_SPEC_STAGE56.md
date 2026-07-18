# Stage 56 — Alert timeframe status UI

## Changes

- Removed the line `X/7 טווחי זמן עם התראות` from alert cards.
- The all-timeframe block remains only at the bottom of each alert card.
- The block title is now `📊 מצב SYMBOL בכל טווחי הזמן`.
- Timeframes are displayed in the fixed order: `12h`, `24h`, `48h`, `3d`, `1w`, `2w`, `1m`, according to the project TIMEFRAMES configuration.
- Status meanings remain:
  - 🟢 active tradable target with score
  - 🟡 active target below the 0.5% minimum
  - 🔴 no active target / Max Pain already taken
- No summary-count line is added after the timeframe list.
