# Stage 68 — HYPE price via Bybit

- HYPE price lookup now prefers Bybit USDT perpetual (`category=linear`, `HYPEUSDT`).
- Futures mark price is preferred; last price is accepted when mark price is absent.
- If the linear market request fails or has no usable ticker, the bot retries Bybit Spot (`category=spot`).
- Explicit Render logs show whether Futures or Spot supplied the price, or the exact failure for each attempt.
- No collection, alert-selection, summary, or scoring logic was changed.
