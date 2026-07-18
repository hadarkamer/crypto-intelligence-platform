# Stage 68 — TradingView G.O.A.T Indicator Connected

- Added `POST /tradingview` to match the URL already configured in TradingView.
- Kept backward-compatible `POST /webhooks/tradingview`.
- Added strict parsing of the indicator's Discord Embed JSON.
- Supported events:
  - ANY BULLISH SIGNAL → LONG
  - ANY BEARISH SIGNAL → SHORT
  - GOAT Score — Strong Zone → NEUTRAL strength event
- Extracted Score, AVG Score, Price, timeframe, Exit and ATR Stop.
- Signals remain Shadow Mode only and do not affect Max Pain scoring.
- Raw TradingView payload is preserved in storage.
- When `TRADINGVIEW_WEBHOOK_SECRET` is configured it is enforced; when absent,
  strict payload validation is used so the already-configured plain URL works.
