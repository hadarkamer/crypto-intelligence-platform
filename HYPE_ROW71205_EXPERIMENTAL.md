# HYPE ROW71205 experimental notification

The owner authorized adding ROW71205 to experimental Telegram notifications on
2026-10-04. This addition does not place exchange orders or alter XRP R2732,
U21, SOL, the manual formulas, or their stored positions.

## Frozen research rule

Canonical recipe: `DOGE_GRAMMAR_HYPE_F3_ROW71205_FULL_SOURCE_HISTORY_RETROSPECTIVE`
in the recovered `rankings/replay_other_leaders.py`, rule `hype71205`.
The source manifest `catalog/join_more_annual.py` states that HYPE uses recovered
**Binance USD-M Futures TRADE** candles throughout, with no Hyperliquid splice.
BTC uses Binance Spot BTCUSDT. Neither HYPE MARK candles nor Hyperliquid candles
are interchangeable with this rule's research source.

At UTC decision time D, every 30 minutes, use only complete one-minute candles
strictly before D:

- Return(h) = `close[D-1 minute] / open[D-h hours] - 1`.
- Range(h) = `100 * (highest_high - lowest_low) / open[D-h hours]`, with highs
  and lows from the h-hour window ending immediately before D.
- `0 < 100 * BTC Return(12) < 0.25`.
- HYPE Range(24) is at most the frozen float `5.1000000000000005`.
- HYPE Range(4) / HYPE Range(24) is strictly greater than `0.5`.
- HYPE Return(4) - BTC Return(4) is at most zero.

All conditions must hold. The SHORT reference entry is the observed **OPEN of
minute D+1**, stop is `entry * 1.005`, and take is `entry * 0.98`. There is no
profit lock, trailing stop, weekend filter, holding-time limit, or limit-entry
pending order. At entry 100 these prices are 100 / 100.5 / 98, reward/risk 4:1.

## Capacity, sizing and observation

One active observation is allowed **in this formula's own scope**. It is
independent of positions in other experimental formulas. A new decision must
be strictly after the previous first-touch exit minute's END. The monitor uses
contiguous completed 1m candles. Stop gaps fill at the open, favorable take gaps
fill at the fixed take, and a double-touch candle is unresolved unless its open
proves the first touch. Unresolved paths keep capacity occupied.

There is **no position sizing and no dollar position ceiling in this alert**.
It sends three reference prices and monitors their path. The research convention
of $5 initial price risk with a 0.5% stop corresponds to a $1,000 notional; the
$5,000 ceiling therefore did not bind. Dollar risk, fees and order quantity are
not execution instructions in this module.

## Durable delivery and operation

`hype_row71205_experimental_store.py` uses a dedicated
`hype-row71205-experimental-cap1-v1:<scope hash>` key in existing `bot_settings`.
Activation, decision cursor, active path and notification intent survive restart.
PostgreSQL advisory and row locks serialize reservation and claim. No runtime
DDL is added. This namespace is not the exact U21 key read by
`alert_cards_forwarder.py`; these alerts are not forwarded as exchange orders.

The existing watch subscription and delivery profile apply. ALL and the two
selected-experimental profiles enable this newly selected rule. Unknown profiles
fail closed. Startup warms required HYPE history first and BTC second, but
activation and monotonic decision fences prohibit historical alert replay.
Notifications expire 90 seconds after the D+1 reference minute. A crossed
barrier before sending vetoes stale delivery without releasing observation
capacity. Uncertain Telegram results are not retried; acknowledgements are
persisted. Tests mock Telegram, so no test notification is sent.

The existing process starts/stops this worker and exposes
`hype_row71205_experimental` in `/health`. The price adapter requests only
`https://fapi.binance.com/fapi/v1/klines` for HYPE and the existing Binance Spot
klines endpoint for BTC. It does not follow redirects or choose another market.
There is no new proxy or alternate-host workaround.

If the official source returns HTTP 451, health reports
`state=EVIDENCE_UNAVAILABLE`, `ready=false`,
`last_error_code=HYPE_BINANCE_HTTP_451`, and `hype_source_available=false`.
An HTTP 451 opens a six-hour source cooldown; transient errors retain the
five-minute retry. On each restricted-source attempt (including a new process),
a separate bounded diagnostic makes at most one request to CoinGlass's documented
`/api/futures/price/history`, for three closed 1m Binance HYPEUSDT candles using
the existing `COINGLASS_API_KEY`. It never follows redirects or retries, and
`source_diagnostic` exposes only fixed status labels, timestamps and numeric
metadata. It exposes neither credentials, raw error text nor prices. This probe
does not feed the strategy, assert provider parity, or enable a fallback.
Even a successful probe requires full source/timing validation before activation.
The formula remains
enabled for future observations, but **cannot issue valid new alerts while
its exact source is unavailable**. This is an enabled rule with unavailable
evidence, not a working price feed. Successful validated HYPE reads refresh
source health on startup, normal decisions, monitoring and pre-send checks.
Verify the exact TRADE route's live availability after deployment; a previous
MARK route health result is not itself that verification.

## Validation

Run the three `hype_row71205_experimental_*_selftest` modules,
`hype_row71205_source_probe_selftest` and
`alert_delivery_policy_selftest`. They cover frozen thresholds and floating-point
arithmetic, missing and future candles, exact source URLs, no fallback on HTTP
451, retry backoff and recovery, durable cap-one and delivery fences, restart,
stale-slot suppression, partial-minute separation and pre-send barrier vetoes.
PostgreSQL integration is opt-in using only an isolated local test database.
Existing XRP R2732 signal/store/worker and U21 worker tests provide regression
coverage. Independent research-fixture comparison is recorded outside the bot
source tree in the task's review evidence.
