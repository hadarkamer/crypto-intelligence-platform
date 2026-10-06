# HYPE ROW71205 experimental notification

The owner authorized adding ROW71205 to experimental Telegram notifications on
2026-10-04. This addition does not place exchange orders or alter XRP R2732,
U21, SOL, the manual formulas, or their stored positions.

## Active source variant (2026-10-05)

The original Binance HYPE TRADE endpoint returned HTTP 451. Following the
owner's request for a different practical solution, the active worker now uses
public **Hyperliquid HYPE perpetual TRADE 1m candles**, with BTC still from Binance
Spot. Its distinct formula ID is `HYPE_ROW71205_HYPERLIQUID_SOURCE_VARIANT_V1`.
The mathematical thresholds and monitoring rules below remain identical, but
Binance historical returns/win rates do **not** validate this venue variant.
Health, Telegram text, and frozen position features identify the new source and
`PROSPECTIVE_SOURCE_VARIANT_NOT_HISTORICALLY_VALIDATED` evidence status.
This is an explicit source change, not an automatic fallback. No subscription,
API key, proxy, exchange order, or paid-plan change is needed.

## Frozen mathematical rule and research parent

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
`hype_row71205_experimental` in `/health`. HYPE uses only the public Hyperliquid
`POST https://api.hyperliquid.xyz/info` candleSnapshot endpoint (`coin=HYPE`,
`interval=1m`). Responses must provide exact contiguous minutes, correct
instrument/interval, finite positive consistent OHLC, and exact minute bounds.
Current-minute rows are fetched separately for reference OPEN and pre-send
extrema veto only. They are never used in completed-bar features or exits.

A one-time, transaction-locked migration accepts only the original deployed
Binance configuration hash, with no active observation and no pending, in-flight
or unknown delivery. It preserves historical evidence/cursors/counts and records
a source migration audit. Activation advances to migration time, preventing old
alerts from being replayed. Subsequent restarts preserve this activation. Any
incompatible or occupied old state blocks migration rather than discarding it.

Hyperliquid provides a rolling 5,000-candle window. Incremental minute polling
keeps ordinary monitoring inside it. If an outage loses a required older candle,
recovery stops visibly with the active observation still occupying capacity;
missing history is not filled from another market. Missing/invalid source data
fails closed with a five-minute retry; HTTP 403/451 has a six-hour cooldown.
The old CoinGlass source probe remains an offline diagnostic module and is no
longer invoked by this worker. A cached history alone cannot establish recovery:
the worker directly validates the latest completed HYPE minute before readiness.

## Validation

Run the three `hype_row71205_experimental_*_selftest` modules,
`hype_row71205_hyperliquid_source_selftest`, the existing standalone Hyperliquid
provider tests, SOL proximity tests, and alert delivery policy regressions.
They cover frozen mathematical thresholds, partial/closed separation, explicit
source provenance, missing/invalid data, no fallback, retry and recovery,
transactional migration, durable cap-one/outbox fences, restart, stale-slot
suppression, and pre-send barrier vetoes. PostgreSQL integration runs only in an
isolated test database; tests never send Telegram notifications.
