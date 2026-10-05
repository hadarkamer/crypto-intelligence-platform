# SOL g65/k49 with 75%/25% take-path stop promotion

Activated as an independent experimental notification and observation formula
at the user's request on 2026-10-05. No order placement, position sizing,
Testnet forwarding or real-money execution is part of this worker.

## Frozen research rule

Source: `SOL_g65_Stop_Profile_20261004.json`, formula
`btcgrid_SOL_SHORT_g65_k049__cap1`, management `LOCK_075TP_025TP`.

At each UTC :00/:30 decision D, require complete, contiguous Binance Spot
BTC trade minutes over [D−12h,D). The last close divided by the first open,
minus one, must be strictly greater than −0.02 and at most −0.01. This rule
has no MaxPain, CVD, OI, session-calendar or weekday condition.

Let R be the SOL Binance Spot trade-minute OPEN at D+1 minute:

| Level | Frozen arithmetic | Example R=100 |
| --- | --- | --- |
| Short limit | R × 1.005 | 100.5 |
| Initial stop | R × 1.0125 | 101.25 |
| Take | R × 0.9825 | 98.25 |
| Cancel pending before fill | R × 0.99 | 99 |
| Lock trigger | entry − .75 × (entry−take) | 98.8125 |
| Promoted stop | entry − .25 × (entry−take) | 99.9375 |

One pending OR open observation is allowed in this formula, per subscription.
There is no fixed pending expiry or holding-time expiry. The first fill or
pending-cancel barrier governs; after a resolved observation only a later
half-hour decision can reserve a new one. This capacity is formula-local;
other SOL formulas can overlap it and may point in either direction.

Before promotion each completed minute is checked against the then-current
stop and take. The fill minute can trigger promotion from its surviving CLOSE
only; subsequent minutes use LOW. The new stop first applies to the following
minute. Adverse opening stop gaps exit at OPEN; favorable take gaps do not
improve the frozen target. Unknown intraminute entry/cancel or stop/take order
is recorded AMBIGUOUS and keeps capacity occupied. Such an event is never
optimistically counted as a winning exit.

## Prospective behavior and delivery

* Fresh activation is persisted once in the existing `bot_settings` table,
  under `sol-g65-k49-lock-cap1-v1`. Restarts preserve pending/open observations,
  cursors, evidence and receipts. No DDL or historical entry replay occurs.
* Only D+1 OPEN is read from an unfinished reference minute. Features and
  position outcomes use completed contiguous minutes only. Missing evidence
  preserves exposure and blocks new admissions.
* A fresh signal reserves a pending observation, not a message. A notification
  is prepared only after a completed candle verifies a limit touch and the
  position remains open. Its expiry is 90 seconds after the fill minute ends.
  This is notification freshness, not pending-position expiry.
* Provisional extrema can veto an already stale message, but cannot change an
  outcome or free capacity. A durable at-most-once outbox claims before send;
  uncertain sends become UNKNOWN and are never resent. Restart orphaned
  attempts become UNKNOWN while retaining the tracked observation.
* BTC history is fetched only for eligible new half-hour decisions, then
  cached incrementally. SOL is polled on minute boundaries only while a
  pending/open observation needs monitoring. Closed SOL cache is pruned.
  403/451 responses get a six-hour retry delay; other errors get five minutes.
  There is no source substitution, redirection, proxy or restricted-source bypass.

## Historical figures and limits

The referenced research spans 2025-09-02 00:30 through 2026-09-18 19:00 in
Israel. It is not an updated October backtest. The promoted cap-one replay had
404 closed trades, 161 positive net outcomes (39.85%), $553.842 net,
$1.37090 mean net and B20 2.20280. Mean holding time was 3.821 hours and
median 1.858 hours. These are retrospective research observations, including
118 full takes, 43 profit-lock exits and 243 initial-stop exits.

Research sizing was $5 initial price risk, $5,000 notional cap and 0.06%
round-trip fee. This geometry uses approximately $670 notional, so the cap
was not binding. These sizing fields are not part of the notification worker.
Historical performance is not a count of delivered or executed live trades.

## Integration and validation

`sol_g65_experimental_worker.WORKER.start(bot, subscription)` starts the loop;
`stop()` and `status()` follow the existing experimental workers' interfaces.
The gate is `alert_delivery_policy.sol_g65_experimental_enabled()` and rule ID
is `SOL_G65_K49_PROFIT_LOCK`. Run `python sol_g65_experimental_selftest.py`.
The optional `TEST_DATABASE_URL` test creates its own disposable local/CI
PostgreSQL database and checks concurrent reservation, fill, claim and restart.
