# XRP R2732 NY weekdays with profit lock — experimental notification

Activated by user request on 2026-10-02. This is separate from U21 and preserves
all existing alerts. It sends to the existing enabled Watch subscription.
No exchange orders or automatic trade execution are added.

## Frozen research rule

Every UTC 15-minute decision D uses the latest closed XRP Binance Spot minute.
Let H be the XRP high of the most recent fully completed NYSE regular session.
The signal is SHORT when:

```
-0.017415876858352997 < close/H - 1 <= -0.004158176397388663
America/New_York weekday is Monday through Friday
```

There is no U21 range-location filter, BTC condition, or restriction to NYSE
opening hours. Weekday exchange holidays remain eligible using the previous
completed session. The audited calendar covers 2025–2026, including holidays,
early closes and DST. Unknown years fail closed until the calendar is reviewed.

Reference entry is D+1 minute OPEN. Initial short stop is entry*1.005 and take
is entry*0.92. The initial risk distance is the exact floating-point distance
from entry to stop. After a minute reaches 2 initial R of favorable movement
(approximately 1%), the stop locks 0.5R (approximately 0.25%) from the following
minute. The trigger minute is first checked against the previously active stop
and take. No holding deadline is imposed; monitoring continues on weekends.

## Delivery and persistence

One tracked position per R2732 Watch scope, independent of U21. Existing
bot_settings persistence and transaction/advisory locks guard activation,
decision cursor, active position and delivery attempts; no new schema.
Activation never backfills historical alerts. Messages expire 90 seconds after
reference entry. Already-observed stop/take/profit-lock moves veto stale entry
messages. Incomplete current bars may only veto; closed bars alone advance the
position. Uncertain sends are never automatically resent. Missing/ambiguous
price outcomes retain capacity. Repeated deployments preserve state.

The initial Telegram message states all reference prices and the conditional
profit-lock instruction. Monitoring tracks the rule internally and does not
place or modify orders. The new state namespace is excluded from the existing
U21/order-card forwarding source queries.

`/health` exposes `xrp_r2732_experimental`, including enablement, last decision,
active position, current stop, lock state and persisted counts. The existing
alert-delivery profile and Watch on/off gate control new messages.

## Verification

Run the three xrp_r2732_experimental_*_selftest.py suites and existing U21 and
alert_delivery_policy tests. Tests cover archived signal fixtures, exact bounds,
NY time boundaries, next-bar promotion, gaps, concurrency, restart, stale
messages and transport uncertainty. The existing production CI discovers all
tracked *_selftest.py files and includes optional local PostgreSQL checks.
