# U21 XRP experimental activation

Requested 2026-09-25: U21 continuous XRP SHORT, one tracked position per Watch
subscription, as the only experimental notification. The production profile
remains `ORDINARY_AND_SELECTED_EXPERIMENTAL`; ordinary alerts and collectors
remain enabled. No order placement or Testnet execution is added.

## Frozen research contract

Every UTC 15-minute decision D uses minute candles strictly before D. Relative
to the last completed NYSE regular session's XRP high H and low L, closing P:

```
0.4122106385646779 < (P-L)/(H-L) <= 0.8393408289241626
-0.017415876858352997 < P/H-1 <= -0.004158176397388663
```

There is no entry-hour or BTC-direction filter. Frozen eligibility requires
continuous seven-day XRP/BTC history, the previous complete UTC week, and
non-flat historical windows. The calendar includes audited 2025/2026 holidays,
early closes and DST; an unknown year stops new entries until explicitly
reviewed. Reference entry is D+1 minute OPEN, stop 1.005 times entry, take 0.92
times entry. These are notification reference levels, not actual fill claims.

Closed one-minute first-touch monitoring retains active exposure through gaps,
uncertain delivery and ambiguous same-bar double touches. Opening gaps resolve
first; otherwise a double touch never releases capacity automatically. Release
time is the touched minute's END, and the next decision must be strictly later.
No position timeout or episode reset is used.

## Operational controls

The independent minute worker incrementally caches Binance Spot XRP/BTC data.
Activation, cursors, active position and outbox are persisted in the existing
`bot_settings` table with transaction locks; no schema migration is required.
The frozen configuration hash must match on restart. New messages expire 90
seconds after the reference entry and historical signals are never backfilled.
Known pre-send barrier touches veto notification without using unfinished bars
to release exposure. There is one transport attempt per message; uncertain
results retain capacity and are not resent. `/health` reports `u21_experimental`.

## Validation

Independent archived replay: 36,640 decisions, 36,064 eligible decisions,
6,291 matches, and 313 cap-one accepted trades. The pure predicate matched 40
archived complete feature windows, and all 313 accepted outcomes and the full
cap-one selection schedule matched exactly. The committed golden fixture
captures representative audited predicate cases. Automated tests cover exact
boundaries/calendar, persistent concurrency, restart and transport uncertainty,
stale/late entry suppression, cache gaps, policy isolation, and optional local
PostgreSQL concurrency in CI. Full historical replay artifacts are retained in
the original research workspace; they are not runtime dependencies.
