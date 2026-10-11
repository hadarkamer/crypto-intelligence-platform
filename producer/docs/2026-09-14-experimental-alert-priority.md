# Experimental alert scope and Watch message priority

Owner request: limit the two-CVD ≥65 notification to ZEC, prefix it with **סף 2%**,
add inverse C0964 for BTC only, put experimental notifications before ordinary
half-hour Watch reports, and render the word **הערה** in bold.

## Formula changes

- The dual-CVD capture still validates all eight source coins. Only ZEC may
  create, claim, or send this notification. Existing non-ZEC pending messages
  expire; a transport guard also rejects any legacy non-ZEC claim. Existing ZEC
  episode and generation deduplication survives the update. All messages,
  including already queued ZEC messages, begin with **סף 2%**.
- C0964 requires a native Magnet-family source with captured liquidity edge
  ≥30 and Spot CVD total aligned to that Magnet ≥25. It applies only to BTC,
  forecasts the opposite direction, and displays the 2% research threshold.
  Its exact note is **הערה**: מבוסס בעיקר על אוגוסט ועל עליות.
- All five manually selected formula renderers use HTML `<b>הערה</b>`.
  Existing per-coin exclusions and notes on the other four rules are retained.

## Priority and truthful delivery state

Previously, manual formula detection waited for ordinary messages to be sent
and their delivered source rows to arrive in the database. The Watch now
prepares the same native source captures before transport. These have explicit
`WATCH_PLANNED_ALERT / NOT_ATTEMPTED` provenance and `watch:<fingerprint>`
identities, never database event IDs or fabricated delivered statuses.

The preview intercepts capture before the memory sink, database writer, or
Sheets. Its synchronous capture-state snapshot is restored even on failure.
Real ordinary-message captures still run only at their actual delivery hooks.
Sequence evaluation uses earlier delivered history and strictly earlier planned
siblings; equal timestamps cannot manufacture a second entry.

ZEC dual-CVD, the five manual formulas, the existing Max Pain/CVD experimental
formula, and already eligible ordered-research notifications are sent before
the ordinary Watch header/cards and Magnet reports. The manual priority drain
is bounded to 128 attempts; rule/scan/coin/direction dedup bounds one normal
scan to at most 50 manual matches. Ordinary Max Pain score confirmations retain
their ordinary position. Destination and Watch state are rechecked before
transport. The independent ordered worker is prevented from interleaving with
an active Watch group. A newly qualified asynchronous result whose source does
not yet exist cannot be sent before that source; its freshness policy remains
unchanged.

The known v1 manual ruleset upgrades atomically in its existing settings row.
Activation, receipts, deduplication and attempted/terminal messages survive.
Only unattempted messages are reformatted, and C0964 receives its own upgrade
activation fence. Unknown versions still fail closed. A transient sequence
read has a bounded preparation retry and preserves a separate ENTRY2 recovery
path without replaying other already evaluated rules.

## Validation and rollout

Focused detector, source, reducer, fake transport and actual Watch-path tests
cover both forecast directions, coin boundaries, old pending messages,
first-touch threshold labels, migration, causal sequences, full group ordering,
stop/destination races, and uncertain transport. PostgreSQL regressions run in
the repository's CI against its disposable test database. Merge only after the
required CI passes; the existing Render main-branch auto-deploy applies the
change. Verify readiness, rule IDs/scopes and deployment commit through health
and normal scheduled operation. No synthetic Telegram test messages are sent.
