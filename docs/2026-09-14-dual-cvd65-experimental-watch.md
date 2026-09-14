# User-requested experimental dual-CVD Watch alert

The user explicitly requested a live experimental alert when Futures CVD and
Spot CVD total scores are both at least 65 and support the same direction. The
prediction follows that shared direction. This is the standalone existing
`CORE_FUTURES_CVD_SPOT_CVD_TOTAL_65` condition, with one canonical identity.

## Behavior

Update: [experimental alert priority](2026-09-14-experimental-alert-priority.md)
limits notifications to ZEC and prefixes **סף 2%**. The all-coin source capture
and original episode identities below remain unchanged.

- A signed bullish total of +65 and bullish Spot total of +65 qualifies LONG;
  bearish -65/-65 qualifies SHORT. Equality is included. Opposing directions
  and either valid magnitude below 65 are NO_MATCH.
- Consume the exact frozen operational score bundle from each new shared Watch
  scan, for all eight captured symbols, before display filtering. Only ZEC is
  eligible for this notification; other coins remain available to research.
- Missing, malformed, stale, future-dated or inconsistent source evidence is
  UNKNOWN. Model WARNING scores are already adjusted by their producing model
  and are not penalized a second time.
- The first fresh qualifying scan after activation creates one experimental
  alert for ZEC. Continued same-direction matches do not repeat. Valid
  NO_MATCH resets immediately; UNKNOWN preserves the prior active state.
  Direction changes can create a new alert. Reusing the same pair of CVD
  candle closes and direction cannot create another delivery.
- A persistent activation timestamp excludes pre-activation captures; this
  rollout does not replay historical scans as live notifications.
- Messages go only to the currently active general Watch destination. The
  subscription and destination are checked again after the database claim.
  Existing Watch stop/start commands remain the subscription controls.

## Delivery and deployment

Migration 053 adds separate scope state, replay receipts and an outbox. Runtime
checks schema readiness without creating tables. The source decision and exact
message are committed before ordinary Watch messages. The Watch supervisor
recovers unattempted pending messages between normal scans.

Pending messages expire at the earlier of ten minutes after their captured
decision or either CVD source reaching its 30-minute freshness limit. Each
message has at most one transport attempt, committed before sending. Ambiguous
failures and orphaned attempts are terminal UNKNOWN and are never automatically
resent. The tradeoff is a possibly missed notification after an uncertain
transport result, instead of duplicate notifications. Each drain is bounded to
two messages and each network attempt to 20 seconds.

The new alert does not change Max Pain transitions, the existing Max Pain+CVD
short rule, operational scores, Watch scheduling, research evaluation versions
or statistical qualification gates. It does not place trades. Its message is
explicitly experimental; no accuracy, target, horizon or statistical validation
claim is inferred from enabling it.

## Verification

Pure detector tests cover boundaries, direction, unknown inputs, freshness,
capture integrity and stable generation identity. PostgreSQL tests cover
transactional state, replay and ordering, activation, generation deduplication,
pending recovery, expiry, single claims and orphan settlement. Fake-bot tests
exercise real Watch hooks and delivery authorization, failures and cancellation
without contacting Telegram. CI runs every tracked self-test with PostgreSQL 18.
Production verification uses the normal scheduled Watch cycle and safe health /
database counts; no manual scans or test Telegram messages are required.
