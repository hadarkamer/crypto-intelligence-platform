# Four explicitly requested experimental alerts

This is the original four-rule rollout. The subsequent
[scope and priority update](2026-09-14-experimental-alert-priority.md) adds BTC
C0964 at 2%, bold note labels and pre-delivery Watch evaluation. It supersedes
the delivery timing described below while retaining the four original rules.

The owner approved activation after the September 14 statistical audit and its
count/pending-outcome corrections. These are manually selected research
notifications, labeled **ניסיוני, לא למסחר**, not automatic statistical approval
and not trade execution. Existing research qualification, ordinary Watch,
dual-CVD, price paths and outcomes are unchanged.

## Frozen selections

| Rule | Movement threshold | Included symbols |
| --- | --- | --- |
| C1274, inverse Magnet | 1% | BTC, BNB, DOGE, HYPE, SOL |
| Price/OI ENTRY2, inverse | 1% | BTC, BNB, DOGE, ETH, SOL, XRP |
| Price/OI + Spot CVD total ≥65, inverse | 2% | BTC, BNB, DOGE, HYPE, SOL, XRP, ZEC |
| Max Pain full consensus, inverse | 2% | BTC, DOGE, ETH, HYPE, SOL, XRP |

Exact per-coin Hebrew notes are in `manual_formula_alert.RULES`, covered for all
eight symbols by the detector tests. Every message begins with its movement
threshold and experimental label. It displays the actual source timestamp in
Israel time and the forecast, inverted once from the native research direction.

C1274 requires a native Magnet-family event, available Futures evidence,
long-family quality ≥0.65 and direction opposing the Magnet, and signed total
Futures score aligned to the Magnet ≤−25. No liquidity-edge test is added.
The Price/OI+Spot rule requires both captured totals aligned ≥65.
Consensus requires the captured valid direction mapping and full valid positive
hits/total, not an additional score threshold.

ENTRY2 preserves the **exact audited v3 ordinal**, including its current-scan
behavior: it counts distinct scan IDs among earlier qualifying events within
30 minutes, then adds one. An earlier sibling event from the current scan can
therefore produce ordinal 2. It is not necessarily a second native Price/OI
message or two different overall scans. Changing that would require a new
formula and a new statistical evaluation. The native event/scan/symbol/direction
population is retained; all-Watch-scan observations are not substituted.

## Source, state and delivery

The source adapter reads fresh native `ALERT / DELIVERED` captures directly,
without depending on delayed research matches or outcomes. Price/OI sequence
history is captured-only, strictly causal and limited to the required 30-minute
window. Missing/oversized history fails closed for ENTRY2 only; eight fair retry
slots accompany 24 fresh slots while events remain younger than ten minutes.
HYPE is eligible; no unsupported-spot-source gate is used for this condition
notification lane.

An isolated versioned `bot_settings` key per destination holds a bounded JSON
outbox, immutable database-clock activation timestamp, ruleset checksum,
processed-source receipts and signal deduplication. No schema changes or
credentials outside the app's configured database workflow are required. One
row lock commits detection, receipts and frozen messages atomically. The lane
does not use a serial-ID high-water mark, so late transaction commits are not
lost. Deduplication is per rule + scan-or-minute + symbol + forecast direction
within the destination. Different formulas may each notify from one event.

Only post-activation events less than ten minutes old can create messages.
Two sends at most are attempted per normal supervisor pass, only for the active
general Watch destination and outside a running scan. The destination and Watch
state are rechecked after DB awaits and immediately before transport.
`/watch_stop` prevents sends; ordinary Watch controls resume them. No synthetic
test messages or manual scans are sent during deployment.

An IN_FLIGHT attempt is committed before transport. Telegram exceptions or
missing positive message IDs are terminal UNKNOWN (known rejects are FAILED).
Expired orphan attempts become UNKNOWN and never retry. A definitely unattempted
claim revoked by a Watch change becomes CANCELLED. Receipts last beyond source
freshness; expired messages cannot replay after pruning or a process restart.
The deliberate tradeoff is occasional lost delivery after an ambiguous network
result rather than duplicate Telegram messages.

Health exposes `manual_formula_experimental`: readiness, activation, four rule
IDs, counters, retry gaps and errors, without chat identifiers or source secrets.
No probability or asymmetry is recomputed in the notification path, and the
user's historical caution notes are not automatic qualification claims.

## Verification and rollout

Pure detector/source/reducer and fake-bot tests cover boundaries, every coin and
note, direction, activation, replay, late commits, causal sequence, retries,
expiry, subscription races, cancellation and uncertain transport. Guarded CI
PostgreSQL tests use disposable local databases, never production. The existing
repository-wide CI must pass before the PR is merged into main. Render's normal
Git auto-deploy then enables the lane for the already-authorized general Watch
destination. Check health/read-only persisted state and normal scheduled cycles.
