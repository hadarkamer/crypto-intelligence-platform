# DOGE Magnet OBSERVATION experimental notification

The owner requested adding the DOGE SHORT Magnet OBSERVATION formula to the
existing experimental Telegram notifications after reviewing its symmetric
1.75% first-touch results. This adds one rule to the existing manual outbox.

## Frozen rule

- ID: `MAGNET_OBSERVATION_DOGE_SHORT`.
- Symbol: DOGE only.
- Native source: a Magnet-family event with a verified research direction of
  SHORT, a LOWER magnet and captured confirmation status `OBSERVATION`.
- Prediction: SHORT in the source direction, without inversion.
- Threshold: 175 basis points. Stop = reference price × 1.0175;
  take profit = reference price × 0.9825.
- No additional MQ55, CVD, liquidity, horizon or market-wave condition.

The existing fresh-source, provenance, recipient and Watch controls apply.
Missing confirmation status does not become OBSERVATION from a low MQ score.
The database projection now retains `magnet_confirmation`, so delivered-source
recovery sees the same status as the planned Watch path.

## Source and price clocks

The current production General Watch is enabled and its Magnet subscriptions
already include DOGE. Its existing native Magnet preview emits observation
events, including those that cannot qualify as confirmed combined alerts.
No ordinary subscription or broader formula source population is changed.

The alert uses the existing captured `MAX_PAIN` price-reference component for
Magnet. It freezes that source-clock reference and its symmetric levels before
delivery. Missing or unverified reference prices remain unavailable; the
renderer does not replace them with a later quote. Historical first-touch
statistics used native event entry prices; this change does not recalculate
those statistics using the displayed reference-price clock.

## Activation and continuity

The manual ruleset advances from v3 to v4. The known v3 outbox is upgraded
transactionally, preserving existing pending messages, receipts, deduplication
keys and the standalone Futures rule's state. Only this new rule receives a
new activation fence. Events at or before that fence cannot create its alerts.
The existing known-v2 migration remains supported for lagging destinations.

The existing scan/rule/symbol/direction identity prevents repeated preview and
native-delivery copies from sending the same experimental alert twice.
No orders are placed.

## Verification

- All 245 frozen DOGE source snapshots from the 7–15 September analysis match
  the new rule as direct SHORT at 175 bps.
- Predicate tests cover symbol/direction restrictions, other and missing
  confirmation states, invalid direction mapping, no extra indicator filters,
  immutable reference levels and rejection of inverted or altered payloads.
- A real Magnet preview with captured reference 0.1 produces one SHORT alert
  with stop 0.10175 and take profit 0.09825; repeated preview and a subsequent
  native-delivery receipt send no second notification.
- Source tests check planned/delivered confirmation-status parity.
- Store tests cover upgrade continuity, the new activation fence and restart.

Sources: `manual_formula_alert.py`, `manual_formula_alert_source.py`,
`manual_formula_alert_store.py`, their self-tests, and the existing
`research_ordered_question_catalog.py` captured-status feature.

## Selected notifications only

The owner subsequently authorized activation and requested pausing every other
automatic alert, leaving only this DOGE rule and C1274 active. Activate this
selection with `ALERT_DELIVERY_PROFILE=SELECTED_EXPERIMENTAL_ONLY` on the
existing production service when deploying this code.

`alert_delivery_policy.py` centralizes that temporary selection. Its `ALL`
profile retains the previous delivery behavior, while an unknown profile
permits no automatic notifications. The health response exposes both the
profile and the exact active/paused manual rule IDs.

The selected profile blocks ordinary Watch cards, headers, combined/formula
alerts, Magnet reports, specific-watch updates, dual-CVD, transition and
research-worker alerts. It retains Watch collection, frozen score bundles and
native source previews needed by the two selected rules. Manual command replies
remain available.

Manual outbox creation skips paused rules but records source receipts. Claiming
cancels any previously pending paused-rule messages, and delivery rechecks the
rule immediately before transport. Existing queued messages therefore cannot
bypass the selection or block the two active formulas at the front of a queue.

Policy tests verify exact selection, no sends from other pending alerts,
receipt preservation, invalid-profile behavior and policy changes across an
awaited claim. Automatic delivery paths have additional suppression tests.

Deployment verification must show ruleset v4 ready, exactly `C1274` and
`MAGNET_OBSERVATION_DOGE_SHORT` in `active_rule_ids`, four other registered manual
rules in `paused_rule_ids`, and `ordinary_alerts_enabled=false`. General Watch
and DOGE Magnet collection must remain enabled. Do not stop the whole Watch
subsystem to pause its ordinary notifications.

## Restore ordinary notifications while retaining the experimental selection

The owner then requested restoring ordinary notifications while leaving the
paused experimental notifications paused. Use
`ALERT_DELIVERY_PROFILE=ORDINARY_AND_SELECTED_EXPERIMENTAL` after deploying the
code that supports it. Do not use `ALL`: that would also restore the paused
experimental rules.

| Profile | Ordinary alerts | DOGE and C1274 | Other experimental alerts |
| --- | --- | --- | --- |
| `ALL` | Enabled | Enabled | Enabled |
| `SELECTED_EXPERIMENTAL_ONLY` | Paused | Enabled | Paused |
| `ORDINARY_AND_SELECTED_EXPERIMENTAL` | Enabled | Enabled | Paused |

The restored ordinary paths include Watch cards and headers, Max Pain score-65
transitions, confirmations, Combined, Magnet reports, and any active specific
watches. Existing subscription settings still apply. The separate experimental
gate keeps dual-CVD, the Max Pain/CVD formula, ordered experimental notifications
and research-formula live notifications paused, in addition to the four paused
manual rules. No formula definitions or activation fences change.

The mixed transition queue filters allowed kinds before claiming messages and
rechecks the claimed kind before transport. Pending experimental messages cannot
send or block an ordinary message. New muted formula intents are not created.

The earlier mute preserved raw scans, scores, source previews and price data,
but ordinary native research events were coupled to ordinary delivery and
stopped accumulating. Restoring those paths resumes their prospective capture;
this change does not backfill the muted interval or relabel undelivered events.

After rollout, health must report the new profile, ordinary delivery enabled,
other experimental delivery disabled, and exactly the same two active manual
rules. Regression checks cover a full Watch cycle, supervisor recovery, mixed
queue filtering, independent experimental workers and profile changes during
awaited claims.
