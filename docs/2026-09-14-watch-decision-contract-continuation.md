# Continuation — inactive Watch decision predicate contract, 2026-09-14

Read `docs/2026-09-14-watch-decision-contract-verification.json` first. This
continues the verified PR43 capture rollout. Implementation and staged frozen
source verification, full CI, merge and deployed CLI verification are COMPLETE.
Continue from the next bounded step below; do not rerun this release.
Do not repeat the earlier capture rollout or broad historical audit.

## Release and scope

- PR44: https://github.com/hadarkamer/crypto-intelligence-platform/pull/44
- Head: `f5ea5383f072d2341758e1dcf602ce007dd099f8`.
- Exact tested tree: `35b1f76e07b9a3bb0c789bb11032adcfb8c67262`.
- Base production: `b1ad1e49b6506620dcff3262bcc715089c4a876d`.
- CI run/job: `34818467670` / `103894297279`.
- Local tests:15 pure contract +6 source/probe tests, all21 passed.
- Full CI: all131 selftest files passed.
- Merged main: `9cfc211a9e33562ef6a060ef57ab6e4f8065e879`, exact tested tree.
- Render deploy: `dep-dajq9f8ae00c73ee1tn0`, live07:41:28.077423UTC.
- Deployed CLI: snapshot1308 passed with identical staged payload hash and
  exact tested module bytes. Health07:42:42UTC verifies all four workers running
  without errors and Watch waiting for08:02:15UTC.
- No migration, Watch edit, startup hook, new worker or coverage activation.

Implementation worktree: `/workspace/scratch/1a8c1bcb93ce/watch-decision-contract`.
Continuation worktree: `/workspace/scratch/1a8c1bcb93ce/watch-state`.
Durable continuation branch: `checkpoint/research-continuation-20260913`.

## What is implemented

`research_watch_decision_contract.py` maps42 unchanged original definitions and
implements an inactive, outcome-blind evaluator. Version is
`watch-decision-contract-v1`; selection version is
`watch-frozen-item-cluster-qualified-group-v1`. Original IDs, definitions,
definition hashes, cutoffs and inverse pair semantics are preserved.

The three explicitly distinct populations are:

- `MAXPAIN_SELECTED_ITEM`:8 literal confirmation predicates on each retained,
  displayable actual selected item. Source SHORT predicts LONG and vice versa.
- `MAGNET_CLUSTER`:8 literal predicates on every captured cluster evaluation,
  including negative/error/missing records. UPPER predicts LONG; LOWER predicts
  SHORT. The first displayable source item's side is not the Magnet direction.
- `QUALIFIED_COMBINED_GROUP`:22 exact top-item average thresholds/bands on the
  separately declared qualified-candidate population. Known unqualified groups
  are NOT_APPLICABLE; missing qualification or selection is UNKNOWN.

Four explicit Combined/event+liquidity predicates remain blocked. An active
candidate is not a recorded event, and the existing event hook includes failed
as well as successful delivery attempts. Event type alone does not prove delivery.
The22 candidate-population predicates are not interchangeable with their older
event-population observations, despite unchanged numerical predicates/hashes.

Four of the8MaxPain label definitions ask for NOT_CONFIRMED/OBSERVATION, which
the current producer cannot emit. They are structurally unreachable diagnostics;
BELOW_SCORE/CONFLICT/UNCONFIRMED remain literal known nonmatches. They are not
renamed or counted as productive formulas. Producer code identity is bound;
unrecognized labels or changed producer evidence become UNKNOWN.

All relevant same-coin MaxPain timeframe clocks are checked: item score consensus,
Magnet maximal-cluster selection and Combined selection use more than the final
chosen item/member. Core positioning/futures/derivative clocks are checked where
used. Unrelated Spot clock errors do not veto these predicates. Missing source
rows, unresolved population truncation, and failed evaluations remain explicit.
Valid complete empty populations produce no synthetic observations or controls.

All support, cohort, false-control, outcome-reuse, delivery and prospective
qualification flags remain false. **Active coverage is still204/298.** The38
probe-evaluable count is a separate source-contract milestone, not activation.

## Read-only source verification

`research_watch_decision_contract_probe.py` is an explicitly invoked CLI. It
opens a read-only10-second-bounded transaction, reads one accepted immutable
source once, checks parent/score/availability linkage, and calls the pure validator
and contract. Default is the latest accepted source, including missing/failed
capture; it never silently skips to an older valid record. An optional full JSON
output is a local audit file. No providers, outcomes, later events, prices, sends,
database writes or recurring scans are invoked.

Staged verification used exactly the tested module bytes on saved snapshot1308:
96 actual units yielded936 oriented predicate evaluations:54MATCH,706NO_MATCH,
176NOT_APPLICABLE and0UNKNOWN. Breakdown:56MaxPain items→448NO_MATCH;
28Magnet clusters→54MATCH/170NO_MATCH;12Combined groups→88NO_MATCH on4qualified
groups and176NOT_APPLICABLE on8known unqualified groups. All8coins were covered.
The54matches concern literal Magnet negative/observation labels, including
inverse counterparts. They are not successful trades or independent cases.

Historical snapshot1306 returned an UNKNOWN source gate with
MISSING_DECISION_CAPTURE and zero invented units. Both outputs retained their
canonical hash through JSON normalization and all execution flags stayed inactive.
The reproduction helper is `docs/verification/decision-contract/audit.py`;
run `PYTHONPATH=. python docs/verification/decision-contract/audit.py` from the
deployed project directory only if a concrete new verification question requires it. Do not repeat completed checks by default.

## Production preservation and experimental alert

At07:41:32UTC there are41accepted scans,328READY coin-v5 and328READY timeframe
samples with zero errors. Active pointers and204/298coverage are unchanged,
with zero definition drift. Every old complete-payload count/digest in six
version/layer groups matches exactly at snapshot_set_id<=1308 using the same
before/after aggregation. There is still ONE independent LIVE BTC parent wave.

The ordinary07:32Watch cycle completed at07:36:07.748286UTC. It also produced
an ACCEPTED dual-CVD65 receipt with1MATCH/7NO_MATCH/0UNKNOWN. Its ZEC LONG intent
was DELIVERED at07:35:54.699869UTC, before this PR's deployment. This is the first
durable successful matching delivery observed for that newly requested rule.
It was a normal scheduled notification, not a test or manual scan. The stored
receipt survives deployment; new-process health resets its in-memory counters
to NOT_OBSERVED and must not be mistaken for missing delivery or a reason to resend.
Actual delivery is now verified; statistical performance remains unqualified.

## Next bounded step

Integrate the tested contract with persistent all-scan research samples and
explicit population-specific cohorts. Do not merely add38 to a supported count.
Freeze the observation identity, availability/entry policy, source/producer and
selection versions, UNKNOWN blocking, tied-member policy and outcome reuse first.
Keep item timeframe, Magnet cluster and Combined group identity in every relevant
partition. A source that lacks the entire decision capture must block rather than
becoming a known-empty control. READY replay must preserve immutable source and
definition hashes. Use bounded incremental processing and a separate activation
pointer so existing v1–v5 and selected-timeframe rows remain unchanged.

Outcomes can be reused only with an exact accepted coin/scan source and entry
contract. The same BTC parent wave remains the independent case across coins,
timeframes, clusters, groups and inverse formulas. Do not sum their appearances
as independent evidence or promote based on this probe. The current verified
measurement population still has one independent LIVE BTC wave.

The four event-gated predicates require separate exact event/top-item/source
liquidity linkage and causal event availability; later delivery cannot classify
an earlier price entry. Combined liquidity lists may refer to another timeframe
and are insufficient for the selected top-item amount contract.

## Ordered remaining plan

1. Research from all scans — in progress; inactive decision contract completed,
   persistent population/cohort integration next.
2. Continuous formula discovery — pending.
3. Historical archive completion — pending.
4. HYPE integration completion — pending.
5. Validation on new independent cases — pending.

The94 unsupported original definitions still comprise42covered by this mapping,
34sequence/family order,10deferred MaxPain components and8absent short MaxPain
horizons. Cluster/CVD component hypotheses, B4 and240m family normalization stay
deferred. HYPE own Spot-price features remain unavailable; its outcomes use the
Hyperliquid PERP TRADE route. Do not start these other phases as a side effect.

Continue one bounded step per user turn. Reuse GitHub and read-only Render.
No manual Watch, test notification, unsupported HYPE price request or trade.
Local requests/dotenv/psycopg remain unavailable; do not reinstall or search again.
CI supplies real dependencies and PostgreSQL. Keep durable code/evidence in GitHub.
