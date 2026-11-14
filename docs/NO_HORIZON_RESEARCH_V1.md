# No-horizon research v1

This additive research path implements a new observation contract and a bounded
offline replay. It does not change the existing v7 tables, stored labels,
registrations, worker schedules, Telegram profile, or trade execution.

## Contract and scope

`research_no_horizon_contract.make_contract` binds candidate and version, cohort,
dataset revision, event, instrument, exact source route, direction, symmetric
threshold, decision time, reference price and BTC-parent policy. The outcome
horizon is `None`. As-of cutoffs are knowledge boundaries, not time-based exits.

This first implementation uses `FIRST_FULL_MINUTE_ACTUAL_OPEN_V1`: enter at the
minute open at or after the decision, rounding a partial minute upward. An exact
minute-boundary decision uses that minute open. The reference must match the
actual first bar open. A reference from an earlier partial-minute signal blocks
the event instead of claiming the unobserved partial minute was safe. This entry
policy is distinct from retained signal-price labels and from a policy that
always waits one full additional minute. It is not a simulation of latency or
exchange fills.

The contract requires an explicit route with exactly these fields:

```json
{"exchange":"BINANCE","market":"SPOT","instrument":"XRPUSDT","price_type":"TRADE","interval_seconds":60}
```

No route, spot/perpetual, quote, mark/trade, or HYPE product fallback is inferred.
Caller-declared source identity does not prove that the prices came from it.

`research_no_horizon_first_touch.initialize`, `advance` and `evaluate` return
JSON checkpoints. A bounded advance consumes only the suffix beginning at
`next_open_utc`. Pass a later cutoff and the unconsumed suffix to resume.

| State | Interpretation |
|---|---|
| `SUCCESS` / `FAILURE` | The first favorable/adverse barrier is established by a contiguous closed-minute prefix |
| `AMBIGUOUS` | Both barriers touched in one minute and the open does not establish order |
| `OPEN` | No decisive touch so far; inspect `progress` for current versus unfinished computation |
| `DATA_MISSING` | A required pre-touch minute is missing; supply its prefix to repair |
| `BLOCKED_ENTRY` | The declared reference does not match the actual entry open |

An opening price beyond a barrier precedes that minute's extrema. Otherwise no
intra-minute order is invented. A gap after a proved terminal prefix does not
erase it. Candle availability is the full minute end, including exchanges that
timestamp close at 59.999 seconds. No future OHLC is consumed before availability.
No-touch stays open indefinitely; processing and input budgets never become
trade timeouts. Checkpoint hashes detect alteration, not maliciously fabricated
data. The module has no authority to attest data or authorize delivery.

## Atomic research gate

`research_no_horizon_gate.evaluate_gate` receives all matched decision rows of
one frozen candidate scope. It first chooses the earliest decision and then
entry ID per BTC parent, without inspecting outcomes. An early missing outcome
cannot be replaced by a later winner. Exact duplicate rows collapse; conflicting
duplicates, unknown membership, heterogeneous scopes and invalid provenance
block qualification. Whitespace variants of parent IDs are rejected.

At least five eligible, resolved parent representatives are required for the
available probability route. The explicit new descriptive policy preserves the
previous numeric heuristic of hit rate at least 70% and Wilson 95% lower bound
at least 40%. Changing its numeric thresholds requires a new policy version.
There is no three-fresh-parent route. These numbers are not a profitability or
multiple-testing-adjusted statistical claim.

The logical contract is `N >= 5 AND (PROBABILITY OR ASYMMETRY)`. In this release,
the asymmetry route is explicitly **unavailable**: the existing fixed-window
MFE/MAE definition has not been silently converted to a no-horizon measure.
Probability alone can satisfy the gate; a legacy asymmetry result cannot.
Adding an asymmetry-only admission route requires a separately versioned metric
definition and matching source evidence. This slice therefore does not complete
all possible implementations of the requested OR gate.

The gate blocks when selected outcomes are missing, corrupt, have invalid causal
metadata, or are open but not processed through the common as-of boundary.
Legitimate caught-up open outcomes and ambiguous outcomes remain separately
reported, outside the decisive denominator. Distinct causal BTC parents remain
an operational grouping, not a proof of statistical independence. Parent context
never becomes a BTC-price condition on success itself.

`experimental_eligible` is a research diagnostic only. Every result keeps
`runtime_authorized`, `telegram_authorized`, and `trading_authorized` false.

## Offline snapshot replay

Run:

```bash
python research_no_horizon_replay.py snapshot.json --output receipt.json --batch-size 1024 --candle-budget 2000000
```

The output path must not already exist. The CLI does not connect to a DB or
exchange and does not import any production workers. Input and work limits are
explicit. A budget-exhausted or incomplete snapshot cannot qualify from a
partially evaluated set of outcomes.

Snapshot structure (replace schematic values with sourced data):

```json
{
  "snapshot_version": "no-horizon-snapshot-v1",
  "dataset_id": "immutable-dataset-revision",
  "cohort_id": "explicit-decision-population",
  "source_route": {"exchange":"BINANCE","market":"SPOT","instrument":"XRPUSDT","price_type":"TRADE","interval_seconds":60},
  "source_coverage_complete": false,
  "cutoff_utc": "2026-09-22T00:00:00Z",
  "source_receipt": {"note":"attach actual extraction and provenance here"},
  "candles": [],
  "opportunities": []
}
```

All opportunities share the single chronological price series, dataset,
cohort and exact candidate scope. Their `contract` field contains the arguments
to `make_contract`, not a prebuilt hashed contract:

```json
{
  "contract": {
    "candidate_id":"frozen-candidate", "candidate_version":"explicit-v1",
    "cohort_id":"explicit-decision-population", "dataset_id":"immutable-dataset-revision",
    "entry_id":"unique-source-event", "symbol":"XRP", "direction":"LONG",
    "decision_time_utc":"2026-09-20T00:00:00Z", "reference_price":1.4104,
    "threshold_pct":1.5,
    "source_route":{"exchange":"BINANCE","market":"SPOT","instrument":"XRPUSDT","price_type":"TRADE","interval_seconds":60},
    "parent_policy_version":"exact-source-parent-policy"
  },
  "btc_parent_movement_id":"verified-parent-id",
  "membership_status":"LIVE", "parent_evidence_eligible":true,
  "parent_start_time_utc":"2026-09-19T12:00:00Z",
  "parent_confirmed_at_utc":"2026-09-19T12:05:00Z",
  "features_observed_at_utc":"2026-09-20T00:00:00Z"
}
```

The dates and IDs in this schema example are not evidence. Membership fields
must be sourced; do not create an independent parent for every entry. `LIVE`
identifies the causal membership policy and does not turn a retrospective run
into prospective validation. Source completeness is externally attested, not
established by this tool accepting `true`. Price-only snapshots without parent
metadata can exercise label calculations but cannot qualify a formula.

Candles use `open_time_utc`, `close_time_utc`, `open`, `high`, `low`, `close`.
Duplicate times, malformed prices, nonfinite JSON, inconsistent source identities
and duplicate entry IDs are rejected. Receipt hashes bind the complete input,
contract, checkpoint and consumed price prefix. Existing receipts are preserved.

## Verification and remaining integration

The new semantic tests exercise late touches beyond 60 minutes, adverse-before-
favorable order, same-minute ambiguity, opening gaps, unavailable minutes,
cutoff extension, checkpoint corruption, deterministic representatives,
three-parent rejection, probability-only acceptance at five, stale outcome
blocking, source binding and computation budgets:

```bash
python research_no_horizon_selftest.py
python research_no_horizon_replay_selftest.py
```

External read-only verification on 2,880 archived XRP spot minutes from September
20–21, 2026 exercised 768 fixed hourly/direction/threshold cases and 8,064 oracle,
chunking, cutoff and input checks. Entry anchors were artificial and predetermined.
This is an engine check, not a formula backtest, V9 replay, profitability result,
or proof of exchange fills. The separate evidence package carries query, inputs,
script and code hashes.

The new v1 does not implement V9's ZEC SL4/TP64 dynamic-stop strategy. That
strategy has a distinct entry, exit, capacity and accounting contract and must
be replayed using its actual saved ledger. The exact V9 archive was unavailable
in this session; no missing V9 result has been inferred from the summary.

Production activation needs an additive storage/queue adapter, source-attested
candidate cohort intake, version-aware validation registration and separate
delivery binding. Do not insert the new results into legacy v7 horizon tables,
reuse old qualification receipts, or remove the old engine's quarantine. No
production migration or worker wiring is included in this change.
