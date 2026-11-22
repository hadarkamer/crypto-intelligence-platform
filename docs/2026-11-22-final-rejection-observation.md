# Final no-order rejection during WAITING

Manager-only follow-up to `f960377247db58225e4a2d056be892711bb4cdb6`.
This candidate preserves that commit's recovery observation, cross-market
isolation and durable decision fixes. It has not been deployed.

Preflight on November 22 verified that the manager remains at
`e2c7de8a529b61c6f32d76756967d126e48e1219` and source/Telegram remains at
`21548c94382b5bbaaa16db7079f78162ba5f80ba`. The f960 branch head and clean local
tree matched. The new isolated branch does not replace either live branch.

## Fixed behavior

After `_rejections` has proved that an action created no order, it atomically
persists the original normalized rejection reply, an OBSERVED request with
REJECTED_NO_ORDER/no OID, and a matching rejection event. The earlier WAITING
collector still demanded an OID, while snapshot normalization could reject the
same request as changed terminal ownership.

The provider and snapshot normalizer now use one shared predicate in the
existing evidence module. It requires the persisted request identity, exact
terminal status, normalized rejection code, matching trade/account/symbol,
matching product adjudication event and valid original attempt/event times.
A known order with the rejected CLOID or a contradictory supplied lookup is
still rejected. No new certificate, schema, runtime transition or state rewrite
was introduced; the existing atomic adjudication is the durable proof.

The rejected action contributes no phantom OID and needs no further CLOID lookup.
Real existing orders and the current position still require complete current
evidence. A rejected modification preserves the original STOP and its original
owner, including after that STOP fills. A missing OID, raw rejection, bare
terminal flag or unresolved outcome cannot substitute for adjudication.

Product changes are limited to:

- `hl_testnet_runtime/experimental_execution_evidence.py`
- `hl_testnet_runtime/experimental_live_provider.py`

## Acceptance

| Tests | Result |
| --- | --- |
| Two new full product PostgreSQL scenarios | Both passed |
| Existing PostgreSQL recovery/isolation scenarios | 11 passed |
| Existing PostgreSQL manual-HYPE closure/gates | 10 passed |
| Focused WAITING/evidence/provider/runtime rejection tests | 28 passed |
| Shared-capacity/isolation and durable cycle regressions | 36 passed |
| Required build checks | 66 passed |

All final runs had zero failures, errors and skips. The same final two new test
cases were also run on exact f960: the positive case reproduces the original
OPEN/RECOVERY_EXTRA_HISTORY_INQUIRY_REQUIRES_CHOICE failure; its negative case
correctly remains blocked. Historical results from f960 were not rewritten.

The positive PostgreSQL case uses the actual authenticated intake, provider,
runtime, `_rejections`, journal and dispatch path, with controlled raw venue,
HTTP and signing boundaries. No terminal flag or adjudication event is seeded
by the test. Its sequence is:

1. SOL entry fills 3.05; the STOP is observed.
2. TP creation receives a raw rejection and initially remains OUTCOME_UNKNOWN.
3. The next product cycle obtains the exact no-order evidence, adjudicates the
   rejection through `_rejections`, and enters WAITING.
4. A normal observation while the position is still open remains WAITING.
5. After restart and the full STOP fill, complete current observation produces
   CLOSED and RESOLVED/POSITION_CLOSED.

Only three wire attempts exist: ENTRY, STOP, and the rejected TP. After WAITING,
there are zero additional sends and zero CLOID inquiries, including restarts.
No trader choice is recorded; round, read allocations, send counters and the
original deadline are unchanged. The rejected request remains byte-for-byte
unchanged. Exact frozen fees 0.120795 and the entire accounting state remain
unchanged after repeated observations and restart.

The negative PostgreSQL case leaves the raw rejection unadjudicated. After the
STOP fills and the process restarts, the request remains OUTCOME_UNKNOWN, the
recovery remains WAITING, and no settlement, send or extra inquiry is permitted.
The updated named unit test now expects observation to continue for an actual
product adjudication; separate tests preserve the unadjudicated and contradictory
proof guards.

Reproducible test names, product/test SHA-256 hashes, PostgreSQL run metadata and
controlled measurements are in `docs/final-rejection-observation-20261122/`.
The 23-case PostgreSQL run took 6.565 seconds. These are isolated measurements,
not a production capacity or live trade claim.

## Deployment boundary

Specific approval is required for the new exact candidate. Deploy manager
service `srv-dakptbh594qs7395460g` only, then verify the active SHA and single
execution owner, natural SOL reconciliation, other positions' protection and
service resources. No synthetic production signal, order or failure is needed.

Four approved formulas, budgets, the one-time $40 allocation and 5% updated
balance risk, original signal windows, exchange quotas, Yoni's choices, old
trade management and manual-HYPE operational closure are unchanged. Active
orders, exposure or unresolved current requests remain real local blockers.
