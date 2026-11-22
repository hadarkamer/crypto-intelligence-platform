# Manager recovery observation and signal decision acceptance

Manager-only candidate based on `e2c7de8a529b61c6f32d76756967d126e48e1219`.
Source remains `21548c94382b5bbaaa16db7079f78162ba5f80ba`. Both live deploys were
rechecked on 2026-11-22. No production state, configuration or exchange action
was changed during this work. Specific approval is required before deployment.

## Problem and resulting behavior

An exhausted recovery round stopped the ordinary lane collector. Recovery then
required that same collector to prove healthy protections or a closed position.
WAITING now permits bounded ordinary observation of known owned OIDs from an
existing complete checkpoint. It permits no action round, CLOID discovery,
recursive history expansion, counter reset or manual-position adoption.
Uncovered account-background history cannot restart a failed WAITING history
query; cached evidence, current inventory and other accounts remain available.
Resolution requires a fresh complete lane snapshot reconciled in the same cycle.

SOL stop `612268286101` had size 3.05 and saved fills 3.05 but status OPEN.
Zero outstanding reducing quantity now retains its nonfinal local ownership
without raising the global negative-capacity error during XRP admission.
Negative outstanding quantity, ownership collisions, reduce-only, shared
exposure and capital checks are unchanged.

Cycle failures are recorded in a separate transaction after rollback, with
allowlisted codes only. Normal gate/protection/candidate scheduling waits also
receive durable reasons. Unsent occurrences receive an explicit terminal result
before archival; missing prior reasons are explicitly unknown. An uncertain or
committed attempt cannot be relabeled never submitted. Original expiry remains
unchanged, and archived November 21 signals are not replayed.

## Acceptance evidence

`hl_testnet_runtime/fixtures/sol_xrp_recovery_20261121.json` contains the frozen
product contracts, occurrence IDs, three SOL OIDs and exact saved fill/fee facts.
It excludes live account identities. Controlled venue responses in tests are
explicitly separate from historical production evidence.

| Coverage | Result |
| --- | --- |
| New full-manager PostgreSQL scenarios | 11 passed, no skips |
| Same run: existing manual-HYPE operational closure/gates | 10 passed, no skips |
| Required build checks | 66 passed |
| New focused observation, durability and shared-capacity tests | 27 passed |
| Existing archive and shared-capacity regressions | 19 + 17 passed |
| Existing PostgreSQL recovery contract | 20 passed |
| Manual closure/gates/review PostgreSQL regression after final guard | 13 passed, overlaps the 10 above |
| Selected existing runtime PostgreSQL regression | 7 passed |
| Existing recovery memory/connection regression | 24 passed |

Two historical fixtures fail identically on exact e2c7 and the candidate before
their intended assertions: `test_incomplete_hype_history_keeps_actual_exposure_but_allows_xrp`
expects a new entry for retired HYPE_ROW71205; and
`test_unproven_peer_lane_does_not_block_proven_filled_lane_protection` looks up
that nonexistent retired-HYPE trade. Neither was used as passing evidence or
fixed by re-enabling it. Active manual HYPE + independent XRP protection, and
failed SOL + XRP entry/STOP priority, are covered by passing product/PG tests.

The same exact SOL/XRP incident test fails on full e2c7 with a rolled-back
transaction and passes on this candidate. Tests cover late STOP fill, active TP,
temporary missing data, unknown send, restart, full-page history, real SQL
rollback, original-window expiry, budget wait, and unchanged manual HYPE closure.
Late terminal proof settles exact frozen fees 0.120795 once; replay/restart does
not book them again. No real exchange or Telegram traffic is used.

## Timing and load

Raw controlled reads advance the test clock by 75 ms each (20 seconds in the
slow-provider case); explicit cycle and budget waits also advance it. CPU and
wall time below are actual local measurements, not synthetic network latency.

| Measurement | Exact baseline | Candidate |
| --- | --- | --- |
| First six warm-cycle actual read counts | 5,5,4,4,3,3 | 5,5,4,4,3,3 |
| SOL/XRP incident decision cycle | Abort; 5 reads | XRP entry; 6 reads |
| Conservative attempt weight / confirmed response weight | 122 / 122 | 124 / 122 |
| Controlled incident-cycle elapsed | 375 ms | 451 ms |
| Actual incident-cycle CPU / wall | 32.672 / 65.959 ms | 64.246 / 107.503 ms |

The extra attempt is the failed SOL status read (weight 2); XRP then progresses.
The candidate additionally proves its STOP has priority on the next cycle.
Fresh-signal transient read recovery reaches entry after 5,451 ms; shared-budget
wait reaches entry after 69,357 ms, both inside the unchanged 90-second contract.
Full-page WAITING cycles perform one fills-page request plus two inventory
requests (attempt/response weight 142), with no split or new recovery round.

Eleven new PG scenarios: 59 cycles, 2.463 seconds total CPU, 5.651 seconds local
wall time, peak process RSS 40,592 KiB; cycle CPU 7.98–72.05 ms. The shared limits
remain 1200 total / 800 background weight per 69-second accounting window.
The existing 24/720 recovery read constants bound ordinary observation but are
not actual request counts or renewed durable recovery reservations. No cadence,
request limit, resources, formula, risk or leverage changed. This is scoped
isolated evidence, not a Render capacity benchmark or a multi-day leak test.

## Production SOL and deployment expectation

The latest read-only regular inventory on November 22 showed zero SOL position
and no active SOL orders. Owned order reconciliation was still stale from
November 21. Current inventory alone is not terminal proof.

After approval, deploy this exact candidate to manager service
`srv-dakptbh594qs7395460g` only. The existing owner should collect current owned
ENTRY `612268082509`, STOP `612268286101`, TP `612268333021`, fills and requests.
If full terminal/flat proof is available, ordinary reconciliation closes the
trade, resolves WAITING and settles through existing accounting. A surviving TP,
exposure, missing proof or unresolved request retains its relevant block; this
candidate does not grant an extra cancel/send round or pretend SOL is manual.

Recovery began around 08:02–08:03 Israel on November 21. The retained record
contains INQUIRY_LIMIT and exhausted application reservations, not a persisted
underlying provider error. Its original cause cannot be reconstructed reliably
from those records, and no exchange rate-limit conclusion is claimed. Durable
filtered diagnostics address this evidence gap prospectively.

Four approved formulas, Yoni-only recovery authorization, one-time $40 per
formula and 5% updated-balance risk, and manual HYPE operational closure remain
unchanged. Live closure and a future natural signal must be verified after
approved deployment; isolated sends are not presented as production trades.
