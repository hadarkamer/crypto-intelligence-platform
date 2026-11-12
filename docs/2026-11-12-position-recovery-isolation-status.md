# Position recovery and isolation — partial implementation, not releasable

## Authority and baseline

The attached team instruction authorizes isolated implementation and testing,
not merge, deployment, production configuration, or live trading. It explicitly
requires approval before establishing a new test environment. No production
state, settings, exchange orders, or Telegram messages were changed in this work.

Render's latest live deployment was read on 2026-11-12: receiver
`srv-dakptbh594qs7395460g`, deployment `dep-dbje9l7lot8c7385ibf0`, commit
`debc658b2b26405999b0029658bce2f4492c67f8`. The isolated branch starts at that
exact commit: `fix/position-recovery-isolation-20261112`.

## Active-path map

| Responsibility | Existing location | Finding / required follow-up |
| --- | --- | --- |
| Source intake and single running loop | `hl_testnet_runtime/experimental_live_service.py`, `experimental_live_startup.py` | Authenticated intake persists before waking the existing worker. Reuse this execution owner. |
| Durable order attempts / dispatch | `experimental_live_runtime.py`, `experimental_live_state.py`, `experimental_live_dispatch.py` | Unknown outcomes remain durable; explicit unsent certificates are separate. Do not retry an unknown send. |
| Native exit placement | `experimental_execution_runtime.py::_maintain_candidate`, `_maintain_doge`; `experimental_live_runtime.py::_maintain_live` | Existing SL/TP creation, amendments, split DOGE exits and approved emergency paths must share the future position-level decision. Do not add another sender. |
| Collection and entry isolation | `experimental_live_provider.py::collect`, `experimental_live_runtime.py::_cycle_proposal`, `experimental_external_activity.py::review_inventory` | Unresolved requests suppress candidates at account scope in more than one layer. Lane collection failures can escalate to account blockers; inventory and final dispatch checks also need coordinated changes. Not corrected in this partial patch. |
| Filled-order mismatch | `card_sync_evidence.py::observe` | Full-fill validation remains unchanged. Original quantity, observed fills and current exposure must not be made artificially equal. |
| Accounting | `experimental_live_runtime.py::_accounting_maintenance`, `formula_accounting.py::funding_task`, `_closed`, `record_funding`, `settle` | Global early returns prevented independent final trades from collecting funding. This is the one implemented correction. |
| Manual ownership | `experimental_external_activity.py::managed`, `apply`, `closure_verified` | Existing durable handoff records and position observations must remain authoritative, including HYPE's operator decision. This patch does not install a live handoff or infer one from silence. |
| Telegram | Source service's `trade_telegram.py` on its separate main branch; receiver `trade_report_store.py` | Existing private allowlist, report commands and callbacks are read-only. The approved recovery outcomes and choices still need implementation and connection to saved position state. |
| Existing tests | `test_experimental_live_service.py`, `test_experimental_live_runtime.py`, `test_formula_accounting.py`; source `trade_telegram_selftest.py` | Simulated exchange and durable test ports exist. Telegram tests use mocked calls; they are not an interactive UI acceptance environment. |

## Implemented correction

Removed two global early-return conditions from `_accounting_maintenance`:
an unresolved request anywhere, and a missing original stop on any active trade.
The existing cycle still attempts actionable protection before background
accounting. `funding_task` still requires the selected trade to be closed,
independently reconciled, attributable, and supplied with actual fee facts.
Its own unresolved requests still prevent the funding read. Funding remains
subject to the unchanged shared background request budget, page bounds, retry
delay and trade-digest check at commit.

No replacement handler, scheduler, endpoint, table, capital policy or strategy
was added. The two new regression tests are in `test_formula_accounting.py`.

## Verification performed

1. Before changes: the existing accounting, cross-blocker acceptance and stop
   recovery suites ran 59 tests: 49 passed, 10 PostgreSQL tests skipped because
   `HL_JOURNAL_CI_URL` was not configured.
2. Added an independent closed-DOGE / unresolved-HYPE regression and a negative
   test for the closed trade's own unresolved request. The independent test
   failed against the original implementation, reproducing the global stall.
3. After the correction, the same suites plus the regressions ran 61 tests:
   51 passed, the same 10 PostgreSQL tests skipped.

Command:

```sh
python -m unittest -q \
  hl_testnet_runtime.test_formula_accounting \
  hl_testnet_runtime.test_experimental_blocker_acceptance \
  hl_testnet_runtime.test_stop_completion_recovery
```

In the new isolated regression the pending independent settlement changes
from zero funding calls (stalled) to one call (settled); the next maintenance
cycle adds zero calls. The unrelated HYPE request is byte-for-byte unchanged.
This is a mock-call assertion, not a measured exchange weight comparison of
the complete requested feature. No complete performance claim is made.

## Outstanding — do not merge or deploy

- Position-scoped admission isolation across collection, planning, inventory
  and final transport validation.
- The saved recovery round, per-exit two-extra-send / 60-second limits, bounded
  inquiry count and weight, and restart-safe enforcement.
- Automatic/manual choices, authorization, stale/duplicate callback rejection,
  notification delivery state and outcome messages.
- Full partial-fill / lost-reply / restart / persistence-failure matrix.
- Complete before/after request and weight comparison.
- Real PostgreSQL verification and interactive UI acceptance.

The existing repository tests do not supply a Telegram test bot/chat and
interactive isolated application path. Before establishing that environment,
the user must identify an existing suitable test environment or approve a
separate test bot/chat connected only to the simulated exchange and test state.
The active trading chat must not receive test messages. Directly calling a
callback with mocks will not be reported as UI acceptance.

The full instruction is therefore NOT implemented or accepted. This branch
is a reviewable partial fix only. No deployment or rollback procedure for the
full feature is approved; preparing one depends on the completed, tested state
and notification changes. Any later rollout must preserve manual management,
request identities, counters and original signal expiry, and must not replay
missed signals or erase unknown outcomes.
