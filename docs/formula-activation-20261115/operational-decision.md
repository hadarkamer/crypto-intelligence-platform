# Operational activation decision — 2026-11-15

This change applies the user's new activation decision to the deployed source
`5b6b88e06e8290d1c0e8f5fe3302f6afac48a198` and manager
`6390aba23781af7ce43f8dc2523e7772bcaaa36c`. The accompanying
`Formula_Activation_Handoff_20261115.json` is the original supplied specification,
not an executable configuration. Earlier specifications and research are retained.

## Immediate roster

| Formula | Operational rule | New signals / entries |
|---|---|---|
| XRP R2732 | `R2732_XRP_SHORT_NY_WEEKDAYS_LOCK` | Enabled, existing conditions and management unchanged |
| SOL SFF1361 | `SFF1361_SOL_RECENT` | Enabled, existing conditions and management unchanged |
| XRP Liquidity inverse | `XRP_LIQUIDITY_RAW_INVERSE` | Enabled, existing conditions and management unchanged |
| CT009 | `CT009_FROZEN_3D` | Paused until an explicit later decision |
| OCONF | `OCONF0034_13` | Paused until an explicit later decision |
| Old HYPE | `HYPE_ROW71205_SHORT` | Retired from new opportunities and entries |
| New HYPE | `HYPE_WATCH_LIQ4050_24H_INVERSE` | Not activated by this three-formula change |

The new HYPE identity does not inherit the old formula's positions, accounting,
allocation, event history or manual choices. Its separately staged implementation
cannot hold up this narrower retirement change.

## Existing state

Historical contracts remain readable. Formula identity, rather than symbol or
account, controls retirement. Source approvals and unsent notices are retired;
the execution manager alone cancels proved owned unfilled entry quantities.
Cancellation requests or lost replies are not cancellation success. Filled
quantities retain protection and lifecycle management. The specific manual HYPE
position and its unresolved request are not adopted or resubmitted.

Ordinary market alerts and source collection stay configured. The accepted
WAITING recovery fix, notification recipients, request budgets, risk, leverage,
account routing and the three retained formulas' calculations are unchanged.

## OCONF research differences retained for review

The installed adapter selects a single candidate and resets its event after a
gap greater than 90 minutes. These are known differences requiring separate
research-conformance review. This change does not repair OCONF or authorize its
automatic reactivation after any future repair.

## Publication boundary

Only this independently tested three-formula pair is a deployment candidate.
No production deployment or state mutation is authorized by committing it.
Deployment requires the user's explicit approval of the exact tested pair and
the coordinated cutover, including cancellation of owned unfilled entry
remainders for the three suspended/retired identities. Ordinary rolling
replacement alone cannot stop the old source from issuing approvals during
overlap; the final deployment plan must explicitly address source draining.
