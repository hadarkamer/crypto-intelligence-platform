# Finite calendars for frozen no-horizon research cohorts

The calendar layer creates a bounded sequence of declarations and registers them
up front in the existing acquisition queue. After each frozen cutoff becomes
due, the existing opted-in worker acquires and executes that cohort. There is no
new scheduling service, migration, source connection or runtime flag.

## Frozen population plan

Start with one explicit valid cohort declaration. Its source interval determines
the cadence: each next interval begins exactly at the preceding interval's end.
All intervals are half-open UTC intervals. The same exact duration shifts the
source boundaries, each transport part and the price cutoff. The observation
lag from source end to cutoff is therefore fixed. Daylight-saving transitions
do not change this UTC cadence; this is not a local wall-clock cron schedule.

The declared time, prior-outcome-knowledge flag, symbol, exact price route,
scopes, gate policy, part resource limits and version bindings stay fixed.
The declared time must be at or before the first source start. The calendar
does not infer unseen outcomes from that caller-supplied timestamp or overwrite
a true prior-outcome-knowledge flag.

Changing acquisition code changes its implementation identity. Existing requests
remain tied to their original runtime; this layer does not upgrade them in
place. Plans built with a different implementation must run with that exact
implementation or be deliberately rebuilt as new plans.

A plan has 1–32 windows and spans at most 366 days from the first start to the
last cutoff. Each window still obeys the original 31-day cohort limit and all
existing per-part and global resource caps. Each derived cohort has a
deterministic identity based on the original declaration and its ordinal.
Changing the number of windows preserves identical earlier window identities.

The retained plan contains every normalized declaration and its hash, the exact
acquisition/executor implementation closure and the calendar module hash.
Validation rebuilds the entire plan and compares it exactly before any database
write. Modified dates, missing windows, reordering, unsupported fields, changed
scopes or a different implementation cannot be silently accepted by rehashing
an incomplete plan. An intentionally different valid plan has a different
identity. Hashes bind contents; they are not signatures or evidence of when a
file was created.

## Actual registration and retry

Calendar registration uses only the explicit
`RESEARCH_NO_HORIZON_DATABASE_URL`. Migrations 056–057 must already be installed.
No source connection is opened, no migration is applied and no outcomes are
evaluated by calendar creation or registration.

Each new request is committed through the existing acquisition registration API.
Its optional `require_before_start=True` check rejects a new late registration
inside that same transaction and rolls the insertion back. It checks both the
database-generated creation timestamp and a fresh database-clock reading before
commit. An exact existing request is reusable after the start only if its
original immutable creation timestamp was at or before its source start.
An existing retrospective request cannot be promoted into an early registration.

The check records database acceptance-time evidence, not a commit-timestamp
attestation or proof that outcomes were unknown. Registration also does not
establish prospective formula validation. That requires a separate policy
binding discovery, candidate versions and eligible unseen causal parents.

Registration is atomic per window, not across the calendar. A failure stops at
that window. Earlier committed requests remain valid; retry the exact plan to
reuse them and complete the rest. No failed or late window is silently skipped,
moved forward or replaced. If a new window misses its start, this plan cannot
finish as an early-registered calendar; a deliberately new future plan is
required. If an output-file write fails after database commits, the same retry
recovers the registration receipt.

Registration does not assign a new alias to an identical existing request.
The receipt records every ordinal, request ID, declaration hash and actual
database creation time. It binds the complete plan and retains false runtime,
Telegram and trading authority. Full source proofs are not loaded to report
registration. Future requests stay in the existing WAITING queue until their
original acquisition eligibility time; the calendar does not alter that rule.

## Commands

These commands use a new explicit declaration; they do not replace the frozen
October experiment:

```bash
python research_no_horizon_calendar_cli.py build \
  --declaration new-first-cohort.json --windows 4 \
  --output new-calendar.json

python research_no_horizon_calendar_cli.py register \
  --plan new-calendar.json --output calendar-registration.json
```

Build is offline and needs no database settings. Both commands use bounded,
strict JSON inputs and create-only output files. Registration validates the
whole plan before opening the destination. Errors return exit 1 and print only
the exception type; they do not expose connection strings or raw data.
Acquisition and execution use the existing worker opt-ins described in
[NO_HORIZON_ACQUISITION_V1.md](NO_HORIZON_ACQUISITION_V1.md).

## Statistical and operational boundary

A calendar is a sequence of separately anchored populations, not one combined
cohort. A BTC parent movement may cross a calendar boundary. Neither counts,
probabilities, candidate ranks nor validation evidence are pooled across
windows. Future discovery/validation must account for those repeated parents
explicitly; nonoverlapping source times alone do not prove independence.

Cutoffs remain frozen evidence boundaries. OPEN outcomes are not turned into
losses, moved into another window or silently resumed under a different dataset.
The original first-touch rule, earliest causal-parent representative selection,
five-independent-parent gate, probability policy and unavailable asymmetry
route are unchanged.

This increment completes finite repeated population generation and durable
registration. Broad discovery/ranking, version-aware prospective validation
and result publication remain separate development work. Production activation
is separate. The October 4–18 experiment and its scheduled evaluation remain
pinned to `e8d25e57`; Hyperliquid files, providers, Telegram, LIVE and trading are
not used by this calendar implementation.
