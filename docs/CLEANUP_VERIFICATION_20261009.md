# Manual activity and card-read cleanup — 9 October 2026

Status: PASSED. The exact candidate completed isolated Render verification on
9 October 2026 at 12:36:45 UTC. It has not been deployed to the live Testnet bot.
Activation on the live Testnet bot requires the user's separate approval.

## Exact candidate

- Live baseline: `f6992f81320e3e6ba003c75c03412b5ae69233ac`.
- GitHub candidate: `93c7c6005dca34f4da10459f508434833e579ef1`.
- Candidate branch: `codex/cleanup-card-snapshot-20261009`.
- Local equivalent commit: `af6a677e2d8d1b1beabc8003db843fd63df80f75`.
- Identical tested tree: `337b2421d25e11d01da2d5f33d5b594630771495`.
- Isolated Render commit: `501a292533d0a2c77b7280f50c0aca0615fb7e7f`.
- Source/dependency manifest: `a30049b717823412cd32f0928b10eab3d21547681d30aedbd39cfb4f19afd794`, covering 532 files.
- Existing isolated static-build service: `srv-db2dv53ncjis73ec9lng`.
- Corrected isolated build: `dep-db4dqdq177jc73fe09o0`.

This document is a verification follow-up. It is outside the source/dependency
manifest and changes no tested Python or dependency file.

## Included changes

1. Retain the previously prepared external/manual-activity correction. External
   origin alone does not block an entire account. An occupied external coin is
   excluded from new bot entries. Intervention in an active bot coin hands that
   coin to human management; the bot does not restore, resize or cancel the
   human's orders. Other coins remain subject to the existing safety gates.
2. Preserve actual exchange quantity and observed executions separately from
   attributable bot fills. Do not invent manual PnL allocation to a formula.
3. Require two distinct complete flat observations, no remaining coin orders
   and no unresolved bot request before releasing human management. The review
   found and fixed an older queued alert reopening the coin after release. A
   new signal must be strictly newer than this account/coin's release cutoff.
   Approved signals use immutable `approved_at`; legacy plans use immutable
   `created_at`. Repeated delivery cannot renew an old decision.
4. `trade_card()` reads active state, archive and source updates in one checked,
   read-only snapshot. PostgreSQL uses an independent repeatable-read connection,
   without the trading advisory lock or `FOR UPDATE`. Writer and archival paths
   retain their existing locks. Reads do not write a journal or query an exchange.
5. Reuse a complete already-fetched split-history tree for manual observation.
   The existing history validator runs against a cache-only reader. Missing,
   malformed or saturated incomplete pages cannot prove completeness. No network
   fallback occurs inside the cache lookup; the existing optional recovery path
   remains responsible for genuinely missing data.

No source formulas, prices, risk sizing, quota constants, account routing,
WebSocket fill-ingestion behavior, Telegram connection or outbound IP settings
were changed by this package.

## Local verification

The initial candidate's full suite was run in a sanitized environment with the existing Python
socket/native PostgreSQL/subprocess guard and synthetic exchange fixtures.

- 2,989 discovered tests, zero failures and errors.
- 563 PostgreSQL cases skipped because no local disposable server was supplied;
  those skips are not counted as verified database behavior.
- Zero attempted external network calls.
- Source/dependency hashes unchanged throughout the suite.
- New tests cover committed archival during a held read, a held writer during
  card reading, source-update consistency, database-enforced read-only access,
  journal immutability, checksums, connection isolation and failure redaction.
- Manual regressions cover partial intervention, restart, flat release, archive,
  later reentry, rejection of stale alerts, account/coin scope, and an intervention
  between entry preparation and final dispatch.
- Eight cache regressions cover complete pagination, missing children,
  wrong account/window/verification pass, malformed pages, saturated leaves,
  filtered superset intervals and detached return values.

## Isolated PostgreSQL verification

The first full run (`dep-db4dlj5g1s2s7393kk20`) executed 2,989 tests with zero
skips and errors, and one assertion failure. The old PostgreSQL successor
fixture rounded its approval timestamp down to the current minute, before the
manual-release cutoff, while expecting entry. The new gate correctly rejected
that stale signal. The fixture now first requires rejection after archive and
restart, then advances to the next closed minute and requires a genuinely new
independent alert to enter. Runtime source was not changed to satisfy the test.
The corrected fixture passed local import/focused checks (48 tests, one database
case skipped), followed by the successful full isolated run below.

All other cases in that first run passed, including the new PostgreSQL snapshot
and concurrency tests. Final verification is the corrected full run below.

The corrected build (`dep-db4dqdq177jc73fe09o0`) passed against the exact
532-file manifest above, with a fresh loopback PostgreSQL 18 database and a
synthetic exchange only. Its final result was recorded at 12:36:45 UTC.

- Full suite: 2,989 tests, zero failures, errors, skips or unexpected successes;
  successful completion with exit code zero.
- Timing suite: 28 tests, zero failures, errors or skips.
- Recorder and feed checks passed; 120 exact feed-state parity comparisons.
- Runtime checks passed; all 80 trace-equality comparisons matched.
- Durable database checks completed; the disposable PostgreSQL server was
  stopped successfully at the end of verification.
- No live exchange, trading account or existing database was accessed. Timing
  on the free isolated instance is not evidence of live latency or capacity.

The only change between the first and final Render candidates was the test
fixture correction and its source-manifest update. Trading runtime source was
identical in both runs.

## Synthetic request measurements

These are controlled scenarios, not live latency, rate-limit capacity or a
four-account load test. The original budget constants are unchanged.

| Scenario | Live-baseline reads / weight | Candidate reads / weight |
| --- | ---: | ---: |
| Reused entry preparation at 5 seconds | 3 / 42 | 3 / 42 |
| Protected position over 60 seconds | 14 / 174 | 14 / 174 |
| Two empty accounts over 60 seconds | 4 / 44 | 6 / 84 |
| Due activity hint on otherwise inactive account | 3 / 43 | 6 / 85 |

The empty-account increases belong to the already prepared manual-activity
history recovery. Complete cached intervals add no request. Optional recovery
defers to protection, uncertain submissions and entry work.

For a separate 500-fill pagination scenario, the prepared manual patch made
three initial history reads and three duplicate reads: six total. This cleanup
uses the three initial responses and adds zero reads: three total, with all
500 fills preserved. This does not imply a 50% reduction in total bot traffic.

## Scope and remaining limits

- Direct consumption of WebSocket fill payloads is a proposal, not part of this
  implementation. The recommended follow-up is one shared fill reducer for
  WebSocket and REST inputs, using the existing atomic state transaction and
  durable duplicate checks. Complete-history checkpoints must advance only on
  a completed history scan, not on the most recently received fill.
- Current external activity records are bounded to 512 recent events and 4,096
  recent fills. They are not a permanent complete manual-trading archive.
- An unfilled order opened and canceled entirely between snapshots may not be
  recorded. Current position alone cannot reveal a prior partial sale.
- PostgreSQL is the live Testnet backend. SQLite's existing journal mode is
  preserved; default rollback mode can still make a writer commit wait for a
  reader. SQLite concurrent-commit tests explicitly use a disposable WAL fixture.
- `trade_card()` is not yet connected to the existing Telegram buttons; that
  integration remains deferred.
- No live account, existing account database or exchange endpoint was used for
  these tests. No production branch or live service configuration was changed.

## Authorization boundary

The initial direct Git push was rejected by automatic approval review for
possible private-source publication. Read-only checks confirmed the repository
is already public and the delta contains source/test fixtures only. A reviewed
retry encountered missing CLI credentials; publication then succeeded through
the authenticated GitHub connector to test branches only.

Updating the isolated manifest environment pin automatically started its build.
An additional manual trigger also queued a second identical isolated build.
The duplicate uses the original source and no live-account configuration. The
corrected rerun was triggered once, automatically by its new manifest pin; no
extra manual trigger was sent. This duplication is not part of the trading runtime or its scheduling.
