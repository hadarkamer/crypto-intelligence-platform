# CoinGlass capacity preparation — 27 November 2026

Preparation candidate, not a production release. The accepted collector is
`e2b91677a016a7fe5dcc721d41b7ebd288b250f8`; the accepted app is
`17279e3018e74b18f17408d44bf7c464e1f35835`. Both remain the comparison and recovery baseline.

## Scope and authority

One existing Tesseract reader, Coin mode, eight symbols (BTC, ETH, SOL, XRP,
HYPE, BNB, DOGE, ZEC), models 1–3, and 12H/24H/48H/1W: 96 identities.
The target is one cycle every six hours, completed and displayed in 15–20 minutes.
No live source collection, production deploy, production migration/configuration
change, resource upgrade, enrichment, or OpenAI call was performed in preparation.
The branch itself is not a deployment instruction.

## Changes

- Propagate symbol/model/range through the same base reader. Preserve observed
  source labels, price precision, axis spacing and column duration. A complete
  source menu is required to classify a combination as unavailable; page errors
  do not prove unavailability.
- Remove repeated checks of already selected controls; recheck after a click.
  Stop redundant pixel sampling once sufficient distinct colors are observed.
  Retain all ten screenshots: they serve chart/control evidence and closing
  consistency. Retain opening/closing DOM checks and durability readback.
- One Firefox/context per continuous queue drain, with configurable
  `COINGLASS_CYCLE_TABS` (default 1; range 1–8). Free lanes accept the next job.
  Emptying and subsequently refilling the queue can start another browser;
  exactly one browser for every six-hour cycle is not yet demonstrated.
- Reuse the existing account lease, per-job ownership, queue and database
  connection. Child readers have no database connection. Parent writes are
  serialized. A shared account gate coordinates renewal and source cooldown.
- Save independent source receipts before publication. Recover a saved receipt
  after a lost acknowledgment without recapture. Fence stale ownership and
  results. A database failure before durable receipt creation cannot guarantee
  recovery across loss of the process; do not describe volatile state as durable.
- Keep optional hover/measure enrichment outside `base_only`; preserve unknown
  amounts as null and never infer money from color or double-count observations.

## Validation evidence

The generated runtime was assembled separately using the pinned dependencies;
12 affected modules imported. The build helper intentionally omits unchanged
test batches; it is not a substitute for the separate test results below.

| Suite | Evidence |
| --- | --- |
| Exact generated runtime, affected combined suites | 33 passed, zero skips. Includes real Firefox against local HTML: two pages in one context, different identities, page reuse and independent closure. No CoinGlass request. |
| Job ledger and HTTP entry | 19 passed against private, temporary PostgreSQL 16.2; includes 96 identities, ownership fencing, checkpoint resume and independent completion. |
| Reader/source reconstruction, retained prior run | 87 passed, one Chromium-only DOM test skipped. Real accepted screenshot reproduced reference 61,680, 13 ticks, 14 levels and 3 regions, one Tesseract call. |
| App affected suites, retained prior run | 121 passed. |
| App final focused rerun | 54 passed and TypeScript no-emit check passed. |
| App SQL, retained isolated run | 96 independent writes and 96 receipt replays; weekly inheritance, field identity and quality guards passed. |

Suites overlap; these counts must not be summed into one coverage total.
Synthetic 96-map coverage is not live source availability or capacity evidence.
Only the accepted BTC/1/12H combination has live end-to-end acceptance.

## Edge cases and remaining limits

Locally exercised: mixed identities and changed closing observations, duplicate
jobs, competing owners, a stalled lane, cancellation cleanup, worker EOF after
receipt, one renewal for concurrent anonymous tabs, persisted/observed cooldown,
OCR process deadline/reaping, unavailable weekly range versus incomplete menu,
decimal prices, stale result fencing and database receipt replay.

Real CoinGlass cross-tab setting interference, rate limits, weekly label/value,
all source combinations, CPU contention and peak memory are unmeasured. A real
OOM kill was not injected. Browser/process cleanup and timeout tests do not prove
survival of every host failure. Concurrent UI waits overlap; Python image/number
processing in the child event loop remains synchronous. Do not infer linear
throughput from the number of tabs.

## App destinations and integration

The read-only mapping found three BTC model fields with three ranges each:
9 destinations. A complete target needs 24 model fields with four ranges each:
96 destinations. Missing: 21 fields and 87 range destinations, including weekly
BTC. DOGE means Dogecoin; ZEC means Zcash. Generic dropdown aliases DOG/ZCASH
alone are not evidence of an existing collection target.

The app candidate includes migration 0059 and an inactive template preparation
script; these have only run in isolated PostgreSQL. Existing names and BTC field
IDs are retained. The currently observed 06:00–00:00 activity window omits early
morning; four daily cycles require a reviewed CoinGlass schedule change.

The candidate is based on accepted app code. Comparison with app main 8cc8007d
showed 18 text conflicts across seven of nine shared changed files and one
deleted file. Migrations 0054–0058 are occupied, including X work. Integration
with current main and applied migration state is required before any production
release. No X quota, permit, account setting or database pool size was changed.

## Proposed bounded live experiment — separate approval required

Same four identities in each stage: BTC/1/12H, DOGE/2/24H, XRP/3/48H,
ETH/3/1W. First one tab, then two; four only if two shows useful overlap without
quality loss, source limits or memory pressure. Four jobs per stage; at most 12
jobs/target navigations planned. Redirects and SPA redraws are measured separately
and may add load; stop escalation on excess. No warmup or retries. No 6–8 tabs
or full 96 run. Use stored session; stop on login requirement.

Proposed collector: one isolated temporary Render 1 CPU / 2 GB instance, at most
30 minutes including setup/cleanup, total additional ceiling $0.02. At $25 per
30-day month, 30 minutes costs approximately $0.01736. Build, storage and egress
must fit existing allowances or the remaining ceiling; otherwise do not launch.
The production collector, scheduler, X workers and production worksheets are
not experimental targets. Use the disposable signed-in app/PostgreSQL harness
already used for baseline acceptance. Confirm the local app-to-collector test
transport and database isolation before any source navigation; if an additional
paid service or production credentials become necessary, revise the plan first.

Allow up to six minutes setup, six per stage, and six cleanup/readback. A stage
overrun cancels later stages. Record all individual map times, source identity,
rendered refresh evidence, partial coverage, failures, CPU seconds/average/peak,
memory average/peak, browser/context/tab counts, screenshots and bytes, OCR,
login submissions, network requests/bytes, DB operations/connections/waits and
cost. Source-group counters must not be summed once per tab. Four maps do not
support a reliable p95. Successful maps/minute includes persistence and refresh.

At the accepted cost of 40.125 CPU-seconds/map, 96 maps require 3,852 CPU-seconds:
3.21 effective cores for 20 minutes or 4.28 for 15. This is conditional arithmetic,
not a capacity forecast; reuse may change per-map cost. The proposed 1-core
experiment diagnoses overlap and bottlenecks, not proves the 96-map target.

## Next decisions

Approve or reject only the bounded experiment above. Subsequently review actual
capacity and source availability before resource changes, 6–8 tabs, a full 96-map
cycle, production schema/template/schedule changes or deployment. The eventual
eight aggregate charts and cluster merge thresholds remain outside this change.
