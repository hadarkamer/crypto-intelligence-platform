# Sheets audit recovery after an invalid transport response

## Recovered stage boundary

PRs 24, 25 and 26 bound formula/outcome publication and completed the native
Telegram backlog. Production baseline is `798304b1a88896e28b49179ab34e010aa85fb5cb`
(tree `98776253dade99f817bf73516779e6317f40544c`). The previous independent readback
at 10:05 UTC on September 13 matched all 15,968 delivered events in the frozen
14-day population ending 10:02 UTC, with no missing, duplicate, unexpected or
type-mismatched identities. This is a dated window check, not all-time coverage.

The unfinished change was the automatic auditor: a 5,500-byte HTML response
with HTTP 200 caused JSON decoding to fail and discarded earlier valid pages.
This patch completes that recovery without changing research calculations.

## Behavior

- A failed READ retains the frozen population, time window, row bounds and
  counters. No failed response is counted or certified; the same page is retried
  on the existing schedule. Authentication/protocol errors remain visible as
  AUDIT_FAILED, rather than being reported as successful or as transient HTTP.
- An explicit changed-boundary response, malformed parsed page, discontinuity,
  invalid row or finish failure discards the scan. Page counters are committed
  only after every row and boundary has been validated.
- Retained failure/reset records contain bounded HTTP metadata and cursor
  information. They exclude response bodies, URLs, queries and secrets. A later
  successful page does not erase the diagnostic evidence.
- Process restarts still start a new scan; this patch does not claim a durable
  disk checkpoint. In-process transport retries are the fault being repaired.

The receiver, delivery ACK contract, repair limit, workload schedule, formula
thresholds, BTC parent identity and Telegram dispatch remain unchanged.

## Verification

Focused tests cover HTTP-200 HTML followed by a valid page, interrupted reads,
explicit invalid bounds, invalid row atomicity, out-of-bounds page overflow,
secret-safe diagnostics and the existing queue repair rules. The repository CI
also runs every tracked self-test with PostgreSQL 18 and the Apps Script suite.
Deployment verification should record the exact active commit, observed audit
cursor/failure state, continued research cycles and a complete frozen readback.
Do not infer scan completion merely from write acknowledgments.

## Wider research continuation

The earlier September 13 analysis already produced the manual discovery map,
nine-formula 240-minute SHORT overlap analysis and a 12-hypothesis component
pilot. Its conclusion was NO_ROBUST_NEW_FORMULA_FOUND, with at most three BTC
parent movements in that pilot. Repeating that exposed sample is not new
prospective evidence. The deployed ordered-v7 acceptance contract remains the
authority; alternative thresholds in the Astra brief were not applied.

Next, quantify and bound the remaining append-growing Snapshots, live display,
MaxPain_TF and Telegram_Events destinations, preserving the canonical database
and the 14-day audit population. The 9-million-cell guard is only a rejection
limit; it is not a retention policy. Avoid discarding sheet history implicitly.

Then verify existing passive/Watch/neutral collectors and add the missing
compact, immutable model-score bundle before Watch filtering, using the already
computed seven-timeframe/two-side data and shared CVD/OI observations. Scores
below 65 must be captured without adding Telegram messages or scraper calls.
Connect this population through a distinct versioned research contract before
testing target-cluster count/spread/distance with CVD against MP65 and component
controls. Complete normalized B4/family hypotheses and freeze selected rules
before assessing new forward evidence. Preserve source-specific HYPE routes.
