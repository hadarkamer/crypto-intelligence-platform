# Single-anchor multipart research cohort v1

This contract provides **one global, outcome-free matched-parent coverage
receipt** for a newly declared population carried in bounded source parts.
It addresses the ordinary export's 256-row boundary without changing that
export's limits or combining independently executed historical experiments.
The global outcome runner is not implemented by this layer.

## One declaration and one anchor

`research_no_horizon_cohort.normalize_declaration` freezes a cohort key,
declaration time, global half-open source interval, one observation cutoff,
symbol, price route, complete explicit scope list, versioned gate policy, and
ordered adjacent source intervals. The intervals must cover the global source
window exactly, including any empty intervals. Every part shares the cutoff;
the global cutoff minus source start cannot exceed 31 days.

The declaration explicitly records `prior_outcomes_observed`. This is a
historical research declaration even when that flag is false. A timestamp and
hash do not establish that historical outcomes were unseen. Source, formula,
feature, catalog, parent, gate and transport versions are frozen with the
declaration. Unknown fields and altered version or resource bindings reject.

`anchor_sql` generates **one SELECT statement** containing all bounded part
queries. The returned root includes every full child manifest: ordered source
IDs, exact UTF-8 payload hashes and lengths, literal pinned parent fragments,
price-page proofs, and cap-plus-one completeness counts. It never returns only
hashes of manifests to be reconstructed later against changing parents.

The root's actual SQL hash is attached by `seal_anchor` to the root and every
child receipt. The root binds the normalized declaration, exact ordered child
set and common statement snapshot. `validate_anchor` rejects different
snapshots, statement times, serializers, query identities, bounds or caps.
The same parent ID must have byte-identical pinned parent text throughout this
single snapshot. An absent parent remains the pinned literal `null`.

The collector must execute the exact generated statement with a read-only
transaction, preserve the SQL and unmodified raw response, and then seal it.
These checks are external provenance attestations; the pure Python validator
does not authenticate the database server. Old independent manifests are not
admitted by matching dates or MVCC strings, and sealing is not a mechanism for
certifying hand-assembled responses as database evidence.

The existing [manifest transport](NO_HORIZON_MANIFEST_TRANSPORT_V1.md) retrieves
each child's exact raw source and candle chunks. Assemble each export against
that child manifest with `assemble_export`. Prices may overlap between parts
because all source intervals share one cutoff; the repeated bytes count toward
the global budget. Source intervals never overlap. Source IDs cannot repeat.

## Bounded resources

| Resource | Limit |
|---|---:|
| Parts in the global cohort | 8 |
| Sources in one part | 256 |
| Sources across the cohort | 2,048 |
| One assembled export | 64 MiB |
| Total declared raw-input budgets | 256 MiB |
| Total actual canonical export bytes | 256 MiB |
| Source rows × declared scopes | 16,384 |
| One root anchor | 8 MiB |
| Global cutoff minus source start | 31 days |

The existing child manifest, chunk, candle and scope bounds also apply. Set
smaller explicit `source_byte_limit` values when declaring more than four
parts; eight default 64-MiB reservations exceed the global declared budget.
Each declared byte cap covers both raw manifest payload bytes and the complete
canonical assembled export. The CLI also bounds the actual input file by that
cap, so whitespace cannot bypass the total reserved input budget.
Overflow in any part rejects the entire anchor. A missing part cannot be
discarded, and a larger interval cannot be hidden behind a shorter child
window. A capacity failure requires a new valid declaration; it never permits
truncation or outcome-dependent omission of sources.

These are serialized-input and ledger bounds, not a promise of a 256-MiB peak
RAM footprint. Existing validation copies and serializes the active part.
The global reducer releases its full source payload before requesting the next
part and retains only manifests, compact source decisions and receipts.

## Global coverage reduction

`research_no_horizon_cohort_coverage.preflight_cohort(declaration, anchor,
load_part)` calls `load_part(ordinal)` exactly once for each declared part.
The loaded object must be a complete ordinary export bound to that exact child
manifest. Missing, malformed, duplicate or foreign input raises before a
successful global receipt can be returned. No caller-supplied coverage or
outcome receipt is trusted as input evidence.

Every captured source is freshly validated and every declared predicate is
evaluated through the existing three-valued evaluator. All decisions survive
in the global per-scope ledger, including unknown sources, known nonmatches,
unverified early matches and contradictory parent evidence. Each record has
its `part_ordinal`, part-local `ordinal`, and consecutive `global_ordinal`.

For each exact scope, the reducer groups **all** verified matching records by
`btc_parent_movement_id`, checks conflicts across the complete group, and
selects the earliest decision then lexical entry ID globally. It does not sum
per-part parent counts or combine local representative lists. Two parts with
three parent groups each and one shared parent contain five global groups.

Per-part `INSUFFICIENT` is not a global blocker. An unknown potentially matching
source or unverified matched parent in any part blocks that scope globally,
even if other parts contain five valid groups. A blocked scope has a null
`matched_parent_upper_bound` and no selected representatives. Counts and
representatives are never pooled across formulas, directions or thresholds.

`POSSIBLE` means only that the complete matched-parent group count reaches the
declared policy minimum. Resolved earliest representatives cannot outnumber
these groups. The receipt does not establish statistical independence, entry
availability, successful outcomes, probability, asymmetry or qualification.
The shared atomic gate is not executed.

The global receipt identifies one declaration and one anchor, lists every
declared scope and part, and has a deterministic content hash. Its
`ready_for_outcome_research` flag means all scopes have possible parent coverage,
as in the single-export feasibility API. It separately states
`outcome_runner_available=false`, `local_experiment_store_compatible=false`
and `coverage_only=true`. There are zero executed outcome trials and no
runtime, Telegram or trading authority. The trial denominator for a future
global runner is the declared scope count, never scope count times part count.

## Local command workflow

The CLI reads explicit files, creates new output files exclusively and never
opens a database, SQLite store or network connection:

```bash
python research_no_horizon_cohort_cli.py anchor-sql \
  --declaration cohort.json --output anchor.sql

# Execute that one SQL statement through the approved read-only collector,
# preserving its raw JSON response in raw-anchor.json.
python research_no_horizon_cohort_cli.py seal-anchor \
  --declaration cohort.json --anchor raw-anchor.json --output anchor.json

# Fetch and assemble each child using the existing exact-proof transport.
python research_no_horizon_cohort_cli.py coverage \
  --declaration cohort.json --anchor anchor.json \
  --part part-0.json --part part-1.json --output coverage.json
```

Repeat `--part` in declared ordinal order, including empty parts. Coverage exits
0 when every scope is `POSSIBLE`, 2 when the complete receipt contains blocked
or insufficient scopes, and 1 for malformed input or file errors. Valid
diagnostic receipts are saved before exit 2. Existing files are never replaced.

An unnormalized declaration may omit the default gate policy, version bindings,
resource budget and individual part caps. Required fields are:

```json
{
  "cohort_version": "no-horizon-single-anchor-multipart-cohort-v1",
  "cohort_key": "example-retrospective-cohort",
  "declared_at_utc": "2026-10-02T12:00:00+00:00",
  "prior_outcomes_observed": true,
  "symbol": "BTC",
  "price_route": "BINANCE_SPOT_TRADE_1M",
  "source_start_utc": "2026-09-20T00:00:00+00:00",
  "source_end_utc": "2026-09-28T00:00:00+00:00",
  "cutoff_utc": "2026-10-02T00:00:00+00:00",
  "scopes": [{"candidate_key": "FUTURES_CVD_TOTAL_65", "base_direction": "SHORT", "threshold_pct": 0.25}],
  "parts": [
    {"ordinal": 0, "source_start_utc": "2026-09-20T00:00:00+00:00", "source_end_utc": "2026-09-24T00:00:00+00:00"},
    {"ordinal": 1, "source_start_utc": "2026-09-24T00:00:00+00:00", "source_end_utc": "2026-09-28T00:00:00+00:00"}
  ]
}
```

The example is documentation, not an executed or approved research declaration.
It does not claim that those parts fit their caps or contain sufficient parents.

## Outcome execution layer

The existing [local experiment store](NO_HORIZON_EXPERIMENT_PLAN_V1.md) remains a
single-export runner. Do not stitch a greater-than-256-row export into it or
submit parts as separate experiments and combine their gates. The separate
[global outcome coordinator](NO_HORIZON_GLOBAL_COHORT_OUTCOMES_V1.md) now freezes
this root identity, persists global representatives and the complete scope
denominator, and reuses the existing local first-touch engine for bounded,
resumable global-scope jobs under the same cutoff and unchanged evidence gate.
The coverage API itself remains outcome-free; its receipt flags describe that
layer. Earlier research outcomes remain historical attempts, not additional
observations silently added to this cohort.
