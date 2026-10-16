# Full-history BTC wave reports in durable parent parts

The recovered October 14 generation contains 31 parents. The current population
contains 33, so the legacy 32-parent in-memory report cannot create a new full
headline. Raising the limit would move the same memory and payload boundary.

## Storage and calculation

`btc-wave-parts-v1` is a storage/provenance contract, not a new research method.
Each generation freezes its observation cutoff and complete ordered parent
population. Parent source, progress, and completed records are persisted in
separate parts. Empty and ineligible parents remain in the source manifest.

The native representative selector and single-parent calculation remain in use.
Selected HYPE events retain their complete original engine snapshot. Other source
events retain their fields, price provenance, and original snapshot digest.
The existing 5,000-event source guard and a 16 MiB source/report part guard remain
active. A part that exceeds a guard blocks publication; it is never truncated.

Closed-path reuse requires unchanged source identity and verified causal
successor evidence. Active and incomplete records are evaluated again. Each
worker pass shares the existing eight-path and 30-second calculation budget;
completed cached parts may advance without consuming additional price paths.

## Complete aggregation and provenance

Aggregation consumes persisted records in the original parent-ID order. It sums
counts and excursions from records, never averages per-parent probabilities or
asymmetry ratios. Missing closed representatives continue to block both final
aggregates. Active results remain explicitly provisional. Every complete record
must retain the original eight threshold outcomes.

The root report contains bounded summaries and versioned references to the
generation's source/record parts. Sheet provenance fields contain these references
instead of repeating an ever-growing all-history ID array in every threshold
cell. The complete event IDs, parent IDs, snapshots, and records remain in the
parts. The source manifest digest has an explicit new format; it is not the old
monolithic JSON digest. Formula eligibility and trading behavior are unchanged.

## Publication and recovery

Staging may span transactions and restarts. Before publication, the worker
rechecks the complete ordered population and every source part in one
repeatable-read transaction. Only this common snapshot can certify the staged
generation. Independent checks performed at different times are insufficient.
Source changes prevent publication and preserve the frozen observation cutoff
when staging restarts.

Root report, source manifest, generation status, and Sheet outbox rows commit
together. The next generation waits for exact acknowledgments of the preceding
report digest. A successful deployment or queued rows alone are not delivery
evidence.

Final verification has bounded Python source memory, but its total work grows
with retained history. This change does not claim constant verification time,
constant database storage, or a complete solution to process memory usage.
Retained generation parts require a separate storage-capacity review; evidence
is not silently pruned by this change.

## Rollout

1. Require exact-head CI, including actual PostgreSQL tests for more than 32
   parents, restart, source revisions, atomic rollback, and exact delivery gates.
2. Apply only `057_btc_wave_report_parts.sql` through the existing explicit
   schema-admin startup mechanism. Use `FORMULA_SCHEMA_APPLY=1` and
   `FORMULA_SCHEMA_APPLY_ONLY=057_btc_wave_report_parts.sql`; disable schema
   application after verified completion. Do not apply unrelated migrations.
3. Verify the deployed commit, migration readiness, frozen generation progress,
   parent/source counts, report publication, and exact Sheet acknowledgments.
4. Keep the separate Hyperliquid service, formulas, experimental registrations,
   and trading configuration unchanged.

Any rollback worker must understand both active parts pointers and published
source/report part references. Finishing a generation removes the active
checkpoint hazard but does not restore compatibility with an older binary.
Prepare a compatible rollback; never delete checkpoints or published evidence
to make an older binary start.
