# Compact BTC report source while loading each wave

The full-wave report loaded every selected event and its decoded engine snapshot
before compacting the complete source. Most non-HYPE snapshot bodies were then
discarded, but they had already contributed to peak allocations.

The final compact source does not require those non-HYPE bodies. Their earlier
release is an allocation improvement that can be verified with synthetic
object-lifetime tests. It is not a claim about a particular runtime's memory
usage or an incident's cause.

## Change

`load_source()` accepts an optional event projector. Its default behavior is
unchanged. The worker's projector applies the same observation cutoff and
parent eligibility rules, then performs the existing exact compaction for each
retained event. The loader releases each raw wave batch before fetching the
next one. Full non-HYPE snapshots no longer accumulate across all waves.

HYPE keeps its full original snapshot. Every retained snapshot receives the
same canonical digest, and the compact source preserves event ordering, IDs,
provenance fields and wave metadata. Source preparation and the final
late-arrival recheck both use the original repeatable-read transactions and
their original frozen observation times.

## Boundaries

- Source SQL, filters, query order, 32-parent and 5,000-event-per-parent limits
  are unchanged. The full per-wave cap is checked before projection or filtering.
- Overflow and projection errors abort the source read. No partial report or
  checkpoint is published from a failed read.
- HYPE snapshots and the complete compact source remain resident as required.
  One raw wave still materializes at a time; this is not an absolute byte bound.
- Existing checkpoint migration, acknowledgement gates, source-change checks,
  atomic publication and the compact-source publication byte limit remain active.
- Formulas, populations, schedules, runtime flags and frozen research
  registrations are unchanged. This introduces no research worker or trading path.

## Verification

Focused regression tests compare the final compact source and digests with the
legacy load-then-compact result. Ownership checks must demonstrate release of
past non-HYPE snapshots before the next wave is loaded. Failure tests cover
unfiltered source caps and discarded events; real PostgreSQL checks cover the
loader and publication contract. The full tracked self-test suite remains the
merge gate.

A successful deployment or one completed report cannot establish long-term
memory stability. RSS may include other workers, caches, driver buffers and
allocator-retained pages. Live measurements must retain process identity,
observation boundaries and missing/partial metric flags.
