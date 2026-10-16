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

HYPE representatives keep their full original snapshots because their derived
price-entry contract hashes the complete original event. Other HYPE events now
retain the same price provenance and original snapshot digest as other coins.
The existing selector chooses representatives from the frozen source population
before outcomes or price availability are consulted. No event row is dropped.
Every retained snapshot receives the same canonical digest, and the compact source preserves event ordering, IDs,
provenance fields and wave metadata. Source preparation and the final
late-arrival recheck both use the original repeatable-read transactions and
their original frozen observation times.

## Boundaries

- Source SQL, filters, query order, 32-parent and 5,000-event-per-parent limits
  are unchanged. The full per-wave cap is checked before projection or filtering.
- Overflow and projection errors abort the source read. No partial report or
  checkpoint is published from a failed read.
- Selected HYPE snapshots and the complete compact source remain resident as required.
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

## Existing checkpoint migration

Checkpoints that already contain snapshot digests can still retain all HYPE
snapshot bodies. They must receive the same deterministic projection as a fresh
source read. Migration changes only source representation and its representation
digest. It preserves observation time, selected pending IDs, progress index and
computed records. Repeating migration has no further effect. Full representative
snapshots and derived-entry source digests remain unchanged.

The final source recheck uses the same projection and the original frozen cutoff.
A changed nonrepresentative snapshot still changes its retained snapshot digest,
and therefore changes the source digest. Late earlier events still trigger the
existing source revision path. The 16 MiB publication guard and atomic report /
Sheet-outbox transaction remain in force.

## Separate historical growth limit

The report still admits at most 32 parent movements. Recovering an older frozen
checkpoint does not make a newer population above that limit publishable. No
parents may be silently removed and no partial population may produce a complete
headline. A durable follow-up requires parent-level checkpointing and a complete
source manifest, preserving whole-population aggregation and provenance without
retaining every parent source graph in one process or JSON value.
