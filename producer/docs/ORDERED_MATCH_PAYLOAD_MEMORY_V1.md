# Ordered match payload allocation

`ingest_matches` builds all decision-time features for an event before checking
the candidate catalog. Previously every matching candidate independently
serialized the same feature mapping and retained a separate JSON string in
its match tuple until the batch was written.

The mapping is now serialized lazily on the first match and the immutable
string is reused by the remaining matches for that event. The next event gets
its own serialization; an event with no matches creates no match-payload
string. There is no persistent cache or new lifetime beyond the existing pass.

All candidates are still evaluated. Match order, normal/inverse direction,
source event identity, decision time, entry fields and canonical payload bytes
remain unchanged. Database statements, screens, source selection, checkpoints,
scope discovery, formulas and publication policy are unchanged.

For one event with M matches, retained match-payload strings change from M to
one (or zero when M is zero). This is an allocation property, not a prediction
of process RSS. Driver buffers, source history, other workers and allocator
retention still contribute to memory. The earlier service OOM is not thereby
attributed or proven resolved.

`research_ordered_match_payload_selftest.py` exercises the actual ingestion
path on synthetic events, with the real candidate matcher. It compares full
written tuples with the prior per-candidate serialization behavior, covers
normal/inverse candidates and multiple events, and checks that shared payloads
cannot cross event boundaries. The full repository CI additionally exercises
existing PostgreSQL, sequence-history, screening and worker behavior.
