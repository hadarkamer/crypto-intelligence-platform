# Eight-coin foundation: immutable work identity

Status: first standalone candidate component, offline tested only. Not wired
into the runtime, application, queue, scheduler, database or AI client.

The user supplied a provisional model handoff on 2026-10-31 while Model 3
preparation is being improved separately. This component can be built without
changing capture behavior. The production branch was read and still points to
`f6adfc6fea05bfd82a59098433ad1c2f31f6267f`; no new production health or
deployment inspection was performed for this offline work.

## Implemented scope

- `standalone_collector/work_identity.py` defines an immutable command scoped
  by tenant, authorized source identity, batch, coin, heatmap model and timeframe.
- An explicit list of one to eight coins expands to nine commands per coin.
  No market list is assumed and no source availability is inferred.
- Repeated/reordered construction under the same batch produces the same work
  identifiers. A new batch, source scope or tenant produces different ones.
- The reviewed configuration snapshot has its own SHA-256 digest. Changing it
  within the same logical job is a conflict, not a second job identifier.
- A separate analysis binding includes the complete command and digests of the
  submitted image and source evidence. Equal images in different jobs are not
  considered the same analysis.
- Records reject missing/extra fields, unsupported model/timeframe, malformed
  identities/digests and tampered identifiers. No credentials belong in them.

Scope and batch identifiers must come from authenticated server state, not
untrusted page text or arbitrary client UUIDs. The batch creation endpoint must
persist a stable operation/schedule occurrence identity and enforce it in the
database. This module does not implement that endpoint.

Work IDs are identifiers, not authorization tokens. Database uniqueness must
also compare the complete logical identity. Command/evidence digest checks are
not a replacement for visual identity, source readiness, row completeness,
price checks or signature/authentication of a returned result.

An analysis key is not authorization to invoke AI. Database ownership and
durable phase state must gate that call. Different evidence in the same job
must never silently authorize another paid request. A lost provider response
must be reconciled or remain indeterminate, not automatically retried.

## Offline verification

Run from `standalone_collector`:

```
python3 -m unittest test_work_identity -v
```

17 tests passed. They cover 72-command coverage, deterministic identity across
reorder/restart/new interpreter, separate identity dimensions, configuration
conflicts, immutable commands, parallel command construction, malformed input,
serialization and evidence binding. TESTA–TESTH are synthetic symbols.

This is component verification (part of layer A). It does **not** prove
database deduplication, concurrent claiming, crash-safe phases, real source
isolation, AI call counts, full layer B, live coin support or capacity.
The module imports only Python standard-library computation/validation tools,
and `prepare.py` does not copy or import it. Existing source files are unchanged.

## Updated plan from the handoff

Preserve each model's capture adapter. A Model 3 reference-price envelope must
not become a fictitious candle Close. Do not normalize AI effort, wait limits,
capture sizes or source thresholds merely for consistency.

The six-hour cadence preference is recorded. The handoff's existing active
window is 06:00–00:00 Israel, with midnight excluded; it is not authorization
to add overnight scans. Cadence change, missing scheduler events and Model 3
rendering improvement remain separate changes owned by Chat 1.

Model 3's short rendering experiment suggests lower CPU may be achievable;
it does not establish final scan time or concurrency. Reassess capacity after
that fix. No compute size, instance count, quota or API key has been changed.
The $100 GitHub budget is not an expansion budget for other services.

## Next integration gate

1. Reconcile with Chat 1's final tested revision and designate the owner of
   shared queue/storage code before editing it.
2. Confirm the eight real symbols and validate model availability; do not copy
   symbols from trading-bot formulas.
3. Integrate identities into the existing queue using database uniqueness and
   atomic ownership, with stale-attempt write protection.
4. Persist source evidence before AI and analysis before application delivery;
   adapt existing merge/version checks without removing manual-edit protection.
5. Run fault cases with the real isolated database/runtime, then the entire
   application flow (layer B). Use recorded provider responses where suitable
   and label them. Live capacity checks require their bounded test scope.

No merge, deployment, database migration, source scan or provider call was
performed. The candidate can be revised or removed before integration.
