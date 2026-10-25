Automatic descriptive evaluation of the existing forward paper study.

The evaluator runs inside the existing bot after research schema readiness.
It does not create alerts,orders,positions or source observations.
The prospective paper policy and its source/engine hashes remain unchanged.

The v3 worker reads the authoritative study snapshot, bounded quote windows,
complete selected source evidence and retained positive formula evidence
in one repeatable-read read-only transaction, then writes only its separate additive
research_forward_paper_evaluations table. A separate advisory lock allows
one evaluation writer during rolling deployments.

Selected source evidence contains every bootstrap or nonempty candidate
packet through the exact snapshot journal cutoff. SQL counts include all
candidate and quote inputs,including omitted empty candidate packets.
Each selected packet receives its true global input ordinal,number of prior
rejections,successful observation sequence,and next-input sequence boundary.
Sparse bootstrap,choice,source-drift and input-rejection decisions are read
from that same snapshot. Counts prove that selected evidence was not silently
truncated. A history containing whole rejected inputs remains pending source
review; it is never quietly approved by omitting those inputs.

The dataset fingerprint excludes heartbeat clocks and changing quote marks.
It retains policy identity,eligible-event records,choices,closed positions,
and relevant active/reservation/uncertainty state. An unchanged successful
assessment is reused; no assessment is added for each quote poll.
Source freshness is checked independently on every evaluation poll.
Lightweight source coverage counts are also checked before cache lookup.
New nonempty/incomplete source packets or sparse source decisions invalidate
the meaningful source marker even before an event becomes eligible. Growing
empty-poll totals do not themselves create a new assessment. Journal/input
integrity booleans remain part of that marker.

Before every successful cache lookup, the worker re-reads full retained
candidate definitions and recorded positive match feature maps, together with
original source event direction, symbol and alert time. Requested identities
come from all known first_seen keys, including bootstrap and incomplete events.
LEFT joins preserve missing definitions, matches or original events as explicit
missing evidence. Retained JSON, version/hash and source metadata are hashed
in full; changes to non-condition fields also invalidate the previous cache.
Only read time and journal cutoff are excluded from this predicate marker.
Mutable feature-screen tables are not consulted.

The first successful v3 assessment also pins a canonical digest for every
retained positive record and its full frozen definition. Later assessments
compare existing records with the latest verified successful v3 assessment
for the same policy, including across implementation fixes. New positive
keys are allowed. A failed assessment never replaces the successful pin
baseline. A changed existing record remains a failure on repeated polls;
restoring the original evidence permits recovery. This prospective comparison
starts with the first v3 capture and cannot prove earlier historical immutability.
An exact older cached PASS is reused only when it preserves every newer
successful pin and its current source cutoff is not reversed. A rollback
failure receives a separate stable context identity tied to the latest PASS
digest; the older successful record remains immutable. Health and stored
assessment provenance expose the base input digest and this pin reference.

PASS means the frozen source-eligibility gates,first raw/eligible receipts,
bootstrap exclusions,immutable parent choices,supplied snapshot calculations
and observed price paths of closed cases agree. It also recomputes the frozen
formula predicates for the complete set of retained recorded positives against
their current captured feature maps and original source identity. This does
not reconstruct raw market inputs,prove complete negative opportunities,
establish historical immutability of retained rows,approve a strategy,
replay the complete quote stream,or prove exchange execution. Book rows reuse
economic cases; BTC parent waves are counted separately. Open and incomplete
waves are not counted as fully comparable completed waves.

The baseline.json pins the original study ID,policy/config hashes and start.
Its original runtime commit is provenance; the running bot may later deploy
unrelated changes while preserving the frozen study.
Evaluator version and a combined normalized hash of the evaluator,source-audit,
predicate-audit and worker modules identify each calculation. The dataset cache
key also pins the critical baseline fields,meaningful source-selection marker
and current retained predicate evidence marker. Component
hashes are exposed separately in sanitized health provenance. Failed/pending
evidence never becomes a quiet successful cache. Missing source,quote or retained
predicate evidence is retried without inventing data. An assessment PASS is immutable;
a previous FAIL may only be upgraded to PASS for the same identity.

Operational bounds are 50,000 quote packets, 256 closed case windows, 5,000 selected
source packets, 20,000 candidate rows and 10,000 sparse source decisions.
An additional 2,000 retained predicate rows and 20 MB of retained predicate evidence are allowed.
A server-side size preflight rejects oversized JSON before transferring it.
A shared 20-second evidence-fetch budget and database/network timeouts bound the work.
Exceeding a cap reports EVIDENCE_LIMIT without truncation or approval. These
are runtime resource limits,not scientific sample-size approval thresholds.

Health key:forward_evaluation. Log prefix:[FORWARD_EVALUATION].
The complete assessment is stored in research_forward_paper_evaluations.
No automatic winner,statistical-approval threshold or trading promotion
is defined. Continue collecting new waves under the original protocol.

CI runs research_forward_evaluation_selftest.py and
research_forward_evaluation_worker_selftest.py and
research_forward_source_audit_selftest.py and
research_forward_predicate_audit_selftest.py under the existing convention.
