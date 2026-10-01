# Manifest-attested bounded source transport

This transport addresses a source-export connection that cannot return the
whole historical corpus in one response. It reconstructs the same projected
dataset committed by one read-only SQL statement, using later bounded reads.
It changes no candidate predicate, entry rule, first-touch label or gate policy.

## Snapshot and transport claims

The anchor manifest must be produced by one ordinary read-only SELECT. Its
source population includes every accepted intake in the declared half-open
window, with a cap-plus-one sentinel. Its source leaf hashes cover the complete
original projected rows, including missing LEFT JOIN values. Its candle-page
hashes cover ordered arrays of the exact declared archive route and symbol.

Each leaf records the SHA-256 and UTF-8 byte length of PostgreSQL's exact
`jsonb::text` payload. The manifest also records source identities, page bounds,
counts, truncation flags, limits, query identity, extraction time and MVCC
snapshot. Full captured score bundles remain necessary for capture validation.

Later fetches use the pinned source IDs and candle-page ranges. They are separate
read-only transactions. The assembler hashes the untouched payload strings
locally before strict JSON parsing. Missing, duplicate, extra, oversized or
changed leaves block the entire export. Existing saved leaves can be reused
only after verifying them again against the same manifest. A changed leaf
cannot be accepted by replacing its hash in the old manifest.

The explicit receipt mode is `MANIFEST_ATTESTED_MULTI_READ_V1`. It is distinct
from a single-statement payload export and a repeatable-read connection. A
complete reconstruction establishes equality to the manifest's projected
snapshot, assuming the trusted database/connector and SHA-256. It does not
establish that the fetch transactions shared a database snapshot, authenticate
an exchange, or prove that intake classification happened before the price
cutoff. New rows after the anchor belong to a later source revision.

## Local validation and retained proof

The assembled export embeds the small anchor manifest and a canonical binding
of its parsed source payload. The source adapter validates that binding before
running its existing capture, parent-membership and candidate checks. The raw
SQL result, exact SQL, raw leaf strings and fetch receipts are retained outside
the export as the independently replayable transport proof. The canonical
binding detects later payload changes; it is not a digital signature and cannot
replace the original byte-level transport proof.

Raw PostgreSQL text hashes and canonical parsed-export hashes have different
roles. Re-serializing parsed JSON is never a substitute for checking the raw
transport bytes. Changes to database serialization settings or historical rows
can legitimately invalidate a later fetch; that is a blocked reconstruction,
not permission to normalize mismatching raw bytes into agreement.

Source, candle, manifest and final-export size limits still apply. The final
export, including its receipt, must also satisfy the frozen experiment's total
admission budget. This transport never authorizes dropping non-matches, splitting
one experiment to evade its bounds, changing the declared research population,
or connecting research results to Telegram or trading.

Historical plans bind their implementation. A source-transport implementation
change therefore requires a new pre-outcome plan declaration that references
the previous declaration, preserves its cohort/scopes, and names the new exact
commit. It must not rewrite the original declaration or imply the new code was
used in an earlier run.
