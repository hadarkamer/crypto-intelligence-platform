# Canonical inverse analysis v1

Only matched inverse candidates queue an original delivered LIVE alert. The
adapter freezes its exact source identity, analysis direction, entry time,
reference price and captured fields. A derived research-only decision sample
flips the original analysis direction exactly once, including alerts whose
display direction was already inverted by MaxPain or Combined. It is never a
delivered Telegram alert or a new BTC-parent evidence unit.

Valid source contracts are strict, deterministic JSON and retain their source
hash. The actual derived event ID satisfies the canonical v7 foreign key;
outcomes are recalculated from the canonical closed Spot 1m path for every
threshold and horizon. Matching, period boundaries and BTC membership retain
the original event ID. Common-window metrics use the derived ID and remap to
that original research entry.

Invalid source records produce an immutable `REJECTED_IMMUTABLE_SOURCE` audit
with original ID, fingerprint and reason. Nonfinite values are preserved as
explicit `invalid_nonfinite_number` string tags, never replaced with zero or
usable market values. Naive source datetimes remain explicitly recorded without
inventing a timezone. Rejected requests do not materialize events or outcomes,
and one rejected input does not prevent another valid source being queued.
Changing a rejected source later cannot silently re-admit it under the same
inverse request/version. A valid queued source changing also fails its frozen
source-contract check.

The queue is bounded and claimed with token/lease ownership. Claims and event
materialization commit before price fetches. Native terminal v7 labels remain
immutable; OPEN or missing paths retry. All changes remain research-only.
