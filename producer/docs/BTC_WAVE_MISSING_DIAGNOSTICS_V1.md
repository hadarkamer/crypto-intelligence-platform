# Existing full-wave record diagnostics

`research_btc_wave_missing_diagnostics.py` reads a local JSON export and writes a
separate explanation. It imports no engine, worker, database driver or network
client. It does not fetch candles, repair data, recompute outcomes, alter an
entry, replace a selected representative, publish a report or enqueue work.

Run with the repository Python environment:

```sh
python -B research_btc_wave_missing_diagnostics.py --input existing-records.json --output missing-diagnostics.json
python -B research_btc_wave_missing_diagnostics_selftest.py
```

The input is an object containing `generation_id` (UUID string) and `records`
(the existing report records, unchanged). For example:

```json
{
  "generation_id": "a347edfb-f296-458f-9a3d-844f3bb02ef9",
  "records": [{
    "method_version": "btc-wave-endpoint-closed-1m-v1",
    "source_event_id": 141769,
    "btc_parent_movement_id": "993ce44640aded6ecd91e0d4646c2b851f0db8031d364d3cf51640bb2a529766",
    "source_scope": "CANONICAL_SPOT_IMMUTABLE_ALERT",
    "coin_scope": "SOL",
    "symbol": "SOL",
    "direction": "SHORT",
    "status": "DATA_MISSING",
    "original_alert_time_utc": "2026-09-23T11:35:02.968731+00:00",
    "wave_end_utc": "2026-09-23T11:35:59.999000+00:00",
    "observed_at_utc": "2026-10-16T12:00:00+00:00",
    "observation_closed": true,
    "missing_reasons": ["ValueError: NO_CLOSED_POST_ENTRY_CANDLES"]
  }]
}
```

This abbreviated example illustrates the input shape; real exports should retain
the original records, including their other fields. The example observation time
is illustrative, not a claim about a production generation. Explicit timezones
are required. Mixed records must retain `component_source_scope`. Unknown method
or entry contracts, contradictory boundaries, duplicate JSON keys and non-finite
values cause a nonzero exit (2), with the offending record index where available.
Malformed input is never silently skipped or converted to a successful diagnosis.
The output file must be new; the tool refuses to overwrite existing evidence or
diagnostics. Omit `--output` to print JSON to stdout.

## Meaning and limits

The tool groups explanations by event, parent and component source. Every missing
input row remains separately identifiable by its original array index and exact
source/coin scope. Duplicate rows remain visible. This grouping does not merge
calculation rows or count ALL, individual coins, canonical and mixed results as
independent observations. Nonmissing rows are validated and counted as input but
do not create missing-diagnostic entries.

| Diagnostic | What the supplied record establishes |
| --- | --- |
| `CLOSED_WAVE_HAS_NO_FULL_POST_ENTRY_MINUTE` | The first admissible full candle closes after the frozen closed-wave endpoint. |
| `AWAITING_FIRST_COMPLETE_POST_ENTRY_MINUTE` | An open observation's frozen boundary does not yet admit a full post-entry minute. |
| `INSUFFICIENT_BOUNDARY_EVIDENCE` | An active record lacks the verified BTC observation boundary needed to calculate an exact window. |
| `DERIVED_ENTRY_UNAVAILABLE_IN_REPORT` | The report contains the exact derived-entry finite-value error in a positive window. This does not establish the cause or whether another stored measurement has a valid entry. |
| `SOURCE_PATH_REQUIRES_REVIEW` | The original symptom remains; timing alone does not establish a source-data gap or another root cause. |

For active records without their BTC boundary, the exact cutoff/count stay null;
a wall-clock upper bound is clearly separate. A zero upper bound can establish
that no full candle is observable yet. A closed flag alone cannot establish a
permanently zero window: at the endpoint's final millisecond, the native latest
closed cutoff can still precede the endpoint. Permanent zero is checked against
the wave endpoint itself, not merely the current observation cutoff.

The CLI's `input_file_sha256` identifies the exact local input bytes only. It is
not a verified production report/source digest, generation-membership proof or
Sheet acknowledgment. The tool cannot establish archive coverage, reconstruct
missing raw candles from a summary/hash, or determine whether other tables hold
a valid derived entry. It does not read any other records or storage system.

## Why the original missing result remains

The existing `btc-wave-endpoint-closed-1m-v1` contract selects representatives
before testing outcomes or price availability (`select_representatives` in
`research_btc_wave_endpoint_report.py`). `_first_open` and `fetch_full_path`
exclude a Spot candle that overlaps pre-alert time and require the whole candle
to close within the endpoint. HYPE's separate MARK/PERP entry is the following
minute's open, including when the alert itself lies exactly on a minute boundary.

For the SOL example the first full candle would open at 11:36:00 and close at
11:36:59.999, after the wave ends at 11:35:59.999. Backfilling prices cannot add an
eligible candle to that interval. Including the 11:35 candle would introduce
pre-alert prices; extending the endpoint or replacing the representative would
change the research contract. The event therefore stays selected and missing,
and the existing aggregate completeness gates and null metrics remain intact.

No report fields or hashes, source evidence, reuse decisions, refresh schedule,
method version, outbox or acknowledgments change. Existing `verified_closed_cache`
continues to reuse only complete READY closed paths with verified unchanged
source. The native zero-window path already stops before requesting prices.
Run this diagnostic on the existing export rather than launching recomputations
or backfill merely to explain that structural case. Other missing records retain
their existing refresh behavior; this tool creates no permanent negative cache.
