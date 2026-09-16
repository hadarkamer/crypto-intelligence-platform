# Current-edge extraction: offline validation — 16 September 2026

## Status
The isolated implementation is in this directory. Executable code snapshot:
`1d2447617e7c7efdb871ea4bcc2c7d8ba4eb132a`. It is NOT imported by the live collector
and was NOT deployed. No worksheet data, credentials, permissions or quotas were
changed. No new CoinGlass capture or OpenAI inference was requested.

## Original evidence
Existing original PNGs were retrieved from the collector's authenticated evidence
endpoint after temporary files were lost in a sandbox reset. These are NOT later
user screenshots or new source scans.

- 12H: job be03f916-8dc5-41f5-b7e4-09f026933316, captured 2026-09-16T10:23:42.890618Z;
  SHA256 3c6bc765fe70d0f2d6e7dc7ff4bf7bd709020e16737c0563206e38a859b979a2.
- 48H: job 09345a5c-d0cb-41d4-b539-d947ad0a5306, captured 2026-09-16T10:29:03.284165Z;
  SHA256 62a8d03a140fab6e92c574b759916b370db5d33bf95378493e1f32575bfad966.

Both images are 1600x2733. Source hashes and axis-label crops were verified.
Per-image fixture rectangles (half-open): plot [371,575,1495,1174], colour bar
[315,597,345,1157]. These are fixture parameters, NOT extraction code constants.
12H anchors (y,price): (578.5,79000),(668.5,78000),(759.5,77000),(1121.5,73000).
48H anchors: (610.5,82000),(728.5,80000),(845.5,78000),(1080.5,74000).
The previously supplied current-price intervals [75930,75990] and [75850,76000]
were retained for side classification; this replay does not independently prove
their accuracy.

## Implementation
- Require colour support in the final strip, preceding strip and terminal columns.
- Calibrate against the image's OWN colour bar; unsupported palettes fail closed.
- Fit pixel-y to price with at least three spanning, monotonic, consistent ticks.
- Separate each connected envelope from its brighter core; never bridge dark gaps.
  Core threshold: at least 80% of local peak, and normalized legend level >=0.40.
- Categories use explicit initial thresholds: many >=0.80, normal >=0.55, few
  >=0.40. These are engineering rules, not calibrated financial quantities.
- Measure lower-level presence separately at 0.10, so weak-but-current is not
  mistaken for absent/historical.
- Convert pixel bounds to price, round outward to $50, keep pixel evidence,
  unrounded bounds, source hash and calibration residual. No model-invented zones.
- Return candidate cores, not production writes. Overlapping rounded labels are
  flagged rather than silently merged.

## Actual results
30 synthetic tests passed locally and in Lovable (exit 0). Original-image replay
passed 6/6 regression checks (exit 0), including unchanged source hashes. The
replay plus sensitivity checks took about six CPU seconds.

Strong core ranges measured in the SAME historical images:
- 12H: 76550-76700 above, 75050-75200 below.
- 48H: 77600-77750, 77750-77900, 78300-78450 above; 74650-74750 below.
Strong core outputs were identical with 12,20,28-pixel strips.

Critical comparisons for 48H:
- Old 75550-75750: current presence 0, significant support 0; not a current candidate.
- Old 75000-75250: presence 0.875, significant support 0; weak-but-present, NOT absent.
- Old 74400-74650: presence 0.7059, significant support 0; weak support exists,
  but the strong yellow core is higher, approximately 74650-74750.
- Previously missed bright area near 78300 recovered as 78300-78450.

All these ranges are pixel-derived estimates of the chosen bright-core definition,
NOT CoinGlass tooltip values, new market data or replacements saved to the app.
Module SHA256 verified in the final replay:
`bb2df3203b27f3e2cffe3e6a0a1eab8660386a835a388e424117897815634433`.

## Scope and remaining integration
This tests TWO stored Model 1 images. It does not prove accuracy on all images,
themes, Models 2/3 or all price-axis layouts. Tick/geometry fixtures were manually
verified for these exact PNGs. Automatic per-new-image axis/geometry verification
and connection to the existing saving contract remain before live activation.
Never reuse these fixture coordinates/prices on a new screenshot. Do not rewrite
historical app values from this replay. No fresh source, inference or sheet write
was needed for these tests; Lovable usage for the stage was 8.3 credits.

Run synthetic tests: `python -m unittest -q test_edge_zones test_presence_metrics`.
Replay: `python replay_saved.py <verified-fixtures.json> <private-output-dir>`.
Fixtures require per-timeframe image_path, plot, legend and anchors, as described
above. Original images and credentials are deliberately not committed.
