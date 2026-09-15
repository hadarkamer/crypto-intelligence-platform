# Same-image price detail test — 2026-09-15

## Change actually deployed

Render service: decision-hub-model1-collector (srv-dakhacou01pc73f58aug).
Code: 530db21495a3e6283d904f7165829d9a81d38faa.
Deploy dep-dakpfn1594qs7393h66g finished live at 2026-09-15T19:10:53.842132Z.

The app-owned collector still captures exactly one selected horizon. It records
the rendered chart rectangle, saves the full PNG, closes the browser, then crops
the rightmost chart area including recent candles and the price axis. The crop
is enlarged 2x with nearest-neighbour pixel replication. Full PNG and crop are
provided in ONE existing vision request, for ONE timeframe/result.

The full PNG is unchanged. Crop provenance includes hashes, rectangle, scale
and dimensions. Code verifies matching source/crop hashes before model input.
No external market-price feed, OCR, synthetic price labels or generated image
content is used. The numeric normalize function is AST-compared before/after
the adaptation and must remain identical, including high price-confidence gate.

Build logs: 63 collector/image tests + 28 existing source/privacy tests + 19
configuration tests passed (110 total). The build did not start a source scan.

## One real 48H test

Request ID: 3db1acf0-d422-4d27-af64-0b2eff22d6cd.
Job ID: 6004a080-d50d-4f06-a534-880fd3ec1eae.
Started: 2026-09-15T19:12:10.950093Z.
Finished: 2026-09-15T19:14:07.683563Z (116.73 seconds).
Final database status: failed; error_code: price_uncertain; result absent.

Sanitized Render failure log at 2026-09-15T19:14:07.673246687Z:
- stage: validation
- observed_timeframe: 48h
- readable: true
- blocking_condition: none
- current_price_confidence: low
- input_tokens: 8261; output_tokens: 696; total_tokens: 8957

The app's existing validateCollectorEnvelope accepted this as a FAILED 48H job,
not a successful numeric result. No worksheet writes or automatic retries took
place. Lovable reported 1.6 credits for test orchestration. No publishing,
hourly schedule, secrets or permissions changes were performed in this step.

## Conclusion and limitation

The 2x crop did NOT resolve the low price-confidence rejection. This is not an
end-to-end success. Source readability is a model assessment, not independent
human verification of prices. The failed-job temporary directory is removed by
the existing worker; the failed full PNG/crop were not retained in PostgreSQL,
so do not claim a later human pixel review of this particular image. Future
investigation should preserve failed validation evidence privately rather than
repeat paid scans without retaining the evidence. Existing high-confidence,
source-identity and numeric side/range guards remain enabled.
