# CoinGlass Model 1: reuse the existing collector

**Status: code integration, not deployed or live-verified.** Branch `integration/coinglass-app-bridge-20260914` is based on `ai-lab-capabilities`. Never merge the entire old lab branch into trading `main`; first identify the actual candidate service.

## One collection path

Decision Hub button -> authenticated app backend -> Model 1 jobs API -> existing `capture_heatmaps` -> existing `analyze_heatmap_images` (OpenAI, once) -> durable PostgreSQL result -> app's existing atomic worksheet commit.

GET/poll/save retries do not recapture or re-analyze. No Firecrawl, Gemini, or Telegram relay is used on this path. No hourly source schedule is enabled.

The existing files `market_vision/coinglass_heatmap_capture.py` and `market_vision/openai_heatmap_scanner.py`, including their source authentication behavior, are UNCHANGED. The new adapter calls their existing public signatures. Source access, browser availability, chart correctness, and current OpenAI configuration still require a real runtime test. The scanner currently does not return usage; `usage: null` is honest, not proof of zero cost.

Only 12H and 24H are supported for automatic collection. The legacy selector maps unsupported horizons to 24H, so do not offer automatic48H. Manual48H remains available. Visual concentration is not a verified dollar amount or price prediction. The adapter requires high confidence in current price, excludes low-confidence/null zones, rejects invalid sides/ranges, and maps very_strong/strong -> many, medium -> normal, weak -> few.

## API contract v1

Every route requires `Authorization: Bearer <dedicated bridge token>`. This credential is NOT the OpenAI key; keep it only in both backends' secret stores. No client-side direct connection or arbitrary source URL is accepted.

- POST `/api/collection/model1/jobs`: exact JSON `{request_id: UUID, timeframe: '12H'|'24H'}`. 202 queued/running, 200 ready/failed. Same request ID retains the same job within seven-day history; different timeframe with same request gives409. Other request IDs reuse active jobs or ready captures at most10min old. Budget errors return429.
- GET `/api/collection/model1/jobs/{job_id}`: stored status/result only; never initiates work. Missing404.
- GET `/api/collection/model1/jobs/{job_id}/evidence`: authenticated PNG, available for six hours.

Envelope: `schema_version: coinglass-model1.v1`, `job_id`, `timeframe`, `status: queued|running|ready|failed`, `result`, `error`.

Ready result: `schema_version`, `run_id` (same as job_id), fixed `source_url`, `symbol: BTC`, `heatmap_model: 1`, timeframe, actual `captured_at`, `source_updated_at: null`, `provider: OpenAI`, actual model, usage object or null, observed_price, zones (side/price_low/price_high/intensity), evidence (sha256/artifact_id equal to job_id/content_type), summary, `quality: visual_estimate`.

The application validates this contract and preserves the original capture timestamp. It signs a short-lived ticket bound to user/workspace/sheet/field/original version, then polls the SAME job. A failed save retains the stored result reference; retry must not start a paid collection again.

## Runtime configuration — not performed by committing

First confirm the Render workspace, identify the candidate service and verify its currently deployed branch and configuration. Do not replace production main with this older branch.

Collector keeps existing DATABASE_URL, OPENAI_API_KEY/model, source access and Chromium. Add only after deployment review:

- `COLLECTION_BRIDGE_ENABLED=true` (default disabled)
- `COINGLASS_COLLECTOR_TOKEN`: new dedicated random bridge credential, >=32 bytes, provisioned securely to both backends
- optional `COLLECTION_BRIDGE_HOURLY_LIMIT=4` (default; maximum20)

App server needs `COINGLASS_COLLECTOR_URL`, the actual verified HTTPS origin, and that same dedicated `COINGLASS_COLLECTOR_TOKEN`. No OpenAI key or browser session is copied into the app. Do not paste credentials into chat or Git. No credentials were created or changed in this code-only integration.

Enabled startup creates ONLY the two isolated tables `ai_collection_bridge_jobs` and `ai_collection_bridge_requests`. It does not change trading/research tables. The queue worker only scans after an authenticated job POST. Ready results are stored before app writes; interrupted/failed attempts are not automatically replayed. Child processing deadline300s is a time/call bound, not an exact monetary/token cap. Image retention6h, result/request retention7days; cleanup runs on new POSTs.

## Actual testing status

`python -m unittest discover -s tests -p test_collection_bridge.py -v`

Local execution: **20 tests passed, 7 PostgreSQL integration tests skipped** because no isolated TEST_DATABASE_URL was available. Compilation of the new Python modules passed. Tests use a local HTTP server plus synthetic source/model fixtures, never CoinGlass/OpenAI or production DB.

PostgreSQL tests are provided but have not run. They must use a disposable database via TEST_DATABASE_URL, never the production DATABASE_URL. The attempted CI workflow was not created; do not report CI as running or passing.

## Before publication

1. Complete isolated database tests and application tests; report any unresolved failures.
2. Deploy disabled to the confirmed candidate service, preserving its existing behavior.
3. Configure bridge secrets securely; run ONE authorized12H source/analysis test and inspect stored evidence.
4. Verify the app save and reload from an existing authorized editor account. Compare unaffected horizons/fields and ensure retry uses GET only.
5. Only then recommend frontend publication. Hourly scheduling remains separate.

Code tests do not resolve the previously observed app-workspace membership issue. Do not widen permissions or impersonate an owner to force a test to pass.
