# CoinGlass Model 1: reuse the existing collector

Status: code integration, NOT a deployed/live-verified connection. This branch is based on ai-lab-capabilities, NOT on the production trading main branch. Do not merge the entire old lab branch into production. Identify the actual existing candidate service before deployment.

## Flow

Decision Hub authenticated button -> app backend -> this API -> existing capture_heatmaps -> existing analyze_heatmap_images (OpenAI, once) -> durable PostgreSQL result -> app's existing atomic worksheet commit. GET/poll/save retries do not recapture or re-analyze. No Firecrawl or Gemini on this route. No Telegram messages are used to transfer results. No hourly source schedule is enabled.

`market_vision/coinglass_heatmap_capture.py`, `market_vision/openai_heatmap_scanner.py`, and their authentication behavior are UNCHANGED. The adapter uses their existing public signatures. The original collector can still fail if the source/session/browser is unavailable; local code tests are not evidence of live access. OpenAI remains configured on the collector, not copied into the app. The old scanner does not expose usage in its return value, so usage may correctly be null.

Only 12H and 24H are exposed: the existing legacy selector maps unsupported horizons to 24H. The app must not label that as 48H. Numeric results are visual estimates, not verified monetary totals or price predictions.

## API

All routes require `Authorization: Bearer <dedicated bridge token>`. The token is NOT the OpenAI key and must exist only in server secrets. No client-side CORS/proxy or arbitrary-source input is supported.

- POST `/api/collection/model1/jobs`: JSON with exactly `request_id` (UUID) and `timeframe` (`12H`/`24H`). Returns 202 for queued/running, 200 for ready/failed. Same request ID always refers to the same job within the seven-day retained history. A request ID reused with a different timeframe gives 409. Separate requests can reuse the same active job or a successful capture <=10 minutes old. Quota failures return 429.
- GET `/api/collection/model1/jobs/{job_id}`: reads persisted status/result only. Never starts a capture. Returns 404 if unavailable.
- GET `/api/collection/model1/jobs/{job_id}/evidence`: authenticated PNG evidence, retained for six hours. No screenshot secret URL is exposed.

Envelope: `schema_version: coinglass-model1.v1`, `job_id`, `timeframe`, `status: queued|running|ready|failed`, `result`, `error`.

Ready result: `schema_version`, `run_id`, fixed `source_url`, `symbol: BTC`, `heatmap_model: 1`, `timeframe`, actual `captured_at`, `source_updated_at: null`, `provider: OpenAI`, actual `model`, `usage` or null, `observed_price`, `zones` (side/price_low/price_high/intensity), `evidence` (sha256/artifact_id/content_type), `summary`, `quality: visual_estimate`.

Intensity mapping is deterministic: very_strong/strong -> many, medium -> normal, weak -> few. Low-confidence/null bounds are not invented. Invalid ranges, sides or current-price confidence fail the job. The app must revalidate before saving and preserve the original timestamp.

## Deployment (not performed by committing)

First confirm the Render workspace and identify the candidate service actually running these modules. Never replace production `main.py` with this older branch. The small registration in `ai_candidate_main.py` preserves existing candidate routes and Telegram behavior.

Collector settings:
- existing DATABASE_URL, existing OPENAI_API_KEY/model configuration, existing supported source access, installed Chromium;
- new `COLLECTION_BRIDGE_ENABLED=true` after deployment review;
- new `COINGLASS_COLLECTOR_TOKEN`: randomly generated dedicated bridge credential, >=32 bytes, securely provisioned to both backends;
- optional `COLLECTION_BRIDGE_HOURLY_LIMIT=4` (default, maximum20).

App server settings:
- `COINGLASS_COLLECTOR_URL`: the verified HTTPS origin of this deployed candidate server;
- the SAME dedicated `COINGLASS_COLLECTOR_TOKEN`. No OpenAI key, browser session or source login is transferred to the app.

Do not paste keys into a chat, commit them, or print them in logs. No secrets have been created or changed by this code-only integration. A new OpenAI subscription/key in the app is not required for this architecture.

When enabled, startup initializes ONLY `ai_collection_bridge_jobs` and `ai_collection_bridge_requests` on the configured PostgreSQL database. Existing trading/research tables are not changed. A source scan occurs only after an authenticated POST; worker polling an empty job queue does not contact sources. Ready results persist before app writes. Images <=4MB are retained six hours; result JSON and request IDs seven days. Cleanup is performed on new POSTs. Interrupted/failed paid attempts are not replayed automatically. Child processing has a 300-second deadline; this is not an exact dollar/token spending cap. Existing scanner output limits remain unchanged.

## Before user publication

1. Run the offline suite, including isolated PostgreSQL tests.
2. Deploy disabled to the confirmed candidate service; verify normal candidate health and unchanged bot behavior.
3. Securely configure the bridge, then perform ONE authorized 12H capture/analysis. Read the persisted job and evidence; verify actual source visibility and chart time.
4. From an existing authorized app editor, verify save in the correct field/session, reload, and compare the untouched 24H/48H/manual fields. Verify failed-save retry uses GET only and duplicate polls do not duplicate the commit.
5. Only then recommend publishing the app. Scheduling remains a separate step.

The code checks do not resolve the previously observed app workspace membership issue. Do not grant a test account permissions or impersonate the workspace owner to force a test to pass.

## Tests

`python -m unittest discover -s tests -p test_collection_bridge.py -v`

Without `TEST_DATABASE_URL`, the seven PostgreSQL tests are explicitly skipped. CI supplies a temporary PostgreSQL service with dummy credentials, never the production DATABASE_URL. Tests use synthetic source/model fixtures and do not call CoinGlass or OpenAI.
