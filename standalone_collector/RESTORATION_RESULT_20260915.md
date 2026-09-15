# Model 1 original-flow restoration — 2026-09-15

## Evidence actually obtained

The shared conversation https://chatgpt.com/share/6aa96ea1-14d8-83ed-9333-248bb110b2e2 was retrieved over normal HTTP and decoded to its ordered visible message graph. It describes the successful authenticated heatmap POC. Many underlying tool outputs are redacted in the shared page; source code was separately read via the authorized GitHub connector. The later conversation discusses research-bot infrastructure, not a second undiscovered heatmap implementation.

Historical baseline: crypto-ai-lab commit0f7e1bf7ad07623a198a1b104aafbe41be890bcd. Heatmap run32278823620 is distinct from Liquidation Map run32288603404. Original root capture copy is pinned at blob d1d75f2c99ea3c1fd72c7e1f9cfb1ec29a0890ab. Same Chromium context handles existing session setup, original visible controls,12h screenshot then24h screenshot.

A controlled replay on the existing app-owned Render service completed2026-09-15T16:44:24Z:
- one original capture call, two screenshots, one OpenAI visual-readability request, zero worksheet writes;
- OpenAI reported BOTH screenshots as BTC/Symbol/Model1 with the actual12h and24h labels, readable=true, obstruction=none and visible account UI;
-12h transcribed price-axis samples80731,80000,78000;24h samples82593,82000,80000. These are diagnostic tick samples, NOT trade entry prices or liquidity zones;
- image hashes27f1f27d9a46933137b39de8b1b7b15413d74cffbc96fa3b2f014f52f1ffcc0c and e7dc6ca0ba6ed0ab9b4166ae4ae9f262dc4207ba9b6a2bfc2695603361566c6a;
- review usage10633 input tokens,284 output tokens.

Readability is an actual model assessment of images, not independent human confirmation or proof of financial precision. This replay is not a controlled causal test isolating every prior failure. Do not assert that capturing24h causes12h to load, or that a particular CSS selector was conclusively the sole cause.

## Installed app adaptation

Commit78803b9eb1d6d9182fc95f2839fa89724a5561dc:
- prepare.py copies the original capture module UNMODIFIED; no custom readiness/timing injection changes that sequence;
- install_original_flow.py makes app jobs capture the original12h/24h pair but analyze only the requested image once;
- the vision output must report actual visible symbol/mode/model/timeframe, no blocker and readable=true;
- existing high current-price confidence and strict numeric/side checks remain mandatory;
- current right-edge bands only, source timestamp remains original, usage retained, max_output_tokens3500;
- source-only diagnostics report capture_completed, not unverified readability;
- deployment ignores obsolete experiment flags and never opens CoinGlass or invokes vision automatically;
- original bot files, trading runtime, worksheet data and publication remain unchanged.

Offline build checks:30 original-flow/replay/page tests +28 source/privacy tests +19 configuration tests passed. These do not substitute for a real numeric job, database integration test or app save/reload.

## Connection status

Standalone Render service: decision-hub-model1-collector, srv-dakhacou01pc73f58aug. Dedicated app-to-service credential has been configured on Render and in the app secret store; its value is intentionally absent from this document. The app already has the collector origin. Existing OpenAI/model and source-session configuration are present.

DATABASE_URL is still missing, and collection remains gated. A database connection must be supplied securely before durable jobs can start. No database password is in Git, chat output or this report. No hourly scheduling or frontend publication has been performed. Verify an authenticated12H job, correct app field save, and reload before recommending publication.
