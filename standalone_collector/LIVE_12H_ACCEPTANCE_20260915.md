# Live Model 1 acceptance check — 2026-09-15

## Executable version

Collector code: 861d1886a623dd03e305744c9db11d9d4ba96b5f.
The optional Legend help-card close is now preparation, not a readability verdict. A failed normal close no longer prevents preserving the actual screenshot. Image-observed identity/readability and numerical interval/zone validators are unchanged. No account controls are bypassed or hidden.
Build checks: 93 collector/browser/range tests + 28 source/privacy tests + 19 configuration tests passed (140 distinct tests).

## Earlier attempt in this turn

48H request f14a82e7-134c-40af-9e7b-ef580bc297e2, job74506840-fc49-43f9-90de-8bc6dcf0f631 failed before an image was saved, with source_not_readable in capture. No OpenAI inference occurred. This was not a successful 48H acceptance test.

## Successful primary 12H test

Request: a56b1c4f-88ac-4d5b-91aa-944ed245e94a.
Job: bf1c5641-10a0-44a9-9b21-291b6054e5d9.
Started: 2026-09-15T20:24:26.981131Z.
Image captured: 2026-09-15T20:26:15.644269Z.
Finished: 2026-09-15T20:26:25.380802Z.
PostgreSQL status: ready; error_code NULL; validated result and private PNG retained.
App validateCollectorEnvelope accepted the actual result for 12H and this job ID.

Reference interval: USD75800–76000; interval confidence medium; visible-axis anchors74000 and76000. observed_price is NULL, price_reference_type is visual_range. No midpoint is substituted for an observed quote.
Accepted zones:
- Above77000–77250: normal.
- Above77700–78050: many.
- Below74850–75050: many.
- Below74050–74250: many.
Omitted ambiguous zones:0. These are visual estimates, not verified monetary amounts or a prediction.

Image SHA256:e014dc63d2a18cfd613fd04b885834f2849a7b3622c9fdb6d30fe077ed2eadf9.
Model:gpt-5.4-mini. One inference:8756 input tokens,819 output tokens,9575 total.
Lovable fetched and viewed the same retained PNG. It reported visible BTC/Symbol/Model1/12hour, an unobscured price axis, and no Legend help card. This is a visual sanity check, not independent measurement accuracy.

## Worksheet gate remains unverified

Target session:ec839591-4be6-48a8-b63d-6c5c24d0a02f.
Target field:3c827a27-4968-4b20-9a02-00f4d33c8cab, Coinglass - Model 1.
The supported preview browser was actually opened; it redirected to /auth. No editor session was available to the test agent. No identity or JWT was manufactured, no permissions were widened, and no privileged worksheet write was used.
No field value or audit entry was written in this turn. The real editor must complete the normal authenticated save/reload check. The result can be reused through the existing collector cache while fresh; a successful backend job alone is not successful worksheet filling.
12H,24H and48H remain implemented in the app contract. Only12H has a successful live numeric result on this corrected version. No hourly schedule or frontend publication has been enabled.

## Cost and safeguards

Internal capture cap was temporarily3/hour for the one additional QA attempt and was restored to2/hour after its terminal result. No provider quota or access restrictions were bypassed; no paid hosting plan was changed. Restoring this configuration triggered a no-scan deploy.
Lovable credits:2.6 initial48H test +1.3 real preview browser check +1.7 primary12H test +1.3 retained-image review =6.9.
