# Range-based extraction and retained-image diagnosis — 15 September 2026

## User requirement

Accept a narrow visual range instead of demanding a falsely precise exact current quote. Preserve source/timeframe identity, reject unreadable charts and invalid zones, and fill only the selected existing 12H/24H/48H table row. No trading recommendations or fabricated precision.

## Implemented

Backend range deployment b7b438ff3bb94b2d8d8fdca34edba2cc7ff886e3 went live at 19:30:41Z. App implementation commit 9dcede1ab084df2504e8cab2f5dd898869509705 adds range validation, null observed_price, audit storage and Hebrew range display, including after reload. Its narrow migration only permits NULL observed_price in v2_liquidity_ai_runs. No user-entered field values or permissions were changed.

The explicit interval uses low/high, interval confidence high or medium, basis last_candle_axis_bracket and two visible numeric axis ticks. The initial 1% width cap is an engineering policy, not calibrated measurement accuracy or a trading threshold. Exact legacy results still use the unchanged original point validator. No midpoint is stored as an observed price.

An above zone must be wholly above interval.high, and a below zone wholly below interval.low. Touching/overlapping intervals are excluded and counted. A response with no unambiguous zones is not accepted as success.

Private evidence now persists BEFORE normal failed-job cleanup: original PNG and allowlisted numeric/enum analysis are stored in ai_collection_bridge_evidence. The existing authenticated evidence route serves ready/failed images for six hours; no public screenshot endpoint or credential publication was added. Evidence persistence is confirmed for the test below. Hard process timeouts are not claimed to be covered by this normal-exit path.

## One real range test — rejected, not worksheet data

Request d457c4f5-4ef6-49f7-b722-c337e8d53dfb; job 4db4753f-ef2f-4802-b3dc-8a6965fce533.
Created 19:39:37.934151Z; started 19:39:38.992772Z; finished 19:42:53.282729Z.
48H result failed with price_range_uncertain. PostgreSQL confirms result=NULL and private image retained.
Model reported readable=true, blocking_condition=none and actual observed_timeframe=48h. It returned interval 75300–76050 with confidence medium, but axis_low=76000 and axis_high=74000. These anchor values are numerically reversed and do not enclose the interval. The validator correctly rejected this; none of those rejected numbers were written to a worksheet.
One inference used 8635 input and 733 output tokens. Original image SHA256: 45a4382fbbea275230bb0f4295d2ce42d8cb751495bac6ef1e03588f5e27c545.

## Same retained image, no new capture

Lovable downloaded the existing authenticated PNG and verified the hash. Its native image-view review found the Legend NEW help card covering part of the right-hand price axis near the latest candles. The review did not establish a precise price; it must not substitute for a validated automated result.
An ordinary public HTML read separately verified the help card's normal X control:
- card anchored to a[href="https://legend.coinglass.com"], Legend and NEW;
- direct div.shou[data-first-child] containing the close SVG path beginning M405 136.798L375.202 107.

model1_legend.py now closes ONLY this verified help card via the normal click before the same screenshot. It does not hide DOM/CSS or bypass login/challenge controls. The prompt also explicitly defines axis_low as the smaller numeric price and axis_high as the larger, regardless of screen position. It retains the original invalid-bracket rejection, not automatic sorting/clamping.

## Latest code verification

Latest executable code commit 490c9f15c05ea8ce2167f15364dfedfa399791af. A local test fixture initially invoked its assigned click handler during page.evaluate; fixed the fixture to install the handler without invoking it, added a visible-card precondition and retained the close/account-preservation assertions.
Final Render build logs at 19:59:20Z show 89 main/range/local-browser tests + 28 source/privacy tests + 19 configuration tests passed: 136 total. App code tests reported 41 core/HTTP and 131 Node tests passed, typecheck/build passed, edited files lint-clean. Repository-wide lint is not clean and is not claimed to be.

No new live source run has yet verified the Legend-close and axis-clarification patch. A passing offline build is not proof of successful live collection or worksheet save. No hourly schedule or frontend publishing was performed. Latest deploy dep-dakq6s1594qs739667sg was still updating when this note was written; verify its final status.

Lovable credits for this turn: 9.7 implementation + 1.9 live test + 1.7 retained image review + 1.2 missing-cache check + 1.6 public-markup inspection = 16.1. No additional OpenAI image inference occurred during the stored-image review or code deployment.

Next gate: one authorized live selected-horizon test after the existing two-jobs/hour window permits it, then normal authenticated app save/reload. Do not impersonate an editor or use a privileged worksheet write just to make a test pass. Do not repeat earlier source scans without reading their retained evidence.
