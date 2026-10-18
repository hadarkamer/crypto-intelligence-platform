# Model 3 isolated test with refreshed cookie header — 19 October 2026 (Israel)

One source attempt only, Model 3 / 48H, on the existing paid Render service
`decision-hub-model1-collector` (`srv-dakhacou01pc73f58aug`), actual 1 CPU / 2 GiB.
Production still uses `cf2413f6a34ff9cac33e27e957990eeab2c3c0f7`.
Tested source reader: `b62bce2ed6aa32282efcd9084cee1c7685897ab1`.
No Model 1/2 attempts, AI calls, application writes, deployment, or permanent environment change.

## Result

**Failed before screenshot, cookie dismissal, or hover collection.**
The source returned the expected Model 3 / BTC / 48 hour map. The failure observer
found no login gate, loading indicator, or challenge. The settings dialog remained visible.

- Total: **52.365 seconds**.
- Failure: `source_timeout`, phase `overlay_dismissal`, action timeout.
- The close-button click reached its 3-second deadline after **3.0042 seconds**.
- The call log showed waiting for visibility, with **no click action started**.
- The existing post-timeout check confirmed the settings dialog was **still visible**.
- Merge Y ticks was verified as `1`.
- Peak cgroup memory (including active server and cache): **1013.98 MiB**.
- Anonymous memory peak: 479.13 MiB; file cache peak: 524 MiB.
- No memory-limit, OOM, or OOM-kill events.
- CPU used 51.728793 seconds; 516 of 524 observed periods were throttled.
  This is evidence of CPU saturation, not proof of the click failure's cause.
- Two HTTP 429 responses were grouped as other hosts. No causal link to this timeout was established.

## What the cookie input proves

The full Cookie header was supplied manually through hidden terminal input and
validated structurally, including presence of the known session cookie. Only the
isolated child process received the replacement header; the production environment
was preserved. No cookie values are in this report or its JSON companion.

This attempt did **not** reach the cookie-banner check, so it cannot establish
whether the new cookies resolve that earlier blocker. It also does not establish
hover collection, numerical validation, GPT interpretation, saving, or display readiness.

## Diagnostic-only changes

`model3_cookie_diagnostic.py` observes calls already made by the existing cookie
handler. It preserves selectors, arguments, timeouts, return values, and exceptions;
it records only fixed facts/timings. Local checks verified forwarding and suppression
of synthetic secret text. `model3_cookie_runtime_check.py` retains the existing
single-case guard and merely installs this observer. Business reader code is unchanged.
The staged bundle SHA-256 is
`0ededf5e60782612a550b7963cccd1f8816cc65250bec05c82389b25a2383c15`.

## Next focused correction

Inspect the actual close button's visible/actionable state within the settings dialog,
then correct that close operation and verify the dialog disappears. Do not infer a
cookie or memory failure from this timeout, and do not blindly increase timeouts.
Only after that should one controlled Model 3 test validate cookie dismissal and hover.

## Runtime cleanup check

No test/browser processes remained. The shared worker lock was available; queued/running
bridge jobs: 0. Health and data-store connectivity were ready. Memory returned to
367.48 MiB. The temporary directory was subsequently removed and the temporary Cookie variable was unset.
