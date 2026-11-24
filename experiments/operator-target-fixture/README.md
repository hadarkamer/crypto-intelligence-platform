# Private operator window target rehearsal

Use this only on an approved fresh Debian 13 amd64 VM dedicated to synthetic Decision Hub data. The single waiting incident and its lease are in-memory fixtures. Its acceptance path connects the application button to actual Chromium, the production isolation checks, admission, noVNC framebuffer, pointer input and cleanup on the intended host. Target execution is still required. The service has no provider credentials, real account, DB/key, jobs or publication routes.

## Target preparation after provisioning approval

Start from an exact reviewed Git commit containing the fixture files and the lifecycle fix. Record the full collector and app SHAs in the receipt. Normal Debian packages, Python venv and systemd are required. Use the application's normal local staging DB/auth setup (below). Before the run, install these packages and prepare the target. Installation may contact repositories for dependencies. Deny access to external destinations for the interactive test itself. Never relax the VM's normal Chromium, kernel or namespace controls to make the check pass.

From the collector checkout, verify its revision against the approved full commit and stage the isolated service once:

```sh
test "$(git rev-parse HEAD)" = "$REVIEWED_COMMIT"
sudo sh experiments/operator-target-fixture/install-native.sh "$REVIEWED_COMMIT"
```

The script installs distro-signed browser/desktop packages, the QA-only Python libraries, a root-owned fixed launcher under the usual read-only /usr mount and a dedicated unprivileged service. It aborts on existing fixture paths or account. The runtime directory and temporary profile are private. There is no host sysctl, sandbox exception, privileged container or VNC TCP listener. The original Python path from the container packaging is replaced only in the generated fixed native QA overlay. This preserves the ordinary Chromium sandbox and all isolation checks. The test rejects an unexpected product source hash, default-denies every outbound CONNECT before DNS, and allows only the fixed local neutral page or a blank page. The verify button cannot save any session. A host which cannot open the full protected window fails this test.

## Connect the two reviewed checkouts

The app adapter pins the exact upstream authenticated worksheet test in app commit `aa96f7c4`. It generates a dedicated target runner beside that test. The real app, GoTrue sign-in and worksheet permission checks are retained. The private window uses an in-memory waiting-incident fixture. Run only with a disposable local Supabase stack and an empty test schema initialized by the app's existing CI setup.

Stage the app adapter in each of two separate disposable app checkouts at the exact reviewed app SHA. Use an admin-owned copy for the existing local database preparation and a copy owned by the ordinary QA app user for the browser run. Never run a root script from a checkout writable by the app user. Follow the native setup above first.

```sh
python3 experiments/operator-target-fixture/stage-app.py stage "$APP_QA_CHECKOUT"
```

Connect trusted TLS reverse proxies on the QA host. Resolve both reserved example.org names exclusively inside its isolated test network and trust the fixture CA in both the test browser and Bun server. The fixture never needs a public VNC or debugging port. Use the separate normal client browser `/usr/bin/google-chrome-stable`, installed from the official signed distribution before isolating the test network. Its ordinary local test trust must accept the fixture certificate. The server browser remains `/usr/bin/chromium` with the fixture-only managed policy. Keep stock browser/kernel restrictions in force.

| QA origin or path | Local upstream |
| --- | --- |
| https://app.example.org/qa-supabase/ | http://127.0.0.1:54321/ with the /qa-supabase prefix stripped |
| Remaining paths at https://app.example.org | http://127.0.0.1:4317/ including the application server routes |
| https://collector.example.org | http://127.0.0.1:8765/ including WebSocket upgrades for /coinglass/operator/display |

Keep these loopback listeners inaccessible outside the fixture deployment. Use correct trusted TLS; no verification exceptions. Restrict the browser test's outbound network to the above local endpoints and independently deny/measure external traffic during the trial. Track HTTP status and byte counts only, never auth bodies, WebSocket payloads or query strings.

In one terminal on the approved VM, the trusted setup operator arms the dedicated unit for a single fixture run. Its argument is the OS account that will run the disposable app fixture, not the service account.

```sh
sudo python3 experiments/operator-target-fixture/arm-native.py "$QA_TEST_OS_USER"
```

The helper generates a single synthetic bridge token under a tester-only 0700 directory in /run. The generated test creates its own GoTrue user and workspace, returns the generated UUID through the local RAM handoff, and configures its backend with that UUID, workspace, fixed collector origin and test token. The helper starts the dedicated nonroot unit, waits for end of test, then stops it and removes transient configuration. The interactive browser has no service keys. Tokens never enter command lines or receipts, and the fixture has no real credentials.

The generated files are `.github/coinglass-auth-browser/prepare-target-db.sh`, `run-target-window.sh`, `target-window.ts` and `target-visual-receipt.mjs`. The adapter checks the reviewed app commit and blob IDs, retains the localhost-only Supabase assertion and binds the app process to the fresh actor/workspace before serving. The test uses the synthetic origins and normal browser sandbox, authenticates through the app and runs the visual assertions on the real mapped worksheet. It never records the UI.

From the admin-owned app checkout, start the disposable local stack in its own terminal. The existing test PostgreSQL schema database `legacy_transaction_test`, local PostgreSQL authentication, Docker/Supabase CLI and cached images must already be available. Plan for its 15-minute fixture timeout.

```sh
sudo env PGHOST=localhost PGPORT=5432 QA_APP_UID="$QA_APP_UID" \
  bash .github/coinglass-auth-browser/prepare-target-db.sh
```

From the test-user-owned app checkout, as that unprivileged user with no Docker access, run:

```sh
CG_QA_PLAYWRIGHT="$PLAYWRIGHT_CORE_1_56_1_INDEX_MJS" \
  NODE_EXTRA_CA_CERTS="$LOCAL_FIXTURE_CA_PEM" \
  bash .github/coinglass-auth-browser/run-target-window.sh
```

Both variables point to dependencies prepared before the run. Keep the TLS intermediates and CA files in this disposable fixture only. The frontend must preserve the external fixture Host/Origin. The generated Vite server explicitly accepts only the additional hostname app.example.org.

The helper accepts only the two fixed fixture origins. It opens the actual Hebrew button, samples three fixed pixels to recognize the blue QA canvas, clicks through the actual noVNC viewer and verifies the green response. A second ordinary viewer in the same authorized fixture browser must be rejected while the first can still change the neutral page. A second worksheet tab must also show the in-use condition. The helper closes the window, abandons an already-issued ticket before admission, waits for the actual timeout and reopens a new window to repeat the input assertion. It never reads the test ticket, types a password, or captures a screenshot.

Require the four positive driver flags, one authenticated second-viewer WebSocket denial logged by the QA-only service, three separate release lines in the unit's journal only after process exit/removal of the temporary directory, and zero provider connections independently verified at the fixture network boundary. Stop the unit after the run and destroy the disposable VM to end billing. The result covers this target and dummy control flow only. If the capability check or any acceptance criterion fails, stop before personal login.
