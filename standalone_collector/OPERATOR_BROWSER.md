# CoinGlass operator window

The existing collector web process serves the viewer. A verified source sign-in challenge opens one incident for the collector account. The database links each affected physical job to that incident. A private desktop is allocated only after the authenticated Decision Hub operator clicks the sign-in button.

## Runtime and build

Use repository root as the container build context. The existing service remains on its current native runtime until an authorized rollout. The following commands build and test the optional image on a staging host with Docker. The test starts native Chromium at about:blank and starts the same Playwright verifier used by the handoff. It needs no provider, identity, database, or deployment credential. The expected exit status is zero, isolated_operator_browser must be true and reason must be ready. A failing probe reports a fixed reason code for the image, kernel namespace, display or browser stage.

```sh
docker build -f standalone_collector/Dockerfile.operator -t coinglass-operator-stage .
docker run --rm --network=none coinglass-operator-stage python standalone_collector/operator_desktop.py --check-runtime
```

Run the same check on the intended host with its ordinary container permissions. The kernel must allow a new user, PID, mount and network namespace, a nested Chromium sandbox and private Unix sockets. A failed probe leaves the private viewer unavailable. The scratch environment used for development denies namespace creation and named Unix sockets. A successful service build or support for HTTPS WebSockets alone does not establish these runtime capabilities.

## Configuration on the existing collector

Apply session_state.sql and operator_login.sql to the collector's private database with a migration identity. Verify schema USAGE and minimum table SELECT/INSERT/UPDATE for the actual worker identity and absence of access for public-facing roles.

The service reads COINGLASS_SESSION_STORE_ENABLED, the encrypted store's separate COINGLASS_SESSION_KEY, and the existing server-to-server COINGLASS_COLLECTOR_TOKEN. Set COINGLASS_OPERATOR_ID to the one approved application user UUID, COINGLASS_APP_ORIGIN to the exact HTTPS origin of the authenticated Decision Hub app, and COINGLASS_OPERATOR_ORIGIN to the exact HTTPS origin serving the viewer. RENDER_EXTERNAL_URL is the collector-origin fallback. Setting COINGLASS_OPERATOR_DESKTOP_ENABLED=true runs the readiness probe on startup. The default is disabled. The provided manager supports the existing single collector replica.

## Private protocol

1. Decision Hub's authenticated server checks the operator identity. The browser generates a fresh 32-byte verifier encoded as base64url without padding. Its challenge is the base64url-encoded SHA-256 digest of the ASCII verifier.
2. The application server sends the incident UUID, verified operator UUID and viewer_challenge to POST /api/collection/coinglass/session-window using its private bearer. The collector returns a rendezvous path and deadline. The application extracts the ticket in memory.
3. The authenticated browser submits ticket and verifier in a native, top-level POST form to /coinglass/operator/admit. The exact application Origin is required. The collector consumes the ticket and issues a short-lived Secure HttpOnly SameSite=Strict cookie, with the viewer path as its scope, then redirects to /coinglass/operator. The displayed address is clean.
4. The viewer's binary WebSocket requires that cookie and the exact collector Origin. It accepts one connection. The real browser address bar stays visible. The private VNC server binds only to a Unix socket. Clipboard transfer, background synchronization and file downloads are disabled. Credentials are entered only on the source's or identity provider's own page.
5. The regular browser runs in a dedicated Linux namespace with a tmpfs profile and no application environment. The source and approved Google domains are reachable over end-to-end TLS through a restricted CONNECT relay. After the viewer attaches, the collector watches for the official login window to return to the known CoinGlass home page with the provider popup closed. That transition starts a separate verifier for the selected protected map. The viewer also offers a manual completion control if the source returns to another page. A protected-map rejection or an interrupted login leaves the incident waiting. The process and transient profile are discarded.
6. The verifier saves only validated CoinGlass cookies. The encrypted state and resolution of the exact incident commit together with revision and lease checks. The verifier keeps its browser open until it receives confirmation of the commit over its private socket. Status is available by incident UUID or durable collector job UUID. These endpoints transmit metadata only. Active scans keep their original work lock independently of the human session. Unused tickets expire after 75 seconds and a desktop lease expires after ten minutes.

The regular browser closes and the isolated verifier reopens its temporary profile between sign-in and map validation. A provider session that does not survive this transition fails verification. Staging acceptance must demonstrate an actual authorized sign-in, the protected requested map, a cold worker restart with the saved scoped state and no repeated sign-in. Browser or identity-provider rejection leaves the existing collection setup in place. Backend unit tests cover ownership and persistence separately from this host and provider check.

Disable the collector's operator desktop flag to withdraw new windows. A closing viewer or the service shutdown hook terminates the active desktop. Its database lease also expires after a process crash.

## Bounded Render QA preflight

The separate QA image runs two neutral checks exactly once per container boot. It has no collector worker, scheduler, sign-in page, application bridge or endpoint for starting another check. It requires no database, provider, identity, exchange, trading or model credential. Its root entry starts the two-UID test supervisor with a clean environment, drops to a third UID and re-execs a read-only status process. The full private-desktop check runs as the nonroot collector UID. Results contain only fixed codes and booleans.

For an authorized disposable web service, use the candidate collector repository at the reviewed commit, repository root as build context, and `standalone_collector/Dockerfile.operator-qa`. Use the image entry point with no custom Docker command. The only environment setting read from the service is `PORT`, normally 10000 on Render. Use `/health` as the health path. The status server answers during testing and after completion, so an unsupported capability does not cause a restart loop.

Read `/status` after `phase` changes from `running` to `complete`. The `dual_uid` object reports whether UID 10002 was denied access to a synthetic file, Unix socket, environment and FD owned by UID 10001. It then tests ordinary sandboxed Chromium using the QA-only local rejecting proxy and reserved `.invalid` URLs. The `private_viewer` object independently reports the original kernel namespace, real private RFB handshake, native Chromium and verification browser probe at `about:blank`. The QA browser policy blocks every real vendor and identity origin. No personal sign-in or live map is tested.

Capture the existing service repository, branch or commit, Dockerfile path, context, start command, health path and auto-deploy setting before an authorized trial. Restore those settings and the prior active deployment afterward. Reusing a QA service temporarily replaces its previous QA endpoint. A positive result from one check does not imply a positive result from the other or establish a real provider sign-in.
