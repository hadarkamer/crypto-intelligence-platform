# CoinGlass operator window

The existing collector web process serves the viewer. A verified source sign-in challenge opens one incident for the collector account. The database links each affected physical job to that incident. A private desktop is allocated only after the authenticated Decision Hub operator clicks the sign-in button.

## Runtime and build

Use repository root as the container build context. The existing service remains on its current native runtime until an authorized rollout. The following commands build and test the optional image on a staging host with Docker. The test starts native Chromium at about:blank and starts the same Playwright verifier used by the handoff. It needs no provider, identity, database, or deployment credential. The expected exit status is zero and isolated_operator_browser must be true.

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
5. The regular browser runs in a dedicated Linux namespace with a tmpfs profile and no application environment. The source and approved Google domains are reachable over end-to-end TLS through a restricted CONNECT relay. On returning to the known CoinGlass home page, a separate verifier checks the selected protected map. The viewer also offers a manual completion control if the source returns to another page. A protected-map rejection or an interrupted login leaves the incident waiting. The process and transient profile are discarded.
6. The verifier saves only validated CoinGlass cookies. The encrypted state and resolution of the exact incident commit together with revision and lease checks. Status is available by incident UUID or durable collector job UUID. These endpoints transmit metadata only. Active scans keep their original work lock independently of the human session. Unused tickets expire after 75 seconds and a desktop lease expires after ten minutes.

The regular browser closes and the isolated verifier reopens its temporary profile between sign-in and map validation. A provider session that does not survive this transition fails verification. Staging acceptance must demonstrate an actual authorized sign-in, the protected requested map, a cold worker restart with the saved scoped state and no repeated sign-in. Browser or identity-provider rejection leaves the existing collection setup in place. Backend unit tests cover ownership and persistence separately from this host and provider check.

Disable the collector's operator desktop flag to withdraw new windows. A closing viewer or the service shutdown hook terminates the active desktop. Its database lease also expires after a process crash.
