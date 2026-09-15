# HYPE Binance Futures MARK egress repair

## Verified incident

The production Watch cycle reported 29 of 32 experimental input references as
READY and `BinanceFuturesMarkPathError`. The three unavailable references were
the HYPE components that require a closed one-minute quote. Earlier production
evidence recorded HTTP 451 from Render for the same Binance USD-M Futures MARK
endpoint.

The approved price contract remains unchanged:

```text
GET https://fapi.binance.com/fapi/v1/markPriceKlines
symbol=HYPEUSDT
interval=1m
```

Hyperliquid Spot, Hyperliquid perpetual trade candles, contract-price klines,
locally aggregated WebSocket samples, and undocumented Binance hosts are not
valid substitutes for this reference.

## Production evidence after PR 48

Render confirmed that the Oregon `standard` service was live on merge commit
`e54828260665a829c09045ff06cc9eb48e66039f`. The Watch scan beginning at
`2026-09-15T17:02:15.072174Z` completed and its outboxes delivered without a
send failure.

The frozen manual-formula outbox preserved the following evidence from that
scan:

- HYPE `CONSENSUS_FULL`, final `LONG`, 2%: delivered at
  `2026-09-15T17:06:02.865204Z`; the message explicitly said that the base
  price was unavailable and did not calculate stop-loss or take-profit.
- HYPE `C1274`, final `SHORT`, 1%: delivered at
  `2026-09-15T17:06:03.122416Z`; it preserved the same explicit missing-price
  result instead of substituting a later quote.
- SOL `C1274`, final `SHORT`, 1%: delivered at
  `2026-09-15T17:06:03.489315Z` with approximate closed-minute base `99.11` at
  `2026-09-15T17:00:00Z`, stop-loss `100.1011`, and take-profit `98.1189`.

The public health record attributed the three missing references to
`BinanceFuturesMarkPathError`. Current logs did not retain an HTTP status for
this exact request, so the current `451` remains a strongly supported diagnosis
from the earlier production probe, not a newly observed status. This change
adds the sanitized status needed to distinguish that condition after deploy.

## Transport configuration

Production may set the secret environment variable
`BINANCE_FUTURES_MARK_HTTPS_PROXY` to a dedicated HTTPS forward proxy in
an egress region where the official endpoint returns HTTP 200. The proxy value
must contain only a scheme and authority, for example an authenticated HTTPS
host and port. Paths, query strings, fragments and SOCKS URLs are rejected.
Plaintext `http://` proxies are rejected, with or without credentials. A
`NO_PROXY` rule that bypasses Binance is also rejected.

Only the HTTPS request to the fixed Binance URL uses this setting. A configured
proxy is passed explicitly to a Requests session with environment discovery
disabled. For an `https://` proxy, urllib3 first establishes TLS to the proxy,
then tunnels the separately validated Binance TLS connection through CONNECT;
proxy credentials therefore do not cross the client-to-proxy hop in plaintext.
The direct route instead uses an explicit empty standard-library proxy handler,
so ambient proxy settings cannot silently change it. Both routes prohibit
redirects, enforce a 15-second connect/read-inactivity timeout and two-megabyte
response limit, and validate the HYPEUSDT one-minute MARK payload. The enclosing
reference-price operation retains its existing 20-second deadline. Proxy
location and credentials are never returned by health diagnostics.

## Rollout gate

1. Probe the exact endpoint through the configured egress from the Render
   service and require HTTP 200 with valid HYPEUSDT MARK rows.
2. Deploy the application commit containing the transport support.
3. Require two consecutive Watch cycles with 32 READY references, zero missing
   references and no HYPE transport error.
4. Inspect one real HYPE experimental payload when a selected formula matches;
   confirm its frozen reference time, MARK close, final direction, threshold,
   stop-loss, take-profit and Telegram delivery status.

If no approved egress exists, keep HYPE references unavailable. Do not replace
them with a later price or a different market series.
